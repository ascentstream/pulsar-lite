use crate::storage::metadata::parse_topic_name;
use crate::storage::{MetadataStore, ParsedTopicName, TopicMetadata};
use anyhow::Result;
use std::collections::HashMap;

/// Shared skeleton for future broker-side resource accessors.
#[derive(Debug, Clone, Default)]
pub struct BaseResources;

impl BaseResources {
    pub fn new() -> Self {
        Self
    }

    // 统一的资源变更持久化逻辑
    pub(crate) fn persist_if_changed(
        &self,
        metadata: &MetadataStore,
        version: u32,
        changed: bool,
    ) -> Result<()> {
        if changed {
            metadata.persist_document(version)?;
        }
        Ok(())
    }

    // 统一 tenant 父资源 ensure
    pub(crate) fn ensure_tenant_parent(&self, metadata: &mut MetadataStore, tenant: &str) -> bool {
        metadata.insert_tenant_metadata(tenant)
    }

    // 统一 namespace 父资源 ensure
    pub(crate) fn ensure_namespace_parents(
        &self,
        metadata: &mut MetadataStore,
        tenant: &str,
        namespace: &str,
    ) -> bool {
        let mut changed = self.ensure_tenant_parent(metadata, tenant);
        changed |= metadata.insert_namespace_metadata(tenant, namespace);
        changed
    }

    // 统一 topic 父资源 ensure + topic 解析
    pub(crate) fn ensure_topic_parents(
        &self,
        metadata: &mut MetadataStore,
        topic: &str,
    ) -> Result<(ParsedTopicName, bool)> {
        let parsed = parse_topic_name(topic)?;
        let changed = self.ensure_namespace_parents(metadata, &parsed.tenant, &parsed.namespace);
        Ok((parsed, changed))
    }

    // 统一 query helper
    pub(crate) fn has_tenant(&self, metadata: &MetadataStore, tenant: &str) -> bool {
        metadata.has_tenant_metadata(tenant)
    }

    pub(crate) fn has_namespace(
        &self,
        metadata: &MetadataStore,
        tenant: &str,
        namespace: &str,
    ) -> bool {
        metadata.has_namespace_metadata(tenant, namespace)
    }

    pub(crate) fn get_topic_metadata<'a>(
        &self,
        metadata: &'a MetadataStore,
        topic: &str,
    ) -> Option<&'a TopicMetadata> {
        metadata.get_topic_metadata(topic)
    }

    pub(crate) fn has_subscription(
        &self,
        metadata: &MetadataStore,
        topic: &str,
        subscription: &str,
    ) -> bool {
        metadata.has_subscription_metadata(topic, subscription)
    }

    pub(crate) fn get_partitioned_topic_metadata(
        &self,
        metadata: &MetadataStore,
    ) -> HashMap<String, usize> {
        metadata.get_partitioned_topic_metadata()
    }
}
