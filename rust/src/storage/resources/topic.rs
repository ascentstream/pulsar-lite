use super::BaseResources;
use crate::storage::{MetadataStore, TopicMetadata};
use anyhow::Result;
use std::collections::HashMap;

/// Topic resource accessor skeleton.
#[derive(Debug, Clone, Default)]
pub struct TopicResources {
    _base: BaseResources,
}

impl TopicResources {
    pub fn new() -> Self {
        Self {
            _base: BaseResources::new(),
        }
    }

    pub fn ensure_topic(
        &mut self,
        metadata: &mut MetadataStore,
        topic: &str,
        partitioned: bool,
        partition_count: usize,
        version: u32,
    ) -> Result<()> {
        let (parsed, mut changed) = self._base.ensure_topic_parents(metadata, topic)?;
        changed |= metadata.upsert_topic_metadata(TopicMetadata {
            full_name: topic.to_string(),
            domain: parsed.domain,
            tenant: parsed.tenant,
            namespace: parsed.namespace,
            local_name: parsed.local_name,
            partitioned,
            partition_count: if partitioned { partition_count } else { 0 },
        });
        self._base.persist_if_changed(metadata, version, changed)
    }

    pub fn ensure_subscription(
        &mut self,
        metadata: &mut MetadataStore,
        topic: &str,
        subscription: &str,
        version: u32,
    ) -> Result<()> {
        let (parsed, mut changed) = self._base.ensure_topic_parents(metadata, topic)?;
        changed |= metadata.upsert_topic_metadata(TopicMetadata {
            full_name: topic.to_string(),
            domain: parsed.domain,
            tenant: parsed.tenant,
            namespace: parsed.namespace,
            local_name: parsed.local_name,
            partitioned: false,
            partition_count: 0,
        });
        changed |= metadata.insert_subscription_metadata(topic, subscription);
        self._base.persist_if_changed(metadata, version, changed)
    }

    pub fn get_partitioned_topic_metadata(
        &self,
        metadata: &MetadataStore,
    ) -> HashMap<String, usize> {
        self._base.get_partitioned_topic_metadata(metadata)
    }

    pub fn get_topic_metadata<'a>(
        &self,
        metadata: &'a MetadataStore,
        topic: &str,
    ) -> Option<&'a TopicMetadata> {
        self._base.get_topic_metadata(metadata, topic)
    }

    pub fn has_subscription(
        &self,
        metadata: &MetadataStore,
        topic: &str,
        subscription: &str,
    ) -> bool {
        self._base.has_subscription(metadata, topic, subscription)
    }
}
