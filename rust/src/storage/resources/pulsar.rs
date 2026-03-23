use super::{NamespaceResources, TenantResources, TopicResources};
use crate::storage::{MetadataStore, TopicMetadata};
#[cfg(test)]
use crate::storage::MetadataDocument;
use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;

/// Aggregated broker resource entrypoint, similar in shape to PulsarResources.
#[derive(Debug)]
pub struct PulsarResources {
    metadata: MetadataStore,
    tenant_resources: TenantResources,
    namespace_resources: NamespaceResources,
    topic_resources: TopicResources,
}

impl PulsarResources {
    pub fn new(path: &Path) -> Result<Self> {
        Ok(Self {
            metadata: MetadataStore::new(path)?,
            tenant_resources: TenantResources::new(),
            namespace_resources: NamespaceResources::new(),
            topic_resources: TopicResources::new(),
        })
    }

    pub fn tenant(&self) -> &TenantResources {
        &self.tenant_resources
    }

    pub fn tenant_mut(&mut self) -> &mut TenantResources {
        &mut self.tenant_resources
    }

    pub fn namespace(&self) -> &NamespaceResources {
        &self.namespace_resources
    }

    pub fn namespace_mut(&mut self) -> &mut NamespaceResources {
        &mut self.namespace_resources
    }

    pub fn topic(&self) -> &TopicResources {
        &self.topic_resources
    }

    pub fn topic_mut(&mut self) -> &mut TopicResources {
        &mut self.topic_resources
    }

    pub fn ensure_tenant(&mut self, tenant: &str, version: u32) -> Result<()> {
        self.tenant_resources
            .ensure_tenant(&mut self.metadata, tenant, version)
    }

    pub fn has_tenant(&self, tenant: &str) -> bool {
        self.tenant_resources.has_tenant(&self.metadata, tenant)
    }

    pub fn ensure_namespace(
        &mut self,
        tenant: &str,
        namespace: &str,
        version: u32,
    ) -> Result<()> {
        self.namespace_resources
            .ensure_namespace(&mut self.metadata, tenant, namespace, version)
    }

    pub fn has_namespace(&self, tenant: &str, namespace: &str) -> bool {
        self.namespace_resources
            .has_namespace(&self.metadata, tenant, namespace)
    }

    pub fn ensure_topic(
        &mut self,
        topic: &str,
        partitioned: bool,
        partition_count: usize,
        version: u32,
    ) -> Result<()> {
        self.topic_resources
            .ensure_topic(&mut self.metadata, topic, partitioned, partition_count, version)
    }

    pub fn ensure_subscription(
        &mut self,
        topic: &str,
        subscription: &str,
        version: u32,
    ) -> Result<()> {
        self.topic_resources
            .ensure_subscription(&mut self.metadata, topic, subscription, version)
    }

    pub fn get_partitioned_topic_metadata(&self) -> HashMap<String, usize> {
        self.topic_resources
            .get_partitioned_topic_metadata(&self.metadata)
    }

    pub fn get_topic_metadata(&self, topic: &str) -> Option<&TopicMetadata> {
        self.topic_resources.get_topic_metadata(&self.metadata, topic)
    }

    pub fn has_subscription(&self, topic: &str, subscription: &str) -> bool {
        self.topic_resources
            .has_subscription(&self.metadata, topic, subscription)
    }

    pub fn metadata(&self) -> &MetadataStore {
        &self.metadata
    }

    #[cfg(test)]
    pub(crate) fn metadata_path(&self) -> &Path {
        self.metadata.metadata_path()
    }

    #[cfg(test)]
    pub(crate) fn build_metadata_document(&self, version: u32) -> MetadataDocument {
        self.metadata.build_metadata_document(version)
    }
}
