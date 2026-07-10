use crate::{NamespaceResources, TenantResources, TopicResources};
use anyhow::Result;
use pulsar_lite_storage_metadata::{
    FileMetadataStore, MetadataDocument, MetadataStore, TopicMetadata,
};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug)]
pub struct PulsarResources<S: MetadataStore = FileMetadataStore> {
    metadata: S,
    tenant_resources: TenantResources,
    namespace_resources: NamespaceResources,
    topic_resources: TopicResources,
}

impl PulsarResources<FileMetadataStore> {
    pub fn new(path: &Path) -> Result<Self> {
        Ok(Self::from_metadata_store(FileMetadataStore::new(path)?))
    }
}

impl<S: MetadataStore> PulsarResources<S> {
    pub fn from_metadata_store(metadata: S) -> Self {
        Self {
            metadata,
            tenant_resources: TenantResources::new(),
            namespace_resources: NamespaceResources::new(),
            topic_resources: TopicResources::new(),
        }
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

    pub fn ensure_namespace(&mut self, tenant: &str, namespace: &str, version: u32) -> Result<()> {
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
        self.topic_resources.ensure_topic(
            &mut self.metadata,
            topic,
            partitioned,
            partition_count,
            version,
        )
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
        self.topic_resources
            .get_topic_metadata(&self.metadata, topic)
    }

    pub fn has_subscription(&self, topic: &str, subscription: &str) -> bool {
        self.topic_resources
            .has_subscription(&self.metadata, topic, subscription)
    }

    pub fn metadata(&self) -> &S {
        &self.metadata
    }

    pub fn metadata_mut(&mut self) -> &mut S {
        &mut self.metadata
    }

    pub fn metadata_path(&self) -> &Path {
        self.metadata.metadata_path()
    }

    pub fn build_metadata_document(&self, version: u32) -> MetadataDocument {
        self.metadata.state().build_metadata_document(version)
    }
}
