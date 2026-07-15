use crate::{NamespaceResources, TenantResources, TopicResources};
use anyhow::Result;
use pulsar_lite_storage_metadata::{
    FileMetadataStore, MetadataDocument, MetadataStore, TopicMetadata,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Debug)]
pub struct PulsarResources<S: MetadataStore = FileMetadataStore> {
    metadata: RwLock<S>,
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
            metadata: RwLock::new(metadata),
            tenant_resources: TenantResources::new(),
            namespace_resources: NamespaceResources::new(),
            topic_resources: TopicResources::new(),
        }
    }

    fn read_metadata(&self) -> RwLockReadGuard<'_, S> {
        self.metadata
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write_metadata(&self) -> RwLockWriteGuard<'_, S> {
        self.metadata
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
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

    pub fn ensure_tenant(&self, tenant: &str, version: u32) -> Result<()> {
        let mut metadata = self.write_metadata();
        self.tenant_resources
            .ensure_tenant(&mut *metadata, tenant, version)
    }

    pub fn has_tenant(&self, tenant: &str) -> bool {
        let metadata = self.read_metadata();
        self.tenant_resources.has_tenant(&*metadata, tenant)
    }

    pub fn ensure_namespace(&self, tenant: &str, namespace: &str, version: u32) -> Result<()> {
        let mut metadata = self.write_metadata();
        self.namespace_resources
            .ensure_namespace(&mut *metadata, tenant, namespace, version)
    }

    pub fn has_namespace(&self, tenant: &str, namespace: &str) -> bool {
        let metadata = self.read_metadata();
        self.namespace_resources
            .has_namespace(&*metadata, tenant, namespace)
    }

    pub fn ensure_topic(
        &self,
        topic: &str,
        partitioned: bool,
        partition_count: usize,
        version: u32,
    ) -> Result<()> {
        let mut metadata = self.write_metadata();
        let mut topic_resources = self.topic_resources.clone();
        topic_resources.ensure_topic(&mut *metadata, topic, partitioned, partition_count, version)
    }

    pub fn ensure_subscription(&self, topic: &str, subscription: &str, version: u32) -> Result<()> {
        let mut metadata = self.write_metadata();
        let mut topic_resources = self.topic_resources.clone();
        topic_resources.ensure_subscription(&mut *metadata, topic, subscription, version)
    }

    pub fn get_partitioned_topic_metadata(&self) -> HashMap<String, usize> {
        let metadata = self.read_metadata();
        self.topic_resources
            .get_partitioned_topic_metadata(&*metadata)
    }

    pub fn get_topic_metadata(&self, topic: &str) -> Option<TopicMetadata> {
        let metadata = self.read_metadata();
        self.topic_resources
            .get_topic_metadata(&*metadata, topic)
            .cloned()
    }

    pub fn has_subscription(&self, topic: &str, subscription: &str) -> bool {
        let metadata = self.read_metadata();
        self.topic_resources
            .has_subscription(&*metadata, topic, subscription)
    }

    pub fn metadata(&self) -> RwLockReadGuard<'_, S> {
        self.read_metadata()
    }

    pub fn metadata_mut(&self) -> RwLockWriteGuard<'_, S> {
        self.write_metadata()
    }

    pub fn metadata_path(&self) -> PathBuf {
        self.read_metadata().metadata_path().to_path_buf()
    }

    pub fn build_metadata_document(&self, version: u32) -> MetadataDocument {
        self.read_metadata()
            .state()
            .build_metadata_document(version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulsar_lite_storage_metadata::InMemoryMetadataStore;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn resources_can_be_shared_across_concurrent_readers_and_writers() {
        let resources = Arc::new(PulsarResources::from_metadata_store(
            InMemoryMetadataStore::new(),
        ));
        let topics: Vec<_> = (0..8)
            .map(|index| format!("persistent://public/default/topic-{index}"))
            .collect();

        let handles: Vec<_> = topics
            .iter()
            .cloned()
            .map(|topic| {
                let resources = Arc::clone(&resources);
                thread::spawn(move || {
                    resources.ensure_topic(&topic, false, 0, 2).unwrap();
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        for topic in topics {
            assert!(resources.get_topic_metadata(&topic).is_some());
        }
    }
}
