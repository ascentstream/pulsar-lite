use crate::key::{namespace_key, subscription_key};
use crate::model::{
    MetadataDocument, NamespaceMetadata, SubscriptionMetadata, TenantMetadata, TopicMetadata,
};
use crate::store::build_document_from_state;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// In-memory metadata store: no persistence, used for tests and ephemeral runs.
///
/// HashMap key/value examples:
///
/// - `tenants["public"] = TenantMetadata { name: "public" }`
/// - `namespaces["public/default"] = NamespaceMetadata { tenant: "public", name: "default" }`
/// - `topics["persistent://public/default/my-topic"] = TopicMetadata { full_name: "persistent://public/default/my-topic", ... }`
/// - `subscriptions["persistent://public/default/my-topic:sub"] = SubscriptionMetadata { topic: "persistent://public/default/my-topic", name: "sub" }`
#[derive(Debug, Default)]
pub struct InMemoryMetadataStore {
    metadata_path: PathBuf,
    tenants: HashMap<String,TenantMetadata>,
    namespaces: HashMap<String,NamespaceMetadata>,
    topics: HashMap<String,TopicMetadata>,
    subscriptions: HashMap<String,SubscriptionMetadata>,
}

impl InMemoryMetadataStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    pub fn persist_document(&self, _version:u32) -> anyhow::Result<()> {
        Ok(())
    }

    pub fn metadata_path(&self) -> &Path {
        &self.metadata_path
    }

    pub fn insert_tenant_metadata(&mut self, tenant: &str) -> bool {
        if self.tenants.contains_key(tenant) {
            return false;
        }
        self.tenants.insert(
            tenant.to_string(),
            TenantMetadata { name: tenant.to_string() },
        );
        true
    }

    pub fn insert_namespace_metadata(&mut self, tenant: &str, namespace: &str) -> bool {
        let key = namespace_key(tenant, namespace);
        if self.namespaces.contains_key(&key) {
            return false;
        }
        self.namespaces.insert(
            key, 
            NamespaceMetadata { 
                tenant: tenant.to_string(), 
                name: namespace.to_string(), 
            },
        );
        true
    }

    pub fn upsert_topic_metadata(&mut self, metadata: TopicMetadata) -> bool {
        let key = metadata.full_name.clone();
        let mut changed = false;
        let entry = self
            .topics
            .entry(key)
            .or_insert_with(|| { changed = true;metadata.clone()});
        if metadata.partitioned {
            let desired = metadata.partition_count.max(1);
            if !entry.partitioned || entry.partition_count != desired {
                entry.partitioned = true;
                entry.partition_count = desired;
                changed = true;
            }
        } else if !entry.partitioned && entry.partition_count != 0 {
            entry.partition_count = 0;
            changed = true
        }
        changed
    }

    pub fn insert_subscription_metadata(&mut self, topic: &str, subscription: &str) -> bool {
        let key = subscription_key(topic,subscription);
        if self.subscriptions.contains_key(&key) {
            return false;
        }
        self.subscriptions.insert(
            key,
            SubscriptionMetadata { 
                topic: topic.to_string(), 
                name: subscription.to_string() },
        );
        true
    }

    pub fn has_tenant_metadata(&self, tenant: &str) -> bool {
        self.tenants.contains_key(tenant)
    }

    pub fn has_namespace_metadata(&self, tenant: &str, namespace: &str) -> bool {
        self.namespaces.contains_key(&namespace_key(tenant, namespace))
    }

    pub fn has_subscription_metadata(&self, topic:&str,subscription: &str) -> bool {
        self.subscriptions.contains_key(&subscription_key(topic, subscription))
    }

    pub fn get_topic_metadata(&self, topic: &str) -> Option<&TopicMetadata> {
        self.topics.get(topic)
    }

    pub fn get_partitioned_topic_metadata(&self) -> HashMap<String, usize> {
        self.topics
            .iter()
            .filter_map(|(topic, metadata)| {
                metadata.partitioned.then_some((topic.clone(), metadata.partition_count))
            })
            .collect()
    }

    pub fn build_metadata_document(&self, version: u32) -> MetadataDocument {
        build_document_from_state(
            &self.metadata_path,
            &self.tenants,
            &self.namespaces,
            &self.topics,
            &self.subscriptions,
            version,
        )
    }
}
