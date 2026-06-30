use crate::key::{namespace_key, subscription_key};
use crate::model::{
    parse_topic_name, MetadataDocument, NamespaceMetadata, SubscriptionMetadata, TenantMetadata,
    TopicMetadata,
};
use crate::store::build_document_from_state;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// File-backed metadata store: persists tenant/namespace/topic/subscription
/// state to `<db_path>.metadata.json` using a `MetadataDocument` snapshot.
#[derive(Debug, Default)]
pub struct FileMetadataStore {
    metadata_path: PathBuf,
    tenants: HashMap<String, TenantMetadata>,
    namespaces: HashMap<String, NamespaceMetadata>,
    topics: HashMap<String, TopicMetadata>,
    subscriptions: HashMap<String, SubscriptionMetadata>,
}

impl FileMetadataStore {
    pub fn new(db_path: &Path) -> Result<Self> {
        let mut store = Self {
            metadata_path: db_path.with_extension("metadata.json"),
            ..Self::default()
        };
        store.load()?;
        Ok(store)
    }

    pub fn load(&mut self) -> Result<()> {
        if let Some(document) = self.load_document()? {
            self.apply_metadata_document(document)?;
        }
        Ok(())
    }

    pub fn persist_document(&self, version: u32) -> Result<()> {
        let document = self.build_metadata_document(version);
        self.save_document(&document)
    }

    pub fn metadata_path(&self) -> &Path {
        &self.metadata_path
    }

    pub fn load_document(&self) -> Result<Option<MetadataDocument>> {
        if !self.metadata_path.exists() {
            return Ok(None);
        }
        let content = fs::read_to_string(&self.metadata_path).map_err(|error| {
            anyhow!(
                "Failed to read metadata file '{}':{error}",
                self.metadata_path.display()
            )
        })?;
        let raw: serde_json::Value = serde_json::from_str(&content).map_err(|error| {
            anyhow!(
                "Failed to parse metadata file '{}':{error}",
                self.metadata_path.display()
            )
        })?;
        if raw.get("tenants").is_some()
            || raw.get("namespaces").is_some()
            || raw.get("topics").is_some()
            || raw.get("subscriptions").is_some()
        {
            return Err(anyhow!(
                "Failed to parse metadata file '{}': old flat MetadataSnapshot format is no longer supported; delete the metadata file and recreate resources",
                self.metadata_path.display()
            ));
        }
        let document: MetadataDocument = serde_json::from_value(raw).map_err(|error| {
            anyhow!(
                "Failed to parse metadata file '{}': {error}",
                self.metadata_path.display()
            )
        })?;
        Ok(Some(document))
    }

    fn apply_metadata_document(&mut self, document: MetadataDocument) -> Result<()> {
        let mut tenants = HashMap::new();
        let mut namespaces = HashMap::new();
        let mut topics = HashMap::new();
        let mut subscriptions = HashMap::new();

        for file_node in document.resource_files.into_values() {
            for (tenant_name, tenant_node) in file_node.tenants {
                tenants.insert(
                    tenant_name.clone(),
                    TenantMetadata {
                        name: tenant_name.clone(),
                    },
                );

                for (namespace_name, namespace_node) in tenant_node.namespaces {
                    namespaces.insert(
                        namespace_key(&tenant_name, &namespace_name),
                        NamespaceMetadata {
                            tenant: tenant_name.clone(),
                            name: namespace_name.clone(),
                        },
                    );

                    for (domain_name, domain_node) in namespace_node.domains {
                        for (topic_name, topic_node) in domain_node.topics {
                            let full_name = format!(
                                "{}://{}/{}/{}",
                                domain_name, tenant_name, namespace_name, topic_name
                            );

                            let parsed = parse_topic_name(&full_name).map_err(|error| {
                                anyhow!(
                                    "Invalid topic in metadata resources '{}':'{}'",
                                    full_name,
                                    error
                                )
                            })?;

                            topics.insert(
                                full_name.clone(),
                                TopicMetadata {
                                    full_name: full_name.clone(),
                                    domain: parsed.domain,
                                    tenant: parsed.tenant,
                                    namespace: parsed.namespace,
                                    local_name: parsed.local_name,
                                    partitioned: false,
                                    partition_count: 0,
                                },
                            );

                            for subscription_name in topic_node.subscriptions.into_keys() {
                                subscriptions.insert(
                                    subscription_key(&full_name, &subscription_name),
                                    SubscriptionMetadata {
                                        topic: full_name.clone(),
                                        name: subscription_name,
                                    },
                                );
                            }
                        }
                    }
                }
            }
        }

        for (topic_name, node) in document.partitioned_topics {
            let parsed = parse_topic_name(&topic_name).map_err(|error| {
                anyhow!(
                    "Invalid partitioned topic metadata '{}':'{}'",
                    topic_name,
                    error
                )
            })?;

            topics.insert(
                topic_name.clone(),
                TopicMetadata {
                    full_name: topic_name,
                    domain: parsed.domain,
                    tenant: parsed.tenant,
                    namespace: parsed.namespace,
                    local_name: parsed.local_name,
                    partitioned: true,
                    partition_count: node.partitions.max(1),
                },
            );
        }

        self.tenants = tenants;
        self.namespaces = namespaces;
        self.topics = topics;
        self.subscriptions = subscriptions;

        Ok(())
    }

    fn save_document(&self, document: &MetadataDocument) -> Result<()> {
        if let Some(parent) = self.metadata_path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                anyhow!(
                    "Failed to create metadata directory '{}':{error}",
                    parent.display()
                )
            })?;
        }

        let serialized = serde_json::to_string_pretty(document)?;
        let tmp_path = PathBuf::from(format!("{}.tmp", self.metadata_path.display()));
        fs::write(&tmp_path, serialized).map_err(|error| {
            anyhow!(
                "Failed to write temporary metadata file '{}':{error}",
                tmp_path.display()
            )
        })?;
        fs::rename(&tmp_path, &self.metadata_path).map_err(|error| {
            anyhow!(
                "Failed to replace metadata file '{}' with '{}': {error}",
                self.metadata_path.display(),
                tmp_path.display()
            )
        })?;
        Ok(())
    }

    pub fn insert_tenant_metadata(&mut self, tenant: &str) -> bool {
        if self.tenants.contains_key(tenant) {
            return false;
        }
        self.tenants.insert(
            tenant.to_string(),
            TenantMetadata {
                name: tenant.to_string(),
            },
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
        let entry = self.topics.entry(key).or_insert_with(|| {
            changed = true;
            metadata.clone()
        });
        if metadata.partitioned {
            let desired = metadata.partition_count.max(1);
            if !entry.partitioned || entry.partition_count != desired {
                entry.partitioned = true;
                entry.partition_count = desired;
                changed = true;
            }
        } else if !entry.partitioned && entry.partition_count != 0 {
            entry.partition_count = 0;
            changed = true;
        }
        changed
    }

    pub fn insert_subscription_metadata(&mut self, topic: &str, subscription: &str) -> bool {
        let key = subscription_key(topic, subscription);
        if self.subscriptions.contains_key(&key) {
            return false;
        }
        self.subscriptions.insert(
            key,
            SubscriptionMetadata {
                topic: topic.to_string(),
                name: subscription.to_string(),
            },
        );
        true
    }

    pub fn has_tenant_metadata(&self, tenant: &str) -> bool {
        self.tenants.contains_key(tenant)
    }

    pub fn has_namespace_metadata(&self, tenant: &str, namespace: &str) -> bool {
        self.namespaces
            .contains_key(&namespace_key(tenant, namespace))
    }

    pub fn has_subscription_metadata(&self, topic: &str, subscription: &str) -> bool {
        self.subscriptions
            .contains_key(&subscription_key(topic, subscription))
    }

    pub fn get_topic_metadata(&self, topic: &str) -> Option<&TopicMetadata> {
        self.topics.get(topic)
    }

    pub fn get_partitioned_topic_metadata(&self) -> HashMap<String, usize> {
        self.topics
            .iter()
            .filter_map(|(topic, metadata)| {
                metadata
                    .partitioned
                    .then_some((topic.clone(), metadata.partition_count))
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
