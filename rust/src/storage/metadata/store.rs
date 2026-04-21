use super::json::JsonFileMetadataStore;
use super::types::{
    logical_topic_name, namespace_key, parse_topic_name, subscription_key, DomainNode,
    MetadataDocument, MetadataFileNode, NamespaceMetadata, PartitionedTopicNode,
    SubscriptionMetadata, TenantMetadata, TopicMetadata,
};
use anyhow::{anyhow, Result};
use log::warn;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

#[derive(Debug, Default)]
pub struct MetadataStore {
    backend: JsonFileMetadataStore,
    tenants: HashMap<String, TenantMetadata>,
    namespaces: HashMap<String, NamespaceMetadata>,
    topics_meta: HashMap<String, TopicMetadata>,
    subscriptions_meta: HashMap<String, SubscriptionMetadata>,
}

impl MetadataStore {
    pub fn new(db_path: &Path) -> Result<Self> {
        let mut store = Self {
            backend: JsonFileMetadataStore::new(db_path),
            ..Self::default()
        };
        store.load()?;
        Ok(store)
    }

    pub fn load(&mut self) -> Result<()> {
        if let Some(document) = self.backend.load_document()? {
            self.apply_metadata_document(document)?;
        }
        Ok(())
    }

    pub fn metadata_path(&self) -> &Path {
        self.backend.metadata_path()
    }

    pub fn build_metadata_document(&self, version: u32) -> MetadataDocument {
        let path_key = self.backend.metadata_path().display().to_string();
        let mut file_node = MetadataFileNode::default();

        for tenant in self.tenants.values() {
            file_node.tenants.entry(tenant.name.clone()).or_default();
        }

        for namespace in self.namespaces.values() {
            file_node
                .tenants
                .entry(namespace.tenant.clone())
                .or_default()
                .namespaces
                .entry(namespace.name.clone())
                .or_default();
        }

        for topic in self.topics_meta.values() {
            if topic.partitioned {
                continue;
            }

            let parsed = match parse_topic_name(&topic.full_name) {
                Ok(parsed) => parsed,
                Err(error) => {
                    warn!(
                        "Skipping topic metadata '{}' while building document: {}",
                        topic.full_name, error
                    );
                    continue;
                }
            };

            file_node
                .tenants
                .entry(parsed.tenant.clone())
                .or_default()
                .namespaces
                .entry(parsed.namespace.clone())
                .or_default()
                .domains
                .entry(parsed.domain.clone())
                .or_insert_with(DomainNode::default)
                .topics
                .entry(parsed.local_name.clone())
                .or_default();
        }

        for subscription in self.subscriptions_meta.values() {
            let parsed = match parse_topic_name(&subscription.topic) {
                Ok(parsed) => parsed,
                Err(error) => {
                    warn!(
                        "Skipping subscription metadata '{}:{}' while building document: {}",
                        subscription.topic, subscription.name, error
                    );
                    continue;
                }
            };

            file_node
                .tenants
                .entry(parsed.tenant.clone())
                .or_default()
                .namespaces
                .entry(parsed.namespace.clone())
                .or_default()
                .domains
                .entry(parsed.domain.clone())
                .or_insert_with(DomainNode::default)
                .topics
                .entry(parsed.local_name.clone())
                .or_default()
                .subscriptions
                .entry(subscription.name.clone())
                .or_default();
        }

        let mut partitioned_topics = BTreeMap::new();
        for topic in self.topics_meta.values() {
            if !topic.partitioned {
                continue;
            }

            let logical_topic = logical_topic_name(&topic.full_name);
            partitioned_topics
                .entry(logical_topic)
                .or_insert_with(|| PartitionedTopicNode {
                    partitions: topic.partition_count.max(1),
                });
        }

        let mut resource_files = BTreeMap::new();
        resource_files.insert(path_key, file_node);

        MetadataDocument {
            version,
            resource_files,
            partitioned_topics,
        }
    }

    pub fn apply_metadata_document(&mut self, document: MetadataDocument) -> Result<()> {
        self.tenants.clear();
        self.namespaces.clear();
        self.topics_meta.clear();
        self.subscriptions_meta.clear();

        for file_node in document.resource_files.into_values() {
            for (tenant_name, tenant_node) in file_node.tenants {
                self.tenants.insert(
                    tenant_name.clone(),
                    TenantMetadata {
                        name: tenant_name.clone(),
                    },
                );

                for (namespace_name, namespace_node) in tenant_node.namespaces {
                    self.namespaces.insert(
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
                                    "Invalid topic in metadata resources '{}': {}",
                                    full_name,
                                    error
                                )
                            })?;

                            self.topics_meta.insert(
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
                                self.subscriptions_meta.insert(
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

        for (logical_topic, partitioned_topic) in document.partitioned_topics {
            if partitioned_topic.partitions == 0 {
                return Err(anyhow!(
                    "Invalid partitioned topic metadata '{}': partitions must be greater than 0",
                    logical_topic
                ));
            }

            let parsed = parse_topic_name(&logical_topic).map_err(|error| {
                anyhow!(
                    "Invalid partitioned topic metadata '{}': {}",
                    logical_topic,
                    error
                )
            })?;

            self.tenants.insert(
                parsed.tenant.clone(),
                TenantMetadata {
                    name: parsed.tenant.clone(),
                },
            );
            self.namespaces.insert(
                namespace_key(&parsed.tenant, &parsed.namespace),
                NamespaceMetadata {
                    tenant: parsed.tenant.clone(),
                    name: parsed.namespace.clone(),
                },
            );

            let entry = self
                .topics_meta
                .entry(logical_topic.clone())
                .or_insert(TopicMetadata {
                    full_name: logical_topic.clone(),
                    domain: parsed.domain,
                    tenant: parsed.tenant,
                    namespace: parsed.namespace,
                    local_name: parsed.local_name,
                    partitioned: true,
                    partition_count: partitioned_topic.partitions,
                });
            entry.partitioned = true;
            entry.partition_count = partitioned_topic.partitions;
        }

        Ok(())
    }

    pub(crate) fn persist_document(&self, version: u32) -> Result<()> {
        let document = self.build_metadata_document(version);
        self.backend.save_document(&document)
    }

    pub(crate) fn insert_tenant_metadata(&mut self, tenant: &str) -> bool {
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

    pub(crate) fn insert_namespace_metadata(&mut self, tenant: &str, namespace: &str) -> bool {
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

    pub(crate) fn upsert_topic_metadata(&mut self, metadata: TopicMetadata) -> bool {
        let key = metadata.full_name.clone();
        let mut changed = false;
        let entry = self.topics_meta.entry(key).or_insert_with(|| {
            changed = true;
            metadata.clone()
        });

        if metadata.partitioned {
            let desired_partition_count = metadata.partition_count.max(1);
            if !entry.partitioned || entry.partition_count != desired_partition_count {
                entry.partitioned = true;
                entry.partition_count = desired_partition_count;
                changed = true;
            }
        } else if !entry.partitioned && entry.partition_count != 0 {
            entry.partition_count = 0;
            changed = true;
        }

        changed
    }

    pub(crate) fn insert_subscription_metadata(&mut self, topic: &str, subscription: &str) -> bool {
        let key = subscription_key(topic, subscription);
        if self.subscriptions_meta.contains_key(&key) {
            return false;
        }

        self.subscriptions_meta.insert(
            key,
            SubscriptionMetadata {
                topic: topic.to_string(),
                name: subscription.to_string(),
            },
        );
        true
    }

    pub(crate) fn get_partitioned_topic_metadata(&self) -> HashMap<String, usize> {
        self.topics_meta
            .iter()
            .filter_map(|(topic, metadata)| {
                metadata
                    .partitioned
                    .then_some((topic.clone(), metadata.partition_count))
            })
            .collect()
    }

    pub(crate) fn has_tenant_metadata(&self, tenant: &str) -> bool {
        self.tenants.contains_key(tenant)
    }

    pub(crate) fn has_namespace_metadata(&self, tenant: &str, namespace: &str) -> bool {
        self.namespaces
            .contains_key(&namespace_key(tenant, namespace))
    }

    pub(crate) fn get_topic_metadata(&self, topic: &str) -> Option<&TopicMetadata> {
        self.topics_meta.get(topic)
    }

    pub(crate) fn has_subscription_metadata(&self, topic: &str, subscription: &str) -> bool {
        self.subscriptions_meta
            .contains_key(&subscription_key(topic, subscription))
    }
}
