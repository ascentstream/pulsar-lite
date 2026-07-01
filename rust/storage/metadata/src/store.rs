use crate::key::{namespace_key, subscription_key};
use crate::model::{
    logical_topic_name, parse_topic_name, MetadataDocument, MetadataFileNode, NamespaceMetadata,
    PartitionedTopicNode, SubscriptionMetadata, TenantMetadata, TopicMetadata,
};
use anyhow::{anyhow, Result};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::path::PathBuf;

/// Shared metadata status
#[derive(Debug, Default)]
pub struct MetadataState {
    metadata_path: PathBuf,
    tenants: HashMap<String, TenantMetadata>,
    namespaces: HashMap<String, NamespaceMetadata>,
    topics: HashMap<String, TopicMetadata>,
    subscriptions: HashMap<String, SubscriptionMetadata>,
}

impl MetadataState {
    pub fn new(metadata_path: PathBuf) -> Self {
        Self {
            metadata_path: metadata_path,
            tenants: HashMap::new(),
            namespaces: HashMap::new(),
            topics: HashMap::new(),
            subscriptions: HashMap::new(),
        }
    }

    pub fn metadata_path(&self) -> &Path {
        &self.metadata_path
    }

    pub fn apply_metadata_document(&mut self, document: MetadataDocument) -> Result<()> {
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

pub trait MetadataStore: Send + Sync {
    fn state(&self) -> &MetadataState;
    fn state_mut(&mut self) -> &mut MetadataState;

    fn load(&mut self) -> Result<()>;
    fn persist_document(&self, version: u32) -> Result<()>;

    fn metadata_path(&self) -> &Path {
        self.state().metadata_path()
    }

    fn insert_tenant_metadata(&mut self, tenant: &str) -> bool {
        self.state_mut().insert_tenant_metadata(tenant)
    }

    fn insert_namespace_metadata(&mut self, tenant: &str, namespace: &str) -> bool {
        self.state_mut()
            .insert_namespace_metadata(tenant, namespace)
    }

    fn upsert_topic_metadata(&mut self, metadata: TopicMetadata) -> bool {
        self.state_mut().upsert_topic_metadata(metadata)
    }

    fn insert_subscription_metadata(&mut self, topic: &str, subscription: &str) -> bool {
        self.state_mut()
            .insert_subscription_metadata(topic, subscription)
    }

    fn has_tenant_metadata(&self, tenant: &str) -> bool {
        self.state().has_tenant_metadata(tenant)
    }

    fn has_namespace_metadata(&self, tenant: &str, namespace: &str) -> bool {
        self.state().has_namespace_metadata(tenant, namespace)
    }

    fn has_subscription_metadata(&self, topic: &str, subscription: &str) -> bool {
        self.state().has_subscription_metadata(topic, subscription)
    }

    fn get_topic_metadata(&self, topic: &str) -> Option<&TopicMetadata> {
        self.state().get_topic_metadata(topic)
    }

    fn get_partitioned_topic_metadata(&self) -> HashMap<String, usize> {
        self.state().get_partitioned_topic_metadata()
    }

    fn build_metadata_document(&self, version: u32) -> MetadataDocument {
        self.state().build_metadata_document(version)
    }
}
/// Build a `MetadataDocument` snapshot from in-memory metadata state.
/// Shared by `FileMetadataStore` and `InMemoryMetadataStore` so both backends
/// serialize the same shape. T; both backends currently expose
/// inherent methods with matching names so the legacy `MetadataStore`
/// re-export alias keeps broker/resource call sites untouched.
pub(crate) fn build_document_from_state(
    metadata_path: &Path,
    tenants: &HashMap<String, TenantMetadata>,
    namespaces: &HashMap<String, NamespaceMetadata>,
    topics: &HashMap<String, TopicMetadata>,
    subscriptions: &HashMap<String, SubscriptionMetadata>,
    version: u32,
) -> MetadataDocument {
    let path_key = metadata_path.display().to_string();
    let mut file_node = MetadataFileNode::default();

    for tenant in tenants.values() {
        file_node.tenants.entry(tenant.name.clone()).or_default();
    }
    for namespace in namespaces.values() {
        file_node
            .tenants
            .entry(namespace.tenant.clone())
            .or_default()
            .namespaces
            .entry(namespace.name.clone())
            .or_default();
    }
    for topic in topics.values() {
        if topic.partitioned {
            continue;
        }

        let parsed = match parse_topic_name(&topic.full_name) {
            Ok(parsed) => parsed,
            Err(error) => {
                log::warn!(
                    "Skipping topic metadata '{}' while building document: {}",
                    topic.full_name,
                    error
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
            .or_default()
            .topics
            .entry(parsed.local_name.clone())
            .or_default();
    }
    for subscription in subscriptions.values() {
        let parsed = match parse_topic_name(&subscription.topic) {
            Ok(parsed) => parsed,
            Err(error) => {
                log::warn!(
                    "Skipping subscription metadata '{}:{}' while building document: {}",
                    subscription.topic,
                    subscription.name,
                    error
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
            .or_default()
            .topics
            .entry(parsed.local_name.clone())
            .or_default()
            .subscriptions
            .entry(subscription.name.clone())
            .or_default();
    }
    let mut partitioned_topics = BTreeMap::new();
    for topic in topics.values() {
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
