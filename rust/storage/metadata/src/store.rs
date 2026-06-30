use crate::model::{
    logical_topic_name, parse_topic_name, MetadataDocument, MetadataFileNode, NamespaceMetadata,
    PartitionedTopicNode, SubscriptionMetadata, TenantMetadata, TopicMetadata,
};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

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
