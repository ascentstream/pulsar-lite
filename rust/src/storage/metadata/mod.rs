mod json;
mod store;
mod types;

pub use json::JsonFileMetadataStore;
pub use store::MetadataStore;
pub use types::{
    DomainNode, MetadataDocument, MetadataFileNode, NamespaceMetadata, NamespaceNode,
    ParsedTopicName, PartitionedTopicNode, SubscriptionMetadata, SubscriptionNode, TenantMetadata,
    TenantNode, TopicMetadata, TopicNode,
};

impl super::Storage {
    #[cfg(test)]
    pub(crate) fn parse_topic_name(topic: &str) -> anyhow::Result<ParsedTopicName> {
        crate::storage::metadata::types::parse_topic_name(topic)
    }

    #[cfg(test)]
    pub(crate) fn metadata_path(&self) -> &std::path::Path {
        self.metadata.metadata_path()
    }

    #[cfg(test)]
    pub(crate) fn build_metadata_document(&self) -> MetadataDocument {
        self.metadata.build_metadata_document(Self::METADATA_VERSION)
    }

    pub fn ensure_tenant(&mut self, tenant: &str) -> anyhow::Result<()> {
        self.metadata.ensure_tenant(tenant, Self::METADATA_VERSION)
    }

    pub fn ensure_namespace(&mut self, tenant: &str, namespace: &str) -> anyhow::Result<()> {
        self.metadata
            .ensure_namespace(tenant, namespace, Self::METADATA_VERSION)
    }

    pub fn ensure_topic_metadata(
        &mut self,
        topic: &str,
        partitioned: bool,
        partition_count: usize,
    ) -> anyhow::Result<()> {
        self.metadata.ensure_topic_metadata(
            topic,
            partitioned,
            partition_count,
            Self::METADATA_VERSION,
        )
    }

    pub fn ensure_subscription_metadata(
        &mut self,
        topic: &str,
        subscription: &str,
    ) -> anyhow::Result<()> {
        self.metadata
            .ensure_subscription_metadata(topic, subscription, Self::METADATA_VERSION)
    }

    pub fn get_partitioned_topic_metadata(&self) -> std::collections::HashMap<String, usize> {
        self.metadata.get_partitioned_topic_metadata()
    }

    pub fn has_tenant_metadata(&self, tenant: &str) -> bool {
        self.metadata.has_tenant_metadata(tenant)
    }

    pub fn has_namespace_metadata(&self, tenant: &str, namespace: &str) -> bool {
        self.metadata.has_namespace_metadata(tenant, namespace)
    }

    pub fn get_topic_metadata(&self, topic: &str) -> Option<&TopicMetadata> {
        self.metadata.get_topic_metadata(topic)
    }

    pub fn has_subscription_metadata(&self, topic: &str, subscription: &str) -> bool {
        self.metadata.has_subscription_metadata(topic, subscription)
    }
}
