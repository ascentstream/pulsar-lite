//! Metadata store re-export shim.
//!
//! Concrete implementation lives in the `pulsar-lite-storage-metadata` crate.
//! `FileMetadataStore` is re-exported as `MetadataStore` so existing call sites
//! (`MetadataStore::new`, `metadata.insert_tenant_metadata`, ...) keep working
//! without touching broker/resource code. The `MetadataStore` *trait* and
//! generic `PulsarResources<S>` are introduced in Phase 4.

pub(crate) use pulsar_lite_storage_metadata::parse_topic_name;
pub use pulsar_lite_storage_metadata::{
    DomainNode, FileMetadataStore, MetadataDocument, MetadataFileNode, MetadataStore,
    NamespaceMetadata, NamespaceNode, ParsedTopicName, PartitionedTopicNode, SubscriptionMetadata,
    SubscriptionNode, TenantMetadata, TenantNode, TopicMetadata, TopicNode,
};

impl super::Storage {
    pub fn parse_topic_name(topic: &str) -> anyhow::Result<ParsedTopicName> {
        pulsar_lite_storage_metadata::parse_topic_name(topic)
    }

    #[cfg(test)]
    pub(crate) fn metadata_path(&self) -> &std::path::Path {
        self.resources.metadata_path()
    }

    #[cfg(test)]
    pub(crate) fn build_metadata_document(&self) -> MetadataDocument {
        self.resources
            .build_metadata_document(Self::METADATA_VERSION)
    }
}
