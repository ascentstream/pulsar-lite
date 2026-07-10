mod managed_ledger;

use anyhow::Result;
pub use pulsar_lite_storage::{ManagedLedgerStore, Storage};

pub use managed_ledger::{
    CursorInitOptions, CursorOpenResult, InMemoryManagedCursor, InMemoryManagedLedger,
    InMemoryManagedLedgerFactory, InMemoryManagedLedgerStorage, InitialPosition, ManagedCursor,
    ManagedCursorState, ManagedLedger, ManagedLedgerConfig, ManagedLedgerFactory,
    ManagedLedgerPosition, ManagedLedgerStorage, MessageId, NonPersistentEntry, StoredMessage,
    SubscriptionCursor,
};
pub use pulsar_lite_storage_metadata::{
    parse_topic_name, DomainNode, FileMetadataStore, MetadataDocument, MetadataFileNode,
    MetadataStore, NamespaceMetadata, NamespaceNode, ParsedTopicName, PartitionedTopicNode,
    SubscriptionMetadata, SubscriptionNode, TenantMetadata, TenantNode, TopicMetadata, TopicNode,
};
pub use pulsar_lite_storage_resources::{
    NamespaceResources, PulsarResources, TenantResources, TopicResources,
};

pub(crate) fn decode_publish_time(metadata: &[u8]) -> Option<u64> {
    use crate::protocol::codec::proto::pulsar::MessageMetadata;
    use prost::Message;
    if metadata.is_empty() {
        return None;
    }
    MessageMetadata::decode(metadata)
        .ok()
        .map(|m| m.publish_time)
}

/// Seek helpers that depend on broker protocol decoding (kept in the main crate).
pub trait StorageSeekExt {
    fn find_message_id_by_publish_time(
        &self,
        topic: &str,
        publish_time: u64,
    ) -> Result<Option<MessageId>>;
}

impl StorageSeekExt for Storage {
    fn find_message_id_by_publish_time(
        &self,
        topic: &str,
        publish_time: u64,
    ) -> Result<Option<MessageId>> {
        let entries = self.get_message_entries(topic);
        if entries.is_empty() {
            return Ok(None);
        }
        let mut last_earlier: Option<usize> = None;
        for (i, entry) in entries.iter().enumerate() {
            match decode_publish_time(&entry.metadata) {
                Some(pt) if pt < publish_time => last_earlier = Some(i),
                Some(_) => break,
                None => {}
            }
        }
        let target = match last_earlier {
            None => Some(entries[0].message_id.clone()),
            Some(i) => entries.get(i + 1).map(|e| e.message_id.clone()),
        };
        Ok(target)
    }
}
