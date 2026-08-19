use crate::backend::ManagedLedgerStore;
use crate::config::{ManagedLedgerBackendConfig, StorageConfig};
use crate::error::StorageResult;
use log::{debug, info};
use prost::Message;
use pulsar_lite_proto::codec::proto::pulsar::MessageMetadata;
use pulsar_lite_storage_managed_ledger::{
    CursorInitOptions, CursorOpenResult, ManagedLedgerPosition, ManagedLedgerStorage, MessageId,
    StoredMessage,
};
use pulsar_lite_storage_metadata::FileMetadataStore;
use pulsar_lite_storage_resources::PulsarResources;
use std::path::Path;

/// Broker storage facade.
///
/// Topic runtime selection happens above this layer from the topic URL domain.
/// Persistent topics use this facade's managed-ledger store, while
/// non-persistent topics use their in-memory runtime path.
#[derive(Debug)]
pub struct Storage {
    resources: PulsarResources<FileMetadataStore>,
    managed_ledger: ManagedLedgerStore,
}

impl Storage {
    pub const METADATA_VERSION: u32 = 2;

    pub fn open(config: StorageConfig) -> StorageResult<Self> {
        let resources = PulsarResources::new(&config.metadata_path)?;
        let managed_ledger = match config.managed_ledger {
            ManagedLedgerBackendConfig::Memory => {
                info!("In-memory storage initialized (MVP version)");
                ManagedLedgerStore::memory()
            }
            #[cfg(feature = "rocksdb-storage")]
            ManagedLedgerBackendConfig::RocksDb { path } => {
                info!("RocksDB managed-ledger storage initialized");
                ManagedLedgerStore::rocksdb(&path)?
            }
        };
        Ok(Self {
            resources,
            managed_ledger,
        })
    }

    /// Create a new storage instance.
    #[cfg(feature = "rocksdb-storage")]
    pub fn new(path: &Path) -> StorageResult<Self> {
        Self::new_rocksdb(path)
    }

    /// Create a new storage instance.
    #[cfg(not(feature = "rocksdb-storage"))]
    pub fn new(path: &Path) -> StorageResult<Self> {
        Self::new_memory(path)
    }

    /// Create a new storage instance backed by the in-memory managed-ledger store.
    pub fn new_memory(path: &Path) -> StorageResult<Self> {
        Self::open(StorageConfig::memory(path))
    }

    /// Create a new storage instance backed by RocksDB managed-ledger state.
    #[cfg(feature = "rocksdb-storage")]
    pub fn new_rocksdb(path: &Path) -> StorageResult<Self> {
        Self::open(StorageConfig::rocksdb(path))
    }

    /// RocksDB storage requires the `rocksdb-storage` feature.
    #[cfg(not(feature = "rocksdb-storage"))]
    pub fn new_rocksdb(_path: &Path) -> StorageResult<Self> {
        Err(anyhow::anyhow!(
            "managed ledger store 'rocksdb' requires the rocksdb-storage feature"
        ))
    }

    pub fn resources(&self) -> &PulsarResources {
        &self.resources
    }

    pub fn resources_mut(&mut self) -> &mut PulsarResources {
        &mut self.resources
    }

    /// Clone a concurrent append handle when the managed-ledger backend provides one.
    ///
    /// Callers can append without holding `Mutex<Storage>` for the duration of the write.
    #[cfg(feature = "rocksdb-storage")]
    pub fn concurrent_appender(
        &self,
    ) -> StorageResult<Option<pulsar_lite_storage_managed_ledger_rocksdb::ConcurrentAppender>> {
        self.managed_ledger.concurrent_appender()
    }

    /// Create a topic.
    pub fn create_topic(&mut self, name: &str) -> StorageResult<()> {
        self.managed_ledger.create_topic(name)
    }

    /// Append a message using the current Pulsar-compatible message id layout.
    pub fn append_message(
        &mut self,
        topic: &str,
        partition: i32,
        data: &[u8],
    ) -> StorageResult<MessageId> {
        let message_id = self.managed_ledger.append_message(topic, partition, data)?;
        debug!(
            "Message appended to {}: ledger={}, entry={}, partition={}",
            topic, message_id.ledger, message_id.entry, message_id.partition
        );
        Ok(message_id)
    }

    /// Append a message with its serialized Pulsar `MessageMetadata`.
    pub fn append_message_with_metadata(
        &mut self,
        topic: &str,
        partition: i32,
        metadata: &[u8],
        payload: &[u8],
    ) -> StorageResult<MessageId> {
        let message_id = self
            .managed_ledger
            .append_message_with_metadata(topic, partition, metadata, payload)?;
        debug!(
            "Message appended to {}: ledger={}, entry={}, partition={}, metadata={} bytes",
            topic,
            message_id.ledger,
            message_id.entry,
            message_id.partition,
            metadata.len()
        );
        Ok(message_id)
    }

    pub fn initialize_or_open_cursor(
        &mut self,
        topic: &str,
        subscription: &str,
        options: CursorInitOptions,
    ) -> StorageResult<CursorOpenResult> {
        self.managed_ledger
            .initialize_or_open_cursor(topic, subscription, options)
    }

    pub fn delete_cursor(&mut self, topic: &str, subscription: &str) -> StorageResult<()> {
        self.managed_ledger.delete_cursor(topic, subscription)
    }

    pub fn seek_cursor(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
        shared: bool,
    ) -> StorageResult<()> {
        self.managed_ledger
            .seek_cursor(topic, subscription, message_id, shared)
    }

    pub fn decode_publish_time(metadata: &[u8]) -> Option<u64> {
        if metadata.is_empty() {
            return None;
        }
        MessageMetadata::decode(metadata)
            .ok()
            .map(|m| m.publish_time)
    }

    pub fn find_message_id_by_publish_time(
        &self,
        topic: &str,
        publish_time: u64,
    ) -> StorageResult<Option<MessageId>> {
        let entries = self.get_message_entries(topic);
        if entries.is_empty() {
            return Ok(None);
        }
        let mut last_earlier: Option<usize> = None;
        for (i, entry) in entries.iter().enumerate() {
            match Self::decode_publish_time(&entry.metadata) {
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

    pub fn first_unacked_position(
        &self,
        topic: &str,
        subscription: &str,
    ) -> StorageResult<Option<ManagedLedgerPosition>> {
        self.managed_ledger
            .first_unacked_position(topic, subscription)
    }

    pub fn read_from(
        &self,
        topic: &str,
        from: &ManagedLedgerPosition,
        limit: usize,
    ) -> StorageResult<Vec<(MessageId, Vec<u8>)>> {
        self.managed_ledger.read_from(topic, from, limit)
    }

    pub fn read_entries_from(
        &self,
        topic: &str,
        from: &ManagedLedgerPosition,
        limit: usize,
    ) -> StorageResult<Vec<StoredMessage>> {
        self.managed_ledger.read_entries_from(topic, from, limit)
    }

    pub fn get_last_position(&self, topic: &str) -> StorageResult<Option<ManagedLedgerPosition>> {
        self.managed_ledger.get_last_position(topic)
    }

    pub fn get_next_position(
        &self,
        topic: &str,
        current: &ManagedLedgerPosition,
    ) -> StorageResult<Option<ManagedLedgerPosition>> {
        self.managed_ledger.get_next_position(topic, current)
    }

    pub fn is_acknowledged(
        &self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
    ) -> StorageResult<bool> {
        self.managed_ledger
            .is_acknowledged(topic, subscription, message_id)
    }

    /// Acknowledge a message under cumulative-style cursor semantics.
    pub fn ack_message(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: MessageId,
    ) -> StorageResult<()> {
        info!(
            "Message acknowledged for topic {} subscription {}: ledger={}, entry={}",
            topic, subscription, message_id.ledger, message_id.entry
        );
        self.managed_ledger
            .ack_message(topic, subscription, message_id)
    }

    /// Acknowledge a message under Shared subscription semantics.
    pub fn ack_message_shared(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: MessageId,
    ) -> StorageResult<()> {
        self.managed_ledger
            .ack_message_shared(topic, subscription, message_id)
    }

    /// Look up a message by its full `MessageId`.
    pub fn get_message_by_id(
        &self,
        topic: &str,
        message_id: &MessageId,
    ) -> Option<(MessageId, Vec<u8>)> {
        self.managed_ledger.get_message_by_id(topic, message_id)
    }

    pub fn get_message_entry_by_id(
        &self,
        topic: &str,
        message_id: &MessageId,
    ) -> Option<StoredMessage> {
        self.managed_ledger
            .get_message_entry_by_id(topic, message_id)
    }

    /// Return the current in-memory message list for a topic.
    pub fn get_messages(&self, topic: &str) -> Vec<(MessageId, Vec<u8>)> {
        self.managed_ledger.get_messages(topic)
    }

    pub fn get_message_entries(&self, topic: &str) -> Vec<StoredMessage> {
        self.managed_ledger.get_message_entries(topic)
    }

    /// Check whether a message is already covered by the Shared ack frontier.
    pub fn is_acknowledged_shared(
        &self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
    ) -> bool {
        self.managed_ledger
            .is_acknowledged_shared(topic, subscription, message_id)
    }

    /// Return the current Shared `mark_delete` frontier for a subscription.
    pub fn get_mark_delete_position(&self, topic: &str, subscription: &str) -> Option<u64> {
        self.managed_ledger
            .get_mark_delete_position(topic, subscription)
    }

    /// Ledger-aware unacked entry count for a subscription cursor.
    pub fn backlog_entries(&self, topic: &str, subscription: &str) -> Option<u64> {
        self.managed_ledger.backlog_entries(topic, subscription)
    }

    /// Bytes durably stored for `topic`.
    pub fn stored_bytes(&self, topic: &str) -> u64 {
        self.managed_ledger.stored_bytes(topic)
    }
}
