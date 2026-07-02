use pulsar_lite_storage_managed_ledger::{
    CursorInitOptions, CursorOpenResult, InMemoryManagedLedgerStorage, ManagedLedgerPosition,
    ManagedLedgerStorage, MessageId, StoredMessage,
};
#[cfg(feature = "rocksdb-storage")]
use crate::storage::rocksdb::RocksDbManagedLedgerStorage;
use anyhow::Result;
#[cfg(feature = "rocksdb-storage")]
use std::path::Path;

/// Concrete managed-ledger storage implementations available to the broker.
///
/// Topic runtime selection still happens above this layer. This enum only
/// chooses how persistent managed-ledger state is stored.
#[derive(Debug)]
pub enum ManagedLedgerStore {
    Memory(InMemoryManagedLedgerStorage),
    #[cfg(feature = "rocksdb-storage")]
    RocksDb(RocksDbManagedLedgerStorage),
}

impl ManagedLedgerStore {
    pub fn memory() -> Self {
        Self::Memory(InMemoryManagedLedgerStorage::new())
    }

    #[cfg(feature = "rocksdb-storage")]
    pub fn rocksdb(path: &Path) -> Result<Self> {
        Ok(Self::RocksDb(RocksDbManagedLedgerStorage::open(path)?))
    }
}

impl ManagedLedgerStorage for ManagedLedgerStore {
    fn create_topic(&mut self, name: &str) -> Result<()> {
        match self {
            Self::Memory(inner) => inner.create_topic(name),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => inner.create_topic(name),
        }
    }

    fn append_message(&mut self, topic: &str, partition: i32, data: &[u8]) -> Result<MessageId> {
        match self {
            Self::Memory(inner) => inner.append_message(topic, partition, data),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => inner.append_message(topic, partition, data),
        }
    }

    fn append_message_with_metadata(
        &mut self,
        topic: &str,
        partition: i32,
        metadata: &[u8],
        payload: &[u8],
    ) -> Result<MessageId> {
        match self {
            Self::Memory(inner) => {
                inner.append_message_with_metadata(topic, partition, metadata, payload)
            }
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => {
                inner.append_message_with_metadata(topic, partition, metadata, payload)
            }
        }
    }

    fn initialize_or_open_cursor(
        &mut self,
        topic: &str,
        subscription: &str,
        options: CursorInitOptions,
    ) -> Result<CursorOpenResult> {
        match self {
            Self::Memory(inner) => inner.initialize_or_open_cursor(topic, subscription, options),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => inner.initialize_or_open_cursor(topic, subscription, options),
        }
    }

    fn delete_cursor(&mut self, topic: &str, subscription: &str) -> Result<()> {
        match self {
            Self::Memory(inner) => inner.delete_cursor(topic, subscription),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => inner.delete_cursor(topic, subscription),
        }
    }

    async fn seek_cursor(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
        shared: bool,
    ) -> Result<()> {
        match self {
            Self::Memory(inner) => {
                inner
                    .seek_cursor(topic, subscription, message_id, shared)
                    .await
            }
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => {
                inner
                    .seek_cursor(topic, subscription, message_id, shared)
                    .await
            }
        }
    }

    fn first_unacked_position(
        &self,
        topic: &str,
        subscription: &str,
    ) -> Result<Option<ManagedLedgerPosition>> {
        match self {
            Self::Memory(inner) => inner.first_unacked_position(topic, subscription),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => inner.first_unacked_position(topic, subscription),
        }
    }

    fn read_from(
        &self,
        topic: &str,
        from: &ManagedLedgerPosition,
        limit: usize,
    ) -> Result<Vec<(MessageId, Vec<u8>)>> {
        match self {
            Self::Memory(inner) => inner.read_from(topic, from, limit),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => inner.read_from(topic, from, limit),
        }
    }

    fn read_entries_from(
        &self,
        topic: &str,
        from: &ManagedLedgerPosition,
        limit: usize,
    ) -> Result<Vec<StoredMessage>> {
        match self {
            Self::Memory(inner) => inner.read_entries_from(topic, from, limit),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => inner.read_entries_from(topic, from, limit),
        }
    }

    fn get_last_position(&self, topic: &str) -> Result<Option<ManagedLedgerPosition>> {
        match self {
            Self::Memory(inner) => inner.get_last_position(topic),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => inner.get_last_position(topic),
        }
    }

    fn get_next_position(
        &self,
        topic: &str,
        current: &ManagedLedgerPosition,
    ) -> Result<Option<ManagedLedgerPosition>> {
        match self {
            Self::Memory(inner) => inner.get_next_position(topic, current),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => inner.get_next_position(topic, current),
        }
    }

    fn is_acknowledged(
        &self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
    ) -> Result<bool> {
        match self {
            Self::Memory(inner) => inner.is_acknowledged(topic, subscription, message_id),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => inner.is_acknowledged(topic, subscription, message_id),
        }
    }

    fn ack_message(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: MessageId,
    ) -> Result<()> {
        match self {
            Self::Memory(inner) => inner.ack_message(topic, subscription, message_id),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => inner.ack_message(topic, subscription, message_id),
        }
    }

    fn ack_message_shared(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: MessageId,
    ) -> Result<()> {
        match self {
            Self::Memory(inner) => inner.ack_message_shared(topic, subscription, message_id),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => inner.ack_message_shared(topic, subscription, message_id),
        }
    }

    fn get_message_by_id(
        &self,
        topic: &str,
        message_id: &MessageId,
    ) -> Option<(MessageId, Vec<u8>)> {
        match self {
            Self::Memory(inner) => inner.get_message_by_id(topic, message_id),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => inner.get_message_by_id(topic, message_id),
        }
    }

    fn get_message_entry_by_id(
        &self,
        topic: &str,
        message_id: &MessageId,
    ) -> Option<StoredMessage> {
        match self {
            Self::Memory(inner) => inner.get_message_entry_by_id(topic, message_id),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => inner.get_message_entry_by_id(topic, message_id),
        }
    }

    fn get_messages(&self, topic: &str) -> Vec<(MessageId, Vec<u8>)> {
        match self {
            Self::Memory(inner) => inner.get_messages(topic),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => inner.get_messages(topic),
        }
    }

    fn get_message_entries(&self, topic: &str) -> Vec<StoredMessage> {
        match self {
            Self::Memory(inner) => inner.get_message_entries(topic),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => inner.get_message_entries(topic),
        }
    }

    fn is_acknowledged_shared(
        &self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
    ) -> bool {
        match self {
            Self::Memory(inner) => inner.is_acknowledged_shared(topic, subscription, message_id),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => inner.is_acknowledged_shared(topic, subscription, message_id),
        }
    }

    fn get_mark_delete_position(&self, topic: &str, subscription: &str) -> Option<u64> {
        match self {
            Self::Memory(inner) => inner.get_mark_delete_position(topic, subscription),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => inner.get_mark_delete_position(topic, subscription),
        }
    }
}
