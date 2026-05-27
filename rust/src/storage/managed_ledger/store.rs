#[cfg(feature = "rocksdb-storage")]
use super::RocksDbManagedLedgerStorage;
use super::{InMemoryManagedLedgerStorage, ManagedLedgerStorage, MessageId};
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

    fn subscribe(&mut self, topic: &str, subscription: &str) -> Result<()> {
        match self {
            Self::Memory(inner) => inner.subscribe(topic, subscription),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => inner.subscribe(topic, subscription),
        }
    }

    fn get_next_unassigned_message(
        &mut self,
        topic: &str,
        subscription: &str,
        consumer_id: u64,
    ) -> Result<Option<(MessageId, Vec<u8>)>> {
        match self {
            Self::Memory(inner) => {
                inner.get_next_unassigned_message(topic, subscription, consumer_id)
            }
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => {
                inner.get_next_unassigned_message(topic, subscription, consumer_id)
            }
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

    fn get_messages(&self, topic: &str) -> Vec<(MessageId, Vec<u8>)> {
        match self {
            Self::Memory(inner) => inner.get_messages(topic),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(inner) => inner.get_messages(topic),
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
