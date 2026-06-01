use super::cursor::{ack_managed_cursor_shared, is_managed_position_acknowledged};
use super::entrylog::EntryLogStore;
use super::factory::RocksDBManagedLedgerFactory;
use super::keys;
use anyhow::Result;
use rocksdb::{Options, DB};
use std::path::Path;
use std::sync::Arc;

use crate::storage::{
    ManagedCursor, ManagedLedger, ManagedLedgerPosition, ManagedLedgerStorage, MessageId,
};

/// RocksDB-backed managed-ledger store for persistent topics.
#[derive(Debug)]
pub struct RocksDbManagedLedgerStorage {
    factory: RocksDBManagedLedgerFactory,
}

impl RocksDbManagedLedgerStorage {
    pub fn open(path: &Path) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = Arc::new(DB::open(&options, path)?);
        let entry_log = Arc::new(EntryLogStore::open(path)?);

        Ok(Self {
            factory: RocksDBManagedLedgerFactory::new(db, entry_log),
        })
    }
}

impl ManagedLedgerStorage for RocksDbManagedLedgerStorage {
    fn create_topic(&mut self, name: &str) -> Result<()> {
        let ledger_name = keys::managed_ledger_name(name);
        self.factory.open_ledger(&ledger_name)?;
        Ok(())
    }

    fn append_message(&mut self, topic: &str, partition: i32, data: &[u8]) -> Result<MessageId> {
        let ledger_name = keys::managed_ledger_name(topic);
        let mut ledger = self.factory.open_ledger(&ledger_name)?;
        let position = ledger.add_entry_with_partition(partition, data)?;
        Ok(MessageId::from(position))
    }

    fn subscribe(&mut self, topic: &str, subscription: &str) -> Result<()> {
        let ledger_name = keys::managed_ledger_name(topic);
        let cursor_name = keys::encode_cursor_name(subscription);
        let mut ledger = self.factory.open_ledger(&ledger_name)?;
        ledger.open_cursor(&cursor_name)?;
        Ok(())
    }

    fn get_next_unassigned_message(
        &mut self,
        topic: &str,
        subscription: &str,
        _consumer_id: u64,
    ) -> Result<Option<(MessageId, Vec<u8>)>> {
        let ledger_name = keys::managed_ledger_name(topic);
        let cursor_name = keys::encode_cursor_name(subscription);
        let mut ledger = self.factory.open_ledger(&ledger_name)?;
        let cursor = ledger.open_cursor(&cursor_name)?;
        for (message_id, payload) in self.get_messages(topic) {
            let position = ManagedLedgerPosition::from(&message_id);
            if is_managed_position_acknowledged(cursor.state(), &position) {
                continue;
            }
            return Ok(Some((message_id, payload)));
        }
        Ok(None)
    }

    fn ack_message(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: MessageId,
    ) -> Result<()> {
        let ledger_name = keys::managed_ledger_name(topic);
        let cursor_name = keys::encode_cursor_name(subscription);
        let mut ledger = self.factory.open_ledger(&ledger_name)?;
        let mut cursor = ledger.open_cursor(&cursor_name)?;
        cursor.mark_delete(ManagedLedgerPosition::from(message_id))
    }

    fn ack_message_shared(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: MessageId,
    ) -> Result<()> {
        let ledger_name = keys::managed_ledger_name(topic);
        let cursor_name = keys::encode_cursor_name(subscription);
        let mut ledger = self.factory.open_ledger(&ledger_name)?;
        let mut cursor = ledger.open_cursor(&cursor_name)?;
        ack_managed_cursor_shared(
            &mut cursor,
            ManagedLedgerPosition::from(message_id),
            &ledger.info,
        )
    }

    fn get_message_by_id(
        &self,
        topic: &str,
        message_id: &MessageId,
    ) -> Option<(MessageId, Vec<u8>)> {
        let ledger_name = keys::managed_ledger_name(topic);
        self.factory
            .open_ledger(&ledger_name)
            .ok()?
            .get_message_by_id(message_id)
    }

    fn get_messages(&self, topic: &str) -> Vec<(MessageId, Vec<u8>)> {
        let ledger_name = keys::managed_ledger_name(topic);
        self.factory
            .open_ledger(&ledger_name)
            .map(|ledger| ledger.messages())
            .unwrap_or_default()
    }

    fn is_acknowledged_shared(
        &self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
    ) -> bool {
        let ledger_name = keys::managed_ledger_name(topic);
        let cursor_name = keys::encode_cursor_name(subscription);
        let mut ledger = match self.factory.open_ledger(&ledger_name) {
            Ok(ledger) => ledger,
            Err(_) => return false,
        };
        ledger
            .open_cursor(&cursor_name)
            .map(|cursor| {
                is_managed_position_acknowledged(
                    cursor.state(),
                    &ManagedLedgerPosition::from(message_id),
                )
            })
            .unwrap_or(false)
    }

    fn get_mark_delete_position(&self, topic: &str, subscription: &str) -> Option<u64> {
        let ledger_name = keys::managed_ledger_name(topic);
        let cursor_name = keys::encode_cursor_name(subscription);
        let mut ledger = self.factory.open_ledger(&ledger_name).ok()?;
        let cursor = ledger.open_cursor(&cursor_name).ok()?;
        cursor
            .state()
            .mark_delete
            .as_ref()
            .map(|position| position.entry_id)
    }
}
