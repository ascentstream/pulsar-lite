use super::cursor::{ack_managed_cursor_shared, is_managed_position_acknowledged, next_position};
use super::entrylog::EntryLogStore;
use super::factory::RocksDBManagedLedgerFactory;
use super::keys;
use anyhow::Result;
use rocksdb::{Options, DB};
use std::path::Path;
use std::sync::Arc;

use crate::storage::{
    first_unacked_from_messages, last_position_from_messages, read_from_messages,
    CursorInitOptions, CursorOpenResult, InitialPosition, ManagedCursor, ManagedLedger,
    ManagedLedgerPosition, ManagedLedgerStorage, MessageId, StoredMessage,
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

    fn cursor_exists(&self, topic: &str, subscription: &str) -> Result<bool> {
        let ledger_name = keys::managed_ledger_name(topic);
        let cursor_name = keys::encode_cursor_name(subscription);
        self.factory.cursor_state_exists(&ledger_name, &cursor_name)
    }

    fn persist_empty_cursor(&self, topic: &str, subscription: &str) -> Result<()> {
        let ledger_name = keys::managed_ledger_name(topic);
        let cursor_name = keys::encode_cursor_name(subscription);
        let mut ledger = self.factory.open_ledger(&ledger_name)?;
        let cursor = ledger.open_cursor(&cursor_name)?;
        cursor.persist_state()
    }

    fn position_before_start_message_id(
        &self,
        topic: &str,
        start: &MessageId,
    ) -> Option<ManagedLedgerPosition> {
        let target = ManagedLedgerPosition::from(start);
        let mut previous = None;

        for (message_id, _) in self.get_messages(topic) {
            let position = ManagedLedgerPosition::from(&message_id);
            if position >= target {
                return previous;
            }
            previous = Some(position);
        }

        previous
    }

    fn apply_latest_cursor(&self, topic: &str, subscription: &str) -> Result<()> {
        let last = last_position_from_messages(&self.get_messages(topic));
        let ledger_name = keys::managed_ledger_name(topic);
        let cursor_name = keys::encode_cursor_name(subscription);
        let mut ledger = self.factory.open_ledger(&ledger_name)?;
        let mut cursor = ledger.open_cursor(&cursor_name)?;

        if let Some(last) = last {
            cursor.mark_delete(last)
        } else {
            cursor.persist_state()
        }
    }

    fn apply_start_message_id_cursor(
        &self,
        topic: &str,
        subscription: &str,
        start: &MessageId,
    ) -> Result<()> {
        let ledger_name = keys::managed_ledger_name(topic);
        let cursor_name = keys::encode_cursor_name(subscription);
        let mut ledger = self.factory.open_ledger(&ledger_name)?;
        let mut cursor = ledger.open_cursor(&cursor_name)?;

        if let Some(previous) = self.position_before_start_message_id(topic, start) {
            cursor.mark_delete(previous)
        } else {
            cursor.persist_state()
        }
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

    fn append_message_with_metadata(
        &mut self,
        topic: &str,
        partition: i32,
        metadata: &[u8],
        payload: &[u8],
    ) -> Result<MessageId> {
        let ledger_name = keys::managed_ledger_name(topic);
        let mut ledger = self.factory.open_ledger(&ledger_name)?;
        let position =
            ledger.add_entry_with_partition_and_metadata(partition, metadata, payload)?;
        Ok(MessageId::from(position))
    }

    fn initialize_or_open_cursor(
        &mut self,
        topic: &str,
        subscription: &str,
        options: CursorInitOptions,
    ) -> Result<CursorOpenResult> {
        if self.cursor_exists(topic, subscription)? {
            return Ok(CursorOpenResult {
                created: false,
                first_unacked: self.first_unacked_position(topic, subscription)?,
            });
        }

        if let Some(start_id) = options.start_message_id.as_ref() {
            self.apply_start_message_id_cursor(topic, subscription, start_id)?;
        } else if options.initial_position == InitialPosition::Latest {
            self.apply_latest_cursor(topic, subscription)?;
        } else {
            self.persist_empty_cursor(topic, subscription)?;
        }

        Ok(CursorOpenResult {
            created: true,
            first_unacked: self.first_unacked_position(topic, subscription)?,
        })
    }

    fn delete_cursor(&mut self, topic: &str, subscription: &str) -> Result<()> {
        let ledger_name = keys::managed_ledger_name(topic);
        let cursor_name = keys::encode_cursor_name(subscription);
        self.factory.delete_cursor_state(&ledger_name, &cursor_name)
    }

    async fn seek_cursor(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
        _shared: bool,
    ) -> Result<()> {
        let ledger_name = keys::managed_ledger_name(topic);
        let cursor_name = keys::encode_cursor_name(subscription);
        let mut ledger = self.factory.open_ledger(&ledger_name)?;
        let mut cursor = ledger.open_cursor(&cursor_name)?;
        let position = ManagedLedgerPosition::from(message_id);
        let mark_delete_position = ledger.previous_position(&position);
        cursor.async_reset_cursor(mark_delete_position).await
    }

    fn first_unacked_position(
        &self,
        topic: &str,
        subscription: &str,
    ) -> Result<Option<ManagedLedgerPosition>> {
        let messages = self.get_messages(topic);
        let ledger_name = keys::managed_ledger_name(topic);
        let cursor_name = keys::encode_cursor_name(subscription);
        let mut ledger = self.factory.open_ledger(&ledger_name)?;
        let cursor = ledger.open_cursor(&cursor_name)?;

        Ok(first_unacked_from_messages(&messages, |id| {
            is_managed_position_acknowledged(cursor.state(), &ManagedLedgerPosition::from(id))
        }))
    }

    fn read_from(
        &self,
        topic: &str,
        from: &ManagedLedgerPosition,
        limit: usize,
    ) -> Result<Vec<(MessageId, Vec<u8>)>> {
        let messages = self.get_messages(topic);
        Ok(read_from_messages(&messages, from, limit))
    }

    fn read_entries_from(
        &self,
        topic: &str,
        from: &ManagedLedgerPosition,
        limit: usize,
    ) -> Result<Vec<StoredMessage>> {
        let messages = self.get_message_entries(topic);
        let mut out = Vec::new();
        for entry in messages {
            let position = ManagedLedgerPosition::from(&entry.message_id);
            if (position.ledger_id, position.entry_id) < (from.ledger_id, from.entry_id) {
                continue;
            }
            out.push(entry);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    fn get_last_position(&self, topic: &str) -> Result<Option<ManagedLedgerPosition>> {
        Ok(last_position_from_messages(&self.get_messages(topic)))
    }

    fn get_next_position(
        &self,
        topic: &str,
        current: &ManagedLedgerPosition,
    ) -> Result<Option<ManagedLedgerPosition>> {
        let ledger_name = keys::managed_ledger_name(topic);
        let ledger = self.factory.open_ledger(&ledger_name)?;
        Ok(next_position(current, &ledger.info))
    }

    fn is_acknowledged(
        &self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
    ) -> Result<bool> {
        Ok(self.is_acknowledged_shared(topic, subscription, message_id))
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

    fn get_message_entry_by_id(
        &self,
        topic: &str,
        message_id: &MessageId,
    ) -> Option<StoredMessage> {
        let ledger_name = keys::managed_ledger_name(topic);
        self.factory
            .open_ledger(&ledger_name)
            .ok()?
            .get_message_entry_by_id(message_id)
    }

    fn get_messages(&self, topic: &str) -> Vec<(MessageId, Vec<u8>)> {
        let ledger_name = keys::managed_ledger_name(topic);
        self.factory
            .open_ledger(&ledger_name)
            .map(|ledger| ledger.messages())
            .unwrap_or_default()
    }

    fn get_message_entries(&self, topic: &str) -> Vec<StoredMessage> {
        let ledger_name = keys::managed_ledger_name(topic);
        self.factory
            .open_ledger(&ledger_name)
            .map(|ledger| ledger.message_entries())
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

    fn find_message_id_by_publish_time(
        &self,
        topic: &str,
        publish_time: u64,
    ) -> Result<Option<MessageId>> {
        let ledger_name = keys::managed_ledger_name(topic);
        let ledger = self.factory.open_ledger(&ledger_name)?;
        Ok(ledger
            .find_position_by_publish_time(publish_time)
            .map(MessageId::from))
    }
}
