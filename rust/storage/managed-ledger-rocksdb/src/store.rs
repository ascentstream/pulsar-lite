use super::cursor::{ack_managed_cursor_shared, is_managed_position_acknowledged, next_position};
use super::entrylog::EntryLogStore;
use super::factory::{RocksDBManagedLedgerFactory, SharedLedger};
use super::keys;
use super::ledger::RocksDBManagedLedger;
use crate::cursor::first_position;
use anyhow::{anyhow, Result};
use rocksdb::{Options, DB};
use std::path::Path;
use std::sync::{Arc, MutexGuard};

use pulsar_lite_storage_managed_ledger::{
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

    fn topic_ledger(&self, topic: &str) -> Result<SharedLedger> {
        let ledger_name = keys::managed_ledger_name(topic);
        self.factory.open_ledger(&ledger_name)
    }

    fn lock_ledger(ledger: &SharedLedger) -> Result<MutexGuard<'_, RocksDBManagedLedger>> {
        ledger
            .lock()
            .map_err(|_| anyhow!("managed ledger lock poisoned"))
    }

    fn cursor_exists(&self, topic: &str, subscription: &str) -> Result<bool> {
        let ledger_name = keys::managed_ledger_name(topic);
        let cursor_name = keys::encode_cursor_name(subscription);
        self.factory.cursor_state_exists(&ledger_name, &cursor_name)
    }

    fn persist_empty_cursor(&self, topic: &str, subscription: &str) -> Result<()> {
        let cursor_name = keys::encode_cursor_name(subscription);
        let shared = self.topic_ledger(topic)?;
        let mut ledger = Self::lock_ledger(&shared)?;
        let cursor = ledger.open_cursor(&cursor_name)?;
        cursor.persist_state()
    }

    fn apply_latest_cursor(&self, topic: &str, subscription: &str) -> Result<()> {
        let cursor_name = keys::encode_cursor_name(subscription);
        let shared = self.topic_ledger(topic)?;
        let mut ledger = Self::lock_ledger(&shared)?;
        let last = ledger.last_position()?;
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
        let cursor_name = keys::encode_cursor_name(subscription);
        let target = ManagedLedgerPosition::from(start);
        let shared = self.topic_ledger(topic)?;
        let mut ledger = Self::lock_ledger(&shared)?;

        let previous = ledger.previous_position(&target);
        let mut cursor = ledger.open_cursor(&cursor_name)?;

        if let Some(previous) = previous {
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
        let shared = self.topic_ledger(topic)?;
        let mut ledger = Self::lock_ledger(&shared)?;
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
        let shared = self.topic_ledger(topic)?;
        let mut ledger = Self::lock_ledger(&shared)?;
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

    fn seek_cursor(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
        _shared: bool,
    ) -> Result<()> {
        let cursor_name = keys::encode_cursor_name(subscription);
        let position = ManagedLedgerPosition::from(message_id);

        let (mut cursor, marker_delete_posistion) = {
            let shared = self.topic_ledger(topic)?;
            let mut ledger = Self::lock_ledger(&shared)?;

            let mark_delete_position = ledger.previous_position(&position);
            let cursor = ledger.open_cursor(&cursor_name)?;
            (cursor, mark_delete_position)
        };
        cursor.reset_cursor(marker_delete_posistion)
    }

    fn first_unacked_position(
        &self,
        topic: &str,
        subscription: &str,
    ) -> Result<Option<ManagedLedgerPosition>> {
        let cursor_name = keys::encode_cursor_name(subscription);
        let shared = self.topic_ledger(topic)?;
        let mut ledger = Self::lock_ledger(&shared)?;

        let cursor = ledger.open_cursor(&cursor_name)?;
        let state = cursor.state();

        let mut candidate = match state.mark_delete.as_ref() {
            Some(mark_delete) => next_position(mark_delete, &ledger.info),
            None => first_position(&ledger.info, -1),
        };

        while let Some(position) = candidate {
            if !is_managed_position_acknowledged(state, &position) {
                return Ok(Some(position));
            }
            candidate = next_position(&position, &ledger.info);
        }
        Ok(None)
    }

    fn read_from(
        &self,
        topic: &str,
        from: &ManagedLedgerPosition,
        limit: usize,
    ) -> Result<Vec<(MessageId, Vec<u8>)>> {
        let shared = self.topic_ledger(topic)?;
        let ledger = Self::lock_ledger(&shared)?;

        Ok(ledger
            .read_entries_from(from, limit)?
            .into_iter()
            .map(|e| (e.message_id, e.payload))
            .collect())
    }

    fn read_entries_from(
        &self,
        topic: &str,
        from: &ManagedLedgerPosition,
        limit: usize,
    ) -> Result<Vec<StoredMessage>> {
        let shared = self.topic_ledger(topic)?;
        let ledger = Self::lock_ledger(&shared)?;
        ledger.read_entries_from(from, limit)
    }

    fn get_last_position(&self, topic: &str) -> Result<Option<ManagedLedgerPosition>> {
        let shared = self.topic_ledger(topic)?;
        let ledger = Self::lock_ledger(&shared)?;
        ledger.last_position()
    }

    fn get_next_position(
        &self,
        topic: &str,
        current: &ManagedLedgerPosition,
    ) -> Result<Option<ManagedLedgerPosition>> {
        let shared = self.topic_ledger(topic)?;
        let ledger = Self::lock_ledger(&shared)?;
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
        let cursor_name = keys::encode_cursor_name(subscription);
        let shared = self.topic_ledger(topic)?;
        let mut ledger = Self::lock_ledger(&shared)?;
        let mut cursor = ledger.open_cursor(&cursor_name)?;
        cursor.mark_delete(ManagedLedgerPosition::from(message_id))
    }

    fn ack_message_shared(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: MessageId,
    ) -> Result<()> {
        let cursor_name = keys::encode_cursor_name(subscription);
        let shared = self.topic_ledger(topic)?;
        let mut ledger = Self::lock_ledger(&shared)?;
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
        let shared = self.topic_ledger(topic).ok()?;
        let ledger = Self::lock_ledger(&shared).ok()?;
        ledger.get_message_by_id(message_id)
    }

    fn get_message_entry_by_id(
        &self,
        topic: &str,
        message_id: &MessageId,
    ) -> Option<StoredMessage> {
        let shared = self.topic_ledger(topic).ok()?;
        let ledger = Self::lock_ledger(&shared).ok()?;
        ledger.get_message_entry_by_id(message_id)
    }

    fn get_messages(&self, topic: &str) -> Vec<(MessageId, Vec<u8>)> {
        let shared = match self.topic_ledger(topic) {
            Ok(shared) => shared,
            Err(error) => {
                log::error!(
                    "Failed to open managed ledger for topic '{}': {}",
                    topic,
                    error
                );
                return Vec::new();
            }
        };
        let ledger = match Self::lock_ledger(&shared) {
            Ok(ledger) => ledger,
            Err(error) => {
                log::error!(
                    "Failed to lock managed ledger for topic '{}': {}",
                    topic,
                    error
                );
                return Vec::new();
            }
        };
        match ledger.messages() {
            Ok(messages) => messages,
            Err(error) => {
                log::error!(
                    "Failed to read messages from managed ledger for topic '{}': {}",
                    topic,
                    error
                );
                Vec::new()
            }
        }
    }

    fn get_message_entries(&self, topic: &str) -> Vec<StoredMessage> {
        let shared = match self.topic_ledger(topic) {
            Ok(shared) => shared,
            Err(error) => {
                log::error!(
                    "Failed to open managed ledger for topic '{}': {}",
                    topic,
                    error
                );
                return Vec::new();
            }
        };
        let ledger = match Self::lock_ledger(&shared) {
            Ok(ledger) => ledger,
            Err(error) => {
                log::error!(
                    "Failed to lock managed ledger for topic '{}': {}",
                    topic,
                    error
                );
                return Vec::new();
            }
        };
        match ledger.message_entries() {
            Ok(entries) => entries,
            Err(error) => {
                log::error!(
                    "Failed to read message entries from managed ledger for topic '{}': {}",
                    topic,
                    error
                );
                Vec::new()
            }
        }
    }

    fn is_acknowledged_shared(
        &self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
    ) -> bool {
        let shared = match self.topic_ledger(topic) {
            Ok(shared) => shared,
            Err(_) => return false,
        };
        let mut ledger = match Self::lock_ledger(&shared) {
            Ok(ledger) => ledger,
            Err(_) => return false,
        };
        let cursor_name = keys::encode_cursor_name(subscription);
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
        let cursor_name = keys::encode_cursor_name(subscription);
        let shared = self.topic_ledger(topic).ok()?;
        let mut ledger = match Self::lock_ledger(&shared) {
            Ok(ledger) => ledger,
            Err(_) => return None,
        };
        let cursor = ledger.open_cursor(&cursor_name).ok()?;
        cursor
            .state()
            .mark_delete
            .as_ref()
            .map(|position| position.entry_id)
    }
}
