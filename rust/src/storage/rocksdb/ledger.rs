use super::cursor::RocksDBManagedCursor;
use super::keys;
use super::metadata::{StoredEntry, StoredManagedLedgerInfo};
use anyhow::Result;
use rocksdb::{WriteBatch, DB};
use std::sync::Arc;

use crate::storage::{ManagedLedger, ManagedLedgerConfig, ManagedLedgerPosition, MessageId};

const DEFAULT_MAX_ENTRIES_PER_LEDGER: u64 = 50_000;

#[derive(Debug, Clone)]
pub(super) struct RocksDBManagedLedger {
    name: String,
    db: Arc<DB>,
    pub(super) info: StoredManagedLedgerInfo,
    entries: Vec<(ManagedLedgerPosition, Vec<u8>)>,
    max_entries_per_ledger: u64,
}

impl RocksDBManagedLedger {
    pub(super) fn open(name: &str, db: Arc<DB>) -> Result<Self> {
        Self::open_with_config(name, db, &ManagedLedgerConfig::default())
    }

    pub(super) fn open_with_config(
        name: &str,
        db: Arc<DB>,
        config: &ManagedLedgerConfig,
    ) -> Result<Self> {
        let key = keys::managed_ledger_key(name);
        let mut info = db
            .get(&key)?
            .map(|bytes| bincode::deserialize::<StoredManagedLedgerInfo>(&bytes))
            .transpose()?
            .unwrap_or_else(StoredManagedLedgerInfo::new);
        let max_entries_per_ledger = config
            .max_entries_per_ledger
            .unwrap_or(DEFAULT_MAX_ENTRIES_PER_LEDGER)
            .max(1);

        info.ensure_writable_ledger(max_entries_per_ledger);

        db.put(&key, bincode::serialize(&info)?)?;

        Ok(Self {
            name: name.to_string(),
            entries: Self::load_entries(name, &db)?,
            db,
            info,
            max_entries_per_ledger,
        })
    }

    fn load_entries(name: &str, db: &DB) -> Result<Vec<(ManagedLedgerPosition, Vec<u8>)>> {
        let prefix = keys::managed_entry_prefix(name);
        let mut entries = Vec::new();

        for item in db.prefix_iterator(&prefix) {
            let (key, value) = item?;
            let Some(suffix) = key.strip_prefix(prefix.as_slice()) else {
                break;
            };
            let suffix = std::str::from_utf8(suffix)?;
            let mut parts = suffix.split('|');
            let Some(ledger_id) = parts.next().and_then(|value| value.parse::<u64>().ok()) else {
                continue;
            };
            let Some(entry_id) = parts.next().and_then(|value| value.parse::<u64>().ok()) else {
                continue;
            };
            let stored_entry: StoredEntry = bincode::deserialize(&value)?;

            entries.push((
                ManagedLedgerPosition {
                    ledger_id,
                    entry_id,
                    partition: stored_entry.partition,
                },
                stored_entry.payload,
            ));
        }

        entries.sort_by_key(|(position, _)| {
            (position.ledger_id, position.entry_id, position.partition)
        });
        Ok(entries)
    }

    pub(super) fn add_entry_with_partition(
        &mut self,
        partition: i32,
        payload: &[u8],
    ) -> Result<ManagedLedgerPosition> {
        let mut next_info = self.info.clone();
        next_info.ensure_writable_ledger(self.max_entries_per_ledger);
        let current_ledger = next_info.current_ledger_mut();
        let position = ManagedLedgerPosition {
            ledger_id: current_ledger.ledger_id,
            entry_id: current_ledger.entries,
            partition,
        };
        current_ledger.entries += 1;
        current_ledger.size += payload.len() as u64;
        next_info.roll_over_current_ledger_if_full(self.max_entries_per_ledger);

        let stored_entry = StoredEntry {
            partition,
            payload: payload.to_vec(),
        };

        let mut batch = WriteBatch::default();
        batch.put(
            keys::managed_entry_key(&self.name, position.ledger_id, position.entry_id),
            bincode::serialize(&stored_entry)?,
        );
        batch.put(
            keys::managed_ledger_key(&self.name),
            bincode::serialize(&next_info)?,
        );
        self.db.write(batch)?;

        self.info = next_info;
        self.entries.push((position.clone(), payload.to_vec()));

        Ok(position)
    }

    pub(super) fn get_message_by_id(&self, message_id: &MessageId) -> Option<(MessageId, Vec<u8>)> {
        self.entries
            .iter()
            .find(|(position, _)| {
                position.ledger_id == message_id.ledger
                    && position.entry_id == message_id.entry
                    && position.partition == message_id.partition
            })
            .map(|(_, payload)| (message_id.clone(), payload.clone()))
    }

    pub(super) fn messages(&self) -> Vec<(MessageId, Vec<u8>)> {
        self.entries
            .iter()
            .map(|(position, payload)| (MessageId::from(position), payload.clone()))
            .collect()
    }
}

impl ManagedLedger for RocksDBManagedLedger {
    type Cursor = RocksDBManagedCursor;

    fn name(&self) -> &str {
        &self.name
    }

    fn add_entry(&mut self, payload: &[u8]) -> Result<ManagedLedgerPosition> {
        self.add_entry_with_partition(-1, payload)
    }

    fn open_cursor(&mut self, name: &str) -> Result<Self::Cursor> {
        RocksDBManagedCursor::open(&self.name, name, Arc::clone(&self.db))
    }

    fn read_entry(&self, position: &ManagedLedgerPosition) -> Option<&[u8]> {
        self.entries
            .iter()
            .find(|(stored_position, _)| stored_position == position)
            .map(|(_, payload)| payload.as_slice())
    }
}
