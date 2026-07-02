use super::cursor::RocksDBManagedCursor;
use super::entrylog::{EntryIndex, EntryLogStore};
use super::keys;
use super::metadata::{StoredEntryLocation, StoredManagedLedgerInfo};
use anyhow::{anyhow, Result};
use rocksdb::{WriteBatch, DB};
use std::sync::Arc;

use pulsar_lite_storage_managed_ledger::{
    ManagedLedger, ManagedLedgerConfig, ManagedLedgerPosition, MessageId, StoredMessage,
};

const DEFAULT_MAX_ENTRIES_PER_LEDGER: u64 = 50_000;

#[derive(Debug, Clone)]
pub struct RocksDBManagedLedger {
    name: String,
    db: Arc<DB>,
    pub info: StoredManagedLedgerInfo,
    entries: Vec<(ManagedLedgerPosition, EntryIndex)>,
    max_entries_per_ledger: u64,
    entry_log: Arc<EntryLogStore>,
}

impl RocksDBManagedLedger {
    pub fn open(name: &str, db: Arc<DB>, entry_log: Arc<EntryLogStore>) -> Result<Self> {
        Self::open_with_config(name, db, entry_log, &ManagedLedgerConfig::default())
    }

    pub fn open_with_config(
        name: &str,
        db: Arc<DB>,
        entry_log: Arc<EntryLogStore>,
        config: &ManagedLedgerConfig,
    ) -> Result<Self> {
        let key = keys::managed_ledger_key(name);
        let max_entries_per_ledger = config
            .max_entries_per_ledger
            .unwrap_or(DEFAULT_MAX_ENTRIES_PER_LEDGER)
            .max(1);

        let mut info = match db.get(&key)? {
            Some(bytes) => StoredManagedLedgerInfo::decode(&bytes)?,
            None => StoredManagedLedgerInfo::new(Self::allocate_ledger_id(&db)?),
        };

        if info.ledgers.is_empty() {
            info.ensure_initialized(Self::allocate_ledger_id(&db)?);
        }
        if info.current_ledger_is_full(max_entries_per_ledger) {
            let next_ledger_id = Self::allocate_ledger_id(&db)?;
            info.roll_over_current_ledger(next_ledger_id);
        }

        db.put(&key, info.encode_to_vec())?;

        Ok(Self {
            name: name.to_string(),
            entries: Self::load_entries(&info, &db)?,
            db,
            entry_log,
            info,
            max_entries_per_ledger,
        })
    }

    fn allocate_ledger_id(db: &DB) -> Result<u64> {
        let key = keys::ledger_id_allocator_key();
        let next_ledger_id = db
            .get(&key)?
            .map(|bytes| bincode::deserialize::<u64>(&bytes))
            .transpose()?
            .unwrap_or_default();
        db.put(key, bincode::serialize(&(next_ledger_id + 1))?)?;
        Ok(next_ledger_id)
    }

    fn load_entries(
        info: &StoredManagedLedgerInfo,
        db: &DB,
    ) -> Result<Vec<(ManagedLedgerPosition, EntryIndex)>> {
        let mut entries = Vec::new();

        for ledger in &info.ledgers {
            for entry_id in 0..ledger.entries {
                let value = db
                    .get(keys::managed_entry_key(ledger.ledger_id, entry_id))?
                    .ok_or_else(|| {
                        anyhow!(
                            "missing entry location for ledger {} entry {}",
                            ledger.ledger_id,
                            entry_id
                        )
                    })?;
                let stored_entry_location: StoredEntryLocation = bincode::deserialize(&value)?;
                let position = ManagedLedgerPosition {
                    ledger_id: ledger.ledger_id,
                    entry_id,
                    partition: stored_entry_location.partition,
                };
                let entry_index = EntryIndex {
                    ledger_id: ledger.ledger_id,
                    entry_id,
                    file_id: stored_entry_location.file_id,
                    offset: stored_entry_location.offset,
                    len: stored_entry_location.len,
                    checksum: stored_entry_location.checksum,
                    partition: stored_entry_location.partition,
                };

                entries.push((position, entry_index));
            }
        }

        entries.sort_by_key(|(position, _)| {
            (position.ledger_id, position.entry_id, position.partition)
        });
        Ok(entries)
    }

    pub fn add_entry_with_partition(
        &mut self,
        partition: i32,
        payload: &[u8],
    ) -> Result<ManagedLedgerPosition> {
        self.add_entry_with_partition_and_metadata(partition, &[], payload)
    }

    pub fn add_entry_with_partition_and_metadata(
        &mut self,
        partition: i32,
        metadata: &[u8],
        payload: &[u8],
    ) -> Result<ManagedLedgerPosition> {
        let mut next_info = self.info.clone();
        if next_info.current_ledger_is_full(self.max_entries_per_ledger) {
            let next_ledger_id = Self::allocate_ledger_id(&self.db)?;
            next_info.roll_over_current_ledger(next_ledger_id);
        }
        let current_ledger = next_info.current_ledger_mut();
        let position = ManagedLedgerPosition {
            ledger_id: current_ledger.ledger_id,
            entry_id: current_ledger.entries,
            partition,
        };
        current_ledger.entries += 1;
        current_ledger.size += metadata.len() as u64 + payload.len() as u64;
        if next_info.current_ledger_is_full(self.max_entries_per_ledger) {
            let next_ledger_id = Self::allocate_ledger_id(&self.db)?;
            next_info.roll_over_current_ledger(next_ledger_id);
        }

        let entry_index = self.entry_log.append_with_metadata(
            position.ledger_id,
            position.entry_id,
            partition,
            metadata,
            payload,
        )?;
        let stored_entry_location = StoredEntryLocation::from(entry_index.clone());

        let mut batch = WriteBatch::default();
        batch.put(
            keys::managed_entry_key(position.ledger_id, position.entry_id),
            bincode::serialize(&stored_entry_location)?,
        );
        batch.put(
            keys::managed_ledger_key(&self.name),
            next_info.encode_to_vec(),
        );
        self.db.write(batch)?;

        self.info = next_info;
        self.entries.push((position.clone(), entry_index));

        Ok(position)
    }

    #[allow(dead_code)]
    pub fn ledger_info(&self) -> &StoredManagedLedgerInfo {
        &self.info
    }

    /// Position immediately before `position` in ledger/entry order.
    /// - entry_id > 0  -> same ledger, entry_id - 1
    /// - entry_id == 0 -> last entry of the previous non-empty ledger
    /// - no previous   -> None ("before first entry", i.e. seek to earliest)
    pub fn previous_position(
        &self,
        position: &ManagedLedgerPosition,
    ) -> Option<ManagedLedgerPosition> {
        if position.entry_id > 0 {
            return Some(ManagedLedgerPosition {
                ledger_id: position.ledger_id,
                entry_id: position.entry_id - 1,
                partition: position.partition,
            });
        }
        let prev = self
            .info
            .ledgers
            .iter()
            .filter(|l| l.ledger_id < position.ledger_id && l.entries > 0)
            .max_by_key(|l| l.ledger_id)?;
        Some(ManagedLedgerPosition {
            ledger_id: prev.ledger_id,
            entry_id: prev.entries - 1,
            partition: position.partition,
        })
    }

    pub fn get_message_by_id(&self, message_id: &MessageId) -> Option<(MessageId, Vec<u8>)> {
        self.get_message_entry_by_id(message_id)
            .map(|entry| (entry.message_id, entry.payload))
    }

    pub fn get_message_entry_by_id(&self, message_id: &MessageId) -> Option<StoredMessage> {
        self.entries
            .iter()
            .find(|(position, _)| {
                position.ledger_id == message_id.ledger
                    && position.entry_id == message_id.entry
                    && position.partition == message_id.partition
            })
            .and_then(|(_, index)| {
                self.entry_log.read(index).ok().and_then(|entry| {
                    (entry.partition == message_id.partition).then_some(StoredMessage::new(
                        message_id.clone(),
                        entry.metadata,
                        entry.payload,
                    ))
                })
            })
    }

    pub fn messages(&self) -> Vec<(MessageId, Vec<u8>)> {
        self.message_entries()
            .into_iter()
            .map(|entry| (entry.message_id, entry.payload))
            .collect()
    }

    pub fn message_entries(&self) -> Vec<StoredMessage> {
        self.entries
            .iter()
            .filter_map(|(position, index)| {
                self.entry_log.read(index).ok().and_then(|entry| {
                    (entry.partition == position.partition).then_some(StoredMessage::new(
                        MessageId::from(position),
                        entry.metadata,
                        entry.payload,
                    ))
                })
            })
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

    fn read_entry(&self, position: &ManagedLedgerPosition) -> Option<Vec<u8>> {
        self.entries
            .iter()
            .find(|(stored_position, _)| stored_position == position)
            .and_then(|(_, index)| {
                self.entry_log.read(index).ok().and_then(|entry| {
                    (entry.partition == position.partition).then_some(entry.payload)
                })
            })
    }
}
