use super::cursor::{next_position, RocksDBManagedCursor};
use super::entrylog::{EntryIndex, EntryLogStore, EntryRecord, EntryToAppend};
use super::keys;
use super::metadata::{StoredEntryLocation, StoredManagedLedgerInfo};
use anyhow::{Ok, Result};
use rocksdb::{WriteBatch, DB};
use std::sync::Arc;

use pulsar_lite_storage_managed_ledger::{
    ManagedLedger, ManagedLedgerConfig, ManagedLedgerPosition, MessageId, StoredMessage,
};

const DEFAULT_MAX_ENTRIES_PER_LEDGER: u64 = 50_000;

/// Status of the ManagedLedger that currently working on
#[derive(Debug, Clone)]
struct ManagedLedgerRuntimeState {
    /// The current ledger ID that the ManagedLedger is working on.
    current_ledger_id: u64,

    /// The number of entries in the current ledger.
    current_ledger_entries: u64,

    /// The size of the current ledger in bytes.
    current_ledger_size: u64,

    /// The last confirmed position in the entire managed ledger.
    last_confirmed_position: Option<ManagedLedgerPosition>,
}

#[derive(Debug, Clone)]
pub struct RocksDBManagedLedger {
    name: String,
    db: Arc<DB>,
    pub info: StoredManagedLedgerInfo,
    runtime: ManagedLedgerRuntimeState,
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

        let runtime = Self::runtime_from_info(&info, &db)?;

        Ok(Self {
            name: name.to_string(),
            db,
            entry_log,
            info,
            runtime,
            max_entries_per_ledger,
        })
    }

    fn runtime_from_info(
        info: &StoredManagedLedgerInfo,
        db: &DB,
    ) -> Result<ManagedLedgerRuntimeState> {
        let current_ledger = info
            .ledgers
            .last()
            .expect("managed ledger info is initialized");

        let last_confirmed_position =
            match info.ledgers.iter().rev().find(|ledger| ledger.entries > 0) {
                Some(last_non_empty_ledger) => {
                    let entry_id = last_non_empty_ledger.entries - 1;

                    let Some(value) = db.get(keys::managed_entry_key(
                        last_non_empty_ledger.ledger_id,
                        entry_id,
                    ))?
                    else {
                        return Ok(ManagedLedgerRuntimeState {
                            current_ledger_id: current_ledger.ledger_id,
                            current_ledger_entries: current_ledger.entries,
                            current_ledger_size: current_ledger.size,
                            last_confirmed_position: None,
                        });
                    };

                    let location: StoredEntryLocation = bincode::deserialize(&value)?;
                    Some(ManagedLedgerPosition {
                        ledger_id: last_non_empty_ledger.ledger_id,
                        entry_id,
                        partition: location.partition,
                    })
                }
                None => None,
            };

        Ok(ManagedLedgerRuntimeState {
            current_ledger_id: current_ledger.ledger_id,
            current_ledger_entries: current_ledger.entries,
            current_ledger_size: current_ledger.size,
            last_confirmed_position,
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

    fn load_entry_index(&self, ledger_id: u64, entry_id: u64) -> Result<Option<EntryIndex>> {
        let Some(value) = self.db.get(keys::managed_entry_key(ledger_id, entry_id))? else {
            return Ok(None);
        };
        let location: StoredEntryLocation = bincode::deserialize(&value)?;
        Ok(Some(EntryIndex {
            ledger_id,
            entry_id,
            file_id: location.file_id,
            offset: location.offset,
            len: location.len,
            checksum: location.checksum,
            partition: location.partition,
        }))
    }

    fn read_entry_record(
        &self,
        ledger_id: u64,
        entry_id: u64,
    ) -> Result<Option<(ManagedLedgerPosition, EntryRecord)>> {
        let Some(index) = self.load_entry_index(ledger_id, entry_id)? else {
            return Ok(None);
        };
        let position = ManagedLedgerPosition {
            ledger_id,
            entry_id,
            partition: index.partition,
        };
        let record = self.entry_log.read(&index)?;
        Ok(Some((position, record)))
    }

    pub fn last_position(&self) -> Result<Option<ManagedLedgerPosition>> {
        Ok(self.runtime.last_confirmed_position.clone())
    }

    /// Returns true if the given position is visible in the ledger.
    fn is_visible(&self, pos: &ManagedLedgerPosition) -> bool {
        self.info
            .ledgers
            .iter()
            .any(|l| l.ledger_id == pos.ledger_id && l.entries > pos.entry_id)
    }

    /// Reads a batch of entries starting from the given position, up to the specified limit.
    pub fn read_entries_from(
        &self,
        from: &ManagedLedgerPosition,
        limit: usize,
    ) -> Result<Vec<StoredMessage>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(limit.min(64));
        let mut current = Some(from.clone());

        while let Some(pos) = current {
            if out.len() >= limit {
                break;
            }

            if self.is_visible(&pos) {
                if let Some((stored, record)) =
                    self.read_entry_record(pos.ledger_id, pos.entry_id)?
                {
                    out.push(StoredMessage {
                        message_id: MessageId::from(&stored),
                        metadata: record.metadata,
                        payload: record.payload,
                    });
                }
            }
            current = next_position(&pos, &self.info);
        }

        Ok(out)
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
        let mut positions = self.add_entries_with_partition_and_metadata(&[(partition,metadata,payload)])?;
        positions.pop().ok_or_else(|| anyhow::anyhow!("add_entries returned empty positions"))
    }

    /// Append many entries with one entrylog flush and one RocksDB WriteBatch.
    ///
    /// - entry_id assignment stays serial (same as single append)
    /// - entrylog is written once via `append_batch` (one write_all + flush)
    /// - entry locations + final managed-ledger info are written once
    /// - runtime state is published only after both durable steps succeed
    pub fn add_entries_with_partition_and_metadata(
        &mut self,
        items: &[(i32, &[u8], &[u8])],
    ) -> Result<Vec<ManagedLedgerPosition>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }

        // Pass 1: allocate positions and next ledger/runtime state in memory.
        let mut next_info = self.info.clone();
        let mut next_runtime = self.runtime.clone();
        let mut positions = Vec::with_capacity(items.len());
        let mut to_append = Vec::with_capacity(items.len());

        for (partition, metadata, payload) in items {
            if next_runtime.current_ledger_entries >= self.max_entries_per_ledger {
                let next_ledger_id = Self::allocate_ledger_id(&self.db)?;
                next_info.roll_over_current_ledger(next_ledger_id);
                next_runtime.current_ledger_id = next_ledger_id;
                next_runtime.current_ledger_entries = 0;
                next_runtime.current_ledger_size = 0;
            }

            let position = ManagedLedgerPosition {
                ledger_id: next_runtime.current_ledger_id,
                entry_id: next_runtime.current_ledger_entries,
                partition: *partition,
            };

            let current_ledger = next_info.current_ledger_mut();
            let message_size = metadata.len() as u64 + payload.len() as u64;
            current_ledger.entries += 1;
            current_ledger.size += message_size;
            next_runtime.current_ledger_entries += 1;
            next_runtime.current_ledger_size += message_size;
            next_runtime.last_confirmed_position = Some(position.clone());

            if next_runtime.current_ledger_entries >= self.max_entries_per_ledger {
                let next_ledger_id = Self::allocate_ledger_id(&self.db)?;
                next_info.roll_over_current_ledger(next_ledger_id);
                next_runtime.current_ledger_entries = 0;
                next_runtime.current_ledger_size = 0;
                next_runtime.current_ledger_id = next_ledger_id;
            }

            to_append.push(EntryToAppend {
                ledger_id: position.ledger_id,
                entry_id: position.entry_id,
                partition: *partition,
                metadata: metadata.to_vec(),
                payload: payload.to_vec(),
            });
            positions.push(position);
        }

        // Pass 2: one entrylog IO for the whole batch.
        let indices = self.entry_log.append_batch(to_append)?;
        if indices.len() != positions.len() {
            anyhow::bail!(
                "entrylog append_batch size mismatch: got {} indices for {} entries",
                indices.len(),
                positions.len()
            );
        }

        // Pass 3: one RocksDB WriteBatch for locations + ledger info.
        let mut batch = WriteBatch::default();
        for (position, entry_index) in positions.iter().zip(indices.into_iter()) {
            let stored_entry_location = StoredEntryLocation::from(entry_index);
            batch.put(
                keys::managed_entry_key(position.ledger_id, position.entry_id),
                bincode::serialize(&stored_entry_location)?,
            );
        }
        batch.put(
            keys::managed_ledger_key(&self.name),
            next_info.encode_to_vec(),
        );
        self.db.write(batch)?;

        // Pass 4: publish in-memory state only after durable writes succeed.
        self.info = next_info;
        self.runtime = next_runtime;
        Ok(positions)
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
        let (position, record) = self
            .read_entry_record(message_id.ledger, message_id.entry)
            .ok()
            .flatten()?;
        if position.partition != message_id.partition {
            return None;
        }
        Some(StoredMessage::new(
            message_id.clone(),
            record.metadata,
            record.payload,
        ))
    }

    pub fn messages(&self) -> Result<Vec<(MessageId, Vec<u8>)>> {
        Ok(self
            .message_entries()?
            .into_iter()
            .map(|entry| (entry.message_id, entry.payload))
            .collect())
    }

    /// TODO: A temporary global scanning interface. Subsequently, all related global scanning codes will be gradually migrated.
    pub fn message_entries(&self) -> Result<Vec<StoredMessage>> {
        let Some(from) =
            self.info
                .ledgers
                .iter()
                .find(|l| l.entries > 0)
                .map(|l| ManagedLedgerPosition {
                    ledger_id: l.ledger_id,
                    entry_id: 0,
                    partition: -1,
                })
        else {
            return Ok(vec![]);
        };

        let total = self
            .info
            .ledgers
            .iter()
            .map(|l| l.entries as usize)
            .sum::<usize>();

        self.read_entries_from(&from, total)
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
        let (stored, record) = self
            .read_entry_record(position.ledger_id, position.entry_id)
            .ok()
            .flatten()?;
        if stored.partition != position.partition {
            return None;
        }
        Some(record.payload)
    }
}
