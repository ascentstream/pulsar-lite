use super::cursor::{next_position, RocksDBManagedCursor};
use super::entrylog::{EntryIndex, EntryLogStore, EntryRecord, EntryToAppend};
use super::keys;
use super::metadata::{StoredEntryLocation, StoredManagedLedgerInfo};
use anyhow::Result;
use arc_swap::ArcSwap;
use rocksdb::{WriteBatch, DB};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use pulsar_lite_storage_managed_ledger::{
    ManagedLedger, ManagedLedgerConfig, ManagedLedgerPosition, MessageId, StoredMessage,
};

const DEFAULT_MAX_ENTRIES_PER_LEDGER: u64 = 50_000;

/// Assignment cursor for the active ledger segment.
/// Only the managed-ledger write-queue worker updates these fields.
#[derive(Debug)]
struct ManagedLedgerRuntimeState {
    current_ledger_id: AtomicU64,
    current_ledger_entries: AtomicU64,
    current_ledger_size: AtomicU64,
}

impl ManagedLedgerRuntimeState {
    fn from_info(info: &StoredManagedLedgerInfo) -> Self {
        let current = info
            .ledgers
            .last()
            .expect("managed ledger info is initialized");
        Self {
            current_ledger_id: AtomicU64::new(current.ledger_id),
            current_ledger_entries: AtomicU64::new(current.entries),
            current_ledger_size: AtomicU64::new(current.size),
        }
    }
}

#[derive(Debug)]
pub struct RocksDBManagedLedger {
    name: String,
    db: Arc<DB>,
    entry_log: Arc<EntryLogStore>,
    max_entries_per_ledger: u64,

    /// Published ledger metadata; readers load this snapshot.
    info: ArcSwap<StoredManagedLedgerInfo>,
    /// Last durable position readers may observe.
    lac: ArcSwap<Option<ManagedLedgerPosition>>,
    /// Write assignment cursor (single-writer: write-queue worker).
    runtime: ManagedLedgerRuntimeState,
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

        let lac = Self::lac_from_info_and_db(&info, &db)?;
        let runtime = ManagedLedgerRuntimeState::from_info(&info);

        Ok(Self {
            name: name.to_string(),
            db,
            entry_log,
            max_entries_per_ledger,
            info: ArcSwap::from_pointee(info),
            lac: ArcSwap::from_pointee(lac),
            runtime,
        })
    }

    fn lac_from_info_and_db(
        info: &StoredManagedLedgerInfo,
        db: &DB,
    ) -> Result<Option<ManagedLedgerPosition>> {
        match info.ledgers.iter().rev().find(|ledger| ledger.entries > 0) {
            Some(last_non_empty_ledger) => {
                let entry_id = last_non_empty_ledger.entries - 1;
                let Some(value) = db.get(keys::managed_entry_key(
                    last_non_empty_ledger.ledger_id,
                    entry_id,
                ))?
                else {
                    return Ok(None);
                };
                let location: StoredEntryLocation = bincode::deserialize(&value)?;
                Ok(Some(ManagedLedgerPosition {
                    ledger_id: last_non_empty_ledger.ledger_id,
                    entry_id,
                    partition: location.partition,
                }))
            }
            None => Ok(None),
        }
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

    pub fn info_snapshot(&self) -> arc_swap::Guard<Arc<StoredManagedLedgerInfo>> {
        self.info.load()
    }

    pub fn last_position(&self) -> Result<Option<ManagedLedgerPosition>> {
        Ok((**self.lac.load()).clone())
    }

    fn is_visible(
        &self,
        pos: &ManagedLedgerPosition,
        info: &StoredManagedLedgerInfo,
        lac: &ManagedLedgerPosition,
    ) -> bool {
        if pos > lac {
            return false;
        }
        info.ledgers
            .iter()
            .any(|l| l.ledger_id == pos.ledger_id && l.entries > pos.entry_id)
    }

    pub fn read_entries_from(
        &self,
        from: &ManagedLedgerPosition,
        limit: usize,
    ) -> Result<Vec<StoredMessage>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let lac_guard = self.lac.load();
        let Some(lac) = lac_guard.as_ref() else {
            return Ok(Vec::new());
        };
        if from > lac {
            return Ok(Vec::new());
        }

        let info = self.info.load();
        let mut out = Vec::with_capacity(limit.min(64));
        let mut current = Some(from.clone());

        while let Some(pos) = current {
            if out.len() >= limit || pos > *lac {
                break;
            }
            if self.is_visible(&pos, info.as_ref(), lac) {
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
            current = next_position(&pos, info.as_ref());
        }
        Ok(out)
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

    /// Durable batch append. Intended for the write-queue worker only (single writer).
    ///
    /// Order: allocate → entrylog + rocks → publish meta → publish LAC.
    pub(crate) fn add_entries_with_partition_and_metadata(
        &self,
        items: &[(i32, &[u8], &[u8])],
    ) -> Result<Vec<ManagedLedgerPosition>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }

        let mut next_info = (**self.info.load()).clone();
        let mut cur_id = self.runtime.current_ledger_id.load(Ordering::Relaxed);
        let mut cur_entries = self.runtime.current_ledger_entries.load(Ordering::Relaxed);
        let mut cur_size = self.runtime.current_ledger_size.load(Ordering::Relaxed);

        let mut positions = Vec::with_capacity(items.len());
        let mut to_append = Vec::with_capacity(items.len());

        for (partition, metadata, payload) in items {
            if cur_entries >= self.max_entries_per_ledger {
                let next_ledger_id = Self::allocate_ledger_id(&self.db)?;
                next_info.roll_over_current_ledger(next_ledger_id);
                cur_id = next_ledger_id;
                cur_entries = 0;
                cur_size = 0;
            }

            let position = ManagedLedgerPosition {
                ledger_id: cur_id,
                entry_id: cur_entries,
                partition: *partition,
            };

            let current_ledger = next_info.current_ledger_mut();
            let message_size = metadata.len() as u64 + payload.len() as u64;
            current_ledger.entries += 1;
            current_ledger.size += message_size;
            cur_entries += 1;
            cur_size += message_size;

            if cur_entries >= self.max_entries_per_ledger {
                let next_ledger_id = Self::allocate_ledger_id(&self.db)?;
                next_info.roll_over_current_ledger(next_ledger_id);
                cur_id = next_ledger_id;
                cur_entries = 0;
                cur_size = 0;
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

        let last_pos = positions.last().cloned();

        let indices = self.entry_log.append_batch(to_append)?;
        if indices.len() != positions.len() {
            anyhow::bail!(
                "entrylog append_batch size mismatch: got {} indices for {} entries",
                indices.len(),
                positions.len()
            );
        }

        let mut batch = WriteBatch::default();
        for (position, entry_index) in positions.iter().zip(indices) {
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

        self.info.store(Arc::new(next_info));
        self.runtime
            .current_ledger_id
            .store(cur_id, Ordering::Relaxed);
        self.runtime
            .current_ledger_entries
            .store(cur_entries, Ordering::Relaxed);
        self.runtime
            .current_ledger_size
            .store(cur_size, Ordering::Relaxed);
        self.lac.store(Arc::new(last_pos));

        Ok(positions)
    }

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
        let info = self.info.load();
        let prev = info
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

    pub fn open_cursor(&self, name: &str) -> Result<RocksDBManagedCursor> {
        RocksDBManagedCursor::open(&self.name, name, Arc::clone(&self.db))
    }

    pub fn get_message_by_id(&self, message_id: &MessageId) -> Option<(MessageId, Vec<u8>)> {
        self.get_message_entry_by_id(message_id)
            .map(|entry| (entry.message_id, entry.payload))
    }

    pub fn get_message_entry_by_id(&self, message_id: &MessageId) -> Option<StoredMessage> {
        let pos = ManagedLedgerPosition::from(message_id);
        let lac_guard = self.lac.load();
        let lac = lac_guard.as_ref().as_ref()?;
        if &pos > lac {
            return None;
        }
        let info = self.info.load();
        if !self.is_visible(&pos, info.as_ref(), lac) {
            return None;
        }
        let (stored, record) = self
            .read_entry_record(message_id.ledger, message_id.entry)
            .ok()
            .flatten()?;
        if stored.partition != message_id.partition {
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

    pub fn message_entries(&self) -> Result<Vec<StoredMessage>> {
        let info = self.info.load();
        let Some(from) =
            info.ledgers
                .iter()
                .find(|l| l.entries > 0)
                .map(|l| ManagedLedgerPosition {
                    ledger_id: l.ledger_id,
                    entry_id: 0,
                    partition: -1,
                })
        else {
            return Ok(Vec::new());
        };
        let total: usize = info.ledgers.iter().map(|l| l.entries as usize).sum();
        self.read_entries_from(&from, total)
    }
}

impl ManagedLedger for RocksDBManagedLedger {
    type Cursor = RocksDBManagedCursor;

    fn name(&self) -> &str {
        &self.name
    }

    fn add_entry(&mut self, _payload: &[u8]) -> Result<ManagedLedgerPosition> {
        anyhow::bail!(
            "RocksDB managed-ledger appends must go through WriteQueue; direct add_entry is disabled"
        )
    }

    fn open_cursor(&mut self, name: &str) -> Result<Self::Cursor> {
        RocksDBManagedCursor::open(&self.name, name, Arc::clone(&self.db))
    }

    fn read_entry(&self, position: &ManagedLedgerPosition) -> Option<Vec<u8>> {
        let lac_guard = self.lac.load();
        let lac = lac_guard.as_ref().as_ref()?;
        if position > lac {
            return None;
        }
        let info = self.info.load();
        if !self.is_visible(position, info.as_ref(), lac) {
            return None;
        }
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
