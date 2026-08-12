use super::keys;
use super::metadata::{StoredManagedCursorState, StoredManagedLedgerInfo};
use anyhow::Result;
use rocksdb::DB;
use std::sync::Arc;

use pulsar_lite_storage_managed_ledger::{
    ManagedCursor, ManagedCursorState, ManagedLedgerPosition,
};

fn cursor_position(position: &ManagedLedgerPosition) -> ManagedLedgerPosition {
    ManagedLedgerPosition {
        ledger_id: position.ledger_id,
        entry_id: position.entry_id,
        partition: -1,
    }
}

#[derive(Debug, Clone)]
pub struct RocksDBManagedCursor {
    managedledger_name: String,
    name: String,
    db: Arc<DB>,
    state: ManagedCursorState,
}

impl RocksDBManagedCursor {
    pub fn open(managedledger_name: &str, name: &str, db: Arc<DB>) -> Result<Self> {
        let key = keys::managed_cursor_key(managedledger_name, name);
        let state = db
            .get(key)?
            .map(|bytes| StoredManagedCursorState::decode(&bytes))
            .transpose()?
            .map(ManagedCursorState::from)
            .unwrap_or_default();

        Ok(Self {
            managedledger_name: managedledger_name.to_string(),
            name: name.to_string(),
            db,
            state,
        })
    }

    pub fn persist_state(&self) -> Result<()> {
        // [TEMP DIAG] every 100k persists, append cursor state to a CSV file
        // (bypasses the logging system entirely) to verify whether the
        // individual-delete range set accumulates while mark_delete stalls.
        use std::sync::atomic::{AtomicU64, Ordering};
        static DIAG_COUNT: AtomicU64 = AtomicU64::new(0);
        let n = DIAG_COUNT.fetch_add(1, Ordering::Relaxed);
        if n % 100_000 == 0 {
            use std::io::Write;
            let mark_delete = self
                .state
                .mark_delete
                .as_ref()
                .map(|p| format!("{}:{}", p.ledger_id, p.entry_id))
                .unwrap_or_else(|| "None".to_string());
            let line = format!(
                "{},\t{},\t{},\t{}\n",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
                self.name,
                mark_delete,
                self.state.individually_deleted_entries.len()
            );
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open("/data/cursor_diag.csv")
            {
                let _ = f.write_all(line.as_bytes());
            }
        }
        let key = keys::managed_cursor_key(&self.managedledger_name, &self.name);
        let stored = StoredManagedCursorState::from(self.state.clone());
        self.db.put(key, stored.encode_to_vec())?;
        Ok(())
    }
}

impl ManagedCursor for RocksDBManagedCursor {
    fn name(&self) -> &str {
        &self.name
    }

    fn state(&self) -> &ManagedCursorState {
        &self.state
    }

    /// Marks a position as deleted in the cursor state.
    ///
    /// The cursor does not need to pay attention to the messages regarding partitioning,
    /// so uniformity has been achieved here.
    /// ManagedLedgerPosition { ledger_id: 1, entry_id: 10, partition: 0 } -> ManagedLedgerPosition { ledger_id: 1, entry_id: 10, partition: -1 }
    fn mark_delete(&mut self, position: ManagedLedgerPosition) -> Result<()> {
        self.state.mark_delete = Some(cursor_position(&position));
        self.persist_state()
    }

    fn delete_individual(&mut self, position: ManagedLedgerPosition) -> Result<()> {
        let position = cursor_position(&position);
        self.state.individually_deleted_entries.insert(position);
        self.persist_state()
    }

    fn reset_cursor(&mut self, position: Option<ManagedLedgerPosition>) -> Result<()> {
        self.state.mark_delete = position.as_ref().map(cursor_position);
        self.state.individually_deleted_entries.clear();
        self.persist_state()
    }
}

pub fn is_managed_position_acknowledged(
    cursor: &ManagedCursorState,
    position: &ManagedLedgerPosition,
) -> bool {
    let position = cursor_position(position);

    cursor
        .mark_delete
        .as_ref()
        .is_some_and(|mark_delete| &position <= mark_delete)
        || cursor.individually_deleted_entries.contains(&position)
}

pub(crate) fn first_position(
    info: &StoredManagedLedgerInfo,
    partition: i32,
) -> Option<ManagedLedgerPosition> {
    info.ledgers
        .iter()
        .find(|ledger| ledger.entries > 0)
        .map(|ledger| ManagedLedgerPosition {
            ledger_id: ledger.ledger_id,
            entry_id: 0,
            partition,
        })
}

pub fn next_position(
    position: &ManagedLedgerPosition,
    info: &StoredManagedLedgerInfo,
) -> Option<ManagedLedgerPosition> {
    let current_ledger = info
        .ledgers
        .iter()
        .find(|ledger| ledger.ledger_id == position.ledger_id)?;

    if position.entry_id + 1 < current_ledger.entries {
        return Some(ManagedLedgerPosition {
            ledger_id: position.ledger_id,
            entry_id: position.entry_id + 1,
            partition: position.partition,
        });
    }

    info.ledgers
        .iter()
        .find(|ledger| ledger.ledger_id > position.ledger_id && ledger.entries > 0)
        .map(|ledger| ManagedLedgerPosition {
            ledger_id: ledger.ledger_id,
            entry_id: 0,
            partition: position.partition,
        })
}

pub fn ack_managed_cursor_shared(
    cursor: &mut RocksDBManagedCursor,
    position: ManagedLedgerPosition,
    info: &StoredManagedLedgerInfo,
) -> Result<()> {
    let position = cursor_position(&position);
    if is_managed_position_acknowledged(cursor.state(), &position) {
        return Ok(());
    }

    match cursor.state().mark_delete.as_ref() {
        None if Some(position.clone()) == first_position(info, position.partition) => {
            cursor.mark_delete(position)?
        }
        None => cursor.delete_individual(position)?,
        Some(mark_delete) if Some(position.clone()) == next_position(mark_delete, info) => {
            cursor.mark_delete(position)?
        }
        Some(mark_delete) if position > *mark_delete => cursor.delete_individual(position)?,
        Some(_) => {}
    }

    // Advance the mark-delete frontier across contiguous acknowledged ranges.
    // `take_covering` consumes a whole coalesced range in one step.
    loop {
        let Some(mark_delete) = cursor.state().mark_delete.clone() else {
            break;
        };
        let Some(next) = next_position(&mark_delete, info) else {
            break;
        };
        match cursor
            .state
            .individually_deleted_entries
            .take_covering(&next)
        {
            Some(end) => cursor.mark_delete(end)?,
            None => break,
        }
    }

    Ok(())
}
