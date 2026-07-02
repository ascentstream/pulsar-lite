use super::keys;
use super::metadata::{StoredManagedCursorState, StoredManagedLedgerInfo};
use anyhow::Result;
use rocksdb::DB;
use std::sync::Arc;

use pulsar_lite_storage_managed_ledger::{ManagedCursor, ManagedCursorState, ManagedLedgerPosition};

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

    fn mark_delete(&mut self, position: ManagedLedgerPosition) -> Result<()> {
        self.state.mark_delete = Some(position);
        self.persist_state()
    }

    fn delete_individual(&mut self, position: ManagedLedgerPosition) -> Result<()> {
        self.state.individually_deleted_entries.insert(position);
        self.persist_state()
    }

    async fn async_reset_cursor(&mut self, position: Option<ManagedLedgerPosition>) -> Result<()> {
        self.state.mark_delete = position;
        self.state.individually_deleted_entries.clear();
        self.persist_state()
    }
}

pub fn is_managed_position_acknowledged(
    cursor: &ManagedCursorState,
    position: &ManagedLedgerPosition,
) -> bool {
    cursor
        .mark_delete
        .as_ref()
        .is_some_and(|mark_delete| position <= mark_delete)
        || cursor.individually_deleted_entries.contains(position)
}

fn first_position(info: &StoredManagedLedgerInfo, partition: i32) -> Option<ManagedLedgerPosition> {
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

    while let Some(mark_delete) = cursor.state().mark_delete.clone() {
        let Some(next) = next_position(&mark_delete, info) else {
            break;
        };
        if cursor.state.individually_deleted_entries.remove(&next) {
            cursor.mark_delete(next)?;
        } else {
            break;
        }
    }

    Ok(())
}
