use crate::storage::{ManagedCursorState, ManagedLedgerPosition};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct StoredEntry {
    pub(super) partition: i32,
    pub(super) payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct StoredManagedCursorState {
    pub(super) mark_delete: Option<ManagedLedgerPosition>,
    pub(super) individually_deleted_entries: BTreeSet<ManagedLedgerPosition>,
}

impl From<ManagedCursorState> for StoredManagedCursorState {
    fn from(value: ManagedCursorState) -> Self {
        Self {
            mark_delete: value.mark_delete,
            individually_deleted_entries: value.individually_deleted_entries,
        }
    }
}

impl From<StoredManagedCursorState> for ManagedCursorState {
    fn from(value: StoredManagedCursorState) -> Self {
        Self {
            mark_delete: value.mark_delete,
            individually_deleted_entries: value.individually_deleted_entries,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct StoredLedgerInfo {
    pub(super) ledger_id: u64,
    pub(super) entries: u64,
    pub(super) size: u64,
    pub(super) timestamp: u64,
    pub(super) is_offloaded: bool,
    pub(super) offloaded_context_uuid: Option<String>,
}

impl StoredLedgerInfo {
    fn new(ledger_id: u64) -> Self {
        Self {
            ledger_id,
            entries: 0,
            size: 0,
            timestamp: 0,
            is_offloaded: false,
            offloaded_context_uuid: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct StoredManagedLedgerInfo {
    pub(super) ledgers: Vec<StoredLedgerInfo>,
}

impl StoredManagedLedgerInfo {
    pub(super) fn new() -> Self {
        Self {
            ledgers: vec![StoredLedgerInfo::new(0)],
        }
    }

    pub(super) fn current_ledger_mut(&mut self) -> &mut StoredLedgerInfo {
        if self.ledgers.is_empty() {
            self.ledgers.push(StoredLedgerInfo::new(0));
        }
        self.ledgers.last_mut().expect("ledger info is initialized")
    }

    pub(super) fn ensure_writable_ledger(&mut self, max_entries_per_ledger: u64) {
        let current_ledger = self.current_ledger_mut();
        if current_ledger.entries >= max_entries_per_ledger {
            let next_ledger_id = current_ledger.ledger_id + 1;
            self.ledgers.push(StoredLedgerInfo::new(next_ledger_id));
        }
    }

    pub(super) fn roll_over_current_ledger_if_full(&mut self, max_entries_per_ledger: u64) {
        let current_ledger = self.current_ledger_mut();
        if current_ledger.entries >= max_entries_per_ledger {
            current_ledger.timestamp = current_time_millis();
            let next_ledger_id = current_ledger.ledger_id + 1;
            self.ledgers.push(StoredLedgerInfo::new(next_ledger_id));
        }
    }
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}
