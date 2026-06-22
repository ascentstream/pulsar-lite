use super::entrylog::EntryIndex;
use crate::storage::{ManagedCursorState, ManagedLedgerPosition};
use anyhow::{anyhow, Result};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) mod proto {
    #![allow(dead_code)]

    include!(concat!(env!("OUT_DIR"), "/mledger.proto.rs"));
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct StoredEntryLocation {
    pub(super) file_id: u64,
    pub(super) offset: u64,
    pub(super) len: u64,
    pub(super) checksum: u64,
    pub(super) partition: i32,
}

impl From<EntryIndex> for StoredEntryLocation {
    fn from(value: EntryIndex) -> Self {
        Self {
            file_id: value.file_id,
            offset: value.offset,
            len: value.len,
            checksum: value.checksum,
            partition: value.partition,
        }
    }
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

impl StoredManagedCursorState {
    pub(super) fn encode_to_vec(&self) -> Vec<u8> {
        let mark_delete = self.mark_delete.as_ref();
        #[allow(deprecated)]
        let info = proto::ManagedCursorInfo {
            cursors_ledger_id: -1,
            mark_delete_ledger_id: mark_delete.map(|position| position.ledger_id as i64),
            mark_delete_entry_id: mark_delete.map(|position| position.entry_id as i64),
            individual_deleted_messages: self
                .individually_deleted_entries
                .iter()
                .map(position_range)
                .collect(),
            properties: Vec::new(),
            last_active: None,
            batched_entry_deletion_index_info: Vec::new(),
            cursor_properties: Vec::new(),
        };
        info.encode_to_vec()
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
        let info = proto::ManagedCursorInfo::decode(bytes)?;
        let mark_delete = match (info.mark_delete_ledger_id, info.mark_delete_entry_id) {
            (Some(ledger_id), Some(entry_id)) => Some(ManagedLedgerPosition {
                ledger_id: ledger_id.try_into()?,
                entry_id: entry_id.try_into()?,
                partition: -1,
            }),
            _ => None,
        };
        let mut individually_deleted_entries = BTreeSet::new();

        for range in info.individual_deleted_messages {
            let lower = range.lower_endpoint;
            let upper = range.upper_endpoint;
            if lower.ledger_id != upper.ledger_id || lower.entry_id != upper.entry_id {
                return Err(anyhow!(
                    "cursor delete range spans multiple positions, which pulsar-lite does not support yet"
                ));
            }
            individually_deleted_entries.insert(ManagedLedgerPosition {
                ledger_id: lower.ledger_id.try_into()?,
                entry_id: lower.entry_id.try_into()?,
                partition: -1,
            });
        }

        Ok(Self {
            mark_delete,
            individually_deleted_entries,
        })
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
    pub(super) fn new(ledger_id: u64) -> Self {
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
    pub(super) fn new(ledger_id: u64) -> Self {
        Self {
            ledgers: vec![StoredLedgerInfo::new(ledger_id)],
        }
    }

    pub(super) fn current_ledger_mut(&mut self) -> &mut StoredLedgerInfo {
        self.ledgers.last_mut().expect("ledger info is initialized")
    }

    pub(super) fn ensure_initialized(&mut self, ledger_id: u64) {
        if self.ledgers.is_empty() {
            self.ledgers.push(StoredLedgerInfo::new(ledger_id));
        }
    }

    pub(super) fn current_ledger_is_full(&mut self, max_entries_per_ledger: u64) -> bool {
        self.current_ledger_mut().entries >= max_entries_per_ledger
    }

    pub(super) fn roll_over_current_ledger(&mut self, next_ledger_id: u64) {
        let current_ledger = self.current_ledger_mut();
        current_ledger.timestamp = current_time_millis();
        self.ledgers.push(StoredLedgerInfo::new(next_ledger_id));
    }

    pub(super) fn encode_to_vec(&self) -> Vec<u8> {
        let info = proto::ManagedLedgerInfo {
            ledger_info: self
                .ledgers
                .iter()
                .map(|ledger| proto::managed_ledger_info::LedgerInfo {
                    ledger_id: ledger.ledger_id as i64,
                    entries: Some(ledger.entries as i64),
                    size: Some(ledger.size as i64),
                    timestamp: Some(ledger.timestamp as i64),
                    properties: Vec::new(),
                })
                .collect(),
            terminated_position: None,
            properties: Vec::new(),
        };
        info.encode_to_vec()
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
        let info = proto::ManagedLedgerInfo::decode(bytes)?;
        let ledgers = info
            .ledger_info
            .into_iter()
            .map(|ledger| {
                Ok(StoredLedgerInfo {
                    ledger_id: ledger.ledger_id.try_into()?,
                    entries: ledger.entries.unwrap_or_default().try_into()?,
                    size: ledger.size.unwrap_or_default().try_into()?,
                    timestamp: ledger.timestamp.unwrap_or_default().try_into()?,
                    is_offloaded: false,
                    offloaded_context_uuid: None,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { ledgers })
    }
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn position_range(position: &ManagedLedgerPosition) -> proto::MessageRange {
    let endpoint = proto::NestedPositionInfo {
        ledger_id: position.ledger_id as i64,
        entry_id: position.entry_id as i64,
    };
    proto::MessageRange {
        lower_endpoint: endpoint,
        upper_endpoint: endpoint,
    }
}
