use super::ManagedLedgerPosition;
use anyhow::Result;
use std::collections::BTreeSet;

/// Managed-cursor state skeleton.
///
/// This mirrors the shape of the current shared-subscription cursor model and
/// gives future durable cursor implementations a stable target type.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManagedCursorState {
    pub mark_delete: Option<ManagedLedgerPosition>,
    pub individually_deleted_entries: BTreeSet<ManagedLedgerPosition>,
}

/// Cursor abstraction for managed-ledger style persistence.
pub trait ManagedCursor: Send + Sync {
    fn name(&self) -> &str;

    fn state(&self) -> &ManagedCursorState;

    fn mark_delete(&mut self, position: ManagedLedgerPosition) -> Result<()>;

    fn delete_individual(&mut self, position: ManagedLedgerPosition) -> Result<()>;
}
