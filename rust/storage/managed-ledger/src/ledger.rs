use crate::cursor::ManagedCursor;
use crate::position::ManagedLedgerPosition;
use anyhow::Result;

/// Managed-ledger abstraction for durable append-only message storage.
pub trait ManagedLedger: Send + Sync {
    type Cursor: ManagedCursor;

    fn name(&self) -> &str;

    fn add_entry(&mut self, payload: &[u8]) -> Result<ManagedLedgerPosition>;

    fn open_cursor(&mut self, name: &str) -> Result<Self::Cursor>;

    fn read_entry(&self, position: &ManagedLedgerPosition) -> Option<Vec<u8>>;
}
