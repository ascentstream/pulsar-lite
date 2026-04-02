use super::{ManagedLedger, ManagedLedgerConfig};
use anyhow::Result;

/// Factory abstraction for opening managed ledgers.
pub trait ManagedLedgerFactory: Send + Sync {
    type Ledger: ManagedLedger;

    fn open(&mut self, name: &str, config: &ManagedLedgerConfig) -> Result<Self::Ledger>;
}
