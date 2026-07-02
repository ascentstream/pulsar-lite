use crate::config::ManagedLedgerConfig;
use crate::ledger::ManagedLedger;
use anyhow::Result;

/// Factory abstraction for opening managed ledgers.
pub trait ManagedLedgerFactory: Send + Sync {
    type Ledger: ManagedLedger;

    fn open(&mut self, name: &str, config: &ManagedLedgerConfig) -> Result<Self::Ledger>;
}
