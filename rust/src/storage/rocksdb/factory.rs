use super::ledger::RocksDBManagedLedger;
use anyhow::Result;
use rocksdb::DB;
use std::sync::Arc;

use crate::storage::{ManagedLedgerConfig, ManagedLedgerFactory};

#[derive(Debug, Clone)]
pub(super) struct RocksDBManagedLedgerFactory {
    db: Arc<DB>,
}

impl RocksDBManagedLedgerFactory {
    pub(super) fn new(db: Arc<DB>) -> Self {
        Self { db }
    }

    pub(super) fn open_ledger(&self, name: &str) -> Result<RocksDBManagedLedger> {
        RocksDBManagedLedger::open(name, Arc::clone(&self.db))
    }

    fn open_ledger_with_config(
        &self,
        name: &str,
        config: &ManagedLedgerConfig,
    ) -> Result<RocksDBManagedLedger> {
        RocksDBManagedLedger::open_with_config(name, Arc::clone(&self.db), config)
    }
}

impl ManagedLedgerFactory for RocksDBManagedLedgerFactory {
    type Ledger = RocksDBManagedLedger;

    fn open(&mut self, name: &str, config: &ManagedLedgerConfig) -> Result<Self::Ledger> {
        self.open_ledger_with_config(name, config)
    }
}
