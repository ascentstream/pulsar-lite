use super::keys;
use super::ledger::RocksDBManagedLedger;
use anyhow::Result;
use rocksdb::DB;
use std::sync::Arc;

use crate::storage::{rocksdb::entrylog::EntryLogStore, ManagedLedgerConfig, ManagedLedgerFactory};

#[derive(Debug, Clone)]
pub(super) struct RocksDBManagedLedgerFactory {
    db: Arc<DB>,
    entry_log: Arc<EntryLogStore>,
}

impl RocksDBManagedLedgerFactory {
    pub(super) fn new(db: Arc<DB>, entry_log: Arc<EntryLogStore>) -> Self {
        Self { db, entry_log }
    }

    pub(super) fn open_ledger(&self, name: &str) -> Result<RocksDBManagedLedger> {
        RocksDBManagedLedger::open(name, Arc::clone(&self.db), Arc::clone(&self.entry_log))
    }

    pub(super) fn cursor_state_exists(&self, ledger_name: &str, cursor_name: &str) -> Result<bool> {
        Ok(self
            .db
            .get(keys::managed_cursor_key(ledger_name, cursor_name))?
            .is_some())
    }

    pub(super) fn delete_cursor_state(&self, ledger_name: &str, cursor_name: &str) -> Result<()> {
        self.db
            .delete(keys::managed_cursor_key(ledger_name, cursor_name))?;
        Ok(())
    }

    fn open_ledger_with_config(
        &self,
        name: &str,
        config: &ManagedLedgerConfig,
    ) -> Result<RocksDBManagedLedger> {
        RocksDBManagedLedger::open_with_config(
            name,
            Arc::clone(&self.db),
            Arc::clone(&self.entry_log),
            config,
        )
    }
}

impl ManagedLedgerFactory for RocksDBManagedLedgerFactory {
    type Ledger = RocksDBManagedLedger;

    fn open(&mut self, name: &str, config: &ManagedLedgerConfig) -> Result<Self::Ledger> {
        self.open_ledger_with_config(name, config)
    }
}
