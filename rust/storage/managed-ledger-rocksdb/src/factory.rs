use super::keys;
use super::ledger::RocksDBManagedLedger;
use anyhow::{anyhow, Result};
use rocksdb::DB;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use crate::entrylog::EntryLogStore;
use pulsar_lite_storage_managed_ledger::{ManagedLedgerConfig, ManagedLedgerFactory};

pub(crate) type SharedLedger = Arc<Mutex<RocksDBManagedLedger>>;
type LedgerCache = HashMap<String, SharedLedger>;

#[derive(Debug, Clone)]
pub struct RocksDBManagedLedgerFactory {
    db: Arc<DB>,
    entry_log: Arc<EntryLogStore>,
    ledgers: Arc<Mutex<LedgerCache>>,
}

impl RocksDBManagedLedgerFactory {
    pub fn new(db: Arc<DB>, entry_log: Arc<EntryLogStore>) -> Self {
        Self {
            db,
            entry_log,
            ledgers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn load_ledger(
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

    fn get_or_open_ledger_with_config(
        &self,
        name: &str,
        config: &ManagedLedgerConfig,
    ) -> Result<SharedLedger> {
        let mut ledgers = self
            .ledgers
            .lock()
            .map_err(|_| anyhow!("managed ledger cache lock poisoned"))?;

        if let Some(ledger) = ledgers.get(name) {
            return Ok(Arc::clone(ledger));
        }

        let ledger = Arc::new(Mutex::new(self.load_ledger(name, config)?));
        ledgers.insert(name.to_string(), Arc::clone(&ledger));

        Ok(ledger)
    }

    pub fn open_ledger(&self, name: &str) -> Result<SharedLedger> {
        self.get_or_open_ledger_with_config(name, &ManagedLedgerConfig::default())
    }

    pub(crate) fn open_owned_ledger(&self, name: &str) -> Result<RocksDBManagedLedger> {
        self.load_ledger(name, &ManagedLedgerConfig::default())
    }

    pub fn cursor_state_exists(&self, ledger_name: &str, cursor_name: &str) -> Result<bool> {
        Ok(self
            .db
            .get(keys::managed_cursor_key(ledger_name, cursor_name))?
            .is_some())
    }

    pub fn delete_cursor_state(&self, ledger_name: &str, cursor_name: &str) -> Result<()> {
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
