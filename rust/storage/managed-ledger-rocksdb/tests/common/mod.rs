//! Shared helpers for RocksDB managed-ledger integration tests.
#![allow(dead_code)]

use pulsar_lite_storage_managed_ledger::ManagedLedgerPosition;
use pulsar_lite_storage_managed_ledger_rocksdb::test_support::{
    keys, EntryLogStore, StoredManagedLedgerInfo,
};
use rocksdb::{Options, DB};
use std::path::Path;
use std::sync::Arc;

pub fn open_test_db(path: &Path) -> Arc<DB> {
    let mut options = Options::default();
    options.create_if_missing(true);
    Arc::new(DB::open(&options, path).unwrap())
}

pub fn open_test_entry_log(path: &Path) -> Arc<EntryLogStore> {
    Arc::new(EntryLogStore::open(path).unwrap())
}

pub fn position(ledger_id: u64, entry_id: u64) -> ManagedLedgerPosition {
    ManagedLedgerPosition {
        ledger_id,
        entry_id,
        partition: -1,
    }
}

pub fn read_managed_ledger_info(db: &DB, ledger_name: &str) -> StoredManagedLedgerInfo {
    let bytes = db
        .get(keys::managed_ledger_key(ledger_name))
        .unwrap()
        .expect("managed ledger info should exist");
    StoredManagedLedgerInfo::decode(&bytes).unwrap()
}

pub fn read_raw_value(db: &DB, key: Vec<u8>) -> Vec<u8> {
    db.get(key).unwrap().expect("value should exist").to_vec()
}
