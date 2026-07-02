//! RocksDB managed-cursor persistence and shared-ack tests.

mod common;

use common::*;
use pulsar_lite_storage_managed_ledger::{ManagedCursor, ManagedLedger};
use pulsar_lite_storage_managed_ledger_rocksdb::test_support::{
    ack_managed_cursor_shared, RocksDBManagedCursor, RocksDBManagedLedger,
    RocksDBManagedLedgerFactory,
};
use pulsar_lite_storage_managed_ledger::{ManagedLedgerConfig, ManagedLedgerFactory};
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn managed_cursor_mark_delete_recovers_after_reopen() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("cursor-mark-delete");
    let mark_delete = position(3, 11);

    {
        let db = open_test_db(&db_path);
        let mut cursor = RocksDBManagedCursor::open("ledger-a", "sub-a", db).unwrap();
        cursor.mark_delete(mark_delete.clone()).unwrap();
    }

    let db = open_test_db(&db_path);
    let cursor = RocksDBManagedCursor::open("ledger-a", "sub-a", db).unwrap();

    assert_eq!(cursor.state().mark_delete, Some(mark_delete));
}

#[test]
fn managed_cursor_individual_delete_recovers_after_reopen() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("cursor-individual-delete");
    let deleted = position(5, 17);

    {
        let db = open_test_db(&db_path);
        let mut cursor = RocksDBManagedCursor::open("ledger-a", "sub-a", db).unwrap();
        cursor.delete_individual(deleted.clone()).unwrap();
    }

    let db = open_test_db(&db_path);
    let cursor = RocksDBManagedCursor::open("ledger-a", "sub-a", db).unwrap();

    assert!(cursor
        .state()
        .individually_deleted_entries
        .contains(&deleted));
}

#[test]
fn managed_cursor_state_is_isolated_by_ledger_and_cursor_name() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("cursor-isolation");
    let ledger_a_sub_a = position(1, 2);
    let ledger_a_sub_b = position(1, 7);

    {
        let db = open_test_db(&db_path);
        let mut cursor_a =
            RocksDBManagedCursor::open("ledger-a", "sub-a", Arc::clone(&db)).unwrap();
        let mut cursor_b =
            RocksDBManagedCursor::open("ledger-a", "sub-b", Arc::clone(&db)).unwrap();

        cursor_a.mark_delete(ledger_a_sub_a.clone()).unwrap();
        cursor_b.mark_delete(ledger_a_sub_b.clone()).unwrap();
    }

    let db = open_test_db(&db_path);
    let cursor_a = RocksDBManagedCursor::open("ledger-a", "sub-a", Arc::clone(&db)).unwrap();
    let cursor_b = RocksDBManagedCursor::open("ledger-a", "sub-b", Arc::clone(&db)).unwrap();
    let cursor_c = RocksDBManagedCursor::open("ledger-b", "sub-a", db).unwrap();

    assert_eq!(cursor_a.state().mark_delete, Some(ledger_a_sub_a));
    assert_eq!(cursor_b.state().mark_delete, Some(ledger_a_sub_b));
    assert_eq!(cursor_c.state().mark_delete, None);
}
#[test]
fn shared_ack_advances_contiguously_across_rolled_ledgers() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("ledger-rollover-shared-ack");
    let db = open_test_db(&db_path);
    let config = ManagedLedgerConfig {
        max_entries_per_ledger: Some(2),
        ..ManagedLedgerConfig::default()
    };
    let entry_log = open_test_entry_log(&db_path);
    let mut factory = RocksDBManagedLedgerFactory::new(Arc::clone(&db), entry_log);
    let mut ledger = factory.open("ledger-a", &config).unwrap();
    let first = ledger.add_entry(b"first").unwrap();
    let second = ledger.add_entry(b"second").unwrap();
    let third = ledger.add_entry(b"third").unwrap();
    let mut cursor = ledger.open_cursor("sub-a").unwrap();

    ack_managed_cursor_shared(&mut cursor, third.clone(), &ledger.ledger_info()).unwrap();
    assert_eq!(cursor.state().mark_delete, None);
    assert!(cursor.state().individually_deleted_entries.contains(&third));

    ack_managed_cursor_shared(&mut cursor, first, &ledger.ledger_info()).unwrap();
    ack_managed_cursor_shared(&mut cursor, second, &ledger.ledger_info()).unwrap();

    assert_eq!(cursor.state().mark_delete, Some(third));
    assert!(cursor.state().individually_deleted_entries.is_empty());
}
#[test]
fn managed_ledger_open_cursor_recovers_cursor_state() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("ledger-cursor-recovery");
    let mark_delete = position(0, 3);

    {
        let db = open_test_db(&db_path);
        let entry_log = open_test_entry_log(&db_path);
        let mut ledger = RocksDBManagedLedger::open("ledger-a", db, entry_log).unwrap();
        let mut cursor = ledger.open_cursor("sub-a").unwrap();
        cursor.mark_delete(mark_delete.clone()).unwrap();
    }

    let db = open_test_db(&db_path);
    let entry_log = open_test_entry_log(&db_path);
    let mut ledger = RocksDBManagedLedger::open("ledger-a", db, entry_log).unwrap();
    let cursor = ledger.open_cursor("sub-a").unwrap();

    assert_eq!(cursor.state().mark_delete, Some(mark_delete));
}
