//! RocksDB managed-cursor persistence and shared-ack tests.

mod common;

use common::*;
use pulsar_lite_storage_managed_ledger::ManagedCursor;
use pulsar_lite_storage_managed_ledger::{
    ManagedLedgerConfig, ManagedLedgerFactory, ManagedLedgerPosition,
};
use pulsar_lite_storage_managed_ledger_rocksdb::test_support::{
    ack_managed_cursor_shared, append_payload, append_with_partition,
    is_managed_position_acknowledged, RocksDBManagedCursor, RocksDBManagedLedger,
    RocksDBManagedLedgerFactory,
};
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
    let ledger = factory.open("ledger-a", &config).unwrap();
    let first = append_payload(&ledger, b"first").unwrap();
    let second = append_payload(&ledger, b"second").unwrap();
    let third = append_payload(&ledger, b"third").unwrap();
    let mut cursor = ledger.open_cursor("sub-a").unwrap();

    ack_managed_cursor_shared(&mut cursor, third.clone(), &ledger.info_snapshot().as_ref())
        .unwrap();
    assert_eq!(cursor.state().mark_delete, None);
    assert!(cursor.state().individually_deleted_entries.contains(&third));

    ack_managed_cursor_shared(&mut cursor, first, &ledger.info_snapshot().as_ref()).unwrap();
    ack_managed_cursor_shared(&mut cursor, second, &ledger.info_snapshot().as_ref()).unwrap();

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
        let ledger = RocksDBManagedLedger::open("ledger-a", db, entry_log).unwrap();
        let mut cursor = ledger.open_cursor("sub-a").unwrap();
        cursor.mark_delete(mark_delete.clone()).unwrap();
    }

    let db = open_test_db(&db_path);
    let entry_log = open_test_entry_log(&db_path);
    let ledger = RocksDBManagedLedger::open("ledger-a", db, entry_log).unwrap();
    let cursor = ledger.open_cursor("sub-a").unwrap();

    assert_eq!(cursor.state().mark_delete, Some(mark_delete));
}

#[test]
fn managed_cursor_mark_delete_normalizes_partition() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("cursor-mark-delete-partition");
    let message_position = ManagedLedgerPosition {
        ledger_id: 3,
        entry_id: 11,
        partition: 7,
    };

    let cursor_position = ManagedLedgerPosition {
        ledger_id: 3,
        entry_id: 11,
        partition: -1,
    };

    {
        let db = open_test_db(&db_path);
        let mut cursor = RocksDBManagedCursor::open("ledger-a", "sub-a", db).unwrap();
        cursor.mark_delete(message_position.clone()).unwrap();
        assert_eq!(cursor.state().mark_delete, Some(cursor_position.clone()));
        assert!(is_managed_position_acknowledged(
            cursor.state(),
            &message_position,
        ))
    }
    let db = open_test_db(&db_path);
    let cursor = RocksDBManagedCursor::open("ledger-a", "sub-a", db).unwrap();
    assert_eq!(cursor.state().mark_delete, Some(cursor_position));
    assert!(is_managed_position_acknowledged(
        cursor.state(),
        &message_position,
    ));
}

#[test]
fn managed_cursor_individual_delete_normalizes_partition() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("cursor-individual-delete-partition");

    let message_position = ManagedLedgerPosition {
        ledger_id: 3,
        entry_id: 11,
        partition: 7,
    };

    let cursor_position = ManagedLedgerPosition {
        ledger_id: 3,
        entry_id: 11,
        partition: -1,
    };

    {
        let db = open_test_db(&db_path);
        let mut cursor = RocksDBManagedCursor::open("ledger-a", "sub-a", db).unwrap();
        cursor.delete_individual(message_position.clone()).unwrap();
        assert_eq!(
            cursor
                .state()
                .individually_deleted_entries
                .contains(&cursor_position),
            true
        );
        assert!(is_managed_position_acknowledged(
            cursor.state(),
            &message_position,
        ))
    }

    let db = open_test_db(&db_path);
    let cursor = RocksDBManagedCursor::open("ledger-a", "sub-a", db).unwrap();
    assert_eq!(
        cursor
            .state()
            .individually_deleted_entries
            .contains(&cursor_position),
        true
    );
    assert!(is_managed_position_acknowledged(
        cursor.state(),
        &message_position,
    ))
}

#[test]
fn managed_cursor_reset_cursor_normalizes_partition() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("cursor-reset-partition");

    let message_position = ManagedLedgerPosition {
        ledger_id: 3,
        entry_id: 11,
        partition: 7,
    };

    let cursor_position = ManagedLedgerPosition {
        ledger_id: 3,
        entry_id: 11,
        partition: -1,
    };

    {
        let db = open_test_db(&db_path);
        let mut cursor = RocksDBManagedCursor::open("ledger-a", "sub-a", db).unwrap();
        cursor.reset_cursor(Some(message_position.clone())).unwrap();
        assert_eq!(cursor.state().mark_delete, Some(cursor_position.clone()));
        assert!(cursor.state().individually_deleted_entries.is_empty());
        assert!(is_managed_position_acknowledged(
            cursor.state(),
            &message_position,
        ))
    }

    let db = open_test_db(&db_path);
    let cursor = RocksDBManagedCursor::open("ledger-a", "sub-a", db).unwrap();
    assert_eq!(cursor.state().mark_delete, Some(cursor_position.clone()));
    assert!(cursor.state().individually_deleted_entries.is_empty());
    assert!(is_managed_position_acknowledged(
        cursor.state(),
        &message_position,
    ))
}

#[test]
fn shared_ack_normalizes_partition_when_advancing_mark_delete() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("shared-ack-partition");
    let db = open_test_db(&db_path);
    let entry_log = open_test_entry_log(&db_path);

    let ledger = RocksDBManagedLedger::open("ledger-a", Arc::clone(&db), entry_log).unwrap();

    let first = append_with_partition(&ledger, 7, b"first").unwrap();

    let second = append_with_partition(&ledger, 7, b"second").unwrap();

    let mut cursor = ledger.open_cursor("sub-a").unwrap();

    // First, reorder ACK item 'second'. It should enter the individual collection
    ack_managed_cursor_shared(&mut cursor, second.clone(), ledger.info_snapshot().as_ref())
        .unwrap();

    assert_eq!(cursor.state().mark_delete, None);
    assert!(cursor
        .state()
        .individually_deleted_entries
        .contains(&ManagedLedgerPosition {
            ledger_id: second.ledger_id,
            entry_id: second.entry_id,
            partition: -1
        }));

    // Then move on to the first item, 'mark_delete'proceed forward
    ack_managed_cursor_shared(&mut cursor, first.clone(), ledger.info_snapshot().as_ref()).unwrap();
    assert_eq!(
        cursor.state().mark_delete,
        Some(ManagedLedgerPosition {
            ledger_id: second.ledger_id,
            entry_id: second.entry_id,
            partition: -1
        })
    );
    assert!(cursor.state().individually_deleted_entries.is_empty());
    assert!(is_managed_position_acknowledged(cursor.state(), &first));
    assert!(is_managed_position_acknowledged(cursor.state(), &second));
}
