//! Managed-ledger key encoding and protobuf metadata value tests.

mod common;

use common::*;
use pulsar_lite_storage_managed_ledger::{ManagedCursor, ManagedLedger};
use pulsar_lite_storage_managed_ledger_rocksdb::test_support::{
    keys, proto, RocksDBManagedCursor, RocksDBManagedLedger,
};
use prost::Message;
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn managed_metadata_keys_follow_pulsar_path_shape() {
    assert_eq!(
        keys::managed_ledger_key("tenant/namespace/persistent/topic"),
        b"/managed-ledgers/tenant/namespace/persistent/topic".to_vec()
    );
    assert_eq!(
        keys::managed_cursor_key("tenant/namespace/persistent/topic", "sub-a",),
        b"/managed-ledgers/tenant/namespace/persistent/topic/sub-a".to_vec()
    );
}

#[test]
fn entry_keys_follow_bookkeeper_style_ledger_entry_lookup() {
    assert_eq!(keys::managed_entry_key(42, 7), b"entry|42|7".to_vec());
}

#[test]
fn managed_ledger_info_value_is_protobuf_encoded() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("ledger-info-protobuf");
    let db = open_test_db(&db_path);

    {
        let entry_log = open_test_entry_log(&db_path);
        let mut ledger =
            RocksDBManagedLedger::open("ledger-a", Arc::clone(&db), entry_log).unwrap();
        ledger.add_entry(b"first").unwrap();
    }

    let bytes = read_raw_value(&db, keys::managed_ledger_key("ledger-a"));
    let info = proto::ManagedLedgerInfo::decode(bytes.as_slice()).unwrap();

    assert_eq!(info.ledger_info.len(), 1);
    assert_eq!(info.ledger_info[0].ledger_id, 0);
    assert_eq!(info.ledger_info[0].entries, Some(1));
    assert_eq!(info.ledger_info[0].size, Some(b"first".len() as i64));
}

#[test]
fn managed_cursor_state_value_is_protobuf_encoded() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("cursor-info-protobuf");
    let db = open_test_db(&db_path);
    let mark_delete = position(0, 3);
    let deleted = position(0, 5);

    {
        let mut cursor = RocksDBManagedCursor::open("ledger-a", "sub-a", Arc::clone(&db)).unwrap();
        cursor.mark_delete(mark_delete).unwrap();
        cursor.delete_individual(deleted).unwrap();
    }

    let bytes = read_raw_value(&db, keys::managed_cursor_key("ledger-a", "sub-a"));
    let info = proto::ManagedCursorInfo::decode(bytes.as_slice()).unwrap();

    assert_eq!(info.cursors_ledger_id, -1);
    assert_eq!(info.mark_delete_ledger_id, Some(0));
    assert_eq!(info.mark_delete_entry_id, Some(3));
    assert_eq!(info.individual_deleted_messages.len(), 1);
}

#[test]
fn managed_ledger_name_normalizes_pulsar_urls_and_keeps_plain_names() {
    assert_eq!(
        keys::managed_ledger_name("persistent://public/default/test"),
        "public/default/persistent/test"
    );
    assert_eq!(
        keys::managed_ledger_name("persistent://public/default/test-partition-0",),
        "public/default/persistent/test-partition-0"
    );
    assert_eq!(keys::managed_ledger_name("test-topic"), "test-topic");
}

#[test]
fn cursor_name_percent_encoding_preserves_path_segment_boundary() {
    assert_eq!(keys::encode_cursor_name("sub-a"), "sub-a");
    assert_eq!(keys::encode_cursor_name("team/a"), "team%2Fa");
    assert_eq!(keys::encode_cursor_name("team%2Fa"), "team%252Fa");
}
