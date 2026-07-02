//! `RocksDbManagedLedgerStorage` keyspace integration tests.

mod common;

use common::*;
use pulsar_lite_storage_managed_ledger::ManagedLedgerStorage;
use pulsar_lite_storage_managed_ledger_rocksdb::{
    test_support::keys, RocksDbManagedLedgerStorage,
};
use tempfile::tempdir;

#[test]
fn storage_writes_managed_ledger_keys_instead_of_legacy_keys() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("storage-managed-keyspace");
    let topic = "tenant/namespace/persistent/topic";
    let subscription = "sub-a";

    let message_id = {
        let mut storage = RocksDbManagedLedgerStorage::open(&db_path).unwrap();
        let message_id = storage.append_message(topic, 7, b"payload").unwrap();
        storage
            .ack_message_shared(topic, subscription, message_id.clone())
            .unwrap();
        message_id
    };

    let db = open_test_db(&db_path);

    assert!(db.get(keys::managed_ledger_key(topic)).unwrap().is_some());
    assert!(db
        .get(keys::managed_cursor_key(topic, subscription))
        .unwrap()
        .is_some());
    assert!(db
        .get(keys::managed_entry_key(message_id.ledger, message_id.entry))
        .unwrap()
        .is_some());

    assert!(db.get(format!("ledger|{topic}")).unwrap().is_none());
    assert!(db
        .get(format!(
            "entry|{topic}|{:020}|{:020}",
            message_id.ledger, message_id.entry
        ))
        .unwrap()
        .is_none());
    assert!(db
        .get(format!("cursor|{topic}|{subscription}"))
        .unwrap()
        .is_none());
    assert!(db
        .get(format!(
            "hole|{topic}|{subscription}|{:020}",
            message_id.entry
        ))
        .unwrap()
        .is_none());
}

#[test]
fn storage_normalizes_topic_url_and_encodes_cursor_name() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("storage-normalized-keyspace");
    let topic = "persistent://public/default/test";
    let ledger_name = "public/default/persistent/test";
    let subscription = "team/a";
    let cursor_name = "team%2Fa";

    let message_id = {
        let mut storage = RocksDbManagedLedgerStorage::open(&db_path).unwrap();
        let message_id = storage.append_message(topic, -1, b"payload").unwrap();
        storage
            .ack_message_shared(topic, subscription, message_id.clone())
            .unwrap();
        message_id
    };

    let db = open_test_db(&db_path);

    assert!(db
        .get(keys::managed_ledger_key(ledger_name))
        .unwrap()
        .is_some());
    assert!(db
        .get(keys::managed_cursor_key(ledger_name, cursor_name))
        .unwrap()
        .is_some());
    assert!(db
        .get(keys::managed_entry_key(message_id.ledger, message_id.entry))
        .unwrap()
        .is_some());
    assert!(db
        .get(format!(
            "managed_entry|{ledger_name}|{:020}|{:020}",
            message_id.ledger, message_id.entry
        ))
        .unwrap()
        .is_none());

    assert!(db.get(keys::managed_ledger_key(topic)).unwrap().is_none());
    assert!(db
        .get(keys::managed_cursor_key(ledger_name, subscription))
        .unwrap()
        .is_none());
}
