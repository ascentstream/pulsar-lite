//! `RocksDbManagedLedgerStorage` keyspace integration tests.

mod common;

use common::*;
use pulsar_lite_storage_managed_ledger::{
    CursorInitOptions, InitialPosition, ManagedLedgerStorage,
};
use pulsar_lite_storage_managed_ledger_rocksdb::{test_support::keys, RocksDbManagedLedgerStorage};
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

#[test]
fn append_after_latest_cursor_is_visible_to_reads() {
    // Reproduces the consumer-0-message bug:
    // 1) subscribe/Latest warms the SharedLedger cache with empty in-memory info
    // 2) producer appends through the write-queue worker's owned ledger copy
    // 3) dispatch reads via SharedLedger and must still see the new entries
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("storage-append-visibility");
    let topic = "persistent://public/default/visibility";
    let subscription = "sub";

    let mut storage = RocksDbManagedLedgerStorage::open(&db_path).unwrap();
    storage.create_topic(topic).unwrap();

    // Warm the reader-side SharedLedger cache the same way subscribe does.
    storage
        .initialize_or_open_cursor(
            topic,
            subscription,
            CursorInitOptions {
                initial_position: InitialPosition::Latest,
                start_message_id: None,
            },
        )
        .unwrap();

    assert_eq!(
        storage.first_unacked_position(topic, subscription).unwrap(),
        None,
        "empty topic with Latest cursor should have no backlog"
    );

    let message_id = storage.append_message(topic, -1, b"hello-after-subscribe").unwrap();

    let first = storage
        .first_unacked_position(topic, subscription)
        .unwrap()
        .expect("appended entry must be visible as first unacked");
    assert_eq!(first.ledger_id, message_id.ledger);
    assert_eq!(first.entry_id, message_id.entry);

    let entries = storage
        .read_entries_from(topic, &first, 1)
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].message_id, message_id);
    assert_eq!(entries[0].payload, b"hello-after-subscribe");
}
