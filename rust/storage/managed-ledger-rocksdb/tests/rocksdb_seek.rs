//! Cursor seek integration tests via the storage facade.

use pulsar_lite_storage_managed_ledger::{
    CursorInitOptions, InitialPosition, ManagedLedgerPosition, ManagedLedgerStorage,
};
use pulsar_lite_storage_managed_ledger_rocksdb::RocksDbManagedLedgerStorage;
use tempfile::tempdir;

#[tokio::test]
async fn seek_cursor_reposition_first_unacked_to_target() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("seek_cursor");
    let topic = "persisitent://public/default/seek";
    let sub = "sub";

    let mut storage = RocksDbManagedLedgerStorage::open(&db_path).unwrap();
    let m0 = storage.append_message(topic, -1, b"m0").unwrap();
    let m1 = storage.append_message(topic, -1, b"m1").unwrap();

    // init cursor and ack
    storage
        .initialize_or_open_cursor(
            topic,
            sub,
            CursorInitOptions {
                initial_position: InitialPosition::Earliest,
                start_message_id: None,
            },
        )
        .unwrap();
    storage.ack_message_shared(topic, sub, m0.clone()).unwrap();

    // seek
    storage.seek_cursor(topic, sub, &m1, true).await.unwrap();
    let first = storage.first_unacked_position(topic, sub).unwrap();
    assert_eq!(first, Some(ManagedLedgerPosition::from(&m1)));
}
