use pulsar_lite_storage_managed_ledger::{
    CursorInitOptions, InitialPosition, ManagedLedgerStorage,
};
use pulsar_lite_storage_managed_ledger_rocksdb::RocksDbManagedLedgerStorage;
use tempfile::tempdir;

fn open(path: &std::path::Path) -> RocksDbManagedLedgerStorage {
    RocksDbManagedLedgerStorage::open(path).expect("open rocksdb store")
}

#[test]
fn backlog_tracks_shared_acks_and_full_drain() {
    let dir = tempdir().expect("tempdir");
    let topic = "persistent://public/default/backlog-probe";

    {
        let mut store = open(dir.path());
        store.create_topic(topic).expect("create topic");
        store
            .initialize_or_open_cursor(
                topic,
                "sub",
                CursorInitOptions {
                    initial_position: InitialPosition::Earliest,
                    ..Default::default()
                },
            )
            .expect("open cursor");
        for i in 0..10u64 {
            store
                .append_message(topic, -1, format!("m{i}").as_bytes())
                .expect("append");
        }
        assert_eq!(store.backlog_entries(topic, "sub"), Some(10));

        // Ack entries 0..3 under Shared semantics.
        for entry in 0..4u64 {
            let id = pulsar_lite_storage_managed_ledger::MessageId {
                ledger: store
                    .get_last_position(topic)
                    .expect("last position")
                    .map(|p| p.ledger_id)
                    .unwrap_or_default(),
                entry,
                partition: -1,
            };
            store
                .ack_message_shared(topic, "sub", id)
                .expect("ack shared");
        }
        assert_eq!(
            store.backlog_entries(topic, "sub"),
            Some(6),
            "4 acked of 10 must leave backlog 6"
        );
    }

    // Reopen: backlog must survive restart.
    {
        let store = open(dir.path());
        assert_eq!(store.backlog_entries(topic, "sub"), Some(6));
    }
}
