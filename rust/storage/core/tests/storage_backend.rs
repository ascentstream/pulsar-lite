//! Managed-ledger backend integration tests for `Storage`.

mod common;

use common::open_earliest_cursor;
use pulsar_lite_storage::Storage;
use pulsar_lite_storage_managed_ledger::MessageId;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::tempdir;

fn create_storage() -> Storage {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("test-storage-{unique}.db"));
    Storage::new_memory(&path).unwrap()
}

    #[test]
    fn new_memory_uses_memory_managed_ledger_store() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("storage.db");
        let mut storage = Storage::new_memory(&db_path).unwrap();

        let topic = "persistent://public/default/memory-store-test";
        storage.create_topic(topic).unwrap();
        let message_id = storage.append_message(topic, -1, b"memory").unwrap();

        assert_eq!(message_id.entry, 0);
        assert_eq!(
            storage.get_message_by_id(topic, &message_id).unwrap().1,
            b"memory".to_vec()
        );
    }

    #[test]
    fn memory_store_reads_message_metadata_with_payload() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("storage.db");
        let mut storage = Storage::new_memory(&db_path).unwrap();

        let topic = "persistent://public/default/memory-metadata-store-test";
        storage.create_topic(topic).unwrap();
        let message_id = storage
            .append_message_with_metadata(topic, -1, b"metadata", b"payload")
            .unwrap();

        let stored = storage
            .get_message_entry_by_id(topic, &message_id)
            .expect("message should be readable");
        assert_eq!(stored.message_id, message_id);
        assert_eq!(stored.metadata, b"metadata".to_vec());
        assert_eq!(stored.payload, b"payload".to_vec());
    }

    #[cfg(feature = "rocksdb-storage")]
    #[test]
    fn new_uses_rocksdb_managed_ledger_store_when_feature_enabled() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("storage.db");
        let topic = "persistent://public/default/default-rocksdb-store-test";

        let message_id = {
            let mut storage = Storage::new(&db_path).unwrap();
            storage.create_topic(topic).unwrap();
            storage
                .append_message(topic, -1, b"default-durable")
                .unwrap()
        };

        let storage = Storage::new(&db_path).unwrap();
        assert_eq!(
            storage.get_message_by_id(topic, &message_id).unwrap().1,
            b"default-durable".to_vec()
        );
    }

    #[cfg(feature = "rocksdb-storage")]
    #[test]
    fn new_rocksdb_persists_messages_across_reopen() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("storage.db");
        let topic = "persistent://public/default/rocksdb-store-test";

        let message_id = {
            let mut storage = Storage::new_rocksdb(&db_path).unwrap();
            storage.create_topic(topic).unwrap();
            storage.append_message(topic, -1, b"durable").unwrap()
        };

        let storage = Storage::new_rocksdb(&db_path).unwrap();
        assert_eq!(
            storage.get_message_by_id(topic, &message_id).unwrap().1,
            b"durable".to_vec()
        );
    }

    #[cfg(feature = "rocksdb-storage")]
    #[test]
    fn rocksdb_persists_message_metadata_across_reopen() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("storage.db");
        let topic = "persistent://public/default/rocksdb-metadata-store-test";

        let message_id = {
            let mut storage = Storage::new_rocksdb(&db_path).unwrap();
            storage.create_topic(topic).unwrap();
            storage
                .append_message_with_metadata(topic, -1, b"metadata", b"payload")
                .unwrap()
        };

        let storage = Storage::new_rocksdb(&db_path).unwrap();
        let stored = storage
            .get_message_entry_by_id(topic, &message_id)
            .expect("message should be readable after reopen");
        assert_eq!(stored.message_id, message_id);
        assert_eq!(stored.metadata, b"metadata".to_vec());
        assert_eq!(stored.payload, b"payload".to_vec());
    }

    #[cfg(feature = "rocksdb-storage")]
    #[test]
    fn rocksdb_persists_cumulative_cursor_across_reopen() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("storage.db");
        let topic = "persistent://public/default/rocksdb-cursor-test";
        let subscription = "sub";

        let acked = {
            let mut storage = Storage::new_rocksdb(&db_path).unwrap();
            storage.create_topic(topic).unwrap();
            open_earliest_cursor(&mut storage, topic, subscription);
            storage.append_message(topic, -1, b"0").unwrap();
            let msg1 = storage.append_message(topic, -1, b"1").unwrap();
            storage
                .ack_message(topic, subscription, msg1.clone())
                .unwrap();
            msg1
        };

        let storage = Storage::new_rocksdb(&db_path).unwrap();
        assert!(storage
            .first_unacked_position(topic, subscription)
            .unwrap()
            .is_none());
        assert_eq!(
            storage.get_mark_delete_position(topic, subscription),
            Some(acked.entry)
        );
    }

    #[cfg(feature = "rocksdb-storage")]
    #[test]
    fn rocksdb_persists_shared_ack_holes_across_reopen() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("storage.db");
        let topic = "persistent://public/default/rocksdb-shared-holes-test";
        let subscription = "sub";

        let (msg0, msg1, msg2) = {
            let mut storage = Storage::new_rocksdb(&db_path).unwrap();
            storage.create_topic(topic).unwrap();
            open_earliest_cursor(&mut storage, topic, subscription);
            let msg0 = storage.append_message(topic, -1, b"0").unwrap();
            let msg1 = storage.append_message(topic, -1, b"1").unwrap();
            let msg2 = storage.append_message(topic, -1, b"2").unwrap();

            storage
                .ack_message_shared(topic, subscription, msg2.clone())
                .unwrap();
            storage
                .ack_message_shared(topic, subscription, msg1.clone())
                .unwrap();
            assert_eq!(storage.get_mark_delete_position(topic, subscription), None);
            (msg0, msg1, msg2)
        };

        let mut storage = Storage::new_rocksdb(&db_path).unwrap();
        assert!(storage.is_acknowledged_shared(topic, subscription, &msg1));
        assert!(storage.is_acknowledged_shared(topic, subscription, &msg2));
        assert_eq!(storage.get_mark_delete_position(topic, subscription), None);

        storage
            .ack_message_shared(topic, subscription, msg0)
            .unwrap();
        assert_eq!(
            storage.get_mark_delete_position(topic, subscription),
            Some(2)
        );
    }
