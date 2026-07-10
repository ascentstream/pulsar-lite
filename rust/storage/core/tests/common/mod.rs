//! Shared helpers for pulsar-lite-storage integration tests.

use pulsar_lite_storage::Storage;
use pulsar_lite_storage_managed_ledger::{CursorInitOptions, InitialPosition};
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

pub fn create_storage() -> (TempDir, Storage) {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = tempfile::tempdir().unwrap();
    let path = std::env::temp_dir().join(format!("test-storage-{unique}.db"));
    let storage = Storage::new_memory(&path).unwrap();
    (dir, storage)
}

pub fn open_earliest_cursor(storage: &mut Storage, topic: &str, subscription: &str) {
    storage
        .initialize_or_open_cursor(
            topic,
            subscription,
            CursorInitOptions {
                initial_position: InitialPosition::Earliest,
                start_message_id: None,
            },
        )
        .unwrap();
}
