//! Entry-log append/read/rollover integration tests.

use pulsar_lite_storage_managed_ledger_rocksdb::test_support::EntryLogStore;
use std::fs;
use tempfile::tempdir;

#[test]
fn entrylog_appends_and_reads_entry_payload() {
    let dir = tempdir().unwrap();
    let store = EntryLogStore::open(dir.path()).unwrap();

    let index = store.append(7, 3, 2, b"payload").unwrap();
    let entry = store.read(&index).unwrap();

    assert_eq!(index.ledger_id, 7);
    assert_eq!(index.entry_id, 3);
    assert_eq!(index.file_id, 0);
    assert_eq!(index.offset, 0);
    assert_eq!(index.len, 44 + b"payload".len() as u64);
    assert_eq!(index.partition, 2);
    assert_eq!(entry.partition, 2);
    assert_eq!(entry.metadata, b"");
    assert_eq!(entry.payload, b"payload");
    assert!(dir.path().join("entrylog").join("0.log").exists());
    assert!(!dir
        .path()
        .join("entrylog")
        .join("entrylog-00000000000000000000.log")
        .exists());
}

#[test]
fn entrylog_appends_and_reads_entry_metadata() {
    let dir = tempdir().unwrap();
    let store = EntryLogStore::open(dir.path()).unwrap();

    let index = store
        .append_with_metadata(7, 3, 2, b"metadata", b"payload")
        .unwrap();
    let entry = store.read(&index).unwrap();

    assert_eq!(
        index.len,
        44 + b"metadata".len() as u64 + b"payload".len() as u64
    );
    assert_eq!(entry.partition, 2);
    assert_eq!(entry.metadata, b"metadata");
    assert_eq!(entry.payload, b"payload");
}

#[test]
fn entrylog_appends_multiple_entries_with_stable_offsets() {
    let dir = tempdir().unwrap();
    let store = EntryLogStore::open(dir.path()).unwrap();

    let first = store.append(7, 0, -1, b"first").unwrap();
    let second = store.append(7, 1, -1, b"second").unwrap();

    assert_eq!(first.file_id, second.file_id);
    assert_eq!(second.offset, first.offset + first.len);
    assert_eq!(store.read(&first).unwrap().payload, b"first");
    assert_eq!(store.read(&second).unwrap().payload, b"second");
}

#[test]
fn entrylog_rejects_index_for_different_position() {
    let dir = tempdir().unwrap();
    let store = EntryLogStore::open(dir.path()).unwrap();
    let mut index = store.append(7, 3, -1, b"payload").unwrap();

    index.entry_id = 4;

    let err = store.read(&index).unwrap_err().to_string();
    assert!(err.contains("entrylog position does not match index"));
}

#[test]
fn entrylog_rejects_index_when_checksum_does_not_match_record() {
    let dir = tempdir().unwrap();
    let store = EntryLogStore::open(dir.path()).unwrap();
    let mut index = store.append(7, 3, -1, b"payload").unwrap();

    index.checksum = index.checksum.wrapping_add(1);

    let err = store.read(&index).unwrap_err().to_string();
    assert!(err.contains("entrylog checksum mismatch"));
}

#[test]
fn entrylog_reopen_allocates_next_file_id() {
    let dir = tempdir().unwrap();

    let first = {
        let store = EntryLogStore::open(dir.path()).unwrap();
        store.append(7, 0, -1, b"first").unwrap()
    };

    let second = {
        let store = EntryLogStore::open(dir.path()).unwrap();
        store.append(7, 1, -1, b"second").unwrap()
    };

    let store = EntryLogStore::open(dir.path()).unwrap();

    assert_eq!(second.file_id, first.file_id + 1);
    assert_eq!(second.offset, 0);
    assert_eq!(store.read(&first).unwrap().payload, b"first");
    assert_eq!(store.read(&second).unwrap().payload, b"second");
}

#[test]
fn entrylog_reopen_uses_decimal_log_file_ids() {
    let dir = tempdir().unwrap();
    let entrylog_dir = dir.path().join("entrylog");
    fs::create_dir_all(&entrylog_dir).unwrap();
    fs::write(entrylog_dir.join("9.log"), b"").unwrap();
    fs::write(entrylog_dir.join("10.log"), b"").unwrap();

    let store = EntryLogStore::open(dir.path()).unwrap();
    let index = store.append(7, 0, -1, b"payload").unwrap();

    assert_eq!(index.file_id, 11);
    assert!(entrylog_dir.join("11.log").exists());
}

#[test]
fn entrylog_rolls_over_when_configured_limit_is_exceeded() {
    let dir = tempdir().unwrap();
    let store = EntryLogStore::open_with_log_size_limit(dir.path(), 88).unwrap();

    let first = store.append(7, 0, -1, &[1; 40]).unwrap();
    let second = store.append(7, 1, -1, &[2; 40]).unwrap();

    assert_eq!(first.file_id, 0);
    assert_eq!(second.file_id, 1);
    assert!(dir.path().join("entrylog").join("0.log").exists());
    assert!(dir.path().join("entrylog").join("1.log").exists());
}

#[test]
fn entrylog_allows_single_entry_larger_than_configured_limit() {
    let dir = tempdir().unwrap();
    let store = EntryLogStore::open_with_log_size_limit(dir.path(), 16).unwrap();

    let first = store.append(7, 0, -1, &[1; 40]).unwrap();
    let second = store.append(7, 1, -1, b"next").unwrap();

    assert_eq!(first.file_id, 0);
    assert_eq!(second.file_id, 1);
}

#[test]
fn entrylog_default_size_limit_matches_bookkeeper_like_threshold() {
    assert_eq!(
        EntryLogStore::default_log_size_limit(),
        2 * 1024 * 1024 * 1024
    );
}
