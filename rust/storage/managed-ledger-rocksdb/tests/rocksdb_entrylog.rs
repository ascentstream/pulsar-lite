//! Entry-log append/read/rollover integration tests.

use pulsar_lite_storage_managed_ledger_rocksdb::test_support::{EntryLogStore, EntryToAppend};
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

#[test]
fn entrylog_handles_concurrent_appends() {
    use std::sync::Arc;
    use std::thread;

    let dir = tempdir().unwrap();
    let store = Arc::new(EntryLogStore::open(dir.path()).unwrap());

    // Fan out concurrent appends through the shared writer queue.
    let handles: Vec<_> = (0..4)
        .map(|i| {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                let payload = format!("msg-{i}");
                store
                    .append(0, i as u64, -1, payload.as_bytes())
                    .expect("concurrent append should succeed")
            })
        })
        .collect();

    let mut indices: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().expect("append thread should not panic"))
        .collect();

    // All four entries must be readable with the expected payload.
    indices.sort_by_key(|index| index.entry_id);
    for (entry_id, index) in indices.iter().enumerate() {
        assert_eq!(index.entry_id, entry_id as u64);
        let entry = store.read(index).unwrap();
        assert_eq!(entry.payload, format!("msg-{entry_id}").as_bytes());
    }

    // Writer assigns offsets serially, so [offset, offset+len) ranges must not overlap.
    let mut ranges: Vec<_> = indices
        .iter()
        .map(|index| (index.offset, index.offset + index.len))
        .collect();
    ranges.sort_unstable();
    for window in ranges.windows(2) {
        assert!(
            window[0].1 <= window[1].0,
            "offset ranges must not overlap: {:?}",
            window
        );
    }
}

#[test]
fn entrylog_append_batch_empty_returns_empty() {
    let dir = tempdir().unwrap();
    let store = EntryLogStore::open(dir.path()).unwrap();
    let indices = store.append_batch(Vec::new()).unwrap();
    assert!(indices.is_empty());
}

#[test]
fn entrylog_append_batch_writes_stable_offsets_and_reads_back() {
    let dir = tempdir().unwrap();
    let store = EntryLogStore::open(dir.path()).unwrap();

    let entries = vec![
        EntryToAppend {
            ledger_id: 7,
            entry_id: 0,
            partition: -1,
            metadata: b"m0".to_vec(),
            payload: b"first".to_vec(),
        },
        EntryToAppend {
            ledger_id: 7,
            entry_id: 1,
            partition: 2,
            metadata: b"m1".to_vec(),
            payload: b"second".to_vec(),
        },
        EntryToAppend {
            ledger_id: 8,
            entry_id: 0,
            partition: -1,
            metadata: Vec::new(),
            payload: b"third".to_vec(),
        },
    ];

    let indices = store.append_batch(entries).unwrap();
    assert_eq!(indices.len(), 3);

    assert_eq!(indices[0].ledger_id, 7);
    assert_eq!(indices[0].entry_id, 0);
    assert_eq!(indices[0].offset, 0);
    assert_eq!(indices[0].file_id, indices[1].file_id);
    assert_eq!(indices[1].offset, indices[0].offset + indices[0].len);
    assert_eq!(indices[2].offset, indices[1].offset + indices[1].len);

    let e0 = store.read(&indices[0]).unwrap();
    assert_eq!(e0.metadata, b"m0");
    assert_eq!(e0.payload, b"first");

    let e1 = store.read(&indices[1]).unwrap();
    assert_eq!(e1.partition, 2);
    assert_eq!(e1.metadata, b"m1");
    assert_eq!(e1.payload, b"second");

    let e2 = store.read(&indices[2]).unwrap();
    assert_eq!(e2.payload, b"third");
}

#[test]
fn entrylog_append_batch_then_single_append_continues_offsets() {
    let dir = tempdir().unwrap();
    let store = EntryLogStore::open(dir.path()).unwrap();

    let batch = store
        .append_batch(vec![
            EntryToAppend {
                ledger_id: 1,
                entry_id: 0,
                partition: -1,
                metadata: Vec::new(),
                payload: b"a".to_vec(),
            },
            EntryToAppend {
                ledger_id: 1,
                entry_id: 1,
                partition: -1,
                metadata: Vec::new(),
                payload: b"b".to_vec(),
            },
        ])
        .unwrap();
    let single = store.append(1, 2, -1, b"c").unwrap();

    assert_eq!(single.file_id, batch[0].file_id);
    assert_eq!(single.offset, batch[1].offset + batch[1].len);
    assert_eq!(store.read(&single).unwrap().payload, b"c");
}
