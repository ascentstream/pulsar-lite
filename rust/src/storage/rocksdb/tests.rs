use super::cursor::{ack_managed_cursor_shared, RocksDBManagedCursor};
use super::entrylog::{EntryIndex, EntryLogStore};
use super::factory::RocksDBManagedLedgerFactory;
use super::keys;
use super::ledger::RocksDBManagedLedger;
use super::metadata::{StoredEntryLocation, StoredManagedLedgerInfo};
use super::storage::RocksDbManagedLedgerStorage;
use crate::storage::{
    ManagedCursor, ManagedLedger, ManagedLedgerConfig, ManagedLedgerFactory, ManagedLedgerPosition,
    ManagedLedgerStorage,
};
use prost::Message;
use rocksdb::{Options, DB};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;

fn open_test_db(path: &Path) -> Arc<DB> {
    let mut options = Options::default();
    options.create_if_missing(true);
    Arc::new(DB::open(&options, path).unwrap())
}

fn open_test_entry_log(path: &Path) -> Arc<EntryLogStore> {
    Arc::new(EntryLogStore::open(path).unwrap())
}

fn position(ledger_id: u64, entry_id: u64) -> ManagedLedgerPosition {
    ManagedLedgerPosition {
        ledger_id,
        entry_id,
        partition: -1,
    }
}

fn read_managed_ledger_info(db: &DB, ledger_name: &str) -> StoredManagedLedgerInfo {
    let bytes = db
        .get(keys::managed_ledger_key(ledger_name))
        .unwrap()
        .expect("managed ledger info should exist");
    StoredManagedLedgerInfo::decode(&bytes).unwrap()
}

fn read_raw_value(db: &DB, key: Vec<u8>) -> Vec<u8> {
    db.get(key).unwrap().expect("value should exist").to_vec()
}

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
fn entrylog_reads_legacy_payload_only_entry_without_metadata() {
    let dir = tempdir().unwrap();
    let log_dir = dir.path().join("entrylog");
    std::fs::create_dir_all(&log_dir).unwrap();
    let path = log_dir.join("entrylog-00000000000000000000.log");
    let payload = b"legacy";
    let checksum = payload
        .iter()
        .fold(0u64, |acc, byte| acc.wrapping_add(*byte as u64));

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .unwrap();
    file.write_all(&0x504C4547u32.to_le_bytes()).unwrap();
    file.write_all(&1u16.to_le_bytes()).unwrap();
    file.write_all(&40u16.to_le_bytes()).unwrap();
    file.write_all(&7u64.to_le_bytes()).unwrap();
    file.write_all(&3u64.to_le_bytes()).unwrap();
    file.write_all(&2i32.to_le_bytes()).unwrap();
    file.write_all(&(payload.len() as u32).to_le_bytes())
        .unwrap();
    file.write_all(&checksum.to_le_bytes()).unwrap();
    file.write_all(payload).unwrap();
    file.flush().unwrap();

    let store = EntryLogStore::open(dir.path()).unwrap();
    let index = EntryIndex {
        ledger_id: 7,
        entry_id: 3,
        file_id: 0,
        offset: 0,
        len: 40 + payload.len() as u64,
        checksum,
        partition: 2,
    };
    let entry = store.read(&index).unwrap();

    assert_eq!(entry.partition, 2);
    assert_eq!(entry.metadata, b"");
    assert_eq!(entry.payload, payload);
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
fn entrylog_reads_legacy_zero_padded_log_file_names() {
    let dir = tempdir().unwrap();
    let store = EntryLogStore::open(dir.path()).unwrap();
    let index = store.append(7, 0, -1, b"legacy").unwrap();
    let entrylog_dir = dir.path().join("entrylog");

    fs::rename(
        entrylog_dir.join("0.log"),
        entrylog_dir.join("entrylog-00000000000000000000.log"),
    )
    .unwrap();

    assert_eq!(store.read(&index).unwrap().payload, b"legacy");
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
    assert_eq!(EntryLogStore::default_log_size_limit(), 1536 * 1024 * 1024);
}

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
    let info = super::metadata::proto::ManagedLedgerInfo::decode(bytes.as_slice()).unwrap();

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
    let info = super::metadata::proto::ManagedCursorInfo::decode(bytes.as_slice()).unwrap();

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
fn managed_ledger_entry_recovers_after_reopen() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("ledger-entry-recovery");

    let first_position = {
        let db = open_test_db(&db_path);
        let entry_log = open_test_entry_log(&db_path);
        let mut ledger = RocksDBManagedLedger::open("ledger-a", db, entry_log).unwrap();
        ledger.add_entry(b"first").unwrap()
    };

    let db = open_test_db(&db_path);
    let entry_log = open_test_entry_log(&db_path);
    let ledger = RocksDBManagedLedger::open("ledger-a", Arc::clone(&db), entry_log).unwrap();

    assert_eq!(first_position.ledger_id, 0);
    assert_eq!(first_position.entry_id, 0);
    assert_eq!(
        ledger.read_entry(&first_position).as_deref(),
        Some(b"first".as_slice())
    );
}

#[test]
fn managed_ledger_reload_uses_metadata_entry_count_with_plain_entry_keys() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("ledger-entry-recovery-plain-key");
    let db = open_test_db(&db_path);
    let entry_log = open_test_entry_log(&db_path);
    let index = entry_log.append(0, 0, -1, b"first").unwrap();
    let mut info = StoredManagedLedgerInfo::new(0);
    info.ledgers[0].entries = 1;
    info.ledgers[0].size = b"first".len() as u64;

    db.put(keys::managed_ledger_key("ledger-a"), info.encode_to_vec())
        .unwrap();
    db.put(
        b"entry|0|0",
        bincode::serialize(&StoredEntryLocation::from(index)).unwrap(),
    )
    .unwrap();

    let ledger = RocksDBManagedLedger::open("ledger-a", Arc::clone(&db), entry_log).unwrap();

    assert_eq!(
        ledger.read_entry(&position(0, 0)).as_deref(),
        Some(b"first".as_slice())
    );
}

#[test]
fn managed_ledger_entry_value_stores_location_not_payload() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("ledger-entry-location-value");
    let db = open_test_db(&db_path);
    let entry_log = open_test_entry_log(&db_path);
    let payload = b"payload-in-entrylog";

    let mut ledger = RocksDBManagedLedger::open("ledger-a", Arc::clone(&db), entry_log).unwrap();
    let position = ledger.add_entry(payload).unwrap();

    let raw_value = read_raw_value(
        &db,
        keys::managed_entry_key(position.ledger_id, position.entry_id),
    );
    let location: StoredEntryLocation = bincode::deserialize(&raw_value).unwrap();

    assert_eq!(location.partition, -1);
    assert_eq!(location.offset, 0);
    assert_eq!(location.len, 44 + payload.len() as u64);
    assert!(!raw_value
        .windows(payload.len())
        .any(|window| window == payload));
}

#[test]
fn managed_ledger_returns_none_for_bad_entry_location() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("ledger-bad-entry-location");
    let db = open_test_db(&db_path);
    let entry_log = open_test_entry_log(&db_path);

    let position = {
        let mut ledger =
            RocksDBManagedLedger::open("ledger-a", Arc::clone(&db), entry_log).unwrap();
        ledger.add_entry(b"payload").unwrap()
    };

    let mut location: StoredEntryLocation = bincode::deserialize(&read_raw_value(
        &db,
        keys::managed_entry_key(position.ledger_id, position.entry_id),
    ))
    .unwrap();
    location.checksum = location.checksum.wrapping_add(1);

    db.put(
        keys::managed_entry_key(position.ledger_id, position.entry_id),
        bincode::serialize(&location).unwrap(),
    )
    .unwrap();

    let entry_log = open_test_entry_log(&db_path);
    let ledger = RocksDBManagedLedger::open("ledger-a", Arc::clone(&db), entry_log).unwrap();

    assert_eq!(ledger.read_entry(&position), None);
}

#[test]
fn managed_ledger_next_entry_id_is_derived_from_last_ledger_entries() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("ledger-next-entry");

    {
        let db = open_test_db(&db_path);
        let entry_log = open_test_entry_log(&db_path);
        let mut ledger = RocksDBManagedLedger::open("ledger-a", db, entry_log).unwrap();
        assert_eq!(ledger.add_entry(b"first").unwrap().entry_id, 0);
        assert_eq!(ledger.add_entry(b"second").unwrap().entry_id, 1);
    }

    let db = open_test_db(&db_path);
    let entry_log = open_test_entry_log(&db_path);
    let mut ledger = RocksDBManagedLedger::open("ledger-a", db, entry_log).unwrap();
    let third_position = ledger.add_entry(b"third").unwrap();

    assert_eq!(third_position.ledger_id, 0);
    assert_eq!(third_position.entry_id, 2);
    assert_eq!(
        ledger.read_entry(&third_position).as_deref(),
        Some(b"third".as_slice())
    );
}

#[test]
fn managed_ledger_rolls_over_after_max_entries_like_pulsar() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("ledger-rollover");
    let db = open_test_db(&db_path);
    let config = ManagedLedgerConfig {
        max_entries_per_ledger: Some(2),
        ..ManagedLedgerConfig::default()
    };
    let entry_log = open_test_entry_log(&db_path);
    let mut factory = RocksDBManagedLedgerFactory::new(Arc::clone(&db), entry_log);

    {
        let mut ledger = factory.open("ledger-a", &config).unwrap();
        assert_eq!(ledger.add_entry(b"first").unwrap(), position(0, 0));
        assert_eq!(ledger.add_entry(b"second").unwrap(), position(0, 1));
        assert_eq!(ledger.add_entry(b"third").unwrap(), position(1, 0));
    }

    let ledger = factory.open("ledger-a", &config).unwrap();

    assert_eq!(ledger.info.ledgers.len(), 2);
    assert_eq!(ledger.info.ledgers[0].ledger_id, 0);
    assert_eq!(ledger.info.ledgers[0].entries, 2);
    assert_eq!(ledger.info.ledgers[1].ledger_id, 1);
    assert_eq!(ledger.info.ledgers[1].entries, 1);
    assert_eq!(
        ledger.read_entry(&position(0, 0)).as_deref(),
        Some(b"first".as_slice())
    );
    assert_eq!(
        ledger.read_entry(&position(0, 1)).as_deref(),
        Some(b"second".as_slice())
    );
    assert_eq!(
        ledger.read_entry(&position(1, 0)).as_deref(),
        Some(b"third".as_slice())
    );
}

#[test]
fn managed_ledger_rollover_metadata_is_persisted_in_rocksdb() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("ledger-rollover-metadata");
    let db = open_test_db(&db_path);
    let config = ManagedLedgerConfig {
        max_entries_per_ledger: Some(2),
        ..ManagedLedgerConfig::default()
    };
    let entry_log = open_test_entry_log(&db_path);
    let mut factory = RocksDBManagedLedgerFactory::new(Arc::clone(&db), entry_log);

    {
        let mut ledger = factory.open("ledger-a", &config).unwrap();
        ledger.add_entry(b"first").unwrap();
        ledger.add_entry(b"second").unwrap();
        ledger.add_entry(b"third").unwrap();
    }

    let info = read_managed_ledger_info(&db, "ledger-a");

    assert_eq!(info.ledgers.len(), 2);
    assert_eq!(info.ledgers[0].ledger_id, 0);
    assert_eq!(info.ledgers[0].entries, 2);
    assert_eq!(
        info.ledgers[0].size,
        b"first".len() as u64 + b"second".len() as u64
    );
    assert!(info.ledgers[0].timestamp > 0);
    assert_eq!(info.ledgers[1].ledger_id, 1);
    assert_eq!(info.ledgers[1].entries, 1);
    assert_eq!(info.ledgers[1].size, b"third".len() as u64);
    assert_eq!(info.ledgers[1].timestamp, 0);
}

#[test]
fn managed_ledger_reopen_continues_from_persisted_rollover_metadata() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("ledger-rollover-reopen");
    let config = ManagedLedgerConfig {
        max_entries_per_ledger: Some(2),
        ..ManagedLedgerConfig::default()
    };

    {
        let db = open_test_db(&db_path);
        let entry_log = open_test_entry_log(&db_path);
        let mut factory = RocksDBManagedLedgerFactory::new(db, entry_log);
        let mut ledger = factory.open("ledger-a", &config).unwrap();
        assert_eq!(ledger.add_entry(b"first").unwrap(), position(0, 0));
        assert_eq!(ledger.add_entry(b"second").unwrap(), position(0, 1));
    }

    {
        let db = open_test_db(&db_path);
        let entry_log = open_test_entry_log(&db_path);
        let mut factory = RocksDBManagedLedgerFactory::new(Arc::clone(&db), entry_log);
        let mut ledger = factory.open("ledger-a", &config).unwrap();
        assert_eq!(ledger.add_entry(b"third").unwrap(), position(1, 0));
        assert_eq!(ledger.add_entry(b"fourth").unwrap(), position(1, 1));
        assert_eq!(ledger.add_entry(b"fifth").unwrap(), position(2, 0));

        let info = read_managed_ledger_info(&db, "ledger-a");
        assert_eq!(info.ledgers.len(), 3);
        assert_eq!(info.ledgers[0].entries, 2);
        assert_eq!(info.ledgers[1].entries, 2);
        assert_eq!(info.ledgers[2].entries, 1);
    }

    let db = open_test_db(&db_path);
    let entry_log = open_test_entry_log(&db_path);
    let ledger =
        RocksDBManagedLedger::open_with_config("ledger-a", db, entry_log, &config).unwrap();
    assert_eq!(
        ledger.read_entry(&position(0, 0)).as_deref(),
        Some(b"first".as_slice())
    );
    assert_eq!(
        ledger.read_entry(&position(1, 0)).as_deref(),
        Some(b"third".as_slice())
    );
    assert_eq!(
        ledger.read_entry(&position(2, 0)).as_deref(),
        Some(b"fifth".as_slice())
    );
}

#[test]
fn managed_ledger_ids_are_global_across_topics() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("ledger-id-global");
    let db = open_test_db(&db_path);
    let entry_log = open_test_entry_log(&db_path);

    let mut orders = RocksDBManagedLedger::open(
        "public/default/persistent/orders",
        Arc::clone(&db),
        Arc::clone(&entry_log),
    )
    .unwrap();
    let mut payments = RocksDBManagedLedger::open(
        "public/default/persistent/payments",
        Arc::clone(&db),
        Arc::clone(&entry_log),
    )
    .unwrap();

    let orders_position = orders.add_entry(b"order-1").unwrap();
    let payments_position = payments.add_entry(b"payment-1").unwrap();

    assert_ne!(orders_position.ledger_id, payments_position.ledger_id);
    assert_eq!(orders_position.entry_id, 0);
    assert_eq!(payments_position.entry_id, 0);
    assert!(db
        .get(keys::managed_entry_key(
            orders_position.ledger_id,
            orders_position.entry_id
        ))
        .unwrap()
        .is_some());
    assert!(db
        .get(keys::managed_entry_key(
            payments_position.ledger_id,
            payments_position.entry_id
        ))
        .unwrap()
        .is_some());
    assert!(db
        .get(format!(
            "managed_entry|public/default/persistent/orders|{:020}|{:020}",
            orders_position.ledger_id, orders_position.entry_id
        ))
        .unwrap()
        .is_none());
}

#[test]
fn rolled_ledgers_allocate_global_ledger_ids() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("ledger-rollover-global-id");
    let db = open_test_db(&db_path);
    let config = ManagedLedgerConfig {
        max_entries_per_ledger: Some(1),
        ..ManagedLedgerConfig::default()
    };
    let entry_log = open_test_entry_log(&db_path);
    let mut factory = RocksDBManagedLedgerFactory::new(Arc::clone(&db), entry_log);

    let mut orders = factory.open("orders", &config).unwrap();
    let mut payments = factory.open("payments", &config).unwrap();

    let orders_first = orders.add_entry(b"order-1").unwrap();
    let payments_first = payments.add_entry(b"payment-1").unwrap();
    let orders_second = orders.add_entry(b"order-2").unwrap();

    assert_ne!(orders_first.ledger_id, payments_first.ledger_id);
    assert_ne!(orders_second.ledger_id, orders_first.ledger_id);
    assert_ne!(orders_second.ledger_id, payments_first.ledger_id);
    assert_eq!(orders_second.entry_id, 0);

    let orders_info = read_managed_ledger_info(&db, "orders");
    let payments_info = read_managed_ledger_info(&db, "payments");
    assert_eq!(orders_info.ledgers[0].ledger_id, orders_first.ledger_id);
    assert_eq!(orders_info.ledgers[1].ledger_id, orders_second.ledger_id);
    assert_eq!(payments_info.ledgers[0].ledger_id, payments_first.ledger_id);
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
    let mut ledger = factory.open("ledger-a", &config).unwrap();
    let first = ledger.add_entry(b"first").unwrap();
    let second = ledger.add_entry(b"second").unwrap();
    let third = ledger.add_entry(b"third").unwrap();
    let mut cursor = ledger.open_cursor("sub-a").unwrap();

    ack_managed_cursor_shared(&mut cursor, third.clone(), &ledger.info).unwrap();
    assert_eq!(cursor.state().mark_delete, None);
    assert!(cursor.state().individually_deleted_entries.contains(&third));

    ack_managed_cursor_shared(&mut cursor, first, &ledger.info).unwrap();
    ack_managed_cursor_shared(&mut cursor, second, &ledger.info).unwrap();

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
        let mut ledger = RocksDBManagedLedger::open("ledger-a", db, entry_log).unwrap();
        let mut cursor = ledger.open_cursor("sub-a").unwrap();
        cursor.mark_delete(mark_delete.clone()).unwrap();
    }

    let db = open_test_db(&db_path);
    let entry_log = open_test_entry_log(&db_path);
    let mut ledger = RocksDBManagedLedger::open("ledger-a", db, entry_log).unwrap();
    let cursor = ledger.open_cursor("sub-a").unwrap();

    assert_eq!(cursor.state().mark_delete, Some(mark_delete));
}

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
