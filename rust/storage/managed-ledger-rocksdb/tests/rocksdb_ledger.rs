//! RocksDB managed-ledger persistence and rollover tests.

mod common;

use common::*;
use pulsar_lite_storage_managed_ledger::{
    ManagedLedger, ManagedLedgerConfig, ManagedLedgerFactory,
};
use pulsar_lite_storage_managed_ledger_rocksdb::test_support::{
    keys, RocksDBManagedLedger, RocksDBManagedLedgerFactory, StoredEntryLocation,
    StoredManagedLedgerInfo,
};
use std::sync::Arc;
use tempfile::tempdir;

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

    assert_eq!(ledger.ledger_info().ledgers.len(), 2);
    assert_eq!(ledger.ledger_info().ledgers[0].ledger_id, 0);
    assert_eq!(ledger.ledger_info().ledgers[0].entries, 2);
    assert_eq!(ledger.ledger_info().ledgers[1].ledger_id, 1);
    assert_eq!(ledger.ledger_info().ledgers[1].entries, 1);
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
fn previous_position_handles_same_ledger_cross_ledger_and_before_first() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("previous-position");
    let db = open_test_db(&db_path);
    let config = ManagedLedgerConfig {
        max_entries_per_ledger: Some(2),
        ..ManagedLedgerConfig::default()
    };
    let entry_log = open_test_entry_log(&db_path);
    let mut factory = RocksDBManagedLedgerFactory::new(Arc::clone(&db), entry_log);

    let mut ledger = factory.open("ledger-a", &config).unwrap();
    ledger.add_entry(b"first").unwrap();
    ledger.add_entry(b"second").unwrap();
    ledger.add_entry(b"third").unwrap();

    // phase 1: entry_id > 0 -> same ledger
    assert_eq!(
        ledger.previous_position(&position(0, 1)),
        Some(position(0, 0))
    );
    // phase 2: entry_id == 0 -> The last entry of the previous non-empty ledger
    assert_eq!(
        ledger.previous_position(&position(1, 0)),
        Some(position(0, 1))
    );
    // phase 3: The first ledger's entry 0 -> None (before the first)
    assert_eq!(ledger.previous_position(&position(0, 0)), None);
}
