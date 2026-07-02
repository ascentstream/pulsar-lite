//! Integration tests for the in-memory managed-ledger small-trait surface
//! (`ManagedLedgerFactory`/`ManagedLedger`).

use pulsar_lite_storage_managed_ledger::{
    InMemoryManagedLedger, InMemoryManagedLedgerFactory, ManagedCursor, ManagedLedger,
    ManagedLedgerConfig, ManagedLedgerFactory, ManagedLedgerPosition,
};

fn open_ledger() -> InMemoryManagedLedger {
    let mut factory = InMemoryManagedLedgerFactory::new();
    factory
        .open("persistent://public/default/ledger", &ManagedLedgerConfig::default())
        .unwrap()
}

#[test]
fn factory_open_returns_ledger_with_name() {
    let ledger = open_ledger();
    assert_eq!(ledger.name(), "persistent://public/default/ledger");
}

#[test]
fn add_entry_appends_and_returns_incrementing_position() {
    let mut ledger = open_ledger();

    let p0 = ledger.add_entry(b"m0").unwrap();
    let p1 = ledger.add_entry(b"m1").unwrap();

    assert_eq!(p0.entry_id, 0);
    assert_eq!(p1.entry_id, 1);
    assert_eq!(p0.ledger_id, p1.ledger_id);
}

#[test]
fn read_entry_returns_payload_for_known_position() {
    let mut ledger = open_ledger();
    let p0 = ledger.add_entry(b"m0").unwrap();
    let _p1 = ledger.add_entry(b"m1").unwrap();

    let payload = ledger.read_entry(&p0).unwrap();
    assert_eq!(payload, b"m0".to_vec());
}

#[test]
fn read_entry_returns_none_for_unknown_entry_id() {
    let mut ledger = open_ledger();
    let p0 = ledger.add_entry(b"m0").unwrap();

    let unknown = ManagedLedgerPosition {
        ledger_id: p0.ledger_id,
        entry_id: 999,
        partition: -1,
    };
    assert!(ledger.read_entry(&unknown).is_none());
}

#[test]
fn read_entry_returns_none_for_wrong_ledger_id() {
    let mut ledger = open_ledger();
    let p0 = ledger.add_entry(b"m0").unwrap();

    let wrong_ledger = ManagedLedgerPosition {
        ledger_id: p0.ledger_id + 999,
        entry_id: p0.entry_id,
        partition: -1,
    };
    assert!(ledger.read_entry(&wrong_ledger).is_none());
}

#[test]
fn open_cursor_returns_cursor_with_name() {
    let mut ledger = open_ledger();
    let cursor = ledger.open_cursor("sub1").unwrap();
    assert_eq!(cursor.name(), "sub1");
}
