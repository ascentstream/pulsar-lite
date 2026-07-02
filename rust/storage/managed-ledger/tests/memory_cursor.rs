//! Integration tests for the in-memory managed-cursor small-trait surface
//! (`ManagedCursor`).

use pulsar_lite_storage_managed_ledger::{
    InMemoryManagedCursor, ManagedCursor, ManagedCursorState, ManagedLedgerPosition,
};

fn position(entry_id: u64) -> ManagedLedgerPosition {
    ManagedLedgerPosition {
        ledger_id: 0,
        entry_id,
        partition: -1,
    }
}

#[test]
fn new_cursor_has_empty_state() {
    let cursor = InMemoryManagedCursor::new("sub1");
    assert_eq!(cursor.name(), "sub1");
    assert_eq!(cursor.state(), &ManagedCursorState::default());
}

#[test]
fn mark_delete_sets_mark_delete_position() {
    let mut cursor = InMemoryManagedCursor::new("sub1");
    let p = position(5);
    cursor.mark_delete(p.clone()).unwrap();
    assert_eq!(cursor.state().mark_delete, Some(p));
    assert!(cursor.state().individually_deleted_entries.is_empty());
}

#[test]
fn delete_individual_inserts_into_individually_deleted() {
    let mut cursor = InMemoryManagedCursor::new("sub1");
    let p0 = position(0);
    let p1 = position(1);
    cursor.delete_individual(p0.clone()).unwrap();
    cursor.delete_individual(p1.clone()).unwrap();
    assert!(cursor.state().individually_deleted_entries.contains(&p0));
    assert!(cursor.state().individually_deleted_entries.contains(&p1));
    assert!(cursor.state().mark_delete.is_none());
}

#[tokio::test]
async fn async_reset_cursor_with_position_sets_mark_delete_and_clears_individual() {
    let mut cursor = InMemoryManagedCursor::new("sub1");
    cursor.delete_individual(position(0)).unwrap();
    cursor.delete_individual(position(1)).unwrap();

    let p = position(3);
    cursor.async_reset_cursor(Some(p.clone())).await.unwrap();
    assert_eq!(cursor.state().mark_delete, Some(p));
    assert!(cursor.state().individually_deleted_entries.is_empty());
}

#[tokio::test]
async fn async_reset_cursor_with_none_clears_everything() {
    let mut cursor = InMemoryManagedCursor::new("sub1");
    cursor.mark_delete(position(5)).unwrap();
    cursor.delete_individual(position(2)).unwrap();

    cursor.async_reset_cursor(None).await.unwrap();
    assert!(cursor.state().mark_delete.is_none());
    assert!(cursor.state().individually_deleted_entries.is_empty());
}
