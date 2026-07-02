use crate::position::ManagedLedgerPosition;
use anyhow::Result;
use std::collections::BTreeSet;
use std::future::Future;

/// Managed-cursor state skeleton.
///
/// This mirrors the shape of the current shared-subscription cursor model and
/// gives future durable cursor implementations a stable target type.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManagedCursorState {
    pub mark_delete: Option<ManagedLedgerPosition>,
    pub individually_deleted_entries: BTreeSet<ManagedLedgerPosition>,
}

/// Cursor abstraction for managed-ledger style persistence.
pub trait ManagedCursor: Send + Sync {
    fn name(&self) -> &str;

    fn state(&self) -> &ManagedCursorState;

    fn mark_delete(&mut self, position: ManagedLedgerPosition) -> Result<()>;

    fn delete_individual(&mut self, position: ManagedLedgerPosition) -> Result<()>;

    fn async_reset_cursor(
        &mut self,
        position: Option<ManagedLedgerPosition>,
    ) -> impl Future<Output = Result<()>> + Send;
}

/// Shared-subscription cursor state used by the current in-memory runtime.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubscriptionCursor {
    pub mark_delete: Option<u64>,
    pub acked_holes: BTreeSet<u64>,
}

pub fn is_message_acknowledged(cursor: Option<&SubscriptionCursor>, entry: u64) -> bool {
    cursor
        .map(|cursor| {
            cursor
                .mark_delete
                .is_some_and(|mark_delete| entry <= mark_delete)
                || cursor.acked_holes.contains(&entry)
        })
        .unwrap_or(false)
}

pub fn advance_mark_delete(cursor: &mut SubscriptionCursor) {
    let mut next_expected = cursor.mark_delete.map_or(0, |mark_delete| mark_delete + 1);
    while cursor.acked_holes.remove(&next_expected) {
        cursor.mark_delete = Some(next_expected);
        next_expected += 1;
    }
}

pub fn ack_shared(cursor: &mut SubscriptionCursor, entry: u64) -> (Option<u64>, usize) {
    if is_message_acknowledged(Some(cursor), entry) {
        return (cursor.mark_delete, cursor.acked_holes.len());
    }

    match cursor.mark_delete {
        None => {
            if entry == 0 {
                cursor.mark_delete = Some(0);
                advance_mark_delete(cursor);
            } else {
                cursor.acked_holes.insert(entry);
            }
        }
        Some(mark_delete) => {
            if entry == mark_delete + 1 {
                cursor.mark_delete = Some(entry);
                advance_mark_delete(cursor);
            } else if entry > mark_delete + 1 {
                cursor.acked_holes.insert(entry);
            }
        }
    }

    (cursor.mark_delete, cursor.acked_holes.len())
}
