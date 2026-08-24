use crate::position::ManagedLedgerPosition;
use crate::range_set::RangeSet;
use anyhow::Result;

/// Managed-cursor state skeleton.
///
/// This mirrors the shape of the current shared-subscription cursor model and
/// gives future durable cursor implementations a stable target type.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManagedCursorState {
    pub mark_delete: Option<ManagedLedgerPosition>,
    /// Out-of-order acknowledgements past the mark-delete frontier, stored as
    /// coalesced ranges (see `RangeSet`) so memory stays O(ranges) even when
    /// millions of individual acks arrive out of order.
    pub individually_deleted_entries: RangeSet<ManagedLedgerPosition>,
}

/// Cursor abstraction for managed-ledger style persistence.
pub trait ManagedCursor: Send + Sync {
    fn name(&self) -> &str;

    fn state(&self) -> &ManagedCursorState;

    fn mark_delete(&mut self, position: ManagedLedgerPosition) -> Result<()>;

    fn delete_individual(&mut self, position: ManagedLedgerPosition) -> Result<()>;

    fn reset_cursor(&mut self, position: Option<ManagedLedgerPosition>) -> Result<()>;
}

/// Shared-subscription cursor state used by the current in-memory runtime.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubscriptionCursor {
    pub mark_delete: Option<u64>,
    pub acked_holes: RangeSet<u64>,
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

/// Advances the mark-delete frontier across contiguous acknowledged ranges.
///
/// `take_covering` removes the whole covering range at once, so a long
/// sequential ack streak is consumed in one step per stored range instead of
/// one position at a time.
pub fn advance_mark_delete(cursor: &mut SubscriptionCursor) {
    loop {
        let next_expected = cursor.mark_delete.map_or(0, |mark_delete| mark_delete + 1);
        match cursor.acked_holes.take_covering(&next_expected) {
            Some(end) => cursor.mark_delete = Some(end),
            None => break,
        }
    }
}

pub fn ack_shared(cursor: &mut SubscriptionCursor, entry: u64) -> (Option<u64>, usize) {
    if is_message_acknowledged(Some(cursor), entry) {
        // Second value is the number of stored ranges (not individual holes).
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

    // Second value is the number of stored ranges (not individual holes).
    (cursor.mark_delete, cursor.acked_holes.len())
}
