use crate::broker::service::SharedStorage;
use crate::storage::ManagedLedgerPosition;

/// Compute the read position a SingleActive dispatcher should rewind to when
/// the active consumer disconnects.
pub async fn rewind_read_position(
    storage: SharedStorage,
    topic: &str,
    subscription: &str,
    pending_positions: impl Iterator<Item = ManagedLedgerPosition>,
) -> Option<ManagedLedgerPosition> {
    let first_unacked = storage
        .lock()
        .await
        .first_unacked_position(topic, subscription)
        .ok()
        .flatten();

    let first_pending = pending_positions.min();

    match (first_unacked, first_pending) {
        (Some(unacked), Some(pending)) => Some(std::cmp::min(unacked, pending)),
        (Some(unacked), None) => Some(unacked),
        (None, Some(pending)) => Some(pending),
        (None, None) => None,
    }
}
