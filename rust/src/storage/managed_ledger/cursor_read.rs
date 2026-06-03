use super::types::{ManagedLedgerPosition, MessageId};

pub fn cursor_subscription_key(topic: &str, subscription: &str) -> String {
    format!("{}:{}", topic, subscription)
}

pub fn position_at_or_after(
    candidate: &ManagedLedgerPosition,
    start: &ManagedLedgerPosition,
) -> bool {
    (candidate.ledger_id, candidate.entry_id) >= (start.ledger_id, start.entry_id)
}

pub fn first_unacked_from_messages(
    messages: &[(MessageId, Vec<u8>)],
    is_acknowledged: impl Fn(&MessageId) -> bool,
) -> Option<ManagedLedgerPosition> {
    for (message_id, _) in messages {
        if !is_acknowledged(message_id) {
            return Some(ManagedLedgerPosition::from(message_id));
        }
    }

    None
}

pub fn read_from_messages(
    messages: &[(MessageId, Vec<u8>)],
    from: &ManagedLedgerPosition,
    limit: usize,
) -> Vec<(MessageId, Vec<u8>)> {
    let mut out = Vec::new();
    for (message_id, payload) in messages {
        let pos = ManagedLedgerPosition::from(message_id);
        if !position_at_or_after(&pos, from) {
            continue;
        }
        out.push((message_id.clone(), payload.clone()));
        if out.len() >= limit {
            break;
        }
    }
    out
}

pub fn last_position_from_messages(
    messages: &[(MessageId, Vec<u8>)],
) -> Option<ManagedLedgerPosition> {
    messages
        .last()
        .map(|(id, _)| ManagedLedgerPosition::from(id))
}

pub fn next_position_single_ledger(
    current: &ManagedLedgerPosition,
) -> Option<ManagedLedgerPosition> {
    Some(ManagedLedgerPosition {
        ledger_id: current.ledger_id,
        entry_id: current.entry_id + 1,
        partition: current.partition,
    })
}
