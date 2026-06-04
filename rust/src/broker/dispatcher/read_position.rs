use crate::broker::service::SharedStorage;
use crate::storage::{ManagedLedgerPosition, MessageId};
use std::sync::RwLock;

pub type DispatchError = Box<dyn std::error::Error + Send + Sync>;

pub struct ReadCandidate {
    pub message_id: MessageId,
    pub payload: Vec<u8>,
    pub next_position: ManagedLedgerPosition,
}

fn logical_next(message_id: &MessageId) -> ManagedLedgerPosition {
    ManagedLedgerPosition {
        ledger_id: message_id.ledger,
        entry_id: message_id.entry + 1,
        partition: message_id.partition,
    }
}

pub async fn next_unacked_candidate(
    storage: SharedStorage,
    topic: &str,
    subscription: &str,
    read_position: &RwLock<Option<ManagedLedgerPosition>>,
) -> Result<Option<ReadCandidate>, DispatchError> {
    loop {
        let current_read_position = {
            let guard = read_position.read().unwrap();
            guard.clone()
        };

        let pos = match current_read_position {
            Some(pos) => pos,
            None => {
                let first_unacked = {
                    let guard = storage.lock().await;
                    guard.first_unacked_position(topic, subscription)?
                };
                *read_position.write().unwrap() = first_unacked.clone();

                match first_unacked {
                    Some(pos) => pos,
                    None => return Ok(None),
                }
            }
        };

        let Some((message_id, payload, next_position, already_acked)) = ({
            let guard = storage.lock().await;
            let batch = guard.read_from(topic, &pos, 1)?;

            if let Some((message_id, payload)) = batch.into_iter().next() {
                let current_position = ManagedLedgerPosition::from(&message_id);
                let next_position = guard
                    .get_next_position(topic, &current_position)?
                    .unwrap_or_else(|| logical_next(&message_id));
                let already_acked = guard.is_acknowledged(topic, subscription, &message_id)?;
                Some((message_id, payload, next_position, already_acked))
            } else {
                None
            }
        }) else {
            return Ok(None);
        };

        if already_acked {
            *read_position.write().unwrap() = Some(next_position);
            continue;
        }

        return Ok(Some(ReadCandidate {
            message_id,
            payload,
            next_position,
        }));
    }
}

pub fn commit_read_position(
    read_position: &RwLock<Option<ManagedLedgerPosition>>,
    next_position: ManagedLedgerPosition,
) {
    *read_position.write().unwrap() = Some(next_position);
}
