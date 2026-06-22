use crate::storage::MessageId;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct PendingAck {
    /// Dispatch time
    pub dispatched_at: Instant,
    /// Redelivery count
    pub redelivery_count: u32,
    /// Sticky key hash used by KeyShared ordered redelivery.
    pub sticky_key_hash: Option<i32>,
}

#[derive(Debug, Default)]
pub struct PendingAcksMap {
    // Map of consumer ID to pending acks
    inner: RwLock<BTreeMap<MessageId, PendingAck>>,
    closed: AtomicBool,
}

impl PendingAcksMap {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
            closed: AtomicBool::new(false),
        }
    }

    pub async fn add_pending_ack(
        &self,
        message_id: MessageId,
        redelivery_count: u32,
        sticky_key_hash: Option<i32>,
    ) -> bool {
        if self.closed.load(Ordering::Acquire) {
            return false;
        }
        let mut inner = self.inner.write().await;
        if self.closed.load(Ordering::Acquire) {
            return false;
        }
        inner.insert(
            message_id,
            PendingAck {
                dispatched_at: Instant::now(),
                redelivery_count,
                sticky_key_hash,
            },
        );
        true
    }

    pub async fn remove(&self, message_id: &MessageId) -> Option<PendingAck> {
        let mut inner = self.inner.write().await;
        inner.remove(message_id)
    }

    pub async fn contains(&self, message_id: &MessageId) -> bool {
        self.inner.read().await.contains_key(message_id)
    }

    pub async fn find_by_position(&self, ledger: u64, entry: u64) -> Option<MessageId> {
        self.inner
            .read()
            .await
            .keys()
            .find(|message_id| message_id.ledger == ledger && message_id.entry == entry)
            .cloned()
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    pub async fn drain(&self) -> Vec<(MessageId, PendingAck)> {
        let mut inner = self.inner.write().await;
        let drained = inner
            .iter()
            .map(|(message_id, ack)| (message_id.clone(), ack.clone()))
            .collect::<Vec<_>>();
        inner.clear();
        drained
    }

    pub async fn len(&self) -> usize {
        self.inner.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.inner.read().await.is_empty()
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(ledger: u64, entry: u64) -> MessageId {
        MessageId {
            ledger,
            entry,
            partition: -1,
        }
    }

    #[tokio::test]
    async fn pending_acks_map_tracks_remove_and_drain() {
        let map = PendingAcksMap::new();

        assert!(map.add_pending_ack(msg(1, 1), 0, None).await);
        assert!(map.contains(&msg(1, 1)).await);
        assert_eq!(map.len().await, 1);

        assert!(map.remove(&msg(1, 1)).await.is_some());
        assert!(!map.contains(&msg(1, 1)).await);

        assert!(map.add_pending_ack(msg(1, 2), 1, None).await);
        assert!(map.add_pending_ack(msg(1, 3), 2, None).await);

        let drained = map.drain().await;
        assert_eq!(drained.len(), 2);
        assert_eq!(map.len().await, 0);

        map.close();
        assert!(!map.add_pending_ack(msg(1, 4), 0, None).await);
    }

    #[tokio::test]
    async fn pending_acks_map_preserves_sticky_key_hash_on_drain() {
        let map = PendingAcksMap::new();

        assert!(map.add_pending_ack(msg(2, 1), 3, Some(1234)).await);

        let drained = map.drain().await;
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].1.redelivery_count, 3);
        assert_eq!(drained[0].1.sticky_key_hash, Some(1234));
    }
}
