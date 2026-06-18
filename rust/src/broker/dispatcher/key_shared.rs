/*
 * Key_Shared Dispatcher
 * Routes persistent messages with the same sticky key to the same active consumer.
 */

use super::read_position::{commit_read_position, next_unacked_candidate, ReadCandidate};
use crate::broker::dispatcher::Dispatcher;
use crate::broker::service::topic::{
    KeySharedHashRange, KeySharedMode, KeySharedPolicy, SubscriptionType,
};
use crate::broker::service::{Consumer, SharedStorage};
use crate::protocol::codec::proto::pulsar::MessageMetadata;
use crate::storage::{ManagedLedgerPosition, MessageId};
use prost::Message;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

const DISPATCHER_MAX_ROUND_ROBIN_BATCH_SIZE: u32 = 20;

pub struct KeySharedDispatcher {
    consumers_by_id: HashMap<u64, Arc<Consumer>>,
    auto_split_assignments: Vec<(KeySharedHashRange, Arc<Consumer>)>,
    sticky_assignments: Vec<(KeySharedHashRange, Arc<Consumer>)>,
    key_shared_policy: KeySharedPolicy,
    total_available_permits: AtomicU32,
    read_position: RwLock<Option<ManagedLedgerPosition>>,
    messages_to_redeliver: RwLock<BTreeMap<MessageId, u32>>,
}

impl KeySharedDispatcher {
    pub fn new(key_shared_policy: Option<KeySharedPolicy>) -> Self {
        Self {
            consumers_by_id: HashMap::new(),
            auto_split_assignments: Vec::new(),
            sticky_assignments: Vec::new(),
            key_shared_policy: key_shared_policy.unwrap_or(KeySharedPolicy {
                mode: KeySharedMode::AutoSplit,
                ranges: Vec::new(),
                allow_out_of_order_delivery: false,
            }),
            total_available_permits: AtomicU32::new(0),
            read_position: RwLock::new(None),
            messages_to_redeliver: RwLock::new(BTreeMap::new()),
        }
    }

    fn has_same_key_shared_policy(&self, policy: Option<&KeySharedPolicy>) -> bool {
        let Some(policy) = policy else {
            return self.key_shared_policy.mode == KeySharedMode::AutoSplit;
        };
        policy.mode == self.key_shared_policy.mode
    }

    fn subtract_total_permits(&self, permits: u32) {
        let _ = self.total_available_permits.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(permits)),
        );
    }

    fn active_assignments(&self) -> &[(KeySharedHashRange, Arc<Consumer>)] {
        match self.key_shared_policy.mode {
            KeySharedMode::AutoSplit => &self.auto_split_assignments,
            KeySharedMode::Sticky => &self.sticky_assignments,
        }
    }

    fn sticky_ranges_for_consumer(consumer: &Arc<Consumer>) -> Vec<KeySharedHashRange> {
        consumer
            .key_shared_policy()
            .map(|policy| policy.ranges.clone())
            .unwrap_or_default()
    }

    fn rebuild_auto_split_assignments(&mut self) {
        const RANGE_SIZE: i32 = (2 << 15) - 1;
        let mut consumers: Vec<_> = self.consumers_by_id.values().cloned().collect();
        consumers.sort_by_key(|consumer| consumer.consumer_id);

        if consumers.is_empty() {
            self.auto_split_assignments.clear();
            return;
        }

        let total = consumers.len() as i32;
        let mut assignments = Vec::with_capacity(consumers.len());
        for (index, consumer) in consumers.into_iter().enumerate() {
            let index = index as i32;
            let start = (index * (RANGE_SIZE + 1)) / total;
            let end = (((index + 1) * (RANGE_SIZE + 1)) / total) - 1;
            assignments.push((KeySharedHashRange { start, end }, consumer));
        }
        self.auto_split_assignments = assignments;
    }

    fn rebuild_sticky_assignments(&mut self) {
        let mut assignments = Vec::new();
        for consumer in self.consumers_by_id.values() {
            for range in Self::sticky_ranges_for_consumer(consumer) {
                assignments.push((range, consumer.clone()));
            }
        }
        assignments.sort_by_key(|(range, consumer)| (range.start, range.end, consumer.consumer_id));
        self.sticky_assignments = assignments;
    }

    fn rebuild_assignments(&mut self) {
        self.rebuild_auto_split_assignments();
        self.rebuild_sticky_assignments();
    }

    fn resolve_sticky_key(metadata: &[u8]) -> Vec<u8> {
        let Ok(metadata) = MessageMetadata::decode(metadata) else {
            return Vec::new();
        };

        if let Some(ordering_key) = metadata.ordering_key {
            return ordering_key;
        }
        if let Some(partition_key) = metadata.partition_key {
            return partition_key.into_bytes();
        }
        if !metadata.producer_name.is_empty() {
            return format!("{}-{}", metadata.producer_name, metadata.sequence_id).into_bytes();
        }

        Vec::new()
    }

    fn murmur3_32(bytes: &[u8], seed: u32) -> u32 {
        const C1: u32 = 0xcc9e2d51;
        const C2: u32 = 0x1b873593;

        let mut hash = seed;
        let mut chunks = bytes.chunks_exact(4);

        for chunk in &mut chunks {
            let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
            k = k.wrapping_mul(C1);
            k = k.rotate_left(15);
            k = k.wrapping_mul(C2);

            hash ^= k;
            hash = hash.rotate_left(13);
            hash = hash.wrapping_mul(5).wrapping_add(0xe6546b64);
        }

        let tail = chunks.remainder();
        let mut k1 = 0u32;
        match tail.len() {
            3 => {
                k1 ^= (tail[2] as u32) << 16;
                k1 ^= (tail[1] as u32) << 8;
                k1 ^= tail[0] as u32;
            }
            2 => {
                k1 ^= (tail[1] as u32) << 8;
                k1 ^= tail[0] as u32;
            }
            1 => {
                k1 ^= tail[0] as u32;
            }
            _ => {}
        }
        if !tail.is_empty() {
            k1 = k1.wrapping_mul(C1);
            k1 = k1.rotate_left(15);
            k1 = k1.wrapping_mul(C2);
            hash ^= k1;
        }

        hash ^= bytes.len() as u32;
        hash ^= hash >> 16;
        hash = hash.wrapping_mul(0x85ebca6b);
        hash ^= hash >> 13;
        hash = hash.wrapping_mul(0xc2b2ae35);
        hash ^= hash >> 16;
        hash
    }

    fn sticky_key_hash(sticky_key: &[u8]) -> i32 {
        const RANGE_SIZE: u32 = 2 << 15;
        (Self::murmur3_32(sticky_key, 0) % RANGE_SIZE) as i32
    }

    fn select_consumer(&self, metadata: &[u8]) -> Option<Arc<Consumer>> {
        let sticky_key = Self::resolve_sticky_key(metadata);
        let sticky_key_hash = Self::sticky_key_hash(&sticky_key);

        for (range, consumer) in self.active_assignments() {
            if sticky_key_hash >= range.start
                && sticky_key_hash <= range.end
                && consumer.available_permits_now() > 0
                && consumer.is_writable()
            {
                return Some(consumer.clone());
            }
        }

        None
    }

    pub fn add_to_redelivery_queue(&self, entries: Vec<(MessageId, u32)>) {
        let mut redeliver = self.messages_to_redeliver.write().unwrap();
        for (message_id, redelivery_count) in entries {
            redeliver
                .entry(message_id)
                .and_modify(|count| *count = (*count).max(redelivery_count))
                .or_insert(redelivery_count);
        }
    }

    fn pop_redelivery_message(&self) -> Option<(MessageId, u32)> {
        self.messages_to_redeliver.write().unwrap().pop_first()
    }

    fn restore_redelivery_message(&self, message_id: MessageId, redelivery_count: u32) {
        self.add_to_redelivery_queue(vec![(message_id, redelivery_count)]);
    }

    async fn is_message_pending(&self, message_id: &MessageId) -> bool {
        for consumer in self.consumers_by_id.values() {
            if consumer.has_pending_ack(message_id).await {
                return true;
            }
        }
        false
    }

    async fn next_dispatchable_message(
        &self,
        storage: SharedStorage,
        topic: &str,
        subscription: &str,
    ) -> Result<Option<ReadCandidate>, Box<dyn std::error::Error + Send + Sync>> {
        loop {
            let Some(candidate) =
                next_unacked_candidate(storage.clone(), topic, subscription, &self.read_position)
                    .await?
            else {
                return Ok(None);
            };

            if self.is_message_pending(&candidate.message_id).await {
                commit_read_position(&self.read_position, candidate.next_position);
                continue;
            }

            return Ok(Some(candidate));
        }
    }

    pub fn on_message_acknowledged(&self, message_id: &MessageId) {
        self.messages_to_redeliver
            .write()
            .unwrap()
            .remove(message_id);
    }

    pub async fn on_ack_state_updated(
        &self,
        storage: SharedStorage,
        topic: &str,
        subscription: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let queued_message_ids = {
            let redeliver = self.messages_to_redeliver.read().unwrap();
            redeliver.keys().cloned().collect::<Vec<_>>()
        };

        let (acked_redeliveries, first_unacked) = {
            let guard = storage.lock().await;
            let mut acked_redeliveries = Vec::new();
            for message_id in queued_message_ids {
                if guard.is_acknowledged(topic, subscription, &message_id)? {
                    acked_redeliveries.push(message_id);
                }
            }
            let first_unacked = guard.first_unacked_position(topic, subscription)?;
            (acked_redeliveries, first_unacked)
        };

        if !acked_redeliveries.is_empty() {
            let mut redeliver = self.messages_to_redeliver.write().unwrap();
            for message_id in acked_redeliveries {
                redeliver.remove(&message_id);
            }
        }

        let mut read_position = self.read_position.write().unwrap();
        match (&*read_position, first_unacked) {
            (Some(current), Some(first_unacked)) if current < &first_unacked => {
                *read_position = Some(first_unacked);
            }
            (Some(_), None) => {
                *read_position = None;
            }
            _ => {}
        }

        Ok(())
    }

    pub async fn remove_consumer_with_recovery(
        &mut self,
        consumer_id: u64,
        _storage: SharedStorage,
        _topic: &str,
        _subscription: &str,
    ) -> Option<Arc<Consumer>> {
        let consumer = self.consumers_by_id.remove(&consumer_id);
        if let Some(consumer) = &consumer {
            self.subtract_total_permits(consumer.available_permits_now());
            consumer.close_pending_acks();
            let pending = consumer.drain_pending_acks().await;
            let mut recovered = Vec::with_capacity(pending.len());
            for (message_id, pending_ack) in pending {
                recovered.push((message_id, pending_ack.redelivery_count));
            }
            self.add_to_redelivery_queue(recovered);
        }
        self.rebuild_assignments();
        consumer
    }
}

impl Dispatcher for KeySharedDispatcher {
    fn get_type(&self) -> SubscriptionType {
        SubscriptionType::KeyShared
    }

    fn is_consumer_connected(&self) -> bool {
        !self.consumers_by_id.is_empty()
    }

    fn get_consumers(&self) -> Vec<Arc<Consumer>> {
        self.consumers_by_id.values().cloned().collect()
    }

    fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String> {
        if self.consumers_by_id.contains_key(&consumer.consumer_id) {
            return Err(format!(
                "Consumer {} already exists in key-shared dispatcher",
                consumer.consumer_id
            ));
        }
        if !self.has_same_key_shared_policy(consumer.key_shared_policy().as_ref()) {
            return Err(
                "Consumer key shared policy is incompatible with the dispatcher".to_string(),
            );
        }
        self.consumers_by_id.insert(consumer.consumer_id, consumer);
        self.rebuild_assignments();
        Ok(())
    }

    fn remove_consumer(&mut self, consumer_id: u64) -> Option<Arc<Consumer>> {
        let consumer = self.consumers_by_id.remove(&consumer_id);
        if let Some(consumer) = &consumer {
            self.subtract_total_permits(consumer.available_permits_now());
        }
        self.rebuild_assignments();
        consumer
    }

    fn init_read_position(&self, pos: Option<ManagedLedgerPosition>) {
        *self.read_position.write().unwrap() = pos;
    }

    fn consumer_flow(&self, consumer_id: u64, additional_permits: u32) {
        if self.consumers_by_id.contains_key(&consumer_id) {
            self.total_available_permits
                .fetch_add(additional_permits, Ordering::Relaxed);
        }
    }

    async fn dispatch_messages(
        &self,
        storage: SharedStorage,
        topic: String,
        subscription: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let max_batch = self
            .total_available_permits
            .load(Ordering::Relaxed)
            .min(DISPATCHER_MAX_ROUND_ROBIN_BATCH_SIZE);
        if max_batch == 0 {
            return Ok(());
        }

        for _ in 0..max_batch {
            if let Some((message_id, redelivery_count)) = self.pop_redelivery_message() {
                let entry = {
                    let guard = storage.lock().await;
                    if guard.is_acknowledged(&topic, &subscription, &message_id)? {
                        continue;
                    }
                    guard.get_message_entry_by_id(&topic, &message_id)
                };

                let Some(entry) = entry else {
                    continue;
                };
                let Some(consumer) = self.select_consumer(&entry.metadata) else {
                    self.restore_redelivery_message(entry.message_id, redelivery_count);
                    break;
                };
                if !consumer.use_permit().await {
                    self.restore_redelivery_message(entry.message_id, redelivery_count);
                    break;
                }
                self.subtract_total_permits(1);

                if consumer
                    .send_message(
                        entry.message_id.clone(),
                        entry.metadata,
                        entry.payload.clone(),
                        redelivery_count + 1,
                    )
                    .await
                {
                    consumer
                        .record_message_dispatched(entry.payload.len())
                        .await;
                } else {
                    self.restore_redelivery_message(entry.message_id, redelivery_count + 1);
                    consumer.add_permits(1).await;
                    self.total_available_permits.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                continue;
            }

            let Some(candidate) = self
                .next_dispatchable_message(storage.clone(), &topic, &subscription)
                .await?
            else {
                break;
            };

            let Some(consumer) = self.select_consumer(&candidate.metadata) else {
                break;
            };
            if !consumer.use_permit().await {
                break;
            }
            self.subtract_total_permits(1);

            if consumer
                .send_message(
                    candidate.message_id.clone(),
                    candidate.metadata,
                    candidate.payload.clone(),
                    0,
                )
                .await
            {
                commit_read_position(&self.read_position, candidate.next_position);
                consumer
                    .record_message_dispatched(candidate.payload.len())
                    .await;
            } else {
                self.restore_redelivery_message(candidate.message_id, 0);
                commit_read_position(&self.read_position, candidate.next_position);
                consumer.add_permits(1).await;
                self.total_available_permits.fetch_add(1, Ordering::Relaxed);
                break;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::service::topic::{Subscription, SubscriptionRuntimeMode};
    use crate::broker::service::ConnectionWriteState;
    use crate::storage::Storage;
    use bytes::Bytes;
    use std::path::Path;
    use tokio::sync::{mpsc, Mutex, RwLock};

    fn create_test_storage() -> SharedStorage {
        Arc::new(Mutex::new(
            Storage::new_memory(Path::new("/tmp/test-key-shared-dispatcher-storage")).unwrap(),
        ))
    }

    fn create_consumer(consumer_id: u64) -> Arc<Consumer> {
        let subscription = Arc::new(RwLock::new(Subscription::new_with_options(
            "sub".to_string(),
            "persistent://public/default/key-shared".to_string(),
            SubscriptionType::KeyShared,
            SubscriptionRuntimeMode::Persistent,
            HashMap::new(),
            None,
            create_test_storage(),
        )));
        let (tx, _rx) = mpsc::channel(16);
        Arc::new(Consumer::new_with_options(
            consumer_id,
            format!("consumer-{consumer_id}"),
            subscription,
            "conn".to_string(),
            tx,
            Arc::new(ConnectionWriteState::new(64 * 1024, 32 * 1024)),
            0,
            None,
        ))
    }

    fn metadata_with_ordering_key(key: &str) -> Bytes {
        Bytes::from(
            MessageMetadata {
                ordering_key: Some(key.as_bytes().to_vec()),
                ..Default::default()
            }
            .encode_to_vec(),
        )
    }

    #[tokio::test]
    async fn removing_owner_requeues_pending_for_surviving_key_owner() {
        let mut dispatcher = KeySharedDispatcher::new(None);
        let owner = create_consumer(1);
        let survivor = create_consumer(2);
        dispatcher.add_consumer(owner.clone()).unwrap();
        dispatcher.add_consumer(survivor.clone()).unwrap();
        survivor.add_permits(1).await;
        dispatcher.consumer_flow(2, 1);

        let message_id = MessageId {
            ledger: 0,
            entry: 7,
            partition: -1,
        };
        owner.track_message_dispatched(&message_id, 0).await;

        let removed = dispatcher
            .remove_consumer_with_recovery(
                1,
                create_test_storage(),
                "persistent://public/default/key-shared",
                "sub",
            )
            .await
            .expect("owner should be removed");

        assert_eq!(removed.consumer_id, 1);
        assert!(!owner.has_pending_ack(&message_id).await);
        assert_eq!(dispatcher.pop_redelivery_message(), Some((message_id, 0)));

        let selected = dispatcher
            .select_consumer(&metadata_with_ordering_key("stable-key"))
            .expect("survivor should own key after rebuild");
        assert_eq!(selected.consumer_id, survivor.consumer_id);
    }
}
