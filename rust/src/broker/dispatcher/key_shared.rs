/*
 * Key_Shared Dispatcher
 * Routes persistent messages with the same sticky key to the same active consumer.
 */

use super::read_position::{commit_read_position, next_unacked_candidate, ReadCandidate};
use super::redelivery_controller::{RedeliveryController, RedeliveryEntry};
use super::sticky_key::sticky_key_hash_from_metadata;
use crate::broker::dispatcher::Dispatcher;
use crate::broker::service::topic::{
    KeySharedHashRange, KeySharedMode, KeySharedPolicy, SubscriptionType,
};
use crate::broker::service::{Consumer, SharedStorage};
use crate::storage::{ManagedLedgerPosition, MessageId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

const DISPATCHER_MAX_ROUND_ROBIN_BATCH_SIZE: u32 = 20;

type DispatchableRedelivery = Option<(
    RedeliveryEntry,
    crate::storage::StoredMessage,
    Arc<Consumer>,
)>;

pub struct KeySharedDispatcher {
    consumers_by_id: HashMap<u64, Arc<Consumer>>,
    auto_split_assignments: Vec<(KeySharedHashRange, Arc<Consumer>)>,
    sticky_assignments: Vec<(KeySharedHashRange, Arc<Consumer>)>,
    key_shared_policy: KeySharedPolicy,
    total_available_permits: AtomicU32,
    read_position: RwLock<Option<ManagedLedgerPosition>>,
    redelivery_controller: RwLock<RedeliveryController>,
}

impl KeySharedDispatcher {
    pub fn new(key_shared_policy: Option<KeySharedPolicy>) -> Self {
        let key_shared_policy = key_shared_policy.unwrap_or(KeySharedPolicy {
            mode: KeySharedMode::AutoSplit,
            ranges: Vec::new(),
            allow_out_of_order_delivery: false,
        });
        let block_hashes = !key_shared_policy.allow_out_of_order_delivery;
        Self {
            consumers_by_id: HashMap::new(),
            auto_split_assignments: Vec::new(),
            sticky_assignments: Vec::new(),
            key_shared_policy,
            total_available_permits: AtomicU32::new(0),
            read_position: RwLock::new(None),
            redelivery_controller: RwLock::new(RedeliveryController::new(block_hashes)),
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

    fn select_consumer_for_hash(&self, sticky_key_hash: i32) -> Option<Arc<Consumer>> {
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
        let entries = entries
            .into_iter()
            .map(|(message_id, redelivery_count)| RedeliveryEntry {
                message_id,
                redelivery_count,
                sticky_key_hash: None,
            })
            .collect();
        self.add_redelivery_entries(entries);
    }

    pub fn add_redelivery_entries(&self, entries: Vec<RedeliveryEntry>) {
        let mut controller = self.redelivery_controller.write().unwrap();
        for entry in entries {
            controller.add(entry);
        }
    }

    fn pop_dispatchable_redelivery_message(
        &self,
        storage: &crate::storage::Storage,
        topic: &str,
        subscription: &str,
    ) -> Result<DispatchableRedelivery, Box<dyn std::error::Error + Send + Sync>> {
        let queued = self.redelivery_controller.read().unwrap().queued_entries();
        for queued_entry in queued {
            if storage.is_acknowledged(topic, subscription, &queued_entry.message_id)? {
                self.redelivery_controller
                    .write()
                    .unwrap()
                    .remove(&queued_entry.message_id);
                continue;
            }
            let Some(stored) = storage.get_message_entry_by_id(topic, &queued_entry.message_id)
            else {
                self.redelivery_controller
                    .write()
                    .unwrap()
                    .remove(&queued_entry.message_id);
                continue;
            };
            let sticky_key_hash = queued_entry
                .sticky_key_hash
                .unwrap_or_else(|| sticky_key_hash_from_metadata(&stored.metadata));
            if self
                .redelivery_controller
                .read()
                .unwrap()
                .has_in_flight_hash(sticky_key_hash)
            {
                continue;
            }
            let Some(consumer) = self.select_consumer_for_hash(sticky_key_hash) else {
                continue;
            };
            let Some(entry) = self
                .redelivery_controller
                .write()
                .unwrap()
                .take_for_delivery_with_hash(&queued_entry.message_id, Some(sticky_key_hash))
            else {
                continue;
            };
            return Ok(Some((entry, stored, consumer)));
        }
        Ok(None)
    }

    fn restore_redelivery_message(&self, entry: RedeliveryEntry) {
        self.redelivery_controller.write().unwrap().restore(entry);
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
        self.redelivery_controller
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
            let controller = self.redelivery_controller.read().unwrap();
            controller.queued_message_ids()
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
            let mut controller = self.redelivery_controller.write().unwrap();
            for message_id in acked_redeliveries {
                controller.remove(&message_id);
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
                recovered.push(RedeliveryEntry {
                    message_id,
                    redelivery_count: pending_ack.redelivery_count + 1,
                    sticky_key_hash: pending_ack.sticky_key_hash,
                });
            }
            self.add_redelivery_entries(recovered);
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

    fn reset_after_seek(&self, pos: Option<ManagedLedgerPosition>) {
        self.init_read_position(pos);
        self.redelivery_controller.write().unwrap().clear();
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

        let mut remaining_dispatches = max_batch;
        while remaining_dispatches > 0 {
            let redelivery = {
                let guard = storage.lock().await;
                self.pop_dispatchable_redelivery_message(&guard, &topic, &subscription)?
            };

            if let Some((redelivery, entry, consumer)) = redelivery {
                let sticky_key_hash = redelivery
                    .sticky_key_hash
                    .unwrap_or_else(|| sticky_key_hash_from_metadata(&entry.metadata));
                if !consumer.use_permit().await {
                    self.restore_redelivery_message(RedeliveryEntry {
                        sticky_key_hash: Some(sticky_key_hash),
                        ..redelivery
                    });
                    break;
                }
                remaining_dispatches -= 1;
                self.subtract_total_permits(1);

                if consumer
                    .send_message_with_sticky_hash(
                        entry.message_id.clone(),
                        entry.metadata,
                        entry.payload.clone(),
                        redelivery.redelivery_count,
                        Some(sticky_key_hash),
                    )
                    .await
                {
                    consumer
                        .record_message_dispatched(entry.payload.len())
                        .await;
                } else {
                    self.restore_redelivery_message(RedeliveryEntry {
                        message_id: entry.message_id,
                        redelivery_count: redelivery.redelivery_count + 1,
                        sticky_key_hash: Some(sticky_key_hash),
                    });
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

            let sticky_key_hash = sticky_key_hash_from_metadata(&candidate.metadata);
            log::debug!(
                "NORMAL SEND {:?} hash={} blocked={}",
                candidate.message_id,
                sticky_key_hash,
                self.redelivery_controller
                    .read()
                    .unwrap()
                    .is_hash_blocked(sticky_key_hash)
            );
            if self
                .redelivery_controller
                .read()
                .unwrap()
                .is_hash_blocked(sticky_key_hash)
            {
                self.add_redelivery_entries(vec![RedeliveryEntry {
                    message_id: candidate.message_id,
                    redelivery_count: 0,
                    sticky_key_hash: Some(sticky_key_hash),
                }]);
                commit_read_position(&self.read_position, candidate.next_position);
                continue;
            }

            let Some(consumer) = self.select_consumer_for_hash(sticky_key_hash) else {
                break;
            };
            if !consumer.use_permit().await {
                break;
            }
            remaining_dispatches -= 1;
            self.subtract_total_permits(1);

            if consumer
                .send_message_with_sticky_hash(
                    candidate.message_id.clone(),
                    candidate.metadata,
                    candidate.payload.clone(),
                    0,
                    Some(sticky_key_hash),
                )
                .await
            {
                commit_read_position(&self.read_position, candidate.next_position);
                consumer
                    .record_message_dispatched(candidate.payload.len())
                    .await;
            } else {
                self.restore_redelivery_message(RedeliveryEntry {
                    message_id: candidate.message_id,
                    redelivery_count: 1,
                    sticky_key_hash: Some(sticky_key_hash),
                });
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
    use crate::broker::service::{ConnectionWriteState, PendingMessage};
    use crate::protocol::codec::proto::pulsar::MessageMetadata;
    use crate::storage::{CursorInitOptions, InitialPosition, Storage};
    use bytes::Bytes;
    use prost::Message;
    use std::path::Path;
    use std::time::Duration;
    use tokio::sync::{mpsc, Mutex, RwLock};
    use tokio::time::timeout;

    fn create_test_storage() -> SharedStorage {
        Arc::new(Mutex::new(
            Storage::new_memory(Path::new("/tmp/test-key-shared-dispatcher-storage")).unwrap(),
        ))
    }

    fn create_consumer(consumer_id: u64) -> Arc<Consumer> {
        create_consumer_with_receiver(consumer_id).0
    }

    fn create_consumer_with_receiver(
        consumer_id: u64,
    ) -> (Arc<Consumer>, mpsc::Receiver<(u64, PendingMessage)>) {
        let subscription = Arc::new(RwLock::new(Subscription::new_with_options(
            "sub".to_string(),
            "persistent://public/default/key-shared".to_string(),
            SubscriptionType::KeyShared,
            SubscriptionRuntimeMode::Persistent,
            HashMap::new(),
            None,
            create_test_storage(),
        )));
        let (tx, rx) = mpsc::channel(16);
        (
            Arc::new(Consumer::new_with_options(
                consumer_id,
                format!("consumer-{consumer_id}"),
                subscription,
                "conn".to_string(),
                tx,
                Arc::new(ConnectionWriteState::new(64 * 1024, 32 * 1024)),
                0,
                None,
            )),
            rx,
        )
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
        let queued = dispatcher
            .redelivery_controller
            .read()
            .unwrap()
            .queued_entries();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].message_id, message_id);
        assert_eq!(queued[0].redelivery_count, 1);

        let selected = dispatcher
            .select_consumer_for_hash(sticky_key_hash_from_metadata(&metadata_with_ordering_key(
                "stable-key",
            )))
            .expect("survivor should own key after rebuild");
        assert_eq!(selected.consumer_id, survivor.consumer_id);
    }

    #[tokio::test]
    async fn redelivery_in_flight_blocks_same_hash_and_allows_different_hash() {
        let topic = "persistent://public/default/key-shared-blocking";
        let subscription = "sub";
        let storage = create_test_storage();
        let same_key_metadata = metadata_with_ordering_key("same-key");
        let other_key_metadata = metadata_with_ordering_key("other-key");

        let (redelivery_id, same_hash_id, other_hash_id) = {
            let mut guard = storage.lock().await;
            guard.create_topic(topic).unwrap();
            guard
                .initialize_or_open_cursor(
                    topic,
                    subscription,
                    CursorInitOptions {
                        initial_position: InitialPosition::Earliest,
                        start_message_id: None,
                    },
                )
                .unwrap();
            let redelivery_id = guard
                .append_message_with_metadata(topic, -1, &same_key_metadata, b"redelivery")
                .unwrap();
            let same_hash_id = guard
                .append_message_with_metadata(topic, -1, &same_key_metadata, b"same")
                .unwrap();
            let other_hash_id = guard
                .append_message_with_metadata(topic, -1, &other_key_metadata, b"other")
                .unwrap();
            (redelivery_id, same_hash_id, other_hash_id)
        };

        let mut dispatcher = KeySharedDispatcher::new(None);
        let (consumer, mut rx) = create_consumer_with_receiver(1);
        dispatcher.add_consumer(consumer.clone()).unwrap();
        consumer.add_permits(4).await;
        dispatcher.consumer_flow(1, 4);
        dispatcher.init_read_position(Some(ManagedLedgerPosition::from(&same_hash_id)));
        dispatcher.add_redelivery_entries(vec![RedeliveryEntry {
            message_id: redelivery_id.clone(),
            redelivery_count: 1,
            sticky_key_hash: None,
        }]);

        dispatcher
            .dispatch_messages(storage.clone(), topic.to_string(), subscription.to_string())
            .await
            .unwrap();

        let first = timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let second = timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(first.1.message_id, redelivery_id);
        assert_eq!(first.1.redelivery_count, 1);
        assert_eq!(second.1.message_id, other_hash_id);
        assert_eq!(second.1.redelivery_count, 0);
        assert!(timeout(Duration::from_millis(100), rx.recv())
            .await
            .is_err());

        let same_hash = sticky_key_hash_from_metadata(&same_key_metadata);
        let queued = dispatcher
            .redelivery_controller
            .read()
            .unwrap()
            .queued_entries();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].message_id, same_hash_id);
        assert_eq!(queued[0].redelivery_count, 0);
        assert_eq!(queued[0].sticky_key_hash, Some(same_hash));
        assert!(dispatcher
            .redelivery_controller
            .read()
            .unwrap()
            .is_hash_blocked(same_hash));

        dispatcher.on_message_acknowledged(&redelivery_id);
        dispatcher
            .dispatch_messages(storage, topic.to_string(), subscription.to_string())
            .await
            .unwrap();

        let replayed_same_hash = timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replayed_same_hash.1.message_id, same_hash_id);
        assert_eq!(replayed_same_hash.1.redelivery_count, 0);
    }

    #[tokio::test]
    async fn reset_after_seek_clears_redelivery_hashes_and_repositions() {
        let dispatcher = KeySharedDispatcher::new(None);
        let hash = 42;
        let mid = MessageId {
            ledger: 0,
            entry: 5,
            partition: -1,
        };

        // add messages with sticky hash -> entries + blocked_hashes
        dispatcher
            .redelivery_controller
            .write()
            .unwrap()
            .add(RedeliveryEntry {
                message_id: mid.clone(),
                redelivery_count: 1,
                sticky_key_hash: Some(hash),
            });
        assert!(dispatcher
            .redelivery_controller
            .read()
            .unwrap()
            .is_hash_blocked(hash));

        // generate in_flight_hashes (with block_hashes set to true, the in_flight branch is taken)
        dispatcher
            .redelivery_controller
            .write()
            .unwrap()
            .take_for_delivery_with_hash(&mid, Some(hash));
        assert!(dispatcher
            .redelivery_controller
            .read()
            .unwrap()
            .has_in_flight_hash(hash));

        // add another one to ensure that the "entries" list is not empty.
        dispatcher
            .redelivery_controller
            .write()
            .unwrap()
            .add(RedeliveryEntry {
                message_id: MessageId {
                    ledger: 0,
                    entry: 6,
                    partition: -1,
                },
                redelivery_count: 0,
                sticky_key_hash: Some(hash),
            });

        dispatcher.reset_after_seek(Some(ManagedLedgerPosition {
            ledger_id: 0,
            entry_id: 3,
            partition: -1,
        }));

        let rc = dispatcher.redelivery_controller.read().unwrap();
        assert!(rc.is_empty(), "entries should be cleared");
        assert!(
            !rc.is_hash_blocked(hash),
            "blocked_hashes should be cleared"
        );
        assert!(
            !rc.has_in_flight_hash(hash),
            "in_flight_hashes should be cleared"
        );
        assert_eq!(
            *dispatcher.read_position.read().unwrap(),
            Some(ManagedLedgerPosition {
                ledger_id: 0,
                entry_id: 3,
                partition: -1
            })
        );
    }
}
