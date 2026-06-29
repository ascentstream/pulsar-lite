/*
 * Shared Dispatcher
 * Implements message distribution for Shared subscription mode
 * Consistent with Apache Pulsar's PersistentDispatcherMultipleConsumers
 */

use super::read_position::{commit_read_position, next_unacked_candidate, ReadCandidate};
use super::redelivery_controller::{RedeliveryController, RedeliveryEntry};
use crate::broker::dispatcher::Dispatcher;
use crate::broker::service::topic::SubscriptionType;
use crate::broker::service::{Consumer, SharedStorage};
use crate::storage::{ManagedLedgerPosition, MessageId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

/// Consistent with Apache Pulsar: dispatcherMaxRoundRobinBatchSize = 20
const DISPATCHER_MAX_ROUND_ROBIN_BATCH_SIZE: u32 = 20;

/// Shared mode dispatcher
pub struct SharedDispatcher {
    /// All consumers for this shared subscription
    consumers: HashMap<u64, Arc<Consumer>>,

    /// Stable consumer order used for priority-aware dispatch.
    /// Sorted by priority level ascending (0 is highest), while preserving
    /// insertion order within the same priority level.
    consumer_order: Vec<u64>,

    /// Round-Robin index for consumer selection (atomic for thread safety)
    round_robin_index: AtomicUsize,

    /// Total available permits across all consumers (atomic for thread safety)
    total_available_permits: AtomicU32,

    /// Flag to prevent reentrant dispatching
    dispatch_in_progress: AtomicBool,

    // Pending messages to redeliver. Shared does not use sticky hash blocking.
    redelivery_controller: Arc<RwLock<RedeliveryController>>,

    /// Next managed-ledger position to read from for new dispatches.
    read_position: RwLock<Option<ManagedLedgerPosition>>,
}

impl SharedDispatcher {
    /// Create a new SharedDispatcher
    pub fn new() -> Self {
        Self {
            consumers: HashMap::new(),
            consumer_order: Vec::new(),
            round_robin_index: AtomicUsize::new(0),
            total_available_permits: AtomicU32::new(0),
            dispatch_in_progress: AtomicBool::new(false),
            redelivery_controller: Arc::new(RwLock::new(RedeliveryController::new(false))),
            read_position: RwLock::new(None),
        }
    }

    fn ordered_consumers(&self) -> Vec<Arc<Consumer>> {
        self.consumer_order
            .iter()
            .filter_map(|consumer_id| self.consumers.get(consumer_id).cloned())
            .collect()
    }

    fn first_consumer_index_of_priority(
        consumers: &[Arc<Consumer>],
        target_priority: i32,
    ) -> Option<usize> {
        consumers
            .iter()
            .position(|consumer| consumer.get_priority_level() == target_priority)
    }

    async fn find_available_consumer_from_higher_priority(
        &self,
        consumers: &[Arc<Consumer>],
        current_index: usize,
        target_priority: i32,
    ) -> Option<usize> {
        for (index, consumer) in consumers.iter().enumerate().take(current_index) {
            if consumer.get_priority_level() >= target_priority {
                break;
            }
            if consumer.get_available_permits().await > 0 {
                return Some(index);
            }
        }
        None
    }

    async fn find_available_consumer_from_same_or_lower_priority(
        &self,
        consumers: &[Arc<Consumer>],
        current_index: usize,
    ) -> Option<usize> {
        let current_consumer = &consumers[current_index];
        let target_priority = current_consumer.get_priority_level();

        if current_consumer.get_available_permits().await > 0 {
            return Some(current_index);
        }

        let mut scan_index = current_index + 1;
        let mut end_priority_level_index = current_index;
        loop {
            match consumers.get(scan_index) {
                Some(scan_consumer) if scan_consumer.get_priority_level() == target_priority => {
                    if scan_consumer.get_available_permits().await > 0 {
                        return Some(scan_index);
                    }
                    scan_index += 1;
                }
                _ => {
                    end_priority_level_index = scan_index;
                    scan_index =
                        Self::first_consumer_index_of_priority(consumers, target_priority)?;
                }
            }

            if scan_index == current_index {
                break;
            }
        }

        for (index, consumer) in consumers.iter().enumerate().skip(end_priority_level_index) {
            if consumer.get_available_permits().await > 0 {
                return Some(index);
            }
        }

        None
    }

    fn insert_consumer_order(&mut self, consumer_id: u64, priority_level: i32) {
        let insert_at = self
            .consumer_order
            .iter()
            .enumerate()
            .find_map(|(index, existing_id)| {
                let existing = self.consumers.get(existing_id)?;
                (existing.get_priority_level() > priority_level).then_some(index)
            })
            .unwrap_or(self.consumer_order.len());
        self.consumer_order.insert(insert_at, consumer_id);
    }

    fn remove_consumer_order(&mut self, consumer_id: u64) {
        if let Some(index) = self.consumer_order.iter().position(|id| *id == consumer_id) {
            self.consumer_order.remove(index);
            let len = self.consumer_order.len();
            if len == 0 {
                self.round_robin_index.store(0, Ordering::Relaxed);
            } else {
                let current = self.round_robin_index.load(Ordering::Relaxed);
                if current > index {
                    self.round_robin_index.store(current - 1, Ordering::Relaxed);
                } else if current >= len {
                    self.round_robin_index
                        .store(current % len, Ordering::Relaxed);
                }
            }
        }
    }

    fn subtract_total_permits(&self, permits: u32) {
        let _ = self.total_available_permits.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(permits)),
        );
    }

    /// Get next available consumer using Round-Robin algorithm
    ///
    /// This implements the same logic as Apache Pulsar's AbstractDispatcherMultipleConsumers.getNextConsumer()
    /// It cycles through consumers in Round-Robin fashion and returns the first one with available permits.
    async fn get_next_available_consumer(&self) -> Option<Arc<Consumer>> {
        if self.consumers.is_empty() {
            return None;
        }

        let consumers = self.ordered_consumers();
        if consumers.is_empty() {
            return None;
        }
        let current_index = self.round_robin_index.load(Ordering::Relaxed) % consumers.len();
        let current_priority = consumers[current_index].get_priority_level();

        if current_priority != 0 {
            if let Some(index) = self
                .find_available_consumer_from_higher_priority(
                    &consumers,
                    current_index,
                    current_priority,
                )
                .await
            {
                self.round_robin_index
                    .store((index + 1) % consumers.len(), Ordering::Relaxed);
                return Some(consumers[index].clone());
            }
        }

        if let Some(index) = self
            .find_available_consumer_from_same_or_lower_priority(&consumers, current_index)
            .await
        {
            self.round_robin_index
                .store((index + 1) % consumers.len(), Ordering::Relaxed);
            return Some(consumers[index].clone());
        }

        log::debug!("No consumer has available permits");
        None
    }

    /// Dispatch a batch of messages to consumers using Round-Robin (Push mode)
    ///
    /// This is the core method that implements Apache Pulsar's message distribution logic:
    /// 1. Check if we have permits
    /// 2. Priority: redelivery queue first
    /// 3. Fall back to unassigned messages when redelivery is empty
    /// 4. Use Round-Robin to select consumers
    /// 5. Enqueue messages to consumer's pending queue
    async fn dispatch_messages_batch(
        &self,
        storage: SharedStorage,
        topic: String,
        subscription: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Check if we have permits
        let total_permits = self.total_available_permits.load(Ordering::Relaxed);
        if total_permits == 0 {
            log::debug!("No permits available, skipping dispatch");
            return Ok(());
        }

        // Dispatch up to DISPATCHER_MAX_ROUND_ROBIN_BATCH_SIZE messages
        let mut dispatched = 0;
        let mut redelivered = 0;
        let max_batch = std::cmp::min(total_permits, DISPATCHER_MAX_ROUND_ROBIN_BATCH_SIZE);

        log::debug!(
            "Starting batch dispatch: max_batch={}, total_permits={}, consumers={}, redelivery_queue={}",
            max_batch, total_permits, self.consumers.len(),
            self.get_redelivery_queue_size()
        );

        for _ in 0..max_batch {
            // Get next consumer using Round-Robin
            let consumer = match self.get_next_available_consumer().await {
                Some(c) => c,
                None => {
                    log::debug!("No available consumer found");
                    break;
                }
            };

            let consumer_id = consumer.consumer_id;

            // Use one permit
            if !consumer.use_permit().await {
                log::warn!("Consumer {} permits exhausted during dispatch", consumer_id);
                break;
            }

            // Decrease total permits
            self.total_available_permits.fetch_sub(1, Ordering::Relaxed);

            // 1. Priority: get message from redelivery queue
            if let Some(redelivery) = self.pop_redelivery_message() {
                let msg_id = redelivery.message_id.clone();
                let redelivery_count = redelivery.redelivery_count;
                let already_acked = {
                    let guard = storage.lock().await;
                    guard.is_acknowledged(&topic, &subscription, &msg_id)?
                };
                if already_acked {
                    consumer.add_permits(1).await;
                    self.total_available_permits.fetch_add(1, Ordering::Relaxed);
                    log::debug!(
                        "Skipping replay for already-acked message {}:{}",
                        msg_id.ledger,
                        msg_id.entry
                    );
                    continue;
                }

                // Get message content from storage
                let message_opt = {
                    let guard = storage.lock().await;
                    guard.get_message_entry_by_id(&topic, &msg_id)
                };

                if let Some(entry) = message_opt {
                    if consumer
                        .send_message(
                            entry.message_id.clone(),
                            entry.metadata.clone(),
                            entry.payload.clone(),
                            redelivery_count,
                        )
                        .await
                    {
                        consumer
                            .record_message_dispatched(entry.payload.len())
                            .await;
                        dispatched += 1;
                        redelivered += 1;

                        log::debug!(
                            "Redelivered message {}:{} to consumer {}, remaining permits={}",
                            entry.message_id.ledger,
                            entry.message_id.entry,
                            consumer_id,
                            self.total_available_permits.load(Ordering::Relaxed)
                        );
                    } else {
                        self.restore_redelivery_message(RedeliveryEntry {
                            message_id: entry.message_id,
                            redelivery_count: redelivery_count + 1,
                            sticky_key_hash: None,
                        });
                        consumer.add_permits(1).await;
                        self.total_available_permits.fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    // Message no longer exists (may have been deleted), restore permit
                    consumer.add_permits(1).await;
                    self.total_available_permits.fetch_add(1, Ordering::Relaxed);
                    log::warn!(
                        "Redelivery message {}:{} not found in storage",
                        msg_id.ledger,
                        msg_id.entry
                    );
                }
                continue;
            }

            // 2. Redelivery queue empty, get new message
            let message_opt = self
                .get_next_dispatchable_message(storage.clone(), &topic, &subscription)
                .await?;

            if let Some(candidate) = message_opt {
                if consumer
                    .send_message(
                        candidate.message_id.clone(),
                        candidate.metadata.clone(),
                        candidate.payload.clone(),
                        0,
                    )
                    .await
                {
                    commit_read_position(&self.read_position, candidate.next_position);
                    consumer
                        .record_message_dispatched(candidate.payload.len())
                        .await;
                    dispatched += 1;

                    log::debug!(
                        "Dispatched new message {}:{} to consumer {} via Round-Robin, remaining permits={}",
                        candidate.message_id.ledger, candidate.message_id.entry, consumer_id,
                        self.total_available_permits.load(Ordering::Relaxed)
                    );
                } else {
                    self.add_to_redelivery_queue(vec![(candidate.message_id, 1)]);
                    commit_read_position(&self.read_position, candidate.next_position);
                    consumer.add_permits(1).await;
                    self.total_available_permits.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                // No more messages, restore permit
                consumer.add_permits(1).await;
                self.total_available_permits.fetch_add(1, Ordering::Relaxed);
                log::debug!("No more dispatchable messages");
                break;
            }
        }

        if dispatched > 0 {
            log::info!(
                "Batch dispatch completed: dispatched={} (redelivered={}), remaining_permits={}",
                dispatched,
                redelivered,
                self.total_available_permits.load(Ordering::Relaxed)
            );
        }

        Ok(())
    }

    /// Add messages to redelivery queue
    pub fn add_to_redelivery_queue(&self, message_ids: Vec<(MessageId, u32)>) {
        self.add_redelivery_entries(
            message_ids
                .into_iter()
                .map(|(message_id, redelivery_count)| RedeliveryEntry {
                    message_id,
                    redelivery_count,
                    sticky_key_hash: None,
                })
                .collect(),
        );
    }

    pub fn add_redelivery_entries(&self, entries: Vec<RedeliveryEntry>) {
        let mut controller = self.redelivery_controller.write().unwrap();
        let count_before = controller.len();

        for entry in entries {
            controller.add(entry);
        }

        log::debug!(
            "Added {} messages to redelivery queue, total={}",
            controller.len() - count_before,
            controller.len()
        );
    }

    /// Pop next message from redelivery queue
    pub fn pop_redelivery_message(&self) -> Option<RedeliveryEntry> {
        self.redelivery_controller.write().unwrap().pop_next()
    }

    fn restore_redelivery_message(&self, entry: RedeliveryEntry) {
        self.redelivery_controller.write().unwrap().restore(entry);
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

    /// Check if message is pending ack by any consumer
    async fn is_message_pending(&self, message_id: &MessageId) -> bool {
        for consumer in self.ordered_consumers() {
            if consumer.has_pending_ack(message_id).await {
                return true;
            }
        }
        false
    }

    /// Get next dispatchable message from storage
    async fn get_next_dispatchable_message(
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

    pub async fn remove_consumer_with_recovery(
        &mut self,
        consumer_id: u64,
        _storage: SharedStorage,
        _topic: &str,
        _subscription: &str,
    ) -> Option<Arc<Consumer>> {
        let consumer = self.consumers.remove(&consumer_id);

        if let Some(ref consumer) = consumer {
            self.remove_consumer_order(consumer_id);
            self.subtract_total_permits(consumer.available_permits_now());
            consumer.close_pending_acks();
            let pending = consumer.drain_pending_acks().await;
            let mut recovered = Vec::with_capacity(pending.len());
            for (message_id, pending_ack) in pending {
                recovered.push((message_id, pending_ack.redelivery_count + 1));
            }
            self.add_to_redelivery_queue(recovered);

            log::info!(
                "Consumer {} removed, {} messages queued for replay",
                consumer_id,
                self.get_redelivery_queue_size()
            );
        }

        consumer
    }

    /// Get redelivery queue size
    pub fn get_redelivery_queue_size(&self) -> usize {
        self.redelivery_controller.read().unwrap().len()
    }

    /// Remove a message from the redelivery queue after it has been acked.
    pub fn on_message_acknowledged(&self, message_id: &MessageId) {
        let mut controller = self.redelivery_controller.write().unwrap();
        if controller.remove(message_id).is_some() {
            log::debug!(
                "Removed acked message {}:{} from redelivery queue",
                message_id.ledger,
                message_id.entry
            );
        }
    }
}

impl Default for SharedDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl Dispatcher for SharedDispatcher {
    fn get_type(&self) -> SubscriptionType {
        SubscriptionType::Shared
    }

    fn is_consumer_connected(&self) -> bool {
        !self.consumers.is_empty()
    }

    fn get_consumers(&self) -> Vec<Arc<Consumer>> {
        self.ordered_consumers()
    }

    fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String> {
        let consumer_id = consumer.consumer_id;

        if self.consumers.contains_key(&consumer_id) {
            return Err(format!("Consumer {} already exists", consumer_id));
        }

        let priority_level = consumer.get_priority_level();
        self.consumers.insert(consumer_id, consumer);
        self.insert_consumer_order(consumer_id, priority_level);
        Ok(())
    }

    fn remove_consumer(&mut self, consumer_id: u64) -> Option<Arc<Consumer>> {
        let consumer = self.consumers.remove(&consumer_id);
        if let Some(ref consumer) = consumer {
            self.remove_consumer_order(consumer_id);
            self.subtract_total_permits(consumer.available_permits_now());
        }
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
        // Consumer-local permit state is updated by the flow handler before it
        // triggers dispatch. The dispatcher only tracks the aggregate count.
        self.total_available_permits
            .fetch_add(additional_permits, Ordering::Relaxed);

        log::info!(
            "Consumer {} flowed {} permits, total={}",
            consumer_id,
            additional_permits,
            self.total_available_permits.load(Ordering::Relaxed)
        );
    }

    async fn dispatch_messages(
        &self,
        storage: SharedStorage,
        topic: String,
        subscription: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Prevent reentrant dispatching
        if self.dispatch_in_progress.swap(true, Ordering::Relaxed) {
            log::debug!("Dispatch already in progress, skipping");
            return Ok(());
        }

        let result = self
            .dispatch_messages_batch(storage, topic, subscription)
            .await;

        // Reset flag
        self.dispatch_in_progress.store(false, Ordering::Relaxed);

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::service::topic::Subscription;
    use crate::storage::{CursorInitOptions, InitialPosition, Storage};
    use std::path::Path;
    use tokio::sync::{mpsc, Mutex, RwLock};

    fn create_test_storage() -> SharedStorage {
        Arc::new(Mutex::new(
            Storage::new_memory(Path::new("/tmp/test-shared-dispatcher-storage")).unwrap(),
        ))
    }

    fn create_test_subscription(storage: SharedStorage) -> Arc<RwLock<Subscription>> {
        Arc::new(RwLock::new(Subscription::new(
            "test-sub".to_string(),
            "persistent://public/default/test-topic".to_string(),
            SubscriptionType::Shared,
            storage,
        )))
    }

    fn create_test_consumer(
        consumer_id: u64,
        priority_level: i32,
        subscription: Arc<RwLock<Subscription>>,
    ) -> Arc<Consumer> {
        let (tx, _rx) = mpsc::channel(8192);
        Arc::new(Consumer::new(
            consumer_id,
            format!("consumer-{}", consumer_id),
            subscription,
            format!("conn-{}", consumer_id),
            tx,
            priority_level,
        ))
    }

    #[tokio::test]
    async fn priority_dispatch_prefers_higher_priority_consumers() {
        let storage = create_test_storage();
        let subscription = create_test_subscription(storage);
        let high = create_test_consumer(1, 0, subscription.clone());
        let low = create_test_consumer(2, 5, subscription);

        high.add_permits(1).await;
        low.add_permits(1).await;

        let mut dispatcher = SharedDispatcher::new();
        dispatcher.add_consumer(high.clone()).unwrap();
        dispatcher.add_consumer(low.clone()).unwrap();

        let selected = dispatcher.get_next_available_consumer().await.unwrap();
        assert_eq!(selected.consumer_id, high.consumer_id);
    }

    #[tokio::test]
    async fn same_priority_consumers_continue_round_robin_selection() {
        let storage = create_test_storage();
        let subscription = create_test_subscription(storage);
        let consumer_a = create_test_consumer(1, 0, subscription.clone());
        let consumer_b = create_test_consumer(2, 0, subscription);

        consumer_a.add_permits(2).await;
        consumer_b.add_permits(2).await;

        let mut dispatcher = SharedDispatcher::new();
        dispatcher.add_consumer(consumer_a).unwrap();
        dispatcher.add_consumer(consumer_b).unwrap();

        let first = dispatcher
            .get_next_available_consumer()
            .await
            .unwrap()
            .consumer_id;
        let second = dispatcher
            .get_next_available_consumer()
            .await
            .unwrap()
            .consumer_id;

        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn lower_priority_consumer_only_selected_after_higher_priority_is_exhausted() {
        let storage = create_test_storage();
        let subscription = create_test_subscription(storage);
        let high = create_test_consumer(1, 0, subscription.clone());
        let low = create_test_consumer(2, 1, subscription);

        high.add_permits(1).await;
        low.add_permits(2).await;

        let mut dispatcher = SharedDispatcher::new();
        dispatcher.add_consumer(high.clone()).unwrap();
        dispatcher.add_consumer(low.clone()).unwrap();

        let first = dispatcher.get_next_available_consumer().await.unwrap();
        assert_eq!(first.consumer_id, high.consumer_id);
        high.use_permit().await;

        let second = dispatcher.get_next_available_consumer().await.unwrap();
        assert_eq!(second.consumer_id, low.consumer_id);
    }

    #[tokio::test]
    async fn removing_consumer_preserves_round_robin_for_remaining_same_priority_group() {
        let storage = create_test_storage();
        let subscription = create_test_subscription(storage);
        let consumer_a = create_test_consumer(1, 0, subscription.clone());
        let consumer_b = create_test_consumer(2, 0, subscription.clone());
        let consumer_c = create_test_consumer(3, 0, subscription);

        consumer_a.add_permits(2).await;
        consumer_b.add_permits(2).await;
        consumer_c.add_permits(2).await;

        let mut dispatcher = SharedDispatcher::new();
        dispatcher.add_consumer(consumer_a).unwrap();
        dispatcher.add_consumer(consumer_b.clone()).unwrap();
        dispatcher.add_consumer(consumer_c.clone()).unwrap();

        let _ = dispatcher.get_next_available_consumer().await.unwrap();
        dispatcher.remove_consumer(1);

        let first_remaining = dispatcher
            .get_next_available_consumer()
            .await
            .unwrap()
            .consumer_id;
        let second_remaining = dispatcher
            .get_next_available_consumer()
            .await
            .unwrap()
            .consumer_id;

        assert_ne!(first_remaining, second_remaining);
        assert!(matches!(first_remaining, 2 | 3));
        assert!(matches!(second_remaining, 2 | 3));
    }

    #[tokio::test]
    async fn ack_state_update_prunes_acked_redelivery_entries() {
        let storage = create_test_storage();
        let topic = "persistent://public/default/test-topic";
        let subscription_name = "test-sub";
        let (acked, pending) = {
            let mut guard = storage.lock().await;
            guard.create_topic(topic).unwrap();
            guard
                .initialize_or_open_cursor(
                    topic,
                    subscription_name,
                    CursorInitOptions {
                        initial_position: InitialPosition::Earliest,
                        start_message_id: None,
                    },
                )
                .unwrap();
            let acked = guard.append_message(topic, -1, b"acked").unwrap();
            let pending = guard.append_message(topic, -1, b"pending").unwrap();
            guard
                .ack_message_shared(topic, subscription_name, acked.clone())
                .unwrap();
            (acked, pending)
        };

        let dispatcher = SharedDispatcher::new();
        dispatcher.add_to_redelivery_queue(vec![(acked, 0), (pending.clone(), 0)]);

        dispatcher
            .on_ack_state_updated(storage, topic, subscription_name)
            .await
            .unwrap();

        assert_eq!(dispatcher.get_redelivery_queue_size(), 1);
        assert_eq!(
            dispatcher.pop_redelivery_message().unwrap().message_id,
            pending
        );
    }

    #[tokio::test]
    async fn reset_after_seek_clears_redelivery_queue_and_repositions_read_cursor() {
        let dispatcher = SharedDispatcher::new();

        // Send a pre-seek red queue message
        dispatcher
            .redelivery_controller
            .write()
            .unwrap()
            .add(RedeliveryEntry {
                message_id: MessageId {
                    ledger: 0,
                    entry: 5,
                    partition: -1,
                },
                redelivery_count: 2,
                sticky_key_hash: None,
            });
        assert!(!dispatcher.redelivery_controller.read().unwrap().is_empty());

        // Set an old "read_position"
        *dispatcher.read_position.write().unwrap() = Some(ManagedLedgerPosition {
            ledger_id: 0,
            entry_id: 10,
            partition: -1,
        });

        // seek -> entry 3
        dispatcher.reset_after_seek(Some(ManagedLedgerPosition {
            ledger_id: 0,
            entry_id: 3,
            partition: -1,
        }));

        // redelivery queue emptied + read_position reset
        assert!(dispatcher.redelivery_controller.read().unwrap().is_empty());
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
