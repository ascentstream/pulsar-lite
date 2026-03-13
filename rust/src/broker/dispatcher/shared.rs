/*
 * Shared Dispatcher
 * Implements message distribution for Shared subscription mode
 * Consistent with Apache Pulsar's PersistentDispatcherMultipleConsumers
 */

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, AtomicUsize, AtomicBool, Ordering};
use crate::broker::service::{Consumer, SharedStorage};
use crate::broker::dispatcher::Dispatcher;
use crate::broker::service::topic::SubscriptionType;
use crate::storage::MessageId;

/// Consistent with Apache Pulsar: dispatcherMaxRoundRobinBatchSize = 20
const DISPATCHER_MAX_ROUND_ROBIN_BATCH_SIZE: u32 = 20;

/// Shared mode dispatcher
pub struct SharedDispatcher {
    /// All consumers for this shared subscription
    consumers: HashMap<u64, Arc<Consumer>>,

    /// Round-Robin index for consumer selection (atomic for thread safety)
    round_robin_index: AtomicUsize,

    /// Total available permits across all consumers (atomic for thread safety)
    total_available_permits: AtomicU32,

    /// Flag to prevent reentrant dispatching
    dispatch_in_progress: AtomicBool,

    // Pending messages to redeliver (ordered for debugging)
    // When a Consumer disconnects, its held messages are added to this queue
    messages_to_redeliver: Arc<RwLock<BTreeMap<MessageId, u32>>>,
}

impl SharedDispatcher {
    /// Create a new SharedDispatcher
    pub fn new() -> Self {
        Self {
            consumers: HashMap::new(),
            round_robin_index: AtomicUsize::new(0),
            total_available_permits: AtomicU32::new(0),
            dispatch_in_progress: AtomicBool::new(false),
            messages_to_redeliver: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Get next available consumer using Round-Robin algorithm
    ///
    /// This implements the same logic as Apache Pulsar's AbstractDispatcherMultipleConsumers.getNextConsumer()
    /// It cycles through consumers in Round-Robin fashion and returns the first one with available permits.
    async fn get_next_available_consumer(&self) -> Option<Arc<Consumer>> {
        if self.consumers.is_empty() {
            return None;
        }

        // Convert to vector for indexed access
        let consumers: Vec<_> = self.consumers.values().cloned().collect();
        let mut best_priority: Option<i32> = None;
        let mut eligible_indices = Vec::new();

        for (index, consumer) in consumers.iter().enumerate() {
            let permits = consumer.get_available_permits().await;
            if permits == 0 {
                continue;
            }

            let priority = consumer.get_priority_level();
            match best_priority {
                Some(current_best) if priority > current_best => {}
                Some(current_best) if priority == current_best => eligible_indices.push(index),
                _ => {
                    best_priority = Some(priority);
                    eligible_indices.clear();
                    eligible_indices.push(index);
                }
            }
        }

        if eligible_indices.is_empty() {
            log::debug!("No consumer has available permits");
            return None;
        }

        let eligible_count = eligible_indices.len();
        let start = self.round_robin_index.fetch_add(1, Ordering::Relaxed) % eligible_count;

        for offset in 0..eligible_count {
            let vector_index = eligible_indices[(start + offset) % eligible_count];
            let consumer = consumers[vector_index].clone();
            let permits = consumer.get_available_permits().await;            if permits > 0 {
                log::debug!(
                    "Priority-aware Round-Robin selected consumer {} (priority {}, index {}) with {} permits",
                    consumer.consumer_id,
                    consumer.get_priority_level(),
                    vector_index,
                    permits
                );
                return Some(consumer);
            }
        }

        // No consumer has available permits
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
            if let Some((msg_id, redelivery_count)) = self.pop_redelivery_message() {
                let already_acked = {
                    let guard = storage.lock().await;
                    guard.is_acknowledged_shared(&topic, &subscription, &msg_id)
                };
                if already_acked {
                    consumer.add_permits(1).await;
                    self.total_available_permits.fetch_add(1, Ordering::Relaxed);
                    log::debug!(
                        "Skipping replay for already-acked message {}:{}",
                        msg_id.ledger, msg_id.entry
                    );
                    continue;
                }

                // Get message content from storage
                let message_opt = {
                    let guard = storage.lock().await;
                    guard.get_message_by_id(&topic, &msg_id)
                };

                if let Some((message_id, payload)) = message_opt {
                    {
                        let mut guard = storage.lock().await;
                        guard.assign_message(&topic, &subscription, &message_id, consumer_id);
                    }

                    if consumer
                        .send_message(
                            message_id.clone(),
                            Vec::new(),
                            payload.clone(),
                            redelivery_count + 1,
                        )
                        .await
                    {
                        consumer.record_message_dispatched(payload.len()).await;
                        dispatched += 1;
                        redelivered += 1;

                        log::debug!(
                            "Redelivered message {}:{} to consumer {}, remaining permits={}",
                            message_id.ledger, message_id.entry, consumer_id,
                            self.total_available_permits.load(Ordering::Relaxed)
                        );
                    } else {
                        let mut guard = storage.lock().await;
                        guard.release_assignment(&topic, &subscription, &message_id, consumer_id);
                        drop(guard);
                        self.add_to_redelivery_queue(vec![(message_id, redelivery_count + 1)]);
                        consumer.add_permits(1).await;
                        self.total_available_permits.fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    // Message no longer exists (may have been deleted), restore permit
                    consumer.add_permits(1).await;
                    self.total_available_permits.fetch_add(1, Ordering::Relaxed);
                    log::warn!("Redelivery message {}:{} not found in storage", msg_id.ledger, msg_id.entry);
                }
                continue;
            }

            // 2. Redelivery queue empty, get new message
            let message_opt = {
                let mut guard = storage.lock().await;
                guard.get_next_unassigned_message(&topic, &subscription, consumer_id)?
            };

            if let Some((message_id, payload)) = message_opt {
                if consumer
                    .send_message(
                        message_id.clone(),
                        Vec::new(),
                        payload.clone(),
                        0,
                    )
                    .await
                {
                    consumer.record_message_dispatched(payload.len()).await;
                    dispatched += 1;

                    log::debug!(
                        "Dispatched new message {}:{} to consumer {} via Round-Robin, remaining permits={}",
                        message_id.ledger, message_id.entry, consumer_id,
                        self.total_available_permits.load(Ordering::Relaxed)
                    );
                } else {
                    let mut guard = storage.lock().await;
                    guard.release_assignment(&topic, &subscription, &message_id, consumer_id);
                    drop(guard);
                    self.add_to_redelivery_queue(vec![(message_id, 0)]);
                    consumer.add_permits(1).await;
                    self.total_available_permits.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                // No more messages, restore permit
                consumer.add_permits(1).await;
                self.total_available_permits.fetch_add(1, Ordering::Relaxed);
                log::debug!("No more unassigned messages");
                break;
            }
        }

        if dispatched > 0 {
            log::info!(
                "Batch dispatch completed: dispatched={} (redelivered={}), remaining_permits={}",
                dispatched, redelivered,
                self.total_available_permits.load(Ordering::Relaxed)
            );
        }

        Ok(())
    }

    /// Add messages to redelivery queue
    pub fn add_to_redelivery_queue(&self, message_ids: Vec<(MessageId, u32)>) {
        let mut redeliver = self.messages_to_redeliver.write().unwrap();
        let count_before = redeliver.len();

        for (msg_id, redelivery_count) in message_ids {
            redeliver
                .entry(msg_id)
                .and_modify(|count| *count = (*count).max(redelivery_count))
                .or_insert(redelivery_count);
        }

        log::debug!(
            "Added {} messages to redelivery queue, total={}",
            redeliver.len() - count_before, redeliver.len()
        );
    }

    /// Pop next message from redelivery queue
    pub fn pop_redelivery_message(&self) -> Option<(MessageId, u32)> {
        let mut redeliver = self.messages_to_redeliver.write().unwrap();
        redeliver.pop_first()
    }

    pub async fn remove_consumer_with_recovery(
        &mut self,
        consumer_id: u64,
        storage: SharedStorage,
        topic: &str,
        subscription: &str,
    ) -> Option<Arc<Consumer>> {
        let consumer = self.consumers.remove(&consumer_id);

        if let Some(ref consumer) = consumer {
            let pending = consumer.drain_pending_acks().await;
            let mut recovered = Vec::with_capacity(pending.len());
            {
                let mut guard = storage.lock().await;
                for (message_id, pending_ack) in pending {
                    guard.release_assignment(topic, subscription, &message_id, consumer_id);
                    recovered.push((message_id, pending_ack.redelivery_count));
                }
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
        self.messages_to_redeliver.read().unwrap().len()
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
        self.consumers.values().cloned().collect()
    }

    fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String> {
        let consumer_id = consumer.consumer_id;

        if self.consumers.contains_key(&consumer_id) {
            return Err(format!("Consumer {} already exists", consumer_id));
        }

        self.consumers.insert(consumer_id, consumer);
        Ok(())
    }

    fn remove_consumer(&mut self, consumer_id: u64) -> Option<Arc<Consumer>> {
        self.consumers.remove(&consumer_id)
    }

    fn consumer_flow(&self, consumer_id: u64, additional_permits: u32) {
        // 1. Update the consumer's own permits
        if let Some(consumer) = self.consumers.get(&consumer_id) {
            // We need to spawn a task since add_permits is async
            let consumer = consumer.clone();
            tokio::spawn(async move {
                consumer.add_permits(additional_permits).await;
            });
        }

        // 2. Increase total permits atomically
        self.total_available_permits.fetch_add(additional_permits, Ordering::Relaxed);

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
        subscription: String
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Prevent reentrant dispatching
        if self.dispatch_in_progress.swap(true, Ordering::Relaxed) {
            log::debug!("Dispatch already in progress, skipping");
            return Ok(());
        }

        let result = self.dispatch_messages_batch(storage, topic, subscription).await;

        // Reset flag
        self.dispatch_in_progress.store(false, Ordering::Relaxed);

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::service::topic::Subscription;
    use crate::storage::Storage;
    use std::path::Path;
    use tokio::sync::{mpsc, Mutex, RwLock};

    fn create_test_storage() -> SharedStorage {
        Arc::new(Mutex::new(Storage::new(Path::new("/tmp/test-shared-dispatcher-storage")).unwrap()))
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
        let (tx, _rx) = mpsc::unbounded_channel();
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

        let first = dispatcher.get_next_available_consumer().await.unwrap().consumer_id;
        let second = dispatcher.get_next_available_consumer().await.unwrap().consumer_id;

        assert_ne!(first, second);
    }
}
