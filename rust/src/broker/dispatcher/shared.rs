/*
 * Shared Dispatcher
 * Implements message distribution for Shared subscription mode
 * Consistent with Apache Pulsar's PersistentDispatcherMultipleConsumers
 */

use std::sync::Arc;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicUsize, AtomicBool, Ordering};
use crate::broker::service::{Consumer, SharedStorage};
use crate::broker::dispatcher::Dispatcher;
use crate::broker::service::topic::SubscriptionType;

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
}

impl SharedDispatcher {
    /// Create a new SharedDispatcher
    pub fn new() -> Self {
        Self {
            consumers: HashMap::new(),
            round_robin_index: AtomicUsize::new(0),
            total_available_permits: AtomicU32::new(0),
            dispatch_in_progress: AtomicBool::new(false),
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
        let consumer_count = consumers.len();

        // Try each consumer in Round-Robin order
        for _ in 0..consumer_count {
            // Get next index atomically
            let index = self.round_robin_index.fetch_add(1, Ordering::Relaxed) % consumer_count;
            let consumer = consumers[index].clone();

            // Check if this consumer has permits
            if consumer.get_available_permits().await > 0 {
                log::debug!(
                    "Round-Robin selected consumer {} (index {}) with {} permits",
                    consumer.consumer_id, index, consumer.get_available_permits().await
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
    /// 2. Use Round-Robin to select consumers
    /// 3. Get unassigned messages from storage
    /// 4. Enqueue messages to consumer's pending queue
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
        let max_batch = std::cmp::min(total_permits, DISPATCHER_MAX_ROUND_ROBIN_BATCH_SIZE);

        log::debug!(
            "Starting batch dispatch: max_batch={}, total_permits={}, consumers={}",
            max_batch, total_permits, self.consumers.len()
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

            // Get next unassigned message
            let message_opt = {
                let mut guard = storage.lock().await;
                guard.get_next_unassigned_message(&topic, &subscription, consumer_id)?
            };

            if let Some((message_id, payload)) = message_opt {
                // Enqueue message to consumer's pending queue
                consumer.enqueue_message(message_id.clone(), payload.clone()).await;

                // Record message dispatched
                consumer.record_message_dispatched(payload.len()).await;

                dispatched += 1;

                log::debug!(
                    "Dispatched message {}:{} to consumer {} via Round-Robin, remaining permits={}",
                    message_id.ledger, message_id.entry, consumer_id,
                    self.total_available_permits.load(Ordering::Relaxed)
                );
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
                "Batch dispatch completed: dispatched={} messages, remaining_permits={}",
                dispatched,
                self.total_available_permits.load(Ordering::Relaxed)
            );
        }

        Ok(())
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
