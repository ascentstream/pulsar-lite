/*
 * Failover Dispatcher
 * Implements message distribution for Failover subscription mode
 * All messages go to the primary (first) consumer, with standby consumers as backup
 * Consistent with Apache Pulsar's PersistentDispatcherMultipleConsumers in Failover mode
 */

use super::read_position::{commit_read_position, next_unacked_candidate};
use super::rewind_read_position;
use crate::broker::dispatcher::Dispatcher;
use crate::broker::service::topic::SubscriptionType;
use crate::broker::service::{Consumer, SharedStorage};
use crate::storage::ManagedLedgerPosition;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

/// Consistent with Apache Pulsar: dispatcherMaxRoundRobinBatchSize = 20
const DISPATCHER_MAX_ROUND_ROBIN_BATCH_SIZE: u32 = 20;

/// Failover mode dispatcher
pub struct FailoverDispatcher {
    /// All consumers for this failover subscription.
    consumers: Vec<Arc<Consumer>>,

    /// Currently active consumer selected by priority and consumer name.
    active_consumer_id: Option<u64>,

    /// Total available permits for the active consumer.
    total_available_permits: AtomicU32,

    /// Next managed-ledger position to read from.
    read_position: RwLock<Option<ManagedLedgerPosition>>,
}

impl FailoverDispatcher {
    /// Create a new FailoverDispatcher
    pub fn new() -> Self {
        Self {
            consumers: Vec::new(),
            active_consumer_id: None,
            total_available_permits: AtomicU32::new(0),
            read_position: RwLock::new(None),
        }
    }

    pub fn get_active_consumer(&self) -> Option<Arc<Consumer>> {
        let active_consumer_id = self.active_consumer_id?;
        self.consumers
            .iter()
            .find(|consumer| consumer.consumer_id == active_consumer_id)
            .cloned()
    }

    fn sort_consumers(&mut self) {
        self.consumers.sort_by(|left, right| {
            right
                .get_priority_level()
                .cmp(&left.get_priority_level())
                .then_with(|| left.get_consumer_name().cmp(right.get_consumer_name()))
                .then_with(|| left.consumer_id.cmp(&right.consumer_id))
        });
    }

    fn select_active_consumer_id(&self) -> Option<u64> {
        self.consumers.first().map(|consumer| consumer.consumer_id)
    }

    fn notify_active_consumer_changed(&self, active_consumer_id: u64) {
        for consumer in &self.consumers {
            consumer.notify_active_consumer_change(active_consumer_id);
        }
    }

    fn clear_active_consumer(&self) {
        for consumer in &self.consumers {
            consumer.clear_active_consumer();
        }
    }

    fn sync_total_available_permits(&self) {
        let permits = self
            .get_active_consumer()
            .map(|consumer| consumer.available_permits_now())
            .unwrap_or(0);
        self.total_available_permits
            .store(permits, Ordering::Relaxed);
    }

    fn pick_and_schedule_active_consumer(&mut self) -> bool {
        let selected_consumer_id = self.select_active_consumer_id();
        if selected_consumer_id == self.active_consumer_id {
            self.sync_total_available_permits();
            return false;
        }

        self.active_consumer_id = selected_consumer_id;
        if let Some(active_consumer_id) = selected_consumer_id {
            self.notify_active_consumer_changed(active_consumer_id);
        } else {
            self.clear_active_consumer();
        }
        self.sync_total_available_permits();
        true
    }

    pub async fn remove_consumer_with_recovery(
        &mut self,
        consumer_id: u64,
        storage: SharedStorage,
        topic: &str,
        subscription: &str,
    ) -> Option<Arc<Consumer>> {
        let removing_active = self.active_consumer_id == Some(consumer_id);
        let pending_positions = if removing_active {
            if let Some(consumer) = self
                .consumers
                .iter()
                .find(|consumer| consumer.consumer_id == consumer_id)
            {
                consumer.close_pending_acks();
                consumer
                    .drain_pending_acks()
                    .await
                    .into_iter()
                    .map(|(message_id, _)| ManagedLedgerPosition::from(message_id))
                    .collect()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        if removing_active {
            let rewound =
                rewind_read_position(storage, topic, subscription, pending_positions.into_iter())
                    .await;
            self.init_read_position(rewound.clone());
            log::info!(
                "Failover active consumer {} closed; rewound read position to {:?}",
                consumer_id,
                rewound
            );
        }

        let removed = self.remove_consumer(consumer_id);
        if removed.is_some() {
            self.sort_consumers();
            self.pick_and_schedule_active_consumer();
        }
        removed
    }
}

impl Dispatcher for FailoverDispatcher {
    fn get_type(&self) -> SubscriptionType {
        SubscriptionType::Failover
    }

    fn is_consumer_connected(&self) -> bool {
        !self.consumers.is_empty()
    }

    fn get_consumers(&self) -> Vec<Arc<Consumer>> {
        self.consumers.clone()
    }

    fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String> {
        if self
            .consumers
            .iter()
            .any(|c| c.consumer_id == consumer.consumer_id)
        {
            return Err(format!("Consumer {} already exists", consumer.consumer_id));
        }
        let added_consumer = consumer.clone();
        self.consumers.push(consumer);
        self.sort_consumers();
        if !self.pick_and_schedule_active_consumer() {
            if let Some(active_consumer_id) = self.active_consumer_id {
                added_consumer.notify_active_consumer_change(active_consumer_id);
            }
        }
        Ok(())
    }

    fn remove_consumer(&mut self, consumer_id: u64) -> Option<Arc<Consumer>> {
        if let Some(pos) = self
            .consumers
            .iter()
            .position(|c| c.consumer_id == consumer_id)
        {
            let removed = self.consumers.remove(pos);
            self.sort_consumers();
            if self.consumers.is_empty() {
                self.active_consumer_id = None;
                self.total_available_permits.store(0, Ordering::Relaxed);
                removed.clear_active_consumer();
            } else {
                self.pick_and_schedule_active_consumer();
            }
            Some(removed)
        } else {
            None
        }
    }

    fn init_read_position(&self, pos: Option<ManagedLedgerPosition>) {
        *self.read_position.write().unwrap() = pos;
    }

    fn consumer_flow(&self, consumer_id: u64, additional_permits: u32) {
        if self.active_consumer_id == Some(consumer_id) {
            self.total_available_permits
                .fetch_add(additional_permits, Ordering::Relaxed);
            log::debug!(
                "Failover consumer {} flowing {} permits, total={}",
                consumer_id,
                additional_permits,
                self.total_available_permits.load(Ordering::Relaxed)
            );
        } else if self
            .consumers
            .iter()
            .any(|consumer| consumer.consumer_id == consumer_id)
        {
            log::debug!(
                "Ignoring standby failover consumer {} flow of {} permits",
                consumer_id,
                additional_permits
            );
        }
    }

    async fn dispatch_messages(
        &self,
        storage: SharedStorage,
        topic: String,
        subscription: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // For Failover, only dispatch to the active consumer.
        if let Some(primary_consumer) = self.get_active_consumer() {
            let available_permits = self.total_available_permits.load(Ordering::Relaxed);
            if available_permits == 0 {
                return Ok(());
            }

            let max_messages =
                std::cmp::min(available_permits, DISPATCHER_MAX_ROUND_ROBIN_BATCH_SIZE);
            let mut dispatched = 0;

            for _ in 0..max_messages {
                if !primary_consumer.use_permit().await {
                    break;
                }
                self.total_available_permits.fetch_sub(1, Ordering::Relaxed);

                let candidate = next_unacked_candidate(
                    storage.clone(),
                    &topic,
                    &subscription,
                    &self.read_position,
                )
                .await?;

                if let Some(candidate) = candidate {
                    if primary_consumer
                        .enqueue_message(
                            candidate.message_id,
                            Vec::new(),
                            candidate.payload.clone(),
                        )
                        .await
                    {
                        commit_read_position(&self.read_position, candidate.next_position);
                        primary_consumer
                            .record_message_dispatched(candidate.payload.len())
                            .await;
                        dispatched += 1;
                    } else {
                        primary_consumer.add_permits(1).await;
                        self.total_available_permits.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                } else {
                    primary_consumer.add_permits(1).await;
                    self.total_available_permits.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }

            if dispatched > 0 {
                log::info!(
                    "Failover dispatched {} messages to primary consumer {}",
                    dispatched,
                    primary_consumer.consumer_id
                );
            }
        }

        Ok(())
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
        Arc::new(Mutex::new(
            Storage::new_memory(Path::new("/tmp/test-failover-dispatcher-storage")).unwrap(),
        ))
    }

    fn create_test_subscription(storage: SharedStorage) -> Arc<RwLock<Subscription>> {
        Arc::new(RwLock::new(Subscription::new(
            "test-sub".to_string(),
            "persistent://public/default/test-topic".to_string(),
            SubscriptionType::Failover,
            storage,
        )))
    }

    fn create_test_consumer(
        consumer_id: u64,
        consumer_name: &str,
        priority_level: i32,
        subscription: Arc<RwLock<Subscription>>,
    ) -> Arc<Consumer> {
        let (tx, _rx) = mpsc::channel(8192);
        Arc::new(Consumer::new(
            consumer_id,
            consumer_name.to_string(),
            subscription,
            format!("conn-{}", consumer_id),
            tx,
            priority_level,
        ))
    }

    #[test]
    fn failover_selects_active_by_priority_name_and_id() {
        let storage = create_test_storage();
        let subscription = create_test_subscription(storage);
        let lower_priority = create_test_consumer(1, "consumer-a", 1, subscription.clone());
        let active = create_test_consumer(3, "consumer-a", 5, subscription.clone());
        let name_tiebreaker = create_test_consumer(2, "consumer-b", 5, subscription);

        let mut dispatcher = FailoverDispatcher::new();
        dispatcher.add_consumer(name_tiebreaker).unwrap();
        dispatcher.add_consumer(lower_priority).unwrap();
        dispatcher.add_consumer(active.clone()).unwrap();

        assert_eq!(
            dispatcher
                .get_active_consumer()
                .map(|consumer| consumer.consumer_id),
            Some(active.consumer_id)
        );
    }
}
