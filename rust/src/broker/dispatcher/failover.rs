/*
 * Failover Dispatcher
 * Implements message distribution for Failover subscription mode
 * All messages go to the primary (first) consumer, with standby consumers as backup
 * Consistent with Apache Pulsar's PersistentDispatcherMultipleConsumers in Failover mode
 */

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use crate::broker::service::{Consumer, SharedStorage};
use crate::broker::dispatcher::Dispatcher;
use crate::broker::service::topic::SubscriptionType;

/// Consistent with Apache Pulsar: dispatcherMaxRoundRobinBatchSize = 20
const DISPATCHER_MAX_ROUND_ROBIN_BATCH_SIZE: u32 = 20;

/// Failover mode dispatcher
pub struct FailoverDispatcher {
    /// All consumers for this failover subscription (first is primary)
    consumers: Vec<Arc<Consumer>>,

    /// Total available permits for primary consumer
    total_available_permits: AtomicU32,
}

impl FailoverDispatcher {
    /// Create a new FailoverDispatcher
    pub fn new() -> Self {
        Self {
            consumers: Vec::new(),
            total_available_permits: AtomicU32::new(0),
        }
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
        if self.consumers.iter().any(|c| c.consumer_id == consumer.consumer_id) {
            return Err(format!("Consumer {} already exists", consumer.consumer_id));
        }
        self.consumers.push(consumer);
        Ok(())
    }

    fn remove_consumer(&mut self, consumer_id: u64) -> Option<Arc<Consumer>> {
        if let Some(pos) = self.consumers.iter().position(|c| c.consumer_id == consumer_id) {
            Some(self.consumers.remove(pos))
        } else {
            None
        }
    }

    fn consumer_flow(&self, consumer_id: u64, additional_permits: u32) {
        if let Some(_consumer) = self.consumers.iter().find(|c| c.consumer_id == consumer_id) {
            self.total_available_permits.fetch_add(additional_permits, Ordering::Relaxed);
            log::debug!(
                "Failover consumer {} flowing {} permits, total={}",
                consumer_id, additional_permits,
                self.total_available_permits.load(Ordering::Relaxed)
            );
        }
    }

    async fn dispatch_messages(
        &self,
        storage: SharedStorage,
        topic: String,
        subscription: String
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // For Failover, only dispatch to the primary (first) consumer
        if let Some(primary_consumer) = self.consumers.first() {
            let available_permits = self.total_available_permits.load(Ordering::Relaxed);
            if available_permits == 0 {
                return Ok(());
            }

            let max_messages = std::cmp::min(available_permits, DISPATCHER_MAX_ROUND_ROBIN_BATCH_SIZE);
            let mut dispatched = 0;

            for _ in 0..max_messages {
                if !primary_consumer.use_permit().await {
                    break;
                }
                self.total_available_permits.fetch_sub(1, Ordering::Relaxed);

                let message_opt = {
                    let mut guard = storage.lock().await;
                    guard.get_next_unassigned_message(&topic, &subscription, primary_consumer.consumer_id)?
                };

                if let Some((message_id, payload)) = message_opt {
                    if primary_consumer
                        .enqueue_message(message_id, Vec::new(), payload.clone())
                        .await
                    {
                        primary_consumer.record_message_dispatched(payload.len()).await;
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
                log::info!("Failover dispatched {} messages to primary consumer {}", dispatched, primary_consumer.consumer_id);
            }
        }

        Ok(())
    }
}
