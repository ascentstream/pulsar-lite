/*
 * Exclusive Dispatcher
 * Implements message distribution for Exclusive subscription mode
 * Only one consumer can subscribe to the topic at a time
 * All messages go to the single consumer
 * Consistent with Apache Pulsar's PersistentDispatcherSingleActiveConsumer
 */

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use crate::broker::service::{Consumer, SharedStorage};
use crate::broker::dispatcher::Dispatcher;
use crate::broker::service::topic::SubscriptionType;

/// Consistent with Apache Pulsar: dispatcherMaxRoundRobinBatchSize = 20
const DISPATCHER_MAX_ROUND_ROBIN_BATCH_SIZE: u32 = 20;

/// Exclusive mode dispatcher
pub struct ExclusiveDispatcher {
    /// The single consumer for this exclusive subscription
    consumer: Option<Arc<Consumer>>,

    /// Total available permits (for the single consumer)
    total_available_permits: AtomicU32,
}

impl ExclusiveDispatcher {
    /// Create a new ExclusiveDispatcher
    pub fn new() -> Self {
        Self {
            consumer: None,
            total_available_permits: AtomicU32::new(0),
        }
    }
}

impl Dispatcher for ExclusiveDispatcher {
    fn get_type(&self) -> SubscriptionType {
        SubscriptionType::Exclusive
    }

    fn is_consumer_connected(&self) -> bool {
        self.consumer.is_some()
    }

    fn get_consumers(&self) -> Vec<Arc<Consumer>> {
        self.consumer.iter().cloned().collect()
    }

    fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String> {
        if self.consumer.is_some() {
            return Err("Exclusive subscription already has a consumer".to_string());
        }
        self.consumer = Some(consumer);
        Ok(())
    }

    fn remove_consumer(&mut self, consumer_id: u64) -> Option<Arc<Consumer>> {
        if let Some(ref consumer) = self.consumer {
            if consumer.consumer_id == consumer_id {
                return self.consumer.take();
            }
        }
        None
    }

    fn consumer_flow(&self, consumer_id: u64, additional_permits: u32) {
        if let Some(ref consumer) = self.consumer {
            if consumer.consumer_id == consumer_id {
                self.total_available_permits.fetch_add(additional_permits, Ordering::Relaxed);
                log::debug!(
                    "Exclusive consumer {} flowing {} permits, total={}",
                    consumer_id, additional_permits,
                    self.total_available_permits.load(Ordering::Relaxed)
                );
            }
        }
    }

    async fn dispatch_messages(
        &self,
        storage: SharedStorage,
        topic: String,
        subscription: String
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // For Exclusive, just dispatch to the single consumer
        if let Some(consumer) = &self.consumer {
            let available_permits = self.total_available_permits.load(Ordering::Relaxed);
            if available_permits == 0 {
                return Ok(());
            }

            let max_messages = std::cmp::min(available_permits, DISPATCHER_MAX_ROUND_ROBIN_BATCH_SIZE);
            let mut dispatched = 0;

            for _ in 0..max_messages {
                if !consumer.use_permit().await {
                    break;
                }
                self.total_available_permits.fetch_sub(1, Ordering::Relaxed);

                let message_opt = {
                    let mut guard = storage.lock().await;
                    guard.get_next_unassigned_message(&topic, &subscription, consumer.consumer_id)?
                };

                if let Some((message_id, payload)) = message_opt {
                    if consumer.enqueue_message(message_id, payload.clone()).await {
                        consumer.record_message_dispatched(payload.len()).await;
                        dispatched += 1;
                    } else {
                        consumer.add_permits(1).await;
                        self.total_available_permits.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                } else {
                    consumer.add_permits(1).await;
                    self.total_available_permits.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }

            if dispatched > 0 {
                log::info!("Exclusive dispatched {} messages to consumer {}", dispatched, consumer.consumer_id);
            }
        }

        Ok(())
    }
}
