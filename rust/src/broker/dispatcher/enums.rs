/*
 * Dispatcher Enum
 * Holds the concrete dispatcher implementation based on subscription type
 */

use super::traits::Dispatcher;
use super::{ExclusiveDispatcher, FailoverDispatcher, SharedDispatcher};
use crate::broker::service::topic::SubscriptionType;
use crate::broker::service::{Consumer, SharedStorage};
use std::sync::Arc;

/// Dispatcher enum - holds the concrete dispatcher implementation
/// This is created based on subscription type when first consumer is added
pub enum DispatcherEnum {
    Exclusive(ExclusiveDispatcher),
    Shared(SharedDispatcher),
    Failover(FailoverDispatcher),
}

impl DispatcherEnum {
    /// Create a new DispatcherEnum based on subscription type
    pub fn new(sub_type: SubscriptionType) -> Self {
        match sub_type {
            SubscriptionType::Exclusive => DispatcherEnum::Exclusive(ExclusiveDispatcher::new()),
            SubscriptionType::Shared => DispatcherEnum::Shared(SharedDispatcher::new()),
            SubscriptionType::Failover => DispatcherEnum::Failover(FailoverDispatcher::new()),
            SubscriptionType::KeyShared => {
                log::warn!("KeyShared not yet implemented, falling back to Shared");
                DispatcherEnum::Shared(SharedDispatcher::new())
            }
        }
    }

    pub fn get_type(&self) -> SubscriptionType {
        match self {
            DispatcherEnum::Exclusive(_) => SubscriptionType::Exclusive,
            DispatcherEnum::Shared(_) => SubscriptionType::Shared,
            DispatcherEnum::Failover(_) => SubscriptionType::Failover,
        }
    }

    pub fn is_consumer_connected(&self) -> bool {
        match self {
            DispatcherEnum::Exclusive(d) => d.is_consumer_connected(),
            DispatcherEnum::Shared(d) => d.is_consumer_connected(),
            DispatcherEnum::Failover(d) => d.is_consumer_connected(),
        }
    }

    pub fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String> {
        match self {
            DispatcherEnum::Exclusive(d) => d.add_consumer(consumer),
            DispatcherEnum::Shared(d) => d.add_consumer(consumer),
            DispatcherEnum::Failover(d) => d.add_consumer(consumer),
        }
    }

    pub fn remove_consumer(&mut self, consumer_id: u64) -> Option<Arc<Consumer>> {
        match self {
            DispatcherEnum::Exclusive(d) => d.remove_consumer(consumer_id),
            DispatcherEnum::Shared(d) => d.remove_consumer(consumer_id),
            DispatcherEnum::Failover(d) => d.remove_consumer(consumer_id),
        }
    }

    pub async fn remove_consumer_with_recovery(
        &mut self,
        consumer_id: u64,
        storage: SharedStorage,
        topic: &str,
        subscription: &str,
    ) -> Option<Arc<Consumer>> {
        match self {
            DispatcherEnum::Shared(d) => {
                d.remove_consumer_with_recovery(consumer_id, storage, topic, subscription)
                    .await
            }
            DispatcherEnum::Exclusive(d) => d.remove_consumer(consumer_id),
            DispatcherEnum::Failover(d) => d.remove_consumer(consumer_id),
        }
    }

    pub fn get_consumers(&self) -> Vec<Arc<Consumer>> {
        match self {
            DispatcherEnum::Exclusive(d) => d.get_consumers(),
            DispatcherEnum::Shared(d) => d.get_consumers(),
            DispatcherEnum::Failover(d) => d.get_consumers(),
        }
    }

    pub fn get_consumer(&self, consumer_id: u64) -> Option<Arc<Consumer>> {
        self.get_consumers()
            .into_iter()
            .find(|c| c.consumer_id == consumer_id)
    }

    /// Handle consumer flow command - update permits
    pub fn consumer_flow(&self, consumer_id: u64, additional_permits: u32) {
        match self {
            DispatcherEnum::Exclusive(d) => d.consumer_flow(consumer_id, additional_permits),
            DispatcherEnum::Shared(d) => d.consumer_flow(consumer_id, additional_permits),
            DispatcherEnum::Failover(d) => d.consumer_flow(consumer_id, additional_permits),
        }
    }

    /// Dispatch messages to consumers (Push mode)
    pub async fn dispatch_messages(
        &self,
        storage: crate::broker::service::SharedStorage,
        topic: String,
        subscription: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self {
            DispatcherEnum::Exclusive(d) => d.dispatch_messages(storage, topic, subscription).await,
            DispatcherEnum::Shared(d) => d.dispatch_messages(storage, topic, subscription).await,
            DispatcherEnum::Failover(d) => d.dispatch_messages(storage, topic, subscription).await,
        }
    }
}

impl std::fmt::Debug for DispatcherEnum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DispatcherEnum::Exclusive(_) => write!(f, "Exclusive"),
            DispatcherEnum::Shared(_) => write!(f, "Shared"),
            DispatcherEnum::Failover(_) => write!(f, "Failover"),
        }
    }
}
