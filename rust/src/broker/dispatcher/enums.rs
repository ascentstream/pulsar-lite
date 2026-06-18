/*
 * Dispatcher Enum
 * Holds the concrete dispatcher implementation based on subscription type
 */

use super::traits::Dispatcher;
use super::{ExclusiveDispatcher, FailoverDispatcher, KeySharedDispatcher, SharedDispatcher};
use crate::broker::service::topic::{KeySharedPolicy, SubscriptionType};
use crate::broker::service::{Consumer, SharedStorage};
use crate::storage::{ManagedLedgerPosition, MessageId};
use std::sync::Arc;

/// Dispatcher enum - holds the concrete dispatcher implementation
/// This is created based on subscription type when first consumer is added
pub enum DispatcherEnum {
    Exclusive(ExclusiveDispatcher),
    Shared(SharedDispatcher),
    Failover(FailoverDispatcher),
    KeyShared(KeySharedDispatcher),
}

impl DispatcherEnum {
    /// Create a new DispatcherEnum based on subscription type
    pub fn new(sub_type: SubscriptionType) -> Self {
        Self::new_with_key_shared_policy(sub_type, None)
    }

    pub fn new_with_key_shared_policy(
        sub_type: SubscriptionType,
        key_shared_policy: Option<KeySharedPolicy>,
    ) -> Self {
        match sub_type {
            SubscriptionType::Exclusive => DispatcherEnum::Exclusive(ExclusiveDispatcher::new()),
            SubscriptionType::Shared => DispatcherEnum::Shared(SharedDispatcher::new()),
            SubscriptionType::Failover => DispatcherEnum::Failover(FailoverDispatcher::new()),
            SubscriptionType::KeyShared => {
                DispatcherEnum::KeyShared(KeySharedDispatcher::new(key_shared_policy))
            }
        }
    }

    pub fn get_type(&self) -> SubscriptionType {
        match self {
            DispatcherEnum::Exclusive(_) => SubscriptionType::Exclusive,
            DispatcherEnum::Shared(_) => SubscriptionType::Shared,
            DispatcherEnum::Failover(_) => SubscriptionType::Failover,
            DispatcherEnum::KeyShared(_) => SubscriptionType::KeyShared,
        }
    }

    pub fn is_consumer_connected(&self) -> bool {
        match self {
            DispatcherEnum::Exclusive(d) => d.is_consumer_connected(),
            DispatcherEnum::Shared(d) => d.is_consumer_connected(),
            DispatcherEnum::Failover(d) => d.is_consumer_connected(),
            DispatcherEnum::KeyShared(d) => d.is_consumer_connected(),
        }
    }

    pub fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String> {
        match self {
            DispatcherEnum::Exclusive(d) => d.add_consumer(consumer),
            DispatcherEnum::Shared(d) => d.add_consumer(consumer),
            DispatcherEnum::Failover(d) => d.add_consumer(consumer),
            DispatcherEnum::KeyShared(d) => d.add_consumer(consumer),
        }
    }

    pub fn remove_consumer(&mut self, consumer_id: u64) -> Option<Arc<Consumer>> {
        match self {
            DispatcherEnum::Exclusive(d) => d.remove_consumer(consumer_id),
            DispatcherEnum::Shared(d) => d.remove_consumer(consumer_id),
            DispatcherEnum::Failover(d) => d.remove_consumer(consumer_id),
            DispatcherEnum::KeyShared(d) => d.remove_consumer(consumer_id),
        }
    }

    pub fn init_read_position(&self, pos: Option<ManagedLedgerPosition>) {
        match self {
            DispatcherEnum::Exclusive(d) => d.init_read_position(pos),
            DispatcherEnum::Shared(d) => d.init_read_position(pos),
            DispatcherEnum::Failover(d) => d.init_read_position(pos),
            DispatcherEnum::KeyShared(d) => d.init_read_position(pos),
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
            DispatcherEnum::Exclusive(d) => {
                d.remove_consumer_with_recovery(consumer_id, storage, topic, subscription)
                    .await
            }
            DispatcherEnum::Failover(d) => {
                d.remove_consumer_with_recovery(consumer_id, storage, topic, subscription)
                    .await
            }
            DispatcherEnum::KeyShared(d) => {
                d.remove_consumer_with_recovery(consumer_id, storage, topic, subscription)
                    .await
            }
        }
    }

    pub fn get_consumers(&self) -> Vec<Arc<Consumer>> {
        match self {
            DispatcherEnum::Exclusive(d) => d.get_consumers(),
            DispatcherEnum::Shared(d) => d.get_consumers(),
            DispatcherEnum::Failover(d) => d.get_consumers(),
            DispatcherEnum::KeyShared(d) => d.get_consumers(),
        }
    }

    pub fn get_consumer(&self, consumer_id: u64) -> Option<Arc<Consumer>> {
        self.get_consumers()
            .into_iter()
            .find(|c| c.consumer_id == consumer_id)
    }

    pub fn get_active_consumer(&self) -> Option<Arc<Consumer>> {
        match self {
            DispatcherEnum::Failover(d) => d.get_active_consumer(),
            DispatcherEnum::Exclusive(d) => d.get_consumers().into_iter().next(),
            DispatcherEnum::Shared(_) | DispatcherEnum::KeyShared(_) => None,
        }
    }

    pub fn redeliver_messages(&mut self, entries: Vec<(MessageId, u32)>) {
        match self {
            DispatcherEnum::Shared(d) => d.add_to_redelivery_queue(entries),
            DispatcherEnum::KeyShared(d) => d.add_to_redelivery_queue(entries),
            DispatcherEnum::Exclusive(_) | DispatcherEnum::Failover(_) => {
                if !entries.is_empty() {
                    log::warn!(
                        "Ignoring redelivery request for non-shared dispatcher, entries={}",
                        entries.len()
                    );
                }
            }
        }
    }

    /// Handle consumer flow command - update permits
    pub fn consumer_flow(&self, consumer_id: u64, additional_permits: u32) {
        match self {
            DispatcherEnum::Exclusive(d) => d.consumer_flow(consumer_id, additional_permits),
            DispatcherEnum::Shared(d) => d.consumer_flow(consumer_id, additional_permits),
            DispatcherEnum::Failover(d) => d.consumer_flow(consumer_id, additional_permits),
            DispatcherEnum::KeyShared(d) => d.consumer_flow(consumer_id, additional_permits),
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
            DispatcherEnum::KeyShared(d) => d.dispatch_messages(storage, topic, subscription).await,
        }
    }

    /// Called after storage acknowledges a message
    pub fn on_message_acknowledged(&mut self, message_id: &MessageId) {
        match self {
            DispatcherEnum::Shared(d) => d.on_message_acknowledged(message_id),
            DispatcherEnum::KeyShared(d) => d.on_message_acknowledged(message_id),
            DispatcherEnum::Exclusive(_) | DispatcherEnum::Failover(_) => {}
        }
    }

    pub async fn on_ack_state_updated(
        &mut self,
        storage: SharedStorage,
        topic: &str,
        subscription: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self {
            DispatcherEnum::Shared(d) => d.on_ack_state_updated(storage, topic, subscription).await,
            DispatcherEnum::KeyShared(d) => {
                d.on_ack_state_updated(storage, topic, subscription).await
            }
            DispatcherEnum::Exclusive(_) | DispatcherEnum::Failover(_) => Ok(()),
        }
    }
}

impl std::fmt::Debug for DispatcherEnum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DispatcherEnum::Exclusive(_) => write!(f, "Exclusive"),
            DispatcherEnum::Shared(_) => write!(f, "Shared"),
            DispatcherEnum::Failover(_) => write!(f, "Failover"),
            DispatcherEnum::KeyShared(_) => write!(f, "KeyShared"),
        }
    }
}
