/*
 * Non-persistent dispatcher family
 *
 * This mirrors native Pulsar's non-persistent dispatcher split at a structural
 * level, while keeping behavior deliberately minimal for now.
 */

mod multiple_consumers;
mod single_active;
mod sticky_key;

use crate::broker::service::topic::{KeySharedPolicy, SubscriptionType};
use crate::broker::service::Consumer;
use crate::storage::NonPersistentEntry;
use std::sync::Arc;

pub use multiple_consumers::NonPersistentDispatcherMultipleConsumers;
pub use single_active::{NonPersistentDispatcherExclusive, NonPersistentDispatcherFailover};
pub use sticky_key::NonPersistentStickyKeyDispatcher;

#[derive(Debug)]
pub enum NonPersistentDispatcherEnum {
    Exclusive(NonPersistentDispatcherExclusive),
    Failover(NonPersistentDispatcherFailover),
    MultipleConsumers(NonPersistentDispatcherMultipleConsumers),
    StickyKey(NonPersistentStickyKeyDispatcher),
}

impl NonPersistentDispatcherEnum {
    pub fn new(
        sub_type: SubscriptionType,
        topic_name: String,
        partition_index: i32,
        key_shared_policy: Option<KeySharedPolicy>,
    ) -> Self {
        match sub_type {
            SubscriptionType::Exclusive => Self::Exclusive(NonPersistentDispatcherExclusive::new()),
            SubscriptionType::Failover => Self::Failover(NonPersistentDispatcherFailover::new(
                topic_name,
                partition_index,
            )),
            SubscriptionType::Shared => {
                Self::MultipleConsumers(NonPersistentDispatcherMultipleConsumers::new())
            }
            SubscriptionType::KeyShared => Self::StickyKey(NonPersistentStickyKeyDispatcher::new(
                key_shared_policy,
            )),
        }
    }

    pub fn get_type(&self) -> SubscriptionType {
        match self {
            Self::Exclusive(d) => d.get_type(),
            Self::Failover(d) => d.get_type(),
            Self::MultipleConsumers(d) => d.get_type(),
            Self::StickyKey(d) => d.get_type(),
        }
    }

    pub fn is_consumer_connected(&self) -> bool {
        match self {
            Self::Exclusive(d) => d.is_consumer_connected(),
            Self::Failover(d) => d.is_consumer_connected(),
            Self::MultipleConsumers(d) => d.is_consumer_connected(),
            Self::StickyKey(d) => d.is_consumer_connected(),
        }
    }

    pub fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String> {
        match self {
            Self::Exclusive(d) => d.add_consumer(consumer),
            Self::Failover(d) => d.add_consumer(consumer),
            Self::MultipleConsumers(d) => d.add_consumer(consumer),
            Self::StickyKey(d) => d.add_consumer(consumer),
        }
    }

    pub fn remove_consumer(&mut self, consumer_id: u64) -> Option<Arc<Consumer>> {
        match self {
            Self::Exclusive(d) => d.remove_consumer(consumer_id),
            Self::Failover(d) => d.remove_consumer(consumer_id),
            Self::MultipleConsumers(d) => d.remove_consumer(consumer_id),
            Self::StickyKey(d) => d.remove_consumer(consumer_id),
        }
    }

    pub fn get_consumers(&self) -> Vec<Arc<Consumer>> {
        match self {
            Self::Exclusive(d) => d.get_consumers(),
            Self::Failover(d) => d.get_consumers(),
            Self::MultipleConsumers(d) => d.get_consumers(),
            Self::StickyKey(d) => d.get_consumers(),
        }
    }

    pub fn get_consumer(&self, consumer_id: u64) -> Option<Arc<Consumer>> {
        self.get_consumers()
            .into_iter()
            .find(|consumer| consumer.consumer_id == consumer_id)
    }

    pub fn get_active_consumer(&self) -> Option<Arc<Consumer>> {
        match self {
            Self::Exclusive(d) => d.get_consumers().into_iter().next(),
            Self::Failover(d) => d.get_active_consumer(),
            Self::MultipleConsumers(_) | Self::StickyKey(_) => None,
        }
    }

    pub fn consumer_flow(&self, consumer_id: u64, additional_permits: u32) {
        match self {
            Self::Exclusive(d) => d.consumer_flow(consumer_id, additional_permits),
            Self::Failover(d) => d.consumer_flow(consumer_id, additional_permits),
            Self::MultipleConsumers(d) => d.consumer_flow(consumer_id, additional_permits),
            Self::StickyKey(d) => d.consumer_flow(consumer_id, additional_permits),
        }
    }

    pub fn dropped_messages(&self) -> u64 {
        match self {
            Self::Exclusive(d) => d.dropped_messages(),
            Self::Failover(d) => d.dropped_messages(),
            Self::MultipleConsumers(d) => d.dropped_messages(),
            Self::StickyKey(d) => d.dropped_messages(),
        }
    }

    pub fn has_same_key_shared_policy(&self, policy: Option<&KeySharedPolicy>) -> bool {
        match self {
            Self::StickyKey(d) => d.has_same_key_shared_policy(policy),
            _ => policy.is_none(),
        }
    }

    pub async fn send_messages(
        &self,
        entries: Vec<NonPersistentEntry>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Self::Exclusive(d) => d.send_messages(entries).await,
            Self::Failover(d) => d.send_messages(entries).await,
            Self::MultipleConsumers(d) => d.send_messages(entries).await,
            Self::StickyKey(d) => d.send_messages(entries).await,
        }
    }
}
