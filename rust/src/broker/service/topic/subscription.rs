/*
 * Subscription Management
 * Manages consumers for a specific subscription on a topic
 * Inspired by Apache Pulsar's PersistentSubscription
 */

use std::sync::Arc;

/// Forward declaration for Consumer type
use super::super::{Consumer, SharedStorage};
use crate::broker::dispatcher::DispatcherEnum;

/// Subscription type (matches Pulsar protocol)
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SubscriptionType {
    Exclusive = 0,
    Shared = 1,
    Failover = 2,
    KeyShared = 3,
}

impl Default for SubscriptionType {
    fn default() -> Self {
        SubscriptionType::Exclusive
    }
}

/// Subscription represents a named subscription on a topic
/// It manages the dispatcher which handles consumers (Apache Pulsar style)
pub struct Subscription {
    /// Subscription name
    pub name: String,
    /// Topic this subscription belongs to
    pub topic: String,
    /// Subscription type (Exclusive, Shared, Failover)
    pub sub_type: SubscriptionType,
    /// Dispatcher for this subscription (created on first consumer)
    /// Apache Pulsar style - subscription holds dispatcher, not consumers directly
    dispatcher: Option<DispatcherEnum>,
    /// Storage backend for reading messages
    storage: SharedStorage,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("name", &self.name)
            .field("topic", &self.topic)
            .field("sub_type", &self.sub_type)
            .field("dispatcher", &self.dispatcher.as_ref().map(|d| d.get_type()))
            .finish()
    }
}

impl Subscription {
    /// Create a new subscription
    pub fn new(name: String, topic: String, sub_type: SubscriptionType, storage: SharedStorage) -> Self {
        Self {
            name,
            topic,
            sub_type,
            dispatcher: None,
            storage,
        }
    }

    /// Get subscription type
    pub fn get_sub_type(&self) -> SubscriptionType {
        self.sub_type
    }

    /// Create or reuse dispatcher based on subscription type (Apache Pulsar style)
    ///
    /// This is inspired by PersistentSubscription.reuseOrCreateDispatcher()
    fn reuse_or_create_dispatcher(&mut self) {
        if self.dispatcher.is_none() {
            log::info!(
                "Creating {} dispatcher for subscription '{}' on topic '{}'",
                match self.sub_type {
                    SubscriptionType::Exclusive => "Exclusive",
                    SubscriptionType::Shared => "Shared",
                    SubscriptionType::Failover => "Failover",
                    SubscriptionType::KeyShared => "KeyShared (fallback to Shared)",
                },
                self.name, self.topic
            );
            self.dispatcher = Some(DispatcherEnum::new(self.sub_type));
        }
    }

    /// Add a consumer to this subscription (Apache Pulsar style)
    ///
    /// This method:
    /// 1. Creates dispatcher if needed
    /// 2. Adds consumer to dispatcher
    pub fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String> {
        // Create dispatcher if needed
        self.reuse_or_create_dispatcher();

        // Add consumer to dispatcher
        if let Some(ref mut dispatcher) = self.dispatcher {
            dispatcher.add_consumer(consumer)
        } else {
            Err("Failed to create dispatcher".to_string())
        }
    }

    /// Remove a consumer from this subscription
    pub fn remove_consumer(&mut self, consumer_id: u64) -> Option<Arc<Consumer>> {
        if let Some(ref mut dispatcher) = self.dispatcher {
            let consumer = dispatcher.remove_consumer(consumer_id);

            // Clear dispatcher if no more consumers
            if !dispatcher.is_consumer_connected() {
                self.dispatcher = None;
            }

            consumer
        } else {
            None
        }
    }

    pub async fn remove_consumer_with_recovery(&mut self, consumer_id: u64) -> Option<Arc<Consumer>> {
        let consumer = if let Some(ref mut dispatcher) = self.dispatcher {
            dispatcher
                .remove_consumer_with_recovery(
                    consumer_id,
                    self.storage.clone(),
                    &self.topic,
                    &self.name,
                )
                .await
        } else {
            None
        };

        if let Some(ref dispatcher) = self.dispatcher {
            if let Err(e) = dispatcher
                .dispatch_messages(self.storage.clone(), self.topic.clone(), self.name.clone())
                .await
            {
                log::error!(
                    "Failed to dispatch replay messages for subscription '{}': {}",
                    self.name, e
                );
            }

            if !dispatcher.is_consumer_connected() {
                self.dispatcher = None;
            }
        }

        consumer
    }

    /// Get a consumer by ID
    pub fn get_consumer(&self, consumer_id: u64) -> Option<Arc<Consumer>> {
        self.dispatcher.as_ref()?.get_consumer(consumer_id)
    }

    /// Get all consumers
    pub fn get_consumers(&self) -> Vec<Arc<Consumer>> {
        self.dispatcher.as_ref().map(|d| d.get_consumers()).unwrap_or_default()
    }

    /// Get active consumers (for Failover, only the primary consumer)
    pub fn get_active_consumers(&self) -> Vec<Arc<Consumer>> {
        // For Failover subscription, only the first consumer is active
        // For Shared and Exclusive, all consumers are active
        match self.sub_type {
            SubscriptionType::Failover => {
                self.dispatcher.as_ref()
                    .and_then(|d| d.get_consumers().into_iter().next())
                    .into_iter()
                    .collect()
            }
            _ => self.get_consumers(),
        }
    }

    /// Get consumer count
    pub fn get_consumer_count(&self) -> usize {
        self.get_consumers().len()
    }

    /// Check if subscription has any consumers
    pub fn has_consumers(&self) -> bool {
        self.dispatcher.as_ref().map(|d| d.is_consumer_connected()).unwrap_or(false)
    }

    /// Get total available permits across all consumers
    pub async fn get_total_permits(&self) -> u32 {
        let mut total = 0;
        for consumer in self.get_consumers() {
            total += consumer.get_available_permits().await;
        }
        total
    }

    /// Get subscription statistics
    pub async fn get_stats(&self) -> SubscriptionStats {
        SubscriptionStats {
            name: self.name.clone(),
            topic: self.topic.clone(),
            sub_type: self.sub_type,
            consumer_count: self.get_consumer_count(),
            total_permits: self.get_total_permits().await,
        }
    }

    // ==================== Message Dispatch (Push mode) ====================

    /// Dispatch messages to consumers (Push mode)
    ///
    /// This is called by Topic.dispatch_to_subscriptions() when a new message is published.
    /// It triggers the dispatcher to push messages to consumers that have available permits.
    pub async fn dispatch_messages(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        log::debug!(
            "Dispatching messages for subscription '{}', consumers={}, permits={}",
            self.name,
            self.get_consumer_count(),
            self.get_total_permits().await
        );

        if let Some(ref dispatcher) = self.dispatcher {
            dispatcher.dispatch_messages(
                self.storage.clone(),
                self.topic.clone(),
                self.name.clone()
            ).await
        } else {
            log::warn!("No dispatcher found for subscription '{}'", self.name);
            Ok(())
        }
    }

    /// Handle consumer flow command (Apache Pulsar style)
    ///
    /// This is called by Consumer.flow_message() -> Subscription.consumer_flow()
    /// It updates dispatcher's total permits and triggers message dispatch.
    ///
    /// # Arguments
    /// * `consumer_id` - The consumer that sent the flow command
    /// * `additional_permits` - Number of permits added
    pub async fn consumer_flow(
        &self,
        consumer_id: u64,
        additional_permits: u32,
    ) {
        if let Some(ref dispatcher) = self.dispatcher {
            log::debug!(
                "Subscription '{}' received flow from consumer {}, permits={}",
                self.name, consumer_id, additional_permits
            );

            // 1. Update dispatcher's total permits
            dispatcher.consumer_flow(consumer_id, additional_permits);

            // 2. Trigger automatic message dispatch
            if let Err(e) = dispatcher.dispatch_messages(
                self.storage.clone(),
                self.topic.clone(),
                self.name.clone()
            ).await {
                log::error!(
                    "Failed to dispatch messages for subscription '{}': {}",
                    self.name, e
                );
            }
        } else {
            log::warn!("No dispatcher available for subscription '{}'", self.name);
        }
    }
}

/// Statistics for a subscription
#[derive(Debug, Clone)]
pub struct SubscriptionStats {
    pub name: String,
    pub topic: String,
    pub sub_type: SubscriptionType,
    pub consumer_count: usize,
    pub total_permits: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::{RwLock, Mutex, mpsc};
    use crate::storage::Storage;
    use std::path::Path;

    fn create_test_storage() -> SharedStorage {
        Arc::new(Mutex::new(Storage::new(Path::new("/tmp/test-subscription-storage")).unwrap()))
    }

    fn create_test_subscription_arc() -> Arc<RwLock<Subscription>> {
        Arc::new(RwLock::new(Subscription::new(
            "test-sub".to_string(),
            "test-topic".to_string(),
            SubscriptionType::Shared,
            create_test_storage(),
        )))
    }

    fn create_test_consumer(id: u64, subscription: Arc<RwLock<Subscription>>) -> Arc<Consumer> {
        let (tx, _rx) = mpsc::unbounded_channel();
        Arc::new(Consumer::new(
            id,
            format!("consumer-{}", id),
            subscription,
            format!("conn-{}", id),
            tx,
        ))
    }

    #[tokio::test]
    async fn test_subscription_creation() {
        let sub = Subscription::new(
            "my-sub".to_string(),
            "my-topic".to_string(),
            SubscriptionType::Shared,
            create_test_storage(),
        );

        assert_eq!(sub.name, "my-sub");
        assert_eq!(sub.topic, "my-topic");
        assert_eq!(sub.sub_type, SubscriptionType::Shared);
        assert_eq!(sub.get_consumer_count(), 0);
    }

    #[tokio::test]
    async fn test_add_consumer_shared() {
        let subscription = create_test_subscription_arc();
        let mut sub = Subscription::new(
            "sub".to_string(),
            "topic".to_string(),
            SubscriptionType::Shared,
            create_test_storage(),
        );

        let consumer1 = create_test_consumer(1, subscription.clone());
        let consumer2 = create_test_consumer(2, subscription);

        assert!(sub.add_consumer(consumer1).is_ok());
        assert!(sub.add_consumer(consumer2).is_ok());
        assert_eq!(sub.get_consumer_count(), 2);
    }

    #[tokio::test]
    async fn test_add_consumer_exclusive() {
        let subscription = create_test_subscription_arc();
        let mut sub = Subscription::new(
            "sub".to_string(),
            "topic".to_string(),
            SubscriptionType::Exclusive,
            create_test_storage(),
        );

        let consumer1 = create_test_consumer(1, subscription.clone());
        let consumer2 = create_test_consumer(2, subscription);

        // First consumer should succeed
        assert!(sub.add_consumer(consumer1).is_ok());

        // Second consumer should fail
        assert!(sub.add_consumer(consumer2).is_err());
        assert_eq!(sub.get_consumer_count(), 1);
    }

    #[tokio::test]
    async fn test_remove_consumer() {
        let subscription = create_test_subscription_arc();
        let mut sub = Subscription::new(
            "sub".to_string(),
            "topic".to_string(),
            SubscriptionType::Shared,
            create_test_storage(),
        );

        let consumer = create_test_consumer(1, subscription);
        sub.add_consumer(consumer).unwrap();

        assert!(sub.remove_consumer(1).is_some());
        assert!(sub.remove_consumer(999).is_none());
        assert_eq!(sub.get_consumer_count(), 0);
    }

    #[tokio::test]
    async fn test_get_active_consumers() {
        let subscription = create_test_subscription_arc();
        let mut sub = Subscription::new(
            "sub".to_string(),
            "topic".to_string(),
            SubscriptionType::Failover,
            create_test_storage(),
        );

        let consumer1 = create_test_consumer(1, subscription.clone());
        let consumer2 = create_test_consumer(2, subscription);

        sub.add_consumer(consumer1).unwrap();
        sub.add_consumer(consumer2).unwrap();

        let active = sub.get_active_consumers();
        assert_eq!(active.len(), 1); // Failover only returns first
        assert_eq!(active[0].consumer_id, 1);
    }

    #[tokio::test]
    async fn test_get_total_permits() {
        let subscription = create_test_subscription_arc();
        let mut sub = Subscription::new(
            "sub".to_string(),
            "topic".to_string(),
            SubscriptionType::Shared,
            create_test_storage(),
        );

        let consumer1 = create_test_consumer(1, subscription.clone());
        let consumer2 = create_test_consumer(2, subscription);

        // Add permits to consumers
        consumer1.add_permits(10).await;
        consumer2.add_permits(15).await;

        sub.add_consumer(consumer1).unwrap();
        sub.add_consumer(consumer2).unwrap();

        assert_eq!(sub.get_total_permits().await, 25);
    }
}
