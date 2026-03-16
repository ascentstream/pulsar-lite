/*
 * Topic Management
 * Manages producers and subscriptions for a specific topic
 */

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::broker::service::{Consumer, SharedStorage, Producer};
use super::{Subscription, SubscriptionStats, SubscriptionType};

/// Type alias for shared subscription
pub type SharedSubscription = Arc<RwLock<Subscription>>;

/// Topic represents a single topic in the messaging system
/// It manages all producers and subscriptions associated with this topic
#[derive(Debug)]
pub struct Topic {
    /// Topic name (e.g., "persistent://public/default/my-topic")
    pub name: String,
    /// Partition index (-1 for non-partitioned topics, 0+ for partitioned)
    pub partition: i32,
    /// Producers currently connected to this topic (Apache Pulsar style)
    producers: HashMap<u64, Arc<Producer>>,
    /// Subscriptions for this topic (Apache Pulsar style - wrapped in Arc<RwLock>)
    subscriptions: HashMap<String, SharedSubscription>,
    /// Storage backend
    storage: SharedStorage,
}

impl Topic {
    /// Create a new topic
    ///
    /// The partition ID is automatically extracted from the topic name.
    /// Format: "topic-partition-{partition_id}" (e.g., "my-topic-partition-0" -> partition=0)
    /// Non-partitioned topics will have partition=-1
    pub fn new(name: String, storage: SharedStorage) -> Self {
        // Extract partition ID from topic name (Apache Pulsar style)
        // Format: "persistent://public/default/topic-partition-0" -> partition=0
        let partition = Self::extract_partition_from_name(&name);

        log::info!("Creating new topic: {} (partition={})", name, partition);
        Self {
            name,
            partition,
            producers: HashMap::new(),
            subscriptions: HashMap::new(),
            storage,
        }
    }

    /// Extract partition ID from topic name
    /// Returns -1 for non-partitioned topics
    fn extract_partition_from_name(name: &str) -> i32 {
        // Apache Pulsar format: topic-partition-{partition_id}
        // e.g., "persistent://public/default/test-shared-round-robin-partition-0" -> 0
        if let Some(pos) = name.rfind("-partition-") {
            let partition_str = &name[pos + 11..]; // Skip "-partition-"
            if let Ok(partition) = partition_str.parse::<i32>() {
                return partition;
            }
        }
        -1 // Non-partitioned topic
    }

    // ==================== Producer Management ====================

    /// Add a producer to this topic
    pub fn add_producer(&mut self, producer: Arc<Producer>) -> Result<(), String> {
        let producer_id = producer.get_producer_id();
        let producer_name = producer.get_producer_name().to_string();

        // Check if producer already exists
        if self.producers.contains_key(&producer_id) {
            return Err(format!("Producer {} already exists in topic", producer_id));
        }

        self.producers.insert(producer_id, producer);
        log::info!(
            "Added producer '{}' (id={}) to topic '{}' (total={})",
            producer_name, producer_id, self.name, self.producers.len()
        );
        Ok(())
    }

    /// Remove a producer from this topic
    pub fn remove_producer(&mut self, producer_id: u64) -> Option<Arc<Producer>> {
        let producer = self.producers.remove(&producer_id);
        if let Some(ref p) = producer {
            log::info!(
                "Removed producer '{}' (id={}) from topic '{}', remaining={}",
                p.get_producer_name(), producer_id, self.name, self.producers.len()
            );
        }
        producer
    }

    /// Get a producer by ID
    pub fn get_producer(&self, producer_id: u64) -> Option<Arc<Producer>> {
        self.producers.get(&producer_id).cloned()
    }

    /// Get all producers
    pub fn get_producers(&self) -> &HashMap<u64, Arc<Producer>> {
        &self.producers
    }

    /// Get producer count
    pub fn get_producer_count(&self) -> usize {
        self.producers.len()
    }

    /// Check if topic has any producers
    pub fn has_producers(&self) -> bool {
        !self.producers.is_empty()
    }

    // ==================== Subscription Management ====================

    /// Get or create a subscription (Apache Pulsar style)
    ///
    /// This will create the subscription in storage if it doesn't exist
    /// Returns Arc<RwLock<Subscription>> so Consumer can hold a reference
    pub async fn get_or_create_subscription(
        &mut self,
        subscription_name: &str,
        sub_type: SubscriptionType,
    ) -> Result<SharedSubscription, String> {
        if !self.subscriptions.contains_key(subscription_name) {
            // Create subscription in storage
            {
                let mut guard = self.storage.lock().await;
                if let Err(e) = guard.subscribe(&self.name, subscription_name) {
                    return Err(format!("Failed to create subscription in storage: {}", e));
                }
            }

            // Create Subscription object wrapped in Arc<RwLock>
            let subscription = Arc::new(RwLock::new(Subscription::new(
                subscription_name.to_string(),
                self.name.clone(),
                sub_type,
                self.storage.clone(),
            )));
            self.subscriptions.insert(subscription_name.to_string(), subscription.clone());

            log::info!(
                "Created subscription '{}' (type={:?}) on topic '{}'",
                subscription_name, sub_type, self.name
            );

            Ok(subscription)
        } else {
            Ok(self.subscriptions.get(subscription_name).unwrap().clone())
        }
    }

    /// Get a subscription by name (returns Arc clone)
    pub fn get_subscription(&self, subscription_name: &str) -> Option<SharedSubscription> {
        self.subscriptions.get(subscription_name).cloned()
    }

    /// Get subscription count
    pub fn get_subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Check if subscription exists
    pub fn has_subscription(&self, subscription_name: &str) -> bool {
        self.subscriptions.contains_key(subscription_name)
    }

    /// Remove a subscription
    pub fn remove_subscription(&mut self, subscription_name: &str) -> Option<SharedSubscription> {
        let subscription = self.subscriptions.remove(subscription_name);
        if subscription.is_some() {
            log::info!(
                "Removed subscription '{}' from topic '{}'",
                subscription_name, self.name
            );
        }
        subscription
    }

    // ==================== Consumer Helpers ====================

    /// Find a consumer across all subscriptions (Apache Pulsar style)
    pub async fn find_consumer(&self, consumer_id: u64) -> Option<(String, Arc<Consumer>)> {
        for (sub_name, subscription) in &self.subscriptions {
            let sub_guard = subscription.read().await;
            if let Some(consumer) = sub_guard.get_consumer(consumer_id) {
                return Some((sub_name.clone(), consumer.clone()));
            }
        }
        None
    }

    /// Get total consumer count across all subscriptions
    pub async fn get_total_consumer_count(&self) -> usize {
        let mut total = 0;
        for subscription in self.subscriptions.values() {
            let sub_guard = subscription.read().await;
            total += sub_guard.get_consumer_count();
        }
        total
    }

    // ==================== Message Publishing ====================

    /// Publish a message to this topic
    /// This method stores the message in the storage backend.
    /// Call dispatch_to_subscriptions() after this to push to consumers.
    pub async fn publish_message(
        &mut self,
        payload: &[u8],
    ) -> Result<crate::storage::MessageId, Box<dyn std::error::Error>> {
        log::debug!("Publishing message to topic '{}' partition '{}' ({} bytes)", self.name, self.partition, payload.len());

        // Store message in storage backend with partition info
        let message_id = {
            let mut guard = self.storage.lock().await;
            guard.append_message(&self.name, self.partition, payload)?
        };

        log::info!("Published message {}:{}:{} to topic '{}'",
            message_id.ledger, message_id.entry, message_id.partition, self.name);

        Ok(message_id)
    }

    /// Dispatch messages to all subscriptions (Push mode - Apache Pulsar style)
    ///
    /// This should be called after publish_message() to push messages to consumers.
    /// It triggers the dispatcher for each subscription to deliver pending messages.
    pub async fn dispatch_to_subscriptions(
        &self
    ) {
        let subscription_count = self.subscriptions.len();
        if subscription_count == 0 {
            log::debug!("No subscriptions on topic '{}', skipping dispatch", self.name);
            return;
        }

        log::debug!(
            "Dispatching messages to {} subscription(s) on topic '{}'",
            subscription_count, self.name
        );

        for (sub_name, subscription) in &self.subscriptions {
            let sub_guard = subscription.read().await;

            // Trigger message dispatch for this subscription
            if let Err(e) = sub_guard.dispatch_messages().await {
                log::error!(
                    "Failed to dispatch message to subscription '{}' on topic '{}': {}",
                    sub_name, self.name, e
                );
            }
        }
    }

    // ==================== Statistics ====================

    /// Get topic statistics
    pub async fn get_stats(&self) -> TopicStats {
        let mut subscription_stats = Vec::new();
        for subscription in self.subscriptions.values() {
            let sub_guard = subscription.read().await;
            subscription_stats.push(sub_guard.get_stats().await);
        }

        TopicStats {
            topic_name: self.name.clone(),
            producer_count: self.producers.len(),
            subscription_count: self.subscriptions.len(),
            consumer_count: self.get_total_consumer_count().await,
            subscriptions: subscription_stats,
        }
    }

    /// Check if topic is idle (no producers or consumers)
    pub async fn is_idle(&self) -> bool {
        self.producers.is_empty() && self.get_total_consumer_count().await == 0
    }
}

/// Statistics for a topic
#[derive(Debug, Clone)]
pub struct TopicStats {
    pub topic_name: String,
    pub producer_count: usize,
    pub subscription_count: usize,
    pub consumer_count: usize,
    pub subscriptions: Vec<SubscriptionStats>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;
    use std::path::Path;
    use std::sync::Arc as StdArc;
    use tokio::sync::{Mutex, RwLock, mpsc};

    fn create_test_storage() -> SharedStorage {
        StdArc::new(Mutex::new(Storage::new(Path::new("/tmp/test-topic-storage")).unwrap()))
    }

    fn create_test_producer(id: u64, topic_ref: Arc<RwLock<Topic>>) -> Arc<Producer> {
        StdArc::new(Producer::new(
            id,
            format!("producer-{}", id),
            topic_ref,
            format!("conn-{}", id),
        ))
    }

    fn create_test_consumer(id: u64, subscription: SharedSubscription) -> Arc<Consumer> {
        let (tx, _rx) = mpsc::unbounded_channel();
        StdArc::new(Consumer::new(
            id,
            format!("consumer-{}", id),
            subscription,
            format!("conn-{}", id),
            tx,
            0,
        ))
    }

    #[tokio::test]
    async fn test_topic_creation() {
        let storage = create_test_storage();
        let topic = Topic::new("test-topic".to_string(), storage);

        assert_eq!(topic.name, "test-topic");
        assert_eq!(topic.get_producer_count(), 0);
        assert_eq!(topic.get_subscription_count(), 0);
    }

    #[tokio::test]
    async fn test_producer_management() {
        let storage = create_test_storage();
        let topic_ref = StdArc::new(RwLock::new(Topic::new("test-topic".to_string(), storage)));
        let mut topic = topic_ref.write().await;

        let producer1 = create_test_producer(1, topic_ref.clone());
        let producer2 = create_test_producer(2, topic_ref.clone());

        // Add producers
        assert!(topic.add_producer(producer1).is_ok());
        assert!(topic.add_producer(producer2).is_ok());
        assert_eq!(topic.get_producer_count(), 2);

        // Try to add duplicate
        let producer1_dup = create_test_producer(1, topic_ref.clone());
        assert!(topic.add_producer(producer1_dup).is_err());

        // Remove producer
        assert!(topic.remove_producer(1).is_some());
        assert_eq!(topic.get_producer_count(), 1);
        assert!(topic.remove_producer(999).is_none());
    }

    #[tokio::test]
    async fn test_subscription_management() {
        let storage = create_test_storage();
        let mut topic = Topic::new("persistent://public/default/test".to_string(), storage);

        // Create subscription
        let sub = topic.get_or_create_subscription("sub1", SubscriptionType::Shared).await;
        assert!(sub.is_ok());
        assert_eq!(topic.get_subscription_count(), 1);

        // Get existing subscription
        let sub2 = topic.get_or_create_subscription("sub1", SubscriptionType::Shared).await;
        assert!(sub2.is_ok());
        assert_eq!(topic.get_subscription_count(), 1); // Should not create duplicate

        // Create another subscription
        let sub3 = topic.get_or_create_subscription("sub2", SubscriptionType::Exclusive).await;
        assert!(sub3.is_ok());
        assert_eq!(topic.get_subscription_count(), 2);

        // Check subscription exists
        assert!(topic.has_subscription("sub1"));
        assert!(!topic.has_subscription("sub999"));
    }

    #[tokio::test]
    async fn test_topic_stats() {
        let storage = create_test_storage();
        let topic_ref = StdArc::new(RwLock::new(Topic::new("test-topic".to_string(), storage)));
        let mut topic = topic_ref.write().await;

        // Add producer
        let producer = create_test_producer(1, topic_ref.clone());
        topic.add_producer(producer).unwrap();

        // Add subscription with consumer
        let sub = topic.get_or_create_subscription("sub1", SubscriptionType::Shared).await.unwrap();
        let consumer = create_test_consumer(1, sub.clone());
        {
            let mut sub_guard = sub.write().await;
            sub_guard.add_consumer(consumer).unwrap();
        }

        // Get stats
        let stats = topic.get_stats().await;
        assert_eq!(stats.topic_name, "test-topic");
        assert_eq!(stats.producer_count, 1);
        assert_eq!(stats.subscription_count, 1);
        assert_eq!(stats.consumer_count, 1);
    }

    #[tokio::test]
    async fn test_is_idle() {
        let storage = create_test_storage();
        let topic_ref = StdArc::new(RwLock::new(Topic::new("test-topic".to_string(), storage)));
        let mut topic = topic_ref.write().await;

        assert!(topic.is_idle().await);

        // Add producer
        let producer = create_test_producer(1, topic_ref.clone());
        topic.add_producer(producer).unwrap();
        assert!(!topic.is_idle().await);

        // Remove producer
        topic.remove_producer(1);
        assert!(topic.is_idle().await);
    }
}
