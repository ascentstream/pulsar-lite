/*
 * Topic Management
 * Manages producers and subscriptions for a specific topic
 */

use bytes::Bytes;
use crate::broker::non_persistent::NonPersistentTopicRuntime;
use crate::broker::service::{Consumer, SharedStorage, Producer};
use crate::storage::NonPersistentEntry;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use super::{
    KeySharedPolicy, Subscription, SubscriptionRuntimeMode, SubscriptionStats, SubscriptionType,
};

/// Type alias for shared subscription
pub type SharedSubscription = Arc<RwLock<Subscription>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TopicRuntimeMode {
    #[default]
    PersistentStyle,
    NonPersistent,
}

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
    /// Internal topic runtime mode.
    runtime_mode: TopicRuntimeMode,
    /// Dedicated non-persistent topic runtime state.
    non_persistent_runtime: Option<NonPersistentTopicRuntime>,
    /// Storage backend
    storage: SharedStorage,
}

impl Topic {
    fn runtime_mode_from_topic_name(name: &str) -> TopicRuntimeMode {
        if name.starts_with("non-persistent://") {
            TopicRuntimeMode::NonPersistent
        } else {
            TopicRuntimeMode::PersistentStyle
        }
    }

    /// Create a new topic
    ///
    /// The partition ID is automatically extracted from the topic name.
    /// Format: "topic-partition-{partition_id}" (e.g., "my-topic-partition-0" -> partition=0)
    /// Non-partitioned topics will have partition=-1
    pub fn new(name: String, storage: SharedStorage) -> Self {
        // Extract partition ID from topic name (Apache Pulsar style)
        // Format: "persistent://public/default/topic-partition-0" -> partition=0
        let partition = Self::extract_partition_from_name(&name);
        let runtime_mode = Self::runtime_mode_from_topic_name(&name);

        log::info!(
            "Creating new topic: {} (partition={}, runtime_mode={:?})",
            name,
            partition,
            runtime_mode
        );
        Self {
            name,
            partition,
            producers: HashMap::new(),
            subscriptions: HashMap::new(),
            runtime_mode,
            non_persistent_runtime: None,
            storage,
        }
    }

    pub fn runtime_mode(&self) -> TopicRuntimeMode {
        self.runtime_mode
    }

    pub fn set_runtime_mode(&mut self, mode: TopicRuntimeMode) {
        self.runtime_mode = mode;
    }

    fn reuse_or_create_non_persistent_runtime(&mut self) {
        if self.non_persistent_runtime.is_none() {
            self.non_persistent_runtime = Some(NonPersistentTopicRuntime::new());
        }
    }

    pub fn non_persistent_pending_message_count(&self) -> usize {
        self.non_persistent_runtime
            .as_ref()
            .map(|runtime| runtime.pending_message_count())
            .unwrap_or(0)
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
        self.get_or_create_subscription_with_options(
            subscription_name,
            sub_type,
            HashMap::new(),
            None,
        )
        .await
    }

    pub async fn get_or_create_subscription_with_options(
        &mut self,
        subscription_name: &str,
        sub_type: SubscriptionType,
        properties: HashMap<String, String>,
        key_shared_policy: Option<KeySharedPolicy>,
    ) -> Result<SharedSubscription, String> {
        if !self.subscriptions.contains_key(subscription_name) {
            if self.runtime_mode == TopicRuntimeMode::PersistentStyle {
                {
                    let mut guard = self.storage.lock().await;
                    if let Err(e) = guard.subscribe(&self.name, subscription_name) {
                        return Err(format!("Failed to create subscription in storage: {}", e));
                    }
                }
            }

            // Create Subscription object wrapped in Arc<RwLock>
            let subscription = Arc::new(RwLock::new(Subscription::new_with_options(
                subscription_name.to_string(),
                self.name.clone(),
                sub_type,
                match self.runtime_mode {
                    TopicRuntimeMode::PersistentStyle => SubscriptionRuntimeMode::PersistentStyle,
                    TopicRuntimeMode::NonPersistent => SubscriptionRuntimeMode::NonPersistent,
                },
                properties,
                key_shared_policy,
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
    /// Persistent-style topics store into the managed-ledger-like backend.
    /// Non-persistent topics append only to runtime memory and avoid storage.
    pub async fn publish_message(
        &mut self,
        payload: &[u8],
    ) -> Result<crate::storage::MessageId, Box<dyn std::error::Error>> {
        self.publish_message_with_metadata(None, payload).await
    }

    pub async fn publish_message_with_metadata(
        &mut self,
        metadata: Option<&[u8]>,
        payload: &[u8],
    ) -> Result<crate::storage::MessageId, Box<dyn std::error::Error>> {
        log::debug!(
            "Publishing message to topic '{}' partition '{}' (metadata={} bytes, payload={} bytes)",
            self.name,
            self.partition,
            metadata.map(|value| value.len()).unwrap_or(0),
            payload.len()
        );

        let message_id = match self.runtime_mode {
            TopicRuntimeMode::PersistentStyle => {
                let mut guard = self.storage.lock().await;
                guard.append_message(&self.name, self.partition, payload)?
            }
            TopicRuntimeMode::NonPersistent => {
                self.reuse_or_create_non_persistent_runtime();
                let entry = NonPersistentEntry::create(
                    0,
                    0,
                    self.partition,
                    Bytes::copy_from_slice(metadata.unwrap_or_default()),
                    Bytes::copy_from_slice(payload),
                );
                self.non_persistent_runtime
                    .as_mut()
                    .expect("non-persistent runtime just initialized")
                    .publish_entry(entry);

                crate::storage::MessageId { ledger: 0, entry: 0, partition: self.partition }
            }
        };

        log::info!("Published message {}:{}:{} to topic '{}'",
            message_id.ledger, message_id.entry, message_id.partition, self.name);

        Ok(message_id)
    }

    /// Dispatch messages to all subscriptions (Push mode - Apache Pulsar style)
    ///
    /// This should be called after publish_message() to push messages to consumers.
    /// It triggers the dispatcher for each subscription to deliver pending messages.
    pub async fn dispatch_to_subscriptions(&self) {
        let subscription_count = self.subscriptions.len();
        match self.runtime_mode {
            TopicRuntimeMode::PersistentStyle => {
                if subscription_count == 0 {
                    log::debug!("No subscriptions on topic '{}', skipping dispatch", self.name);
                    return;
                }
                log::debug!(
                    "Dispatching messages to {} subscription(s) on topic '{}' with runtime={:?}",
                    subscription_count, self.name, self.runtime_mode
                );
                for (sub_name, subscription) in &self.subscriptions {
                    let sub_guard = subscription.read().await;
                    if let Err(e) = sub_guard.dispatch_messages().await {
                        log::error!(
                            "Failed to dispatch message to subscription '{}' on topic '{}': {}",
                            sub_name, self.name, e
                        );
                    }
                }
            }
            TopicRuntimeMode::NonPersistent => {
                log::debug!(
                    "Skipping non-persistent dispatch for topic '{}' in foundation runtime (subscriptions={}, pending_entries={})",
                    self.name,
                    subscription_count,
                    self.non_persistent_pending_message_count()
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

    pub async fn get_last_message_id(&self) -> Result<Option<crate::storage::MessageId>, String> {
        match self.runtime_mode {
            TopicRuntimeMode::PersistentStyle => Ok(self
                .storage
                .lock()
                .await
                .get_messages(&self.name)
                .last()
                .map(|(message_id, _)| message_id.clone())),
            TopicRuntimeMode::NonPersistent => {
                Err("getLastMessageId is unsupported for non-persistent topics".to_string())
            }
        }
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
    use crate::protocol::codec::proto::pulsar::MessageMetadata;
    use crate::storage::Storage;
    use prost::Message;
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

    fn create_test_consumer_with_rx(
        id: u64,
        subscription: SharedSubscription,
    ) -> (
        Arc<Consumer>,
        mpsc::UnboundedReceiver<(u64, crate::broker::service::PendingMessage)>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            StdArc::new(Consumer::new(
                id,
                format!("consumer-{}", id),
                subscription,
                format!("conn-{}", id),
                tx,
                0,
            )),
            rx,
        )
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
    async fn test_non_persistent_publish_uses_runtime_only_path() {
        let storage = create_test_storage();
        let mut topic = Topic::new("test-topic".to_string(), storage.clone());
        topic.set_runtime_mode(TopicRuntimeMode::NonPersistent);

        let message_id = topic.publish_message(b"hello").await.unwrap();

        assert_eq!(message_id.ledger, 0);
        assert_eq!(message_id.entry, 0);
        assert_eq!(topic.non_persistent_pending_message_count(), 1);
        assert!(storage.lock().await.get_messages("test-topic").is_empty());
    }

    #[tokio::test]
    async fn test_non_persistent_without_subscriptions_releases_pending_entries() {
        let storage = create_test_storage();
        let mut topic = Topic::new("test-topic".to_string(), storage);
        topic.set_runtime_mode(TopicRuntimeMode::NonPersistent);

        topic.publish_message(b"hello").await.unwrap();
        assert_eq!(topic.non_persistent_pending_message_count(), 1);

        topic.dispatch_to_subscriptions().await;
        assert_eq!(topic.non_persistent_pending_message_count(), 1);
    }

    #[tokio::test]
    async fn test_non_persistent_publish_preserves_metadata_in_runtime_buffer() {
        let storage = create_test_storage();
        let mut topic = Topic::new("test-topic".to_string(), storage);
        topic.set_runtime_mode(TopicRuntimeMode::NonPersistent);

        let metadata = MessageMetadata {
            producer_name: "producer-1".to_string(),
            sequence_id: 9,
            ordering_key: Some(b"order-key".to_vec()),
            ..Default::default()
        }
        .encode_to_vec();

        let message_id = topic
            .publish_message_with_metadata(Some(&metadata), b"hello")
            .await
            .unwrap();
        assert_eq!(message_id.ledger, 0);
        assert_eq!(message_id.entry, 0);

        let entries = topic
            .non_persistent_runtime
            .as_mut()
            .expect("runtime initialized")
            .drain_published_messages();
        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert_eq!(entry.metadata(), metadata.as_slice());
        assert_eq!(entry.payload(), b"hello");
    }

    #[tokio::test]
    async fn test_non_persistent_dispatch_foundation_keeps_entries_buffered() {
        let storage = create_test_storage();
        let mut topic = Topic::new("test-topic".to_string(), storage);
        topic.set_runtime_mode(TopicRuntimeMode::NonPersistent);

        let subscription = topic
            .get_or_create_subscription("sub1", SubscriptionType::Shared)
            .await
            .unwrap();
        let (consumer1, _rx1) = create_test_consumer_with_rx(1, subscription.clone());
        let (consumer2, _rx2) = create_test_consumer_with_rx(2, subscription.clone());
        {
            let mut sub_guard = subscription.write().await;
            sub_guard.add_consumer(consumer1).unwrap();
            sub_guard.add_consumer(consumer2).unwrap();
        }
        let consumer1 = {
            let sub_guard = subscription.read().await;
            sub_guard.get_consumer(1).unwrap()
        };
        let consumer2 = {
            let sub_guard = subscription.read().await;
            sub_guard.get_consumer(2).unwrap()
        };
        consumer1.add_permits(1).await;
        consumer2.add_permits(1).await;
        {
            let sub_guard = subscription.read().await;
            sub_guard.consumer_flow(1, 1).await;
            sub_guard.consumer_flow(2, 1).await;
        }

        topic.publish_message(b"first").await.unwrap();
        topic.publish_message(b"second").await.unwrap();
        topic.dispatch_to_subscriptions().await;

        assert_eq!(topic.non_persistent_pending_message_count(), 2);
    }

    #[tokio::test]
    async fn test_non_persistent_exclusive_runtime_tracks_consumer_and_flow() {
        let storage = create_test_storage();
        let mut topic = Topic::new("test-topic".to_string(), storage);
        topic.set_runtime_mode(TopicRuntimeMode::NonPersistent);

        let subscription = topic
            .get_or_create_subscription("sub1", SubscriptionType::Exclusive)
            .await
            .unwrap();
        let (consumer, _rx) = create_test_consumer_with_rx(1, subscription.clone());
        {
            let mut sub_guard = subscription.write().await;
            sub_guard.add_consumer(consumer).unwrap();
        }

        let consumer = {
            let sub_guard = subscription.read().await;
            sub_guard.get_consumer(1).unwrap()
        };
        consumer.add_permits(1).await;
        {
            let sub_guard = subscription.read().await;
            sub_guard.consumer_flow(1, 1).await;
        }

        topic.publish_message(b"allowed").await.unwrap();
        topic.dispatch_to_subscriptions().await;
        assert_eq!(topic.non_persistent_pending_message_count(), 1);
    }

    #[tokio::test]
    async fn test_non_persistent_failover_promotes_standby_consumer() {
        let storage = create_test_storage();
        let mut topic = Topic::new("test-topic".to_string(), storage);
        topic.set_runtime_mode(TopicRuntimeMode::NonPersistent);

        let subscription = topic
            .get_or_create_subscription("sub1", SubscriptionType::Failover)
            .await
            .unwrap();
        let (consumer1, mut rx1) = create_test_consumer_with_rx(1, subscription.clone());
        let (consumer2, mut rx2) = create_test_consumer_with_rx(2, subscription.clone());
        {
            let mut sub_guard = subscription.write().await;
            sub_guard.add_consumer(consumer2.clone()).unwrap();
            sub_guard.add_consumer(consumer1.clone()).unwrap();
        }

        consumer1.add_permits(1).await;
        {
            let sub_guard = subscription.read().await;
            sub_guard.consumer_flow(1, 1).await;
        }

        topic.publish_message(b"first").await.unwrap();
        topic.dispatch_to_subscriptions().await;
        assert!(rx2.try_recv().is_err());
        assert!(rx1.try_recv().is_err());

        {
            let mut sub_guard = subscription.write().await;
            assert!(sub_guard.remove_consumer(1).is_some());
        }

        consumer2.add_permits(1).await;
        {
            let sub_guard = subscription.read().await;
            sub_guard.consumer_flow(2, 1).await;
        }

        topic.publish_message(b"second").await.unwrap();
        topic.dispatch_to_subscriptions().await;
        let active = {
            let sub_guard = subscription.read().await;
            sub_guard.get_active_consumers()
        };
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].consumer_id, 2);
    }

    #[tokio::test]
    async fn test_non_persistent_failover_uses_partition_selection() {
        let storage = create_test_storage();
        let mut topic = Topic::new("test-topic-partition-1".to_string(), storage);
        topic.set_runtime_mode(TopicRuntimeMode::NonPersistent);

        let subscription = topic
            .get_or_create_subscription("sub1", SubscriptionType::Failover)
            .await
            .unwrap();
        let (consumer1, mut rx1) = create_test_consumer_with_rx(1, subscription.clone());
        let (consumer2, mut rx2) = create_test_consumer_with_rx(2, subscription.clone());
        {
            let mut sub_guard = subscription.write().await;
            sub_guard.add_consumer(consumer1.clone()).unwrap();
            sub_guard.add_consumer(consumer2.clone()).unwrap();
        }

        consumer2.add_permits(1).await;
        {
            let sub_guard = subscription.read().await;
            sub_guard.consumer_flow(2, 1).await;
        }

        topic.publish_message(b"first").await.unwrap();
        topic.dispatch_to_subscriptions().await;
        assert!(rx1.try_recv().is_err());
        assert!(rx2.try_recv().is_err());

        {
            let mut sub_guard = subscription.write().await;
            assert!(sub_guard.remove_consumer(2).is_some());
        }

        let active = {
            let sub_guard = subscription.read().await;
            sub_guard.get_active_consumers()
        };
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].consumer_id, 1);
    }

    #[tokio::test]
    async fn test_non_persistent_foundation_reports_zero_drop_counts() {
        let storage = create_test_storage();
        let mut topic = Topic::new("test-topic".to_string(), storage);
        topic.set_runtime_mode(TopicRuntimeMode::NonPersistent);

        let subscription = topic
            .get_or_create_subscription("sub1", SubscriptionType::Shared)
            .await
            .unwrap();
        let (consumer, _rx) = create_test_consumer_with_rx(1, subscription.clone());
        {
            let mut sub_guard = subscription.write().await;
            sub_guard.add_consumer(consumer).unwrap();
        }

        topic.publish_message(b"drop-me").await.unwrap();
        topic.dispatch_to_subscriptions().await;

        let stats = subscription.read().await.get_stats().await;
        assert_eq!(stats.dropped_messages, 0);
    }

    #[tokio::test]
    async fn test_non_persistent_last_message_id_is_unsupported() {
        let storage = create_test_storage();
        let mut topic = Topic::new("test-topic".to_string(), storage);
        topic.set_runtime_mode(TopicRuntimeMode::NonPersistent);
        topic.publish_message(b"hello").await.unwrap();

        let error = topic.get_last_message_id().await.unwrap_err();
        assert!(error.contains("unsupported"));
    }

    #[tokio::test]
    async fn test_non_persistent_topic_domain_sets_runtime_mode() {
        let storage = create_test_storage();
        let topic = Topic::new(
            "non-persistent://public/default/test-topic".to_string(),
            storage,
        );

        assert_eq!(topic.runtime_mode(), TopicRuntimeMode::NonPersistent);
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
