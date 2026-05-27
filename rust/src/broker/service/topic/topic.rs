/*
 * Topic Management
 * Manages producers and subscriptions for a specific topic
 */

use super::{
    KeySharedPolicy, Subscription, SubscriptionRuntimeMode, SubscriptionStats, SubscriptionType,
};

use crate::broker::service::{Consumer, Producer, SharedStorage};
use crate::storage::{MessageId, NonPersistentEntry, Storage};
use bytes::Bytes;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Type alias for shared subscription
pub type SharedSubscription = Arc<RwLock<Subscription>>;

pub struct NonPersistentPublish {
    message_id: MessageId,
    entry: NonPersistentEntry,
    subscriptions: Vec<SharedSubscription>,
}

impl NonPersistentPublish {
    pub fn message_id(&self) -> MessageId {
        self.message_id.clone()
    }
    pub async fn dispatch_sequential(self) {
        let entry = self.entry;
        for subscription in self.subscriptions {
            let result = {
                let sub_guard = subscription.read().await;
                sub_guard
                    .send_non_persistent_entries(vec![entry.retained_duplicate()])
                    .await
            };

            if let Err(e) = result {
                log::error!(
                    "Failed to dispatch non-persistent entry to subscription: {}",
                    e
                )
            }
        }
        entry.release();
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TopicPublishRate {
    pub messages_per_sec: u64,
    pub bytes_per_sec: u64,
}

#[derive(Debug)]
struct TopicPublishRateLimiter {
    limits: TopicPublishRate,
    window_started_at: Instant,
    messages_in_window: u64,
    bytes_in_window: u64,
}

impl TopicPublishRateLimiter {
    fn new() -> Self {
        Self {
            limits: TopicPublishRate::default(),
            window_started_at: Instant::now(),
            messages_in_window: 0,
            bytes_in_window: 0,
        }
    }

    fn set_limits(&mut self, limits: TopicPublishRate) {
        self.limits = limits;
        self.window_started_at = Instant::now();
        self.messages_in_window = 0;
        self.bytes_in_window = 0;
    }

    fn allow_publish(&mut self, message_count: u64, bytes: u64) -> bool {
        if self.limits.messages_per_sec == 0 && self.limits.bytes_per_sec == 0 {
            return true;
        }

        if self.window_started_at.elapsed() >= Duration::from_secs(1) {
            self.window_started_at = Instant::now();
            self.messages_in_window = 0;
            self.bytes_in_window = 0;
        }

        let next_messages = self.messages_in_window.saturating_add(message_count);
        let next_bytes = self.bytes_in_window.saturating_add(bytes);
        let messages_ok =
            self.limits.messages_per_sec == 0 || next_messages <= self.limits.messages_per_sec;
        let bytes_ok = self.limits.bytes_per_sec == 0 || next_bytes <= self.limits.bytes_per_sec;

        if messages_ok && bytes_ok {
            self.messages_in_window = next_messages;
            self.bytes_in_window = next_bytes;
            true
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub struct TopicPublishRateExceeded {
    topic_name: String,
}

impl fmt::Display for TopicPublishRateExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Topic publish rate exceeded for '{}'", self.topic_name)
    }
}

impl std::error::Error for TopicPublishRateExceeded {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TopicRuntimeMode {
    #[default]
    Persistent,
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
    /// Storage backend
    storage: SharedStorage,
    publish_rate_limiter: TopicPublishRateLimiter,
}

impl Topic {
    fn runtime_mode_from_topic_name(name: &str) -> TopicRuntimeMode {
        match Storage::parse_topic_name(name) {
            Ok(parsed) if parsed.domain == "non-persistent" => TopicRuntimeMode::NonPersistent,
            _ => TopicRuntimeMode::Persistent,
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
            storage,
            publish_rate_limiter: TopicPublishRateLimiter::new(),
        }
    }

    pub fn runtime_mode(&self) -> TopicRuntimeMode {
        self.runtime_mode
    }

    pub fn set_runtime_mode(&mut self, mode: TopicRuntimeMode) {
        self.runtime_mode = mode;
    }

    pub fn set_publish_rate(&mut self, limits: TopicPublishRate) {
        self.publish_rate_limiter.set_limits(limits);
    }

    fn build_non_persistent_publish(
        &self,
        metadata: Option<Bytes>,
        payload: Bytes,
    ) -> NonPersistentPublish {
        let message_id = MessageId {
            ledger: 0,
            entry: 0,
            partition: self.partition,
        };

        let entry = NonPersistentEntry::create(
            message_id.ledger,
            message_id.entry,
            message_id.partition,
            metadata.unwrap_or_default(),
            payload,
        );

        let subscriptions = self.subscriptions.values().cloned().collect();

        NonPersistentPublish {
            message_id,
            entry,
            subscriptions,
        }
    }

    fn validate_publish_rate(
        &mut self,
        metadata: Option<&Bytes>,
        payload_len: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let total_bytes = metadata
            .map(|value| value.len() as u64)
            .unwrap_or(0)
            .saturating_add(payload_len as u64);

        if !self.publish_rate_limiter.allow_publish(1, total_bytes) {
            return Err(Box::new(TopicPublishRateExceeded {
                topic_name: self.name.clone(),
            }));
        }
        Ok(())
    }

    pub fn prepare_non_persistent_publish(
        &mut self,
        metadata: Option<Bytes>,
        payload: Bytes,
    ) -> Result<NonPersistentPublish, Box<dyn std::error::Error + Send + Sync>> {
        if self.runtime_mode != TopicRuntimeMode::NonPersistent {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "prepare_non_persistent_publish called for persistent topic",
            )));
        }

        log::debug!(
            "Publishing non-persistent message to topic '{}' partition '{}' (metadata={} bytes, payload={} bytes)",
            self.name,
            self.partition,
            metadata.as_ref().map(|value| value.len()).unwrap_or(0),
            payload.len()
        );

        self.validate_publish_rate(metadata.as_ref(), payload.len())?;

        Ok(self.build_non_persistent_publish(metadata, payload))
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

        // Evict stale producer with the same ID (client reconnected on a new connection)
        if let Some(old) = self.producers.remove(&producer_id) {
            log::debug!(
                "Evicted stale producer '{}' (id={}) from topic '{}'",
                old.get_producer_name(),
                producer_id,
                self.name
            );
        }

        self.producers.insert(producer_id, producer);
        log::info!(
            "Added producer '{}' (id={}) to topic '{}' (total={})",
            producer_name,
            producer_id,
            self.name,
            self.producers.len()
        );
        Ok(())
    }

    /// Remove a producer from this topic
    pub fn remove_producer(&mut self, producer_id: u64) -> Option<Arc<Producer>> {
        let producer = self.producers.remove(&producer_id);
        if let Some(ref p) = producer {
            log::info!(
                "Removed producer '{}' (id={}) from topic '{}', remaining={}",
                p.get_producer_name(),
                producer_id,
                self.name,
                self.producers.len()
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
            if self.runtime_mode == TopicRuntimeMode::Persistent {
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
                    TopicRuntimeMode::Persistent => SubscriptionRuntimeMode::Persistent,
                    TopicRuntimeMode::NonPersistent => SubscriptionRuntimeMode::NonPersistent,
                },
                properties,
                key_shared_policy,
                self.storage.clone(),
            )));
            self.subscriptions
                .insert(subscription_name.to_string(), subscription.clone());

            log::info!(
                "Created subscription '{}' (type={:?}) on topic '{}'",
                subscription_name,
                sub_type,
                self.name
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
                subscription_name,
                self.name
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
    /// Persistent topics store into the managed-ledger-like backend.
    /// Non-persistent topics append only to runtime memory and avoid storage.
    pub async fn publish_message(
        &mut self,
        metadata: Option<Bytes>,
        payload: Bytes,
    ) -> Result<crate::storage::MessageId, Box<dyn std::error::Error + Send + Sync>> {
        log::debug!(
            "Publishing message to topic '{}' partition '{}' (metadata={} bytes, payload={} bytes)",
            self.name,
            self.partition,
            metadata.as_ref().map(|value| value.len()).unwrap_or(0),
            payload.len()
        );

        self.validate_publish_rate(metadata.as_ref(), payload.len())?;

        let message_id = match self.runtime_mode {
            TopicRuntimeMode::Persistent => {
                let mut guard = self.storage.lock().await;
                guard.append_message(&self.name, self.partition, &payload)?
            }
            TopicRuntimeMode::NonPersistent => {
                let publish = self.build_non_persistent_publish(metadata, payload);
                let message_id = publish.message_id();
                publish.dispatch_sequential().await;
                message_id
            }
        };

        log::debug!(
            "Published message {}:{}:{} to topic '{}'",
            message_id.ledger,
            message_id.entry,
            message_id.partition,
            self.name
        );

        Ok(message_id)
    }

    /// Dispatch messages to all subscriptions (Push mode - Apache Pulsar style)
    ///
    /// This should be called after publish_message() to push messages to consumers.
    /// It triggers the dispatcher for each subscription to deliver pending messages.
    pub async fn dispatch_to_subscriptions(&mut self) {
        let subscription_count = self.subscriptions.len();
        match self.runtime_mode {
            TopicRuntimeMode::Persistent => {
                if subscription_count == 0 {
                    log::debug!(
                        "No subscriptions on topic '{}', skipping dispatch",
                        self.name
                    );
                    return;
                }
                log::debug!(
                    "Dispatching messages to {} subscription(s) on topic '{}' with runtime={:?}",
                    subscription_count,
                    self.name,
                    self.runtime_mode
                );
                for (sub_name, subscription) in &self.subscriptions {
                    let sub_guard = subscription.read().await;
                    if let Err(e) = sub_guard.dispatch_messages().await {
                        log::error!(
                            "Failed to dispatch message to subscription '{}' on topic '{}': {}",
                            sub_name,
                            self.name,
                            e
                        );
                    }
                }
            }
            TopicRuntimeMode::NonPersistent => {
                log::debug!(
                    "Non-persistent topic '{}' dispatches immediately on publish; no topic backlog to drain",
                    self.name
                );
            }
        }
    }

    /// Returns cloned Arc references to all subscriptions.
    /// Call while holding any lock on Topic, then use the Arcs after releasing.
    pub fn get_subscription_refs(&self) -> Vec<SharedSubscription> {
        self.subscriptions.values().cloned().collect()
    }

    // ==================== Statistics ====================

    /// Get topic statistics
    pub async fn get_stats(&self) -> TopicStats {
        let mut subscription_stats = Vec::new();
        let mut consumer_count = 0;
        for subscription in self.subscriptions.values() {
            let sub_guard = subscription.read().await;
            let stats = sub_guard.get_stats().await;
            consumer_count += stats.consumer_count;
            subscription_stats.push(stats);
        }

        TopicStats {
            topic_name: self.name.clone(),
            producer_count: self.producers.len(),
            subscription_count: self.subscriptions.len(),
            consumer_count,
            subscriptions: subscription_stats,
        }
    }

    /// Check if topic is idle (no producers or consumers)
    pub async fn is_idle(&self) -> bool {
        self.producers.is_empty() && self.get_total_consumer_count().await == 0
    }

    pub async fn get_last_message_id(&self) -> Result<Option<crate::storage::MessageId>, String> {
        match self.runtime_mode {
            TopicRuntimeMode::Persistent => Ok(self
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
    use std::time::Instant;
    use tokio::sync::{mpsc, Mutex, RwLock};

    fn create_test_storage() -> SharedStorage {
        StdArc::new(Mutex::new(
            Storage::new_memory(Path::new("/tmp/test-topic-storage")).unwrap(),
        ))
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
        let (tx, _rx) = mpsc::channel(8192);
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
        mpsc::Receiver<(u64, crate::broker::service::PendingMessage)>,
    ) {
        let (tx, rx) = mpsc::channel(8192);
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
    async fn test_non_persistent_publish_dispatches_immediately_without_topic_backlog() {
        let storage = create_test_storage();
        let mut topic = Topic::new("test-topic".to_string(), storage.clone());
        topic.set_runtime_mode(TopicRuntimeMode::NonPersistent);

        let message_id = topic
            .publish_message(None, Bytes::from_static(b"hello"))
            .await
            .unwrap();

        assert_eq!(message_id.ledger, 0);
        assert_eq!(message_id.entry, 0);
        assert!(storage.lock().await.get_messages("test-topic").is_empty());
    }

    #[tokio::test]
    async fn test_non_persistent_without_subscriptions_does_not_leave_topic_backlog() {
        let storage = create_test_storage();
        let mut topic = Topic::new("test-topic".to_string(), storage);
        topic.set_runtime_mode(TopicRuntimeMode::NonPersistent);

        topic
            .publish_message(None, Bytes::from_static(b"hello"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_non_persistent_publish_preserves_metadata_through_dispatch() {
        let storage = create_test_storage();
        let mut topic = Topic::new("test-topic".to_string(), storage);
        topic.set_runtime_mode(TopicRuntimeMode::NonPersistent);

        let subscription = topic
            .get_or_create_subscription("sub1", SubscriptionType::Exclusive)
            .await
            .unwrap();
        let (consumer, mut rx) = create_test_consumer_with_rx(1, subscription.clone());
        consumer.add_permits(1).await;
        {
            let mut sub_guard = subscription.write().await;
            sub_guard.add_consumer(consumer).unwrap();
        }

        let metadata = MessageMetadata {
            producer_name: "producer-1".to_string(),
            sequence_id: 9,
            ordering_key: Some(b"order-key".to_vec()),
            ..Default::default()
        }
        .encode_to_vec();

        let message_id = topic
            .publish_message(
                Some(Bytes::from(metadata.clone())),
                Bytes::from_static(b"hello"),
            )
            .await
            .unwrap();
        assert_eq!(message_id.ledger, 0);
        assert_eq!(message_id.entry, 0);

        let (consumer_id, pending) = rx.recv().await.expect("message dispatched");
        assert_eq!(consumer_id, 1);
        assert_eq!(pending.metadata, metadata);
        assert_eq!(pending.payload, b"hello".to_vec());
    }

    #[tokio::test]
    async fn test_prepare_non_persistent_publish_does_not_dispatch_until_requested() {
        let storage = create_test_storage();
        let mut topic = Topic::new("test-topic".to_string(), storage);
        topic.set_runtime_mode(TopicRuntimeMode::NonPersistent);

        let subscription = topic
            .get_or_create_subscription("sub1", SubscriptionType::Exclusive)
            .await
            .unwrap();
        let (consumer, mut rx) = create_test_consumer_with_rx(1, subscription.clone());
        consumer.add_permits(1).await;
        {
            let mut sub_guard = subscription.write().await;
            sub_guard.add_consumer(consumer).unwrap();
        }

        let publish = topic
            .prepare_non_persistent_publish(None, Bytes::from_static(b"hello"))
            .unwrap();

        assert!(rx.try_recv().is_err());

        publish.dispatch_sequential().await;

        let (consumer_id, pending) = rx.recv().await.expect("message dispatched");
        assert_eq!(consumer_id, 1);
        assert_eq!(pending.payload, b"hello".to_vec());
    }

    #[tokio::test]
    async fn test_non_persistent_shared_dispatch_round_robins_across_consumers() {
        let storage = create_test_storage();
        let mut topic = Topic::new("test-topic".to_string(), storage);
        topic.set_runtime_mode(TopicRuntimeMode::NonPersistent);

        let subscription = topic
            .get_or_create_subscription("sub1", SubscriptionType::Shared)
            .await
            .unwrap();
        let (consumer1, mut rx1) = create_test_consumer_with_rx(1, subscription.clone());
        let (consumer2, mut rx2) = create_test_consumer_with_rx(2, subscription.clone());
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

        topic
            .publish_message(None, Bytes::from_static(b"first"))
            .await
            .unwrap();
        topic
            .publish_message(None, Bytes::from_static(b"second"))
            .await
            .unwrap();
        topic.dispatch_to_subscriptions().await;

        let first = rx1.recv().await.expect("consumer1 receives a message");
        let second = rx2.recv().await.expect("consumer2 receives a message");

        assert_eq!(first.0, 1);
        assert_eq!(second.0, 2);
        assert_eq!(first.1.payload, b"first".to_vec());
        assert_eq!(second.1.payload, b"second".to_vec());
    }

    #[tokio::test]
    async fn test_non_persistent_dispatches_entries_per_subscription_in_order() {
        let storage = create_test_storage();
        let mut topic = Topic::new("test-topic".to_string(), storage);
        topic.set_runtime_mode(TopicRuntimeMode::NonPersistent);

        let subscription1 = topic
            .get_or_create_subscription("sub1", SubscriptionType::Exclusive)
            .await
            .unwrap();
        let subscription2 = topic
            .get_or_create_subscription("sub2", SubscriptionType::Exclusive)
            .await
            .unwrap();

        let (consumer1, mut rx1) = create_test_consumer_with_rx(1, subscription1.clone());
        let (consumer2, mut rx2) = create_test_consumer_with_rx(2, subscription2.clone());

        consumer1.add_permits(2).await;
        consumer2.add_permits(2).await;

        {
            let mut sub_guard = subscription1.write().await;
            sub_guard.add_consumer(consumer1).unwrap();
        }
        {
            let mut sub_guard = subscription2.write().await;
            sub_guard.add_consumer(consumer2).unwrap();
        }
        {
            let sub_guard = subscription1.read().await;
            sub_guard.consumer_flow(1, 2).await;
        }
        {
            let sub_guard = subscription2.read().await;
            sub_guard.consumer_flow(2, 2).await;
        }

        topic
            .publish_message(None, Bytes::from_static(b"first"))
            .await
            .unwrap();
        topic
            .publish_message(None, Bytes::from_static(b"second"))
            .await
            .unwrap();
        topic.dispatch_to_subscriptions().await;

        let sub1_first = rx1.recv().await.expect("sub1 gets first message");
        let sub1_second = rx1.recv().await.expect("sub1 gets second message");
        let sub2_first = rx2.recv().await.expect("sub2 gets first message");
        let sub2_second = rx2.recv().await.expect("sub2 gets second message");

        assert_eq!(sub1_first.0, 1);
        assert_eq!(sub1_second.0, 1);
        assert_eq!(sub2_first.0, 2);
        assert_eq!(sub2_second.0, 2);
        assert_eq!(sub1_first.1.payload, b"first".to_vec());
        assert_eq!(sub1_second.1.payload, b"second".to_vec());
        assert_eq!(sub2_first.1.payload, b"first".to_vec());
        assert_eq!(sub2_second.1.payload, b"second".to_vec());
    }

    #[tokio::test]
    async fn test_non_persistent_topic_immediately_drops_for_blocked_subscription() {
        let storage = create_test_storage();
        let mut topic = Topic::new("test-topic".to_string(), storage);
        topic.set_runtime_mode(TopicRuntimeMode::NonPersistent);

        let ready_subscription = topic
            .get_or_create_subscription("sub-ready", SubscriptionType::Exclusive)
            .await
            .unwrap();
        let blocked_subscription = topic
            .get_or_create_subscription("sub-blocked", SubscriptionType::Exclusive)
            .await
            .unwrap();

        let (ready_consumer, mut ready_rx) =
            create_test_consumer_with_rx(1, ready_subscription.clone());
        let (blocked_consumer, _blocked_rx) =
            create_test_consumer_with_rx(2, blocked_subscription.clone());

        ready_consumer.add_permits(1).await;

        {
            let mut sub_guard = ready_subscription.write().await;
            sub_guard.add_consumer(ready_consumer).unwrap();
        }
        {
            let mut sub_guard = blocked_subscription.write().await;
            sub_guard.add_consumer(blocked_consumer).unwrap();
        }
        {
            let sub_guard = ready_subscription.read().await;
            sub_guard.consumer_flow(1, 1).await;
        }

        topic
            .publish_message(None, Bytes::from_static(b"hello"))
            .await
            .unwrap();
        topic.dispatch_to_subscriptions().await;

        let delivered = ready_rx
            .recv()
            .await
            .expect("ready subscription gets message");
        assert_eq!(delivered.1.payload, b"hello".to_vec());

        let ready_stats = ready_subscription.read().await.get_stats().await;
        let blocked_stats = blocked_subscription.read().await.get_stats().await;
        assert_eq!(ready_stats.received_messages, 1);
        assert_eq!(ready_stats.dispatched_messages, 1);
        assert_eq!(blocked_stats.received_messages, 1);
        assert_eq!(blocked_stats.dispatched_messages, 0);
        assert_eq!(blocked_stats.dropped_messages, 1);
    }

    #[tokio::test]
    #[ignore]
    async fn perf_non_persistent_shared_topic_dispatch_1_subscription_2_consumers_10k_entries() {
        const ENTRY_COUNT: usize = 10_000;

        let storage = create_test_storage();
        let mut topic = Topic::new("test-topic".to_string(), storage);
        topic.set_runtime_mode(TopicRuntimeMode::NonPersistent);

        let subscription = topic
            .get_or_create_subscription("sub1", SubscriptionType::Shared)
            .await
            .unwrap();
        let (consumer1, mut rx1) = create_test_consumer_with_rx(1, subscription.clone());
        let (consumer2, mut rx2) = create_test_consumer_with_rx(2, subscription.clone());
        {
            let mut sub_guard = subscription.write().await;
            sub_guard.add_consumer(consumer1.clone()).unwrap();
            sub_guard.add_consumer(consumer2.clone()).unwrap();
        }

        consumer1.add_permits(ENTRY_COUNT as u32).await;
        consumer2.add_permits(ENTRY_COUNT as u32).await;
        {
            let sub_guard = subscription.read().await;
            sub_guard.consumer_flow(1, ENTRY_COUNT as u32).await;
            sub_guard.consumer_flow(2, ENTRY_COUNT as u32).await;
        }

        for entry_id in 0..ENTRY_COUNT {
            let payload = format!("shared-topic-{entry_id}");
            topic
                .publish_message(None, Bytes::from(payload))
                .await
                .unwrap();
        }

        let start = Instant::now();
        topic.dispatch_to_subscriptions().await;
        let elapsed = start.elapsed();

        println!(
            "PERF non-persistent shared topic dispatch: subscriptions=1, consumers=2, entries={ENTRY_COUNT}, elapsed_ms={}",
            elapsed.as_millis()
        );

        let mut received = 0;
        while rx1.try_recv().is_ok() {
            received += 1;
        }
        while rx2.try_recv().is_ok() {
            received += 1;
        }

        assert_eq!(received, ENTRY_COUNT);

        let stats = subscription.read().await.get_stats().await;
        assert_eq!(stats.dropped_messages, 0);
    }

    #[tokio::test]
    async fn test_non_persistent_exclusive_dispatch_requires_flow_permits() {
        let storage = create_test_storage();
        let mut topic = Topic::new("test-topic".to_string(), storage);
        topic.set_runtime_mode(TopicRuntimeMode::NonPersistent);

        let subscription = topic
            .get_or_create_subscription("sub1", SubscriptionType::Exclusive)
            .await
            .unwrap();
        let (consumer, mut rx) = create_test_consumer_with_rx(1, subscription.clone());
        {
            let mut sub_guard = subscription.write().await;
            sub_guard.add_consumer(consumer).unwrap();
        }

        topic
            .publish_message(None, Bytes::from_static(b"blocked"))
            .await
            .unwrap();
        topic.dispatch_to_subscriptions().await;
        assert!(rx.try_recv().is_err());

        let consumer = {
            let sub_guard = subscription.read().await;
            sub_guard.get_consumer(1).unwrap()
        };
        consumer.add_permits(1).await;
        {
            let sub_guard = subscription.read().await;
            sub_guard.consumer_flow(1, 1).await;
        }

        topic
            .publish_message(None, Bytes::from_static(b"allowed"))
            .await
            .unwrap();
        topic.dispatch_to_subscriptions().await;

        let dispatched = rx.recv().await.expect("message delivered after flow");
        assert_eq!(dispatched.0, 1);
        assert_eq!(dispatched.1.payload, b"allowed".to_vec());
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

        topic
            .publish_message(None, Bytes::from_static(b"first"))
            .await
            .unwrap();
        topic.dispatch_to_subscriptions().await;
        let first = rx1
            .recv()
            .await
            .expect("active failover consumer receives message");
        assert_eq!(first.0, 1);
        assert!(rx2.try_recv().is_err());

        {
            let mut sub_guard = subscription.write().await;
            assert!(sub_guard.remove_consumer(1).is_some());
        }

        consumer2.add_permits(1).await;
        {
            let sub_guard = subscription.read().await;
            sub_guard.consumer_flow(2, 1).await;
        }

        topic
            .publish_message(None, Bytes::from_static(b"second"))
            .await
            .unwrap();
        topic.dispatch_to_subscriptions().await;
        let second = rx2.recv().await.expect("standby consumer is promoted");
        assert_eq!(second.0, 2);
        assert_eq!(second.1.payload, b"second".to_vec());
    }

    #[tokio::test]
    async fn test_non_persistent_failover_uses_partition_selection_and_notifies_consumers() {
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

        let stats1 = consumer1.get_stats().await;
        let stats2 = consumer2.get_stats().await;
        assert_eq!(stats1.active_consumer_id, Some(2));
        assert!(!stats1.is_active_consumer);
        assert_eq!(stats2.active_consumer_id, Some(2));
        assert!(stats2.is_active_consumer);

        consumer2.add_permits(1).await;
        {
            let sub_guard = subscription.read().await;
            sub_guard.consumer_flow(2, 1).await;
        }

        topic
            .publish_message(None, Bytes::from_static(b"first"))
            .await
            .unwrap();
        topic.dispatch_to_subscriptions().await;
        let first = rx2
            .recv()
            .await
            .expect("partition-selected failover consumer receives");
        assert_eq!(first.0, 2);
        assert!(rx1.try_recv().is_err());

        {
            let mut sub_guard = subscription.write().await;
            assert!(sub_guard.remove_consumer(2).is_some());
        }

        let stats1 = consumer1.get_stats().await;
        assert_eq!(stats1.active_consumer_id, Some(1));
        assert!(stats1.is_active_consumer);
    }

    #[tokio::test]
    async fn test_non_persistent_drop_counts_are_exposed_in_subscription_stats() {
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

        topic
            .publish_message(None, Bytes::from_static(b"drop-me"))
            .await
            .unwrap();
        topic.dispatch_to_subscriptions().await;

        let stats = subscription.read().await.get_stats().await;
        assert_eq!(stats.dropped_messages, 1);
    }

    #[tokio::test]
    async fn test_non_persistent_last_message_id_is_unsupported() {
        let storage = create_test_storage();
        let mut topic = Topic::new("test-topic".to_string(), storage);
        topic.set_runtime_mode(TopicRuntimeMode::NonPersistent);
        topic
            .publish_message(None, Bytes::from_static(b"hello"))
            .await
            .unwrap();

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
    async fn test_persistent_topic_domain_sets_runtime_mode() {
        let storage = create_test_storage();
        let topic = Topic::new(
            "persistent://public/default/test-topic".to_string(),
            storage,
        );

        assert_eq!(topic.runtime_mode(), TopicRuntimeMode::Persistent);
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

        // Add duplicate producer_id evicts old entry (cross-connection reconnect)
        let producer1_dup = create_test_producer(1, topic_ref.clone());
        assert!(topic.add_producer(producer1_dup).is_ok());
        assert_eq!(topic.get_producer_count(), 2); // evicted old, inserted new — count unchanged

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
        let sub = topic
            .get_or_create_subscription("sub1", SubscriptionType::Shared)
            .await;
        assert!(sub.is_ok());
        assert_eq!(topic.get_subscription_count(), 1);

        // Get existing subscription
        let sub2 = topic
            .get_or_create_subscription("sub1", SubscriptionType::Shared)
            .await;
        assert!(sub2.is_ok());
        assert_eq!(topic.get_subscription_count(), 1); // Should not create duplicate

        // Create another subscription
        let sub3 = topic
            .get_or_create_subscription("sub2", SubscriptionType::Exclusive)
            .await;
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
        let sub = topic
            .get_or_create_subscription("sub1", SubscriptionType::Shared)
            .await
            .unwrap();
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

    #[tokio::test]
    async fn test_topic_publish_rate_limiter_rejects_second_message_in_window() {
        let storage = create_test_storage();
        let mut topic = Topic::new("test-topic".to_string(), storage);
        topic.set_publish_rate(TopicPublishRate {
            messages_per_sec: 1,
            bytes_per_sec: 0,
        });

        topic
            .publish_message(None, Bytes::from_static(b"first"))
            .await
            .expect("first message should pass rate limiter");

        let error = topic
            .publish_message(None, Bytes::from_static(b"second"))
            .await
            .expect_err("second message should be rejected in the same window");
        assert!(error.downcast_ref::<TopicPublishRateExceeded>().is_some());
    }
}
