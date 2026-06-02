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
