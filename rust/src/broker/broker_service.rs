/*
 * Broker Service
 * Global manager for all topics in the broker
 * Inspired by Apache Pulsar's BrokerService
 */

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use crate::broker::service::topic::{Topic, TopicStats, PartitionedTopic, PartitionedTopicStats, SharedPartitionedTopic};
use crate::storage::Storage;

/// Shared Topic wrapped in Arc<RwLock>
pub type SharedTopic = Arc<RwLock<Topic>>;

/// Shared BrokerService wrapped in Arc<RwLock>
pub type SharedBrokerService = Arc<RwLock<BrokerService>>;

/// Reference to either a partitioned or non-partitioned topic
#[derive(Debug, Clone)]
pub enum TopicRef {
    /// A non-partitioned topic
    NonPartitioned(SharedTopic),
    /// A partitioned topic (contains all partitions)
    Partitioned(SharedPartitionedTopic),
    /// A specific partition of a partitioned topic
    Partition(SharedTopic),
}

/// BrokerService manages all topics in the broker
/// It provides a global registry for topic instances
/// Similar to Apache Pulsar's org.apache.pulsar.broker.service.BrokerService
#[derive(Debug)]
pub struct BrokerService {
    /// All non-partitioned topics indexed by topic name
    topics: HashMap<String, SharedTopic>,
    /// All partitioned topics indexed by topic name
    partitioned_topics: HashMap<String, SharedPartitionedTopic>,
    /// Partition metadata (topic_name -> partition_count)
    partition_metadata: HashMap<String, usize>,
    /// Storage backend
    storage: Arc<Mutex<Storage>>,
    /// Default number of partitions for topics (0 = non-partitioned)
    default_partitions: usize,
}

impl BrokerService {
    /// Create a new BrokerService with default settings (non-partitioned topics)
    pub fn new(storage: Arc<Mutex<Storage>>) -> Self {
        Self::with_config(storage, 0)
    }

    /// Create a new BrokerService with custom default partition count
    pub fn with_config(storage: Arc<Mutex<Storage>>, default_partitions: usize) -> Self {
        log::info!("Creating BrokerService (default_partitions={})", default_partitions);
        Self {
            topics: HashMap::new(),
            partitioned_topics: HashMap::new(),
            partition_metadata: HashMap::new(),
            storage,
            default_partitions,
        }
    }

    /// Get the default number of partitions
    pub fn get_default_partitions(&self) -> usize {
        self.default_partitions
    }

    /// Set the default number of partitions
    pub fn set_default_partitions(&mut self, partitions: usize) {
        self.default_partitions = partitions;
        log::info!("Set default partitions to {}", partitions);
    }

    /// Restore persisted partition metadata at broker startup.
    pub fn restore_partition_metadata(&mut self, partition_metadata: HashMap<String, usize>) {
        self.partition_metadata.extend(partition_metadata);
        log::info!(
            "Restored {} partitioned topic metadata entries",
            self.partition_metadata.len()
        );
    }

    // ==================== Non-Partitioned Topic Management ====================

    /// Get or create a non-partitioned topic by name
    ///
    /// This will create the topic if it doesn't exist
    pub async fn get_or_create_topic(&mut self, topic_name: &str) -> SharedTopic {
        if !self.topics.contains_key(topic_name) {
            log::info!("Creating new topic: {}", topic_name);
            {
                let mut guard = self.storage.lock().await;
                if let Err(error) = guard.ensure_topic_metadata(topic_name, false, 0) {
                    log::warn!(
                        "Skipping metadata persistence for topic '{}': {}",
                        topic_name, error
                    );
                }
            }
            let topic = Topic::new(topic_name.to_string(), self.storage.clone());
            self.topics.insert(topic_name.to_string(), Arc::new(RwLock::new(topic)));
        }

        self.topics.get(topic_name).unwrap().clone()
    }

    /// Get a non-partitioned topic by name (returns None if doesn't exist)
    pub fn get_topic(&self, topic_name: &str) -> Option<SharedTopic> {
        self.topics.get(topic_name).cloned()
    }

    /// Remove a non-partitioned topic by name
    pub fn remove_topic(&mut self, topic_name: &str) -> Option<SharedTopic> {
        let topic = self.topics.remove(topic_name);
        if topic.is_some() {
            log::info!("Removed topic: {}", topic_name);
        }
        topic
    }

    /// Check if a non-partitioned topic exists
    pub fn has_topic(&self, topic_name: &str) -> bool {
        self.topics.contains_key(topic_name)
    }

    /// Get all non-partitioned topics
    pub fn get_all_topics(&self) -> &HashMap<String, SharedTopic> {
        &self.topics
    }

    /// Get non-partitioned topic count
    pub fn get_topic_count(&self) -> usize {
        self.topics.len()
    }

    // ==================== Partitioned Topic Management ====================

    /// Get or create a partitioned topic with the specified number of partitions
    ///
    /// This will create the partitioned topic if it doesn't exist
    pub async fn get_or_create_partitioned_topic(
        &mut self,
        topic_name: &str,
        partition_count: usize,
    ) -> SharedPartitionedTopic {
        if !self.partitioned_topics.contains_key(topic_name) {
            log::info!(
                "Creating new partitioned topic: {} with {} partitions",
                topic_name, partition_count
            );
            {
                let mut guard = self.storage.lock().await;
                if let Err(error) = guard.ensure_topic_metadata(topic_name, true, partition_count) {
                    log::warn!(
                        "Skipping metadata persistence for partitioned topic '{}': {}",
                        topic_name, error
                    );
                }
            }
            let partitioned_topic = PartitionedTopic::new(
                topic_name.to_string(),
                partition_count,
                self.storage.clone(),
            );
            self.partitioned_topics.insert(topic_name.to_string(), Arc::new(RwLock::new(partitioned_topic)));
            self.partition_metadata.insert(topic_name.to_string(), partition_count);
        }

        self.partitioned_topics.get(topic_name).unwrap().clone()
    }

    /// Get a partitioned topic by name (returns None if doesn't exist)
    pub fn get_partitioned_topic(&self, topic_name: &str) -> Option<SharedPartitionedTopic> {
        self.partitioned_topics.get(topic_name).cloned()
    }

    /// Remove a partitioned topic by name
    pub fn remove_partitioned_topic(&mut self, topic_name: &str) -> Option<SharedPartitionedTopic> {
        let topic = self.partitioned_topics.remove(topic_name);
        self.partition_metadata.remove(topic_name);
        if topic.is_some() {
            log::info!("Removed partitioned topic: {}", topic_name);
        }
        topic
    }

    /// Check if a partitioned topic exists
    pub fn has_partitioned_topic(&self, topic_name: &str) -> bool {
        self.partitioned_topics.contains_key(topic_name)
    }

    /// Get partition count for a partitioned topic
    pub fn get_partition_count(&self, topic_name: &str) -> Option<usize> {
        self.partition_metadata.get(topic_name).copied()
    }

    /// Get all partitioned topics
    pub fn get_all_partitioned_topics(&self) -> &HashMap<String, SharedPartitionedTopic> {
        &self.partitioned_topics
    }

    /// Get partitioned topic count
    pub fn get_partitioned_topic_count(&self) -> usize {
        self.partitioned_topics.len()
    }

    // ==================== Topic Type Detection ====================

    /// Check if a topic should be partitioned based on metadata or default config
    ///
    /// Priority:
    /// 1. Explicit partition metadata exists -> partitioned
    /// 2. Already exists as partitioned topic -> partitioned
    /// 3. Default partitions > 0 -> partitioned
    /// 4. Otherwise -> non-partitioned
    pub fn should_be_partitioned(&self, topic_name: &str) -> bool {
        // Check if it's already registered as partitioned
        if self.partition_metadata.contains_key(topic_name) {
            return true;
        }

        // Check if it already exists as a partitioned topic
        if self.partitioned_topics.contains_key(topic_name) {
            return true;
        }

        // Check default config
        self.default_partitions > 0
    }

    /// Get or create a topic automatically (partitioned or non-partitioned based on config)
    ///
    /// This method automatically determines whether to create a partitioned or non-partitioned topic:
    /// - If partition metadata exists -> partitioned
    /// - If default_partitions > 0 -> partitioned
    /// - Otherwise -> non-partitioned
    pub async fn get_or_create_topic_auto(&mut self, topic_name: &str) -> TopicRef {
        // If it's a partition name, get the specific partition
        if self.is_partition_name(topic_name) {
            if let Some(base_name) = self.get_base_topic_name(topic_name) {
                if let Some(partition_idx) = self.get_partition_index(topic_name) {
                    // Ensure the partitioned topic exists
                    let partition_count = self.partition_metadata.get(&base_name).copied()
                        .unwrap_or(self.default_partitions);

                    if partition_count > 0 {
                        let partitioned_topic = self.get_or_create_partitioned_topic(&base_name, partition_count).await;
                        let guard = partitioned_topic.read().await;
                        if let Some(partition) = guard.get_partition(partition_idx) {
                            return TopicRef::Partition(partition);
                        }
                    }
                }
            }
        }

        // Determine if it should be partitioned
        if self.should_be_partitioned(topic_name) {
            let partition_count = self.partition_metadata.get(topic_name).copied()
                .unwrap_or(self.default_partitions);

            let partitioned_topic = self.get_or_create_partitioned_topic(topic_name, partition_count).await;
            TopicRef::Partitioned(partitioned_topic)
        } else {
            let topic = self.get_or_create_topic(topic_name).await;
            TopicRef::NonPartitioned(topic)
        }
    }

    /// Get the base topic name from a partition name
    /// E.g., "my-topic-partition-0" -> "my-topic"
    pub fn get_base_topic_name(&self, partition_name: &str) -> Option<String> {
        let (base_name, partition_index) = partition_name.rsplit_once("-partition-")?;
        partition_index.parse::<usize>().ok()?;
        Some(base_name.to_string())
    }

    /// Get partition index from partition name
    /// E.g., "my-topic-partition-0" -> 0
    pub fn get_partition_index(&self, partition_name: &str) -> Option<usize> {
        let (_, partition_index) = partition_name.rsplit_once("-partition-")?;
        partition_index.parse::<usize>().ok()
    }

    /// Check if a topic name is a partition name (e.g., "topic-partition-0")
    pub fn is_partition_name(&self, topic_name: &str) -> bool {
        self.get_partition_index(topic_name).is_some()
    }

    // ==================== Statistics ====================

    /// Get statistics for all topics (both partitioned and non-partitioned)
    pub async fn get_all_stats(&self) -> Vec<TopicStats> {
        let mut stats = Vec::new();

        // Non-partitioned topics
        for topic in self.topics.values() {
            let topic_guard = topic.read().await;
            stats.push(topic_guard.get_stats().await);
        }

        stats
    }

    /// Get statistics for all partitioned topics
    pub async fn get_all_partitioned_stats(&self) -> Vec<PartitionedTopicStats> {
        let mut stats = Vec::new();

        for partitioned_topic in self.partitioned_topics.values() {
            let guard = partitioned_topic.read().await;
            stats.push(guard.get_stats().await);
        }

        stats
    }

    // ==================== Cleanup ====================

    /// Clean up idle topics (no producers or consumers)
    ///
    /// Returns the number of topics removed
    pub async fn cleanup_idle_topics(&mut self) -> usize {
        let mut to_remove = Vec::new();

        // Find idle non-partitioned topics
        for (topic_name, topic) in &self.topics {
            let topic_guard = topic.read().await;
            if topic_guard.is_idle().await {
                to_remove.push(topic_name.clone());
            }
        }

        // Remove idle non-partitioned topics
        let removed_count = to_remove.len();
        for topic_name in to_remove {
            self.topics.remove(&topic_name);
            log::info!("Cleaned up idle topic: {}", topic_name);
        }

        removed_count
    }

    /// Clean up idle partitioned topics (no producers or consumers)
    ///
    /// Returns the number of partitioned topics removed
    pub async fn cleanup_idle_partitioned_topics(&mut self) -> usize {
        let mut to_remove = Vec::new();

        // Find idle partitioned topics
        for (topic_name, partitioned_topic) in &self.partitioned_topics {
            let guard = partitioned_topic.read().await;
            if guard.is_idle().await {
                to_remove.push(topic_name.clone());
            }
        }

        // Remove idle partitioned topics
        let removed_count = to_remove.len();
        for topic_name in to_remove {
            self.partitioned_topics.remove(&topic_name);
            self.partition_metadata.remove(&topic_name);
            log::info!("Cleaned up idle partitioned topic: {}", topic_name);
        }

        removed_count
    }

    /// Clean up all idle topics (both partitioned and non-partitioned)
    ///
    /// Returns the total number of topics removed
    pub async fn cleanup_all_idle_topics(&mut self) -> usize {
        let non_partitioned = self.cleanup_idle_topics().await;
        let partitioned = self.cleanup_idle_partitioned_topics().await;
        non_partitioned + partitioned
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::service::{Consumer, Producer};
    use crate::broker::service::topic::SubscriptionType;
    use std::path::Path;
    use tokio::sync::mpsc;

    fn create_test_storage() -> Arc<Mutex<Storage>> {
        Arc::new(Mutex::new(Storage::new(Path::new("/tmp/test-topic-manager")).unwrap()))
    }

    fn create_test_producer(id: u64, topic_ref: SharedTopic) -> Arc<Producer> {
        Arc::new(Producer::new(
            id,
            format!("producer-{}", id),
            topic_ref,
            format!("conn-{}", id),
        ))
    }

    #[tokio::test]
    async fn test_topic_manager_creation() {
        let storage = create_test_storage();
        let manager = BrokerService::new(storage);

        assert_eq!(manager.get_topic_count(), 0);
    }

    #[tokio::test]
    async fn test_get_or_create_topic() {
        let storage = create_test_storage();
        let mut manager = BrokerService::new(storage);

        // Create first topic
        let topic1 = manager.get_or_create_topic("topic1").await;
        assert_eq!(manager.get_topic_count(), 1);

        // Get existing topic
        let topic1_again = manager.get_or_create_topic("topic1").await;
        assert_eq!(manager.get_topic_count(), 1); // Should not create duplicate

        // Create second topic
        let topic2 = manager.get_or_create_topic("topic2").await;
        assert_eq!(manager.get_topic_count(), 2);

        // Verify they are different topics
        assert!(!Arc::ptr_eq(&topic1, &topic2));
        assert!(Arc::ptr_eq(&topic1, &topic1_again));
    }

    #[tokio::test]
    async fn test_get_topic() {
        let storage = create_test_storage();
        let mut manager = BrokerService::new(storage);

        // Topic doesn't exist
        assert!(manager.get_topic("nonexistent").is_none());

        // Create topic
        manager.get_or_create_topic("test-topic").await;

        // Topic exists
        assert!(manager.get_topic("test-topic").is_some());
    }

    #[tokio::test]
    async fn test_remove_topic() {
        let storage = create_test_storage();
        let mut manager = BrokerService::new(storage);

        // Create topic
        manager.get_or_create_topic("test-topic").await;
        assert_eq!(manager.get_topic_count(), 1);

        // Remove topic
        let removed = manager.remove_topic("test-topic");
        assert!(removed.is_some());
        assert_eq!(manager.get_topic_count(), 0);

        // Remove non-existent topic
        let removed_again = manager.remove_topic("test-topic");
        assert!(removed_again.is_none());
    }

    #[tokio::test]
    async fn test_has_topic() {
        let storage = create_test_storage();
        let mut manager = BrokerService::new(storage);

        assert!(!manager.has_topic("test-topic"));

        manager.get_or_create_topic("test-topic").await;
        assert!(manager.has_topic("test-topic"));
    }

    #[tokio::test]
    async fn test_get_all_stats() {
        let storage = create_test_storage();
        let mut manager = BrokerService::new(storage);

        // Create topics
        let topic1 = manager.get_or_create_topic("topic1").await;
        let _topic2 = manager.get_or_create_topic("topic2").await;

        // Add producer to topic1
        {
            let mut topic1_guard = topic1.write().await;
            let producer = create_test_producer(1, topic1.clone());
            topic1_guard.add_producer(producer).unwrap();
        }

        // Get stats
        let stats = manager.get_all_stats().await;
        assert_eq!(stats.len(), 2);

        // Verify stats
        let topic1_stats = stats.iter().find(|s| s.topic_name == "topic1").unwrap();
        assert_eq!(topic1_stats.producer_count, 1);

        let topic2_stats = stats.iter().find(|s| s.topic_name == "topic2").unwrap();
        assert_eq!(topic2_stats.producer_count, 0);
    }

    #[tokio::test]
    async fn test_cleanup_idle_topics() {
        let storage = create_test_storage();
        let mut manager = BrokerService::new(storage);

        // Create two topics
        let topic1 = manager.get_or_create_topic("topic1").await;
        let topic2 = manager.get_or_create_topic("topic2").await;

        // Verify both topics exist before cleanup
        assert_eq!(manager.get_topic_count(), 2);
        assert!(manager.has_topic("topic1"));
        assert!(manager.has_topic("topic2"));

        // Add producer to topic1 to make it non-idle
        {
            let mut topic1_guard = topic1.write().await;
            let producer = create_test_producer(1, topic1.clone());
            topic1_guard.add_producer(producer).unwrap();
        }

        // topic2 is idle (no producers or consumers)
        // Verify topic2 is idle
        {
            let topic2_guard = topic2.read().await;
            assert!(topic2_guard.is_idle().await);
        }

        // Cleanup idle topics
        let removed_count = manager.cleanup_idle_topics().await;
        assert_eq!(removed_count, 1);
        assert_eq!(manager.get_topic_count(), 1);

        // Verify topic1 still exists
        assert!(manager.has_topic("topic1"));
        assert!(!manager.has_topic("topic2"));
    }

    #[tokio::test]
    async fn test_topic_with_subscription_and_consumer() {
        let storage = create_test_storage();
        let mut manager = BrokerService::new(storage);

        // Create topic
        let topic = manager.get_or_create_topic("test-topic").await;

        // Add subscription and consumer
        {
            let mut topic_guard = topic.write().await;
            let subscription = topic_guard
                .get_or_create_subscription("sub1", SubscriptionType::Shared)
                .await
                .unwrap();

            // Create Consumer with Arc<Subscription> reference
            let (tx, _rx) = mpsc::unbounded_channel();
            let consumer = Arc::new(Consumer::new(
                1,
                "consumer-1".to_string(),
                subscription.clone(),
                "conn-1".to_string(),
                tx,
                0,
            ));

            // Add consumer to subscription
            {
                let mut sub_guard = subscription.write().await;
                sub_guard.add_consumer(consumer).unwrap();
            }
        }

        // Verify stats
        let stats = manager.get_all_stats().await;
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].consumer_count, 1);
        assert_eq!(stats[0].subscription_count, 1);
    }

    // ==================== Partitioned Topic Tests ====================

    #[tokio::test]
    async fn test_partitioned_topic_creation() {
        let storage = create_test_storage();
        let mut manager = BrokerService::new(storage);

        // Create partitioned topic
        let partitioned_topic = manager.get_or_create_partitioned_topic("my-topic", 3).await;

        // Verify creation
        assert_eq!(manager.get_partitioned_topic_count(), 1);
        assert!(manager.has_partitioned_topic("my-topic"));
        assert_eq!(manager.get_partition_count("my-topic"), Some(3));

        // Get existing partitioned topic
        let same_topic = manager.get_or_create_partitioned_topic("my-topic", 3).await;
        assert!(Arc::ptr_eq(&partitioned_topic, &same_topic));
    }

    #[tokio::test]
    async fn test_get_partitioned_topic() {
        let storage = create_test_storage();
        let mut manager = BrokerService::new(storage);

        // Topic doesn't exist
        assert!(manager.get_partitioned_topic("nonexistent").is_none());

        // Create partitioned topic
        manager.get_or_create_partitioned_topic("test-topic", 5).await;

        // Topic exists
        assert!(manager.get_partitioned_topic("test-topic").is_some());
    }

    #[tokio::test]
    async fn test_remove_partitioned_topic() {
        let storage = create_test_storage();
        let mut manager = BrokerService::new(storage);

        // Create partitioned topic
        manager.get_or_create_partitioned_topic("test-topic", 3).await;
        assert_eq!(manager.get_partitioned_topic_count(), 1);

        // Remove partitioned topic
        let removed = manager.remove_partitioned_topic("test-topic");
        assert!(removed.is_some());
        assert_eq!(manager.get_partitioned_topic_count(), 0);
        assert_eq!(manager.get_partition_count("test-topic"), None);

        // Remove non-existent topic
        let removed_again = manager.remove_partitioned_topic("test-topic");
        assert!(removed_again.is_none());
    }

    #[tokio::test]
    async fn test_partition_name_utilities() {
        let storage = create_test_storage();
        let manager = BrokerService::new(storage);

        // Test partition name detection
        assert!(manager.is_partition_name("my-topic-partition-0"));
        assert!(manager.is_partition_name("persistent://public/default/topic-partition-5"));
        assert!(manager.is_partition_name(
            "persistent://public/default/metadata-partition-sub-partition-2"
        ));
        assert!(!manager.is_partition_name("my-topic"));

        // Test base topic name extraction
        assert_eq!(manager.get_base_topic_name("my-topic-partition-0"), Some("my-topic".to_string()));
        assert_eq!(
            manager.get_base_topic_name("persistent://public/default/topic-partition-5"),
            Some("persistent://public/default/topic".to_string())
        );
        assert_eq!(
            manager.get_base_topic_name(
                "persistent://public/default/metadata-partition-sub-partition-2"
            ),
            Some("persistent://public/default/metadata-partition-sub".to_string())
        );
        assert_eq!(manager.get_base_topic_name("my-topic"), None);

        // Test partition index extraction
        assert_eq!(manager.get_partition_index("my-topic-partition-0"), Some(0));
        assert_eq!(manager.get_partition_index("my-topic-partition-10"), Some(10));
        assert_eq!(
            manager.get_partition_index(
                "persistent://public/default/metadata-partition-sub-partition-2"
            ),
            Some(2)
        );
        assert_eq!(manager.get_partition_index("my-topic"), None);
    }

    #[tokio::test]
    async fn test_partitioned_topic_stats() {
        let storage = create_test_storage();
        let mut manager = BrokerService::new(storage);

        // Create partitioned topic
        let partitioned_topic = manager.get_or_create_partitioned_topic("my-topic", 3).await;

        // Add producer to partition 0
        {
            let guard = partitioned_topic.write().await;
            let partition = guard.get_partition(0).unwrap();
            let mut partition_guard = partition.write().await;
            let producer = create_test_producer(1, partition.clone());
            partition_guard.add_producer(producer).unwrap();
        }

        // Get stats
        let stats = manager.get_all_partitioned_stats().await;
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].topic_name, "my-topic");
        assert_eq!(stats[0].partition_count, 3);
        assert_eq!(stats[0].total_producers, 1);
    }

    #[tokio::test]
    async fn test_cleanup_idle_partitioned_topics() {
        let storage = create_test_storage();
        let mut manager = BrokerService::new(storage);

        // Create two partitioned topics
        let topic1 = manager.get_or_create_partitioned_topic("topic1", 2).await;
        let _topic2 = manager.get_or_create_partitioned_topic("topic2", 3).await;

        // Verify both exist
        assert_eq!(manager.get_partitioned_topic_count(), 2);

        // Add producer to topic1
        {
            let guard = topic1.write().await;
            let partition = guard.get_partition(0).unwrap();
            let mut partition_guard = partition.write().await;
            let producer = create_test_producer(1, partition.clone());
            partition_guard.add_producer(producer).unwrap();
        }

        // Cleanup idle topics
        let removed = manager.cleanup_idle_partitioned_topics().await;
        assert_eq!(removed, 1);
        assert_eq!(manager.get_partitioned_topic_count(), 1);
        assert!(manager.has_partitioned_topic("topic1"));
        assert!(!manager.has_partitioned_topic("topic2"));
    }

    #[tokio::test]
    async fn persisted_partition_metadata_restores_partitioned_topic_shape() {
        let storage = create_test_storage();
        {
            let mut guard = storage.lock().await;
            guard
                .ensure_topic_metadata("persistent://public/default/restored-topic", true, 3)
                .unwrap();
        }

        let partition_metadata = {
            let guard = storage.lock().await;
            guard.get_partitioned_topic_metadata()
        };

        let mut manager = BrokerService::with_config(storage.clone(), 0);
        manager.restore_partition_metadata(partition_metadata);

        assert_eq!(
            manager.get_partition_count("persistent://public/default/restored-topic"),
            Some(3)
        );
        assert!(manager.should_be_partitioned("persistent://public/default/restored-topic"));

        let topic = manager
            .get_or_create_topic_auto("persistent://public/default/restored-topic")
            .await;
        assert!(matches!(topic, TopicRef::Partitioned(_)));
    }
}
