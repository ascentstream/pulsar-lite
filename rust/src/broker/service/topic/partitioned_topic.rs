/*
 * Partitioned Topic
 * A topic that contains multiple partitions for parallel processing
 */

use std::sync::Arc;
use tokio::sync::RwLock;
use crate::broker::service::{Consumer, SharedStorage, Producer};
use super::{Topic, SubscriptionType, SharedSubscription};

/// Shared PartitionedTopic wrapped in Arc<RwLock>
pub type SharedPartitionedTopic = Arc<RwLock<PartitionedTopic>>;

/// PartitionedTopic represents a topic with multiple partitions
///
/// In Pulsar's architecture:
/// - The CLIENT is responsible for choosing which partition to send to
/// - The BROKER just receives messages for a specific partition topic
/// - Each partition is an independent Topic with its own producers/subscriptions/consumers
///
/// This means the broker doesn't need routing logic - it just needs to:
/// 1. Track the partition count (for PartitionMetadata responses)
/// 2. Provide access to individual partition topics
#[derive(Debug)]
pub struct PartitionedTopic {
    /// Base topic name (e.g., "persistent://public/default/my-topic")
    topic_name: String,
    /// Number of partitions
    partition_count: usize,
    /// Partition topics (each partition is a regular Topic)
    partitions: Vec<Arc<RwLock<Topic>>>,
    /// Storage backend (kept for potential future use)
    #[allow(dead_code)]
    storage: SharedStorage,
}


impl PartitionedTopic {
    /// Create a new partitioned topic with the specified number of partitions
    pub fn new(
        topic_name: String,
        partition_count: usize,
        storage: SharedStorage,
    ) -> Self {
        log::info!(
            "Creating partitioned topic '{}' with {} partitions",
            topic_name, partition_count
        );

        // Create partition topics
        let partitions: Vec<Arc<RwLock<Topic>>> = (0..partition_count)
            .map(|i| {
                let partition_name = format!("{}-partition-{}", topic_name, i);
                Arc::new(RwLock::new(Topic::new(partition_name, storage.clone())))
            })
            .collect();

        Self {
            topic_name,
            partition_count,
            partitions,
            storage,
        }
    }

    /// Get the topic name
    pub fn get_topic_name(&self) -> &str {
        &self.topic_name
    }

    /// Get the number of partitions
    pub fn get_partition_count(&self) -> usize {
        self.partition_count
    }

    /// Get a specific partition by index
    pub fn get_partition(&self, index: usize) -> Option<Arc<RwLock<Topic>>> {
        if index < self.partition_count {
            Some(self.partitions[index].clone())
        } else {
            None
        }
    }

    /// Get all partitions
    pub fn get_all_partitions(&self) -> &[Arc<RwLock<Topic>>] {
        &self.partitions
    }

    // ==================== Producer Management ====================

    /// Add a producer to a specific partition
    pub async fn add_producer_to_partition(
        &mut self,
        partition_index: usize,
        producer: Arc<Producer>,
    ) -> Result<(), String> {
        if partition_index >= self.partition_count {
            return Err(format!("Partition index {} out of bounds (max: {})", partition_index, self.partition_count - 1));
        }

        let mut partition = self.partitions[partition_index].write().await;
        partition.add_producer(producer)
    }

    /// Remove a producer from a specific partition
    pub async fn remove_producer_from_partition(
        &mut self,
        partition_index: usize,
        producer_id: u64,
    ) -> Option<Arc<Producer>> {
        if partition_index >= self.partition_count {
            return None;
        }

        let mut partition = self.partitions[partition_index].write().await;
        partition.remove_producer(producer_id)
    }

    /// Find which partition contains a producer
    pub async fn find_producer_partition(&self, producer_id: u64) -> Option<usize> {
        for (i, partition) in self.partitions.iter().enumerate() {
            let guard = partition.read().await;
            if guard.get_producer(producer_id).is_some() {
                return Some(i);
            }
        }
        None
    }

    /// Get total producer count across all partitions
    pub async fn get_total_producer_count(&self) -> usize {
        let mut total = 0;
        for partition in &self.partitions {
            let guard = partition.read().await;
            total += guard.get_producer_count();
        }
        total
    }

    // ==================== Consumer Management ====================

    /// Add a consumer to a subscription on a specific partition
    pub async fn add_consumer_to_partition(
        &mut self,
        partition_index: usize,
        subscription_name: &str,
        consumer: Arc<Consumer>,
    ) -> Result<(), String> {
        if partition_index >= self.partition_count {
            return Err(format!("Partition index {} out of bounds", partition_index));
        }

        let mut partition = self.partitions[partition_index].write().await;

        // Get or create subscription
        let subscription = {
            let sub_type = SubscriptionType::Shared; // Default to Shared
            partition.get_or_create_subscription(subscription_name, sub_type).await?
        };

        // Add consumer to subscription
        let mut sub_guard = subscription.write().await;
        sub_guard.add_consumer(consumer)
    }

    /// Find which partition contains a consumer
    pub async fn find_consumer_partition(&self, consumer_id: u64) -> Option<(usize, String)> {
        for (i, partition) in self.partitions.iter().enumerate() {
            let guard = partition.read().await;
            if let Some((sub_name, _)) = guard.find_consumer(consumer_id).await {
                return Some((i, sub_name));
            }
        }
        None
    }

    /// Get total consumer count across all partitions
    pub async fn get_total_consumer_count(&self) -> usize {
        let mut total = 0;
        for partition in &self.partitions {
            let guard = partition.read().await;
            total += guard.get_total_consumer_count().await;
        }
        total
    }

    // ==================== Subscription Management ====================

    /// Get or create a subscription on a specific partition
    pub async fn get_or_create_subscription_on_partition(
        &mut self,
        partition_index: usize,
        subscription_name: &str,
        sub_type: SubscriptionType,
    ) -> Result<SharedSubscription, String> {
        if partition_index >= self.partition_count {
            return Err(format!("Partition index {} out of bounds", partition_index));
        }

        let mut partition = self.partitions[partition_index].write().await;
        partition.get_or_create_subscription(subscription_name, sub_type).await
    }

    /// Get total subscription count across all partitions
    pub async fn get_total_subscription_count(&self) -> usize {
        let mut total = 0;
        for partition in &self.partitions {
            let guard = partition.read().await;
            total += guard.get_subscription_count();
        }
        total
    }

    // ==================== Message Publishing ====================

    /// Publish a message to a specific partition
    ///
    /// Note: In Pulsar, the client is responsible for choosing the partition,
    /// and the broker simply receives messages for a specific partition topic.
    /// This method is kept for internal/testing purposes.
    pub async fn publish_message_to_partition(
        &mut self,
        partition_index: usize,
        metadata: Option<&[u8]>,
        payload: &[u8],
    ) -> Result<crate::storage::MessageId, Box<dyn std::error::Error>> {
        if partition_index >= self.partition_count {
            return Err(format!("Partition index {} out of bounds", partition_index).into());
        }

        log::debug!(
            "Publishing message to partitioned topic '{}' partition {} ({} bytes)",
            self.topic_name, partition_index, payload.len()
        );

        let partition = self.partitions[partition_index].clone();
        let mut guard = partition.write().await;
        guard.publish_message_with_metadata(metadata, payload).await
    }

    // ==================== Statistics ====================

    /// Get statistics for the partitioned topic
    pub async fn get_stats(&self) -> PartitionedTopicStats {
        let mut partition_stats = Vec::new();
        let mut total_producers = 0;
        let mut total_consumers = 0;
        let mut total_subscriptions = 0;

        for (i, partition) in self.partitions.iter().enumerate() {
            let guard = partition.read().await;
            let stats = guard.get_stats().await;
            total_producers += stats.producer_count;
            total_consumers += stats.consumer_count;
            total_subscriptions += stats.subscription_count;
            partition_stats.push(PartitionStats {
                partition_index: i,
                producer_count: stats.producer_count,
                consumer_count: stats.consumer_count,
                subscription_count: stats.subscription_count,
            });
        }

        PartitionedTopicStats {
            topic_name: self.topic_name.clone(),
            partition_count: self.partition_count,
            total_producers: total_producers,
            total_consumers: total_consumers,
            total_subscriptions: total_subscriptions,
            partitions: partition_stats,
        }
    }

    /// Check if partitioned topic is idle (no producers or consumers)
    pub async fn is_idle(&self) -> bool {
        self.get_total_producer_count().await == 00 && self.get_total_consumer_count().await == 0
    }
}

/// Statistics for a partition
#[derive(Debug, Clone)]
pub struct PartitionStats {
    pub partition_index: usize,
    pub producer_count: usize,
    pub consumer_count: usize,
    pub subscription_count: usize,
}

/// Statistics for a partitioned topic
#[derive(Debug, Clone)]
pub struct PartitionedTopicStats {
    pub topic_name: String,
    pub partition_count: usize,
    pub total_producers: usize,
    pub total_consumers: usize,
    pub total_subscriptions: usize,
    pub partitions: Vec<PartitionStats>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;
    use std::path::Path;
    use tokio::sync::Mutex;

    fn create_test_storage() -> SharedStorage {
        Arc::new(Mutex::new(Storage::new(Path::new("/tmp/test-partitioned-topic")).unwrap()))
    }

    fn create_test_producer(id: u64, topic_ref: Arc<RwLock<Topic>>) -> Arc<Producer> {
        Arc::new(Producer::new(
            id,
            format!("producer-{}", id),
            topic_ref,
            format!("conn-{}", id),
        ))
    }

    #[tokio::test]
    async fn test_partitioned_topic_creation() {
        let storage = create_test_storage();
        let topic = PartitionedTopic::new("test-topic".to_string(), 3, storage);

        assert_eq!(topic.get_topic_name(), "test-topic");
        assert_eq!(topic.get_partition_count(), 3);
        assert_eq!(topic.get_all_partitions().len(), 3);
    }

    #[tokio::test]
    async fn test_get_partition() {
        let storage = create_test_storage();
        let topic = PartitionedTopic::new("test-topic".to_string(), 3, storage);

        // Get valid partitions
        assert!(topic.get_partition(0).is_some());
        assert!(topic.get_partition(1).is_some());
        assert!(topic.get_partition(2).is_some());

        // Get invalid partition
        assert!(topic.get_partition(3).is_none());
        assert!(topic.get_partition(10).is_none());
    }

    #[tokio::test]
    async fn test_partition_names() {
        let storage = create_test_storage();
        let topic = PartitionedTopic::new("persistent://public/default/my-topic".to_string(), 3, storage);

        // Check partition names
        let p0 = topic.get_partition(0).unwrap();
        let p0_guard = p0.read().await;
        assert_eq!(p0_guard.name, "persistent://public/default/my-topic-partition-0");

        let p1 = topic.get_partition(1).unwrap();
        let p1_guard = p1.read().await;
        assert_eq!(p1_guard.name, "persistent://public/default/my-topic-partition-1");
    }

    #[tokio::test]
    async fn test_add_producer_to_partition() {
        let storage = create_test_storage();
        let mut topic = PartitionedTopic::new("test-topic".to_string(), 3, storage);

        // Get partition topic reference
        let partition_topic = topic.get_partition(0).unwrap();
        let producer = create_test_producer(1, partition_topic);

        // Add producer to partition 0
        let result = topic.add_producer_to_partition(0, producer).await;
        assert!(result.is_ok());

        // Verify producer is in partition 0
        let partition = topic.get_partition(0).unwrap();
        let guard = partition.read().await;
        assert_eq!(guard.get_producer_count(), 1);

        // Verify total count
        assert_eq!(topic.get_total_producer_count().await, 1);
    }

    #[tokio::test]
    async fn test_add_producer_to_invalid_partition() {
        let storage = create_test_storage();
        let mut topic = PartitionedTopic::new("test-topic".to_string(), 3, storage);

        let partition_topic = topic.get_partition(0).unwrap();
        let producer = create_test_producer(1, partition_topic);

        // Try to add to invalid partition
        let result = topic.add_producer_to_partition(10, producer).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_find_producer_partition() {
        let storage = create_test_storage();
        let mut topic = PartitionedTopic::new("test-topic".to_string(), 3, storage);

        // Add producers to different partitions
        for i in 0..3 {
            let partition_topic = topic.get_partition(i).unwrap();
            let producer = create_test_producer(i as u64 + 1, partition_topic);
            topic.add_producer_to_partition(i, producer).await.unwrap();
        }

        // Find producers
        assert_eq!(topic.find_producer_partition(1).await, Some(0));
        assert_eq!(topic.find_producer_partition(2).await, Some(1));
        assert_eq!(topic.find_producer_partition(3).await, Some(2));
        assert_eq!(topic.find_producer_partition(999).await, None);
    }

    #[tokio::test]
    async fn test_remove_producer() {
        let storage = create_test_storage();
        let mut topic = PartitionedTopic::new("test-topic".to_string(), 3, storage);

        // Add producer
        let partition_topic = topic.get_partition(0).unwrap();
        let producer = create_test_producer(1, partition_topic);
        topic.add_producer_to_partition(0, producer).await.unwrap();

        // Remove producer
        let removed = topic.remove_producer_from_partition(0, 1).await;
        assert!(removed.is_some());

        // Verify removal
        assert_eq!(topic.get_total_producer_count().await, 0);
    }

    #[tokio::test]
    async fn test_get_stats() {
        let storage = create_test_storage();
        let mut topic = PartitionedTopic::new("test-topic".to_string(), 3, storage);

        // Add producers to different partitions
        for i in 0..3 {
            let partition_topic = topic.get_partition(i).unwrap();
            let producer = create_test_producer(i as u64 + 1, partition_topic);
            topic.add_producer_to_partition(i, producer).await.unwrap();
        }

        // Get stats
        let stats = topic.get_stats().await;
        assert_eq!(stats.topic_name, "test-topic");
        assert_eq!(stats.partition_count, 3);
        assert_eq!(stats.total_producers, 3);
        assert_eq!(stats.partitions.len(), 3);

        // Check each partition has 1 producer
        for partition_stat in &stats.partitions {
            assert_eq!(partition_stat.producer_count, 1);
        }
    }

    #[tokio::test]
    async fn test_is_idle() {
        let storage = create_test_storage();
        let mut topic = PartitionedTopic::new("test-topic".to_string(), 3, storage);

        // Initially idle
        assert!(topic.is_idle().await);

        // Add producer
        let partition_topic = topic.get_partition(0).unwrap();
        let producer = create_test_producer(1, partition_topic);
        topic.add_producer_to_partition(0, producer).await.unwrap();

        // Not idle
        assert!(!topic.is_idle().await);
    }
}
