use crate::broker::service::topic::{PartitionedTopic, Topic};
use crate::broker::service::{Producer, SharedStorage};
use crate::storage::Storage;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

fn create_test_storage() -> SharedStorage {
    Arc::new(Mutex::new(
        Storage::new_memory(Path::new("/tmp/test-partitioned-topic")).unwrap(),
    ))
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
    let topic = PartitionedTopic::new(
        "persistent://public/default/my-topic".to_string(),
        3,
        storage,
    );

    // Check partition names
    let p0 = topic.get_partition(0).unwrap();
    let p0_guard = p0.read().await;
    assert_eq!(
        p0_guard.name,
        "persistent://public/default/my-topic-partition-0"
    );

    let p1 = topic.get_partition(1).unwrap();
    let p1_guard = p1.read().await;
    assert_eq!(
        p1_guard.name,
        "persistent://public/default/my-topic-partition-1"
    );
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
