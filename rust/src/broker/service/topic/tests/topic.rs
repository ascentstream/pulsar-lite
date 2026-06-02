use crate::broker::service::topic::{
    SharedSubscription, SubscriptionType, Topic, TopicPublishRate, TopicPublishRateExceeded,
    TopicRuntimeMode,
};
use crate::broker::service::{Consumer, Producer, SharedStorage};
use crate::protocol::codec::proto::pulsar::MessageMetadata;
use crate::storage::Storage;
use bytes::Bytes;
use prost::Message;
use std::path::Path;
use std::sync::{Arc, Arc as StdArc};
use std::time::Instant;
#[cfg(feature = "rocksdb-storage")]
use tempfile::tempdir;
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

#[cfg(feature = "rocksdb-storage")]
#[tokio::test]
async fn persistent_topic_publish_recovers_from_rocksdb() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("broker-topic-storage.db");
    let topic_name = "persistent://public/default/topic-publish-recovery";

    let message_id = {
        let storage = StdArc::new(Mutex::new(Storage::new(&db_path).unwrap()));
        let mut topic = Topic::new(topic_name.to_string(), storage);
        assert_eq!(topic.runtime_mode(), TopicRuntimeMode::Persistent);
        topic
            .publish_message(None, Bytes::from_static(b"payload"))
            .await
            .unwrap()
    };

    let storage = Storage::new(&db_path).unwrap();
    let messages = storage.get_messages(topic_name);
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].0, message_id);
    assert_eq!(messages[0].1, b"payload".to_vec());
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
