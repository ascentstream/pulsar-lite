use crate::broker::service::topic::{
    KeySharedMode, KeySharedPolicy, Subscription, SubscriptionRuntimeMode, SubscriptionType,
};
use crate::broker::service::{Consumer, PendingMessage, SharedStorage};
use crate::storage::MessageId;
use crate::storage::NonPersistentEntry;
use crate::storage::Storage;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};

fn create_test_storage() -> SharedStorage {
    Arc::new(Mutex::new(
        Storage::new_memory(Path::new("/tmp/test-subscription-storage")).unwrap(),
    ))
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
    let (tx, _rx) = mpsc::channel(8192);
    Arc::new(Consumer::new(
        id,
        format!("consumer-{}", id),
        subscription,
        format!("conn-{}", id),
        tx,
        0,
    ))
}

fn create_test_consumer_with_capacity(
    id: u64,
    subscription: Arc<RwLock<Subscription>>,
    capacity: usize,
) -> (Arc<Consumer>, mpsc::Receiver<(u64, PendingMessage)>) {
    let (tx, rx) = mpsc::channel(capacity);
    (
        Arc::new(Consumer::new(
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

    assert!(sub.add_consumer(consumer1).is_ok());
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
async fn test_non_persistent_dispatch_does_not_pause_on_unacked_budget() {
    let subscription = Arc::new(RwLock::new(Subscription::new_with_runtime_mode(
        "sub".to_string(),
        "topic".to_string(),
        SubscriptionType::Shared,
        SubscriptionRuntimeMode::NonPersistent,
        create_test_storage(),
    )));

    let (consumer, mut rx) = create_test_consumer_with_capacity(1, subscription.clone(), 8192);
    consumer.add_permits(1).await;

    {
        let mut sub = subscription.write().await;
        sub.add_consumer(consumer.clone())
            .expect("consumer should register");
        sub.consumer_flow(consumer.consumer_id, 1).await;
    }

    // Fill pending acks well past the current inflight-budget default.
    for i in 0..50000u64 {
        consumer
            .track_message_dispatched(
                &MessageId {
                    ledger: 0,
                    entry: i,
                    partition: -1,
                },
                0,
            )
            .await;
    }

    subscription
        .read()
        .await
        .send_non_persistent_entries(vec![NonPersistentEntry::create(
            1,
            1,
            -1,
            bytes::Bytes::new(),
            bytes::Bytes::from_static(b"hello"),
        )])
        .await
        .expect("dispatch should still proceed");

    let delivered = rx.recv().await.expect("message should be delivered");
    assert_eq!(delivered.1.payload, b"hello".to_vec());
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
    assert_eq!(active.len(), 1);
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

    consumer1.add_permits(10).await;
    consumer2.add_permits(15).await;

    sub.add_consumer(consumer1).unwrap();
    sub.add_consumer(consumer2).unwrap();

    assert_eq!(sub.get_total_permits().await, 25);
}

#[tokio::test]
async fn test_non_persistent_subscription_capability_boundaries_are_noop() {
    let mut sub = Subscription::new(
        "np-sub".to_string(),
        "topic".to_string(),
        SubscriptionType::Shared,
        create_test_storage(),
    );
    sub.set_runtime_mode(SubscriptionRuntimeMode::NonPersistent);

    assert!(sub.clear_backlog().await.is_ok());
    assert!(sub.skip_messages(10).await.is_ok());
    assert!(sub.reset_cursor().await.is_ok());
    assert_eq!(sub.backlog_size().await.unwrap(), 0);
}

#[tokio::test]
async fn test_subscription_defaults_to_persistent_runtime_mode() {
    let sub = Subscription::new(
        "persistent-sub".to_string(),
        "persistent://public/default/topic".to_string(),
        SubscriptionType::Shared,
        create_test_storage(),
    );

    assert_eq!(sub.runtime_mode(), SubscriptionRuntimeMode::Persistent);
}

#[tokio::test]
async fn test_non_persistent_subscription_can_be_fenced() {
    let mut sub = Subscription::new_with_options(
        "np-sub".to_string(),
        "topic".to_string(),
        SubscriptionType::KeyShared,
        SubscriptionRuntimeMode::NonPersistent,
        HashMap::from([(String::from("env"), String::from("test"))]),
        Some(KeySharedPolicy {
            mode: KeySharedMode::AutoSplit,
            ranges: Vec::new(),
            allow_out_of_order_delivery: false,
        }),
        create_test_storage(),
    );

    assert!(!sub.is_fenced());
    sub.fence();
    assert!(sub.is_fenced());
    assert_eq!(sub.properties().get("env"), Some(&String::from("test")));
    assert_eq!(
        sub.key_shared_policy().map(|policy| policy.mode),
        Some(KeySharedMode::AutoSplit)
    );
}

#[tokio::test]
async fn test_fenced_non_persistent_subscription_rejects_new_consumer() {
    let subscription = Arc::new(RwLock::new(Subscription::new_with_options(
        "np-sub".to_string(),
        "topic".to_string(),
        SubscriptionType::Shared,
        SubscriptionRuntimeMode::NonPersistent,
        HashMap::new(),
        None,
        create_test_storage(),
    )));
    subscription.write().await.fence();

    let consumer = create_test_consumer(1, subscription.clone());
    let result = subscription.write().await.add_consumer(consumer);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("fenced"));
}
