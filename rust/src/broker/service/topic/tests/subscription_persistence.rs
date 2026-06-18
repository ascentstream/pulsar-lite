use crate::broker::service::topic::{Subscription, SubscriptionType};
use crate::broker::service::{Consumer, PendingMessage, SharedStorage};
use crate::storage::{CursorInitOptions, InitialPosition, Storage};
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::{timeout, Duration};

fn create_persistent_storage(path: &Path) -> SharedStorage {
    Arc::new(Mutex::new(Storage::new(path).unwrap()))
}

fn open_earliest_cursor(storage: &mut Storage, topic: &str, subscription: &str) {
    storage
        .initialize_or_open_cursor(
            topic,
            subscription,
            CursorInitOptions {
                initial_position: InitialPosition::Earliest,
                start_message_id: None,
            },
        )
        .unwrap();
}

fn create_persistent_subscription(
    storage: SharedStorage,
    topic: &str,
    subscription: &str,
    sub_type: SubscriptionType,
) -> Arc<RwLock<Subscription>> {
    Arc::new(RwLock::new(Subscription::new(
        subscription.to_string(),
        topic.to_string(),
        sub_type,
        storage,
    )))
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
            format!("consumer-{id}"),
            subscription,
            format!("conn-{id}"),
            tx,
            0,
        )),
        rx,
    )
}

async fn add_consumer_and_flow(
    subscription: &Arc<RwLock<Subscription>>,
    consumer_id: u64,
    permits: u32,
) -> Arc<Consumer> {
    let consumer = {
        let sub_guard = subscription.read().await;
        sub_guard.get_consumer(consumer_id).unwrap()
    };
    consumer.add_permits(permits).await;
    {
        let sub_guard = subscription.read().await;
        sub_guard.consumer_flow(consumer_id, permits).await;
    }
    consumer
}

#[tokio::test]
async fn persistent_exclusive_dispatches_recovered_backlog() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("exclusive-backlog.db");
    let topic = "persistent://public/default/exclusive-recovered-backlog";
    let subscription_name = "sub";

    {
        let mut storage = Storage::new(&db_path).unwrap();
        storage.create_topic(topic).unwrap();
        open_earliest_cursor(&mut storage, topic, subscription_name);
        storage.append_message(topic, -1, b"backlog").unwrap();
    }

    let storage = create_persistent_storage(&db_path);
    let subscription = create_persistent_subscription(
        storage,
        topic,
        subscription_name,
        SubscriptionType::Exclusive,
    );
    let (consumer, mut rx) = create_test_consumer_with_capacity(1, subscription.clone(), 8);
    {
        let mut sub = subscription.write().await;
        sub.add_consumer(consumer).unwrap();
    }
    add_consumer_and_flow(&subscription, 1, 1).await;

    let delivered = rx.recv().await.expect("recovered backlog should dispatch");
    assert_eq!(delivered.0, 1);
    assert_eq!(delivered.1.payload, b"backlog".to_vec());
}

#[tokio::test]
async fn persistent_exclusive_ack_cursor_recovers_after_reopen() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("exclusive-cursor.db");
    let topic = "persistent://public/default/exclusive-cursor-recovery";
    let subscription_name = "sub";

    {
        let mut storage = Storage::new(&db_path).unwrap();
        storage.create_topic(topic).unwrap();
        open_earliest_cursor(&mut storage, topic, subscription_name);
        storage.append_message(topic, -1, b"acked").unwrap();
        storage.append_message(topic, -1, b"pending").unwrap();
    }

    {
        let storage = create_persistent_storage(&db_path);
        let subscription = create_persistent_subscription(
            storage.clone(),
            topic,
            subscription_name,
            SubscriptionType::Exclusive,
        );
        let (consumer, mut rx) = create_test_consumer_with_capacity(1, subscription.clone(), 8);
        {
            let mut sub = subscription.write().await;
            sub.add_consumer(consumer).unwrap();
        }
        add_consumer_and_flow(&subscription, 1, 1).await;

        let delivered = rx.recv().await.expect("first message should dispatch");
        assert_eq!(delivered.1.payload, b"acked".to_vec());
        storage
            .lock()
            .await
            .ack_message(topic, subscription_name, delivered.1.message_id)
            .unwrap();
        assert!(subscription.write().await.remove_consumer(1).is_some());
    }

    let storage = create_persistent_storage(&db_path);
    let subscription = create_persistent_subscription(
        storage,
        topic,
        subscription_name,
        SubscriptionType::Exclusive,
    );
    let (consumer, mut rx) = create_test_consumer_with_capacity(2, subscription.clone(), 8);
    {
        let mut sub = subscription.write().await;
        sub.add_consumer(consumer).unwrap();
    }
    add_consumer_and_flow(&subscription, 2, 1).await;

    let delivered = rx
        .recv()
        .await
        .expect("unacked message should dispatch after reopen");
    assert_eq!(delivered.0, 2);
    assert_eq!(delivered.1.payload, b"pending".to_vec());
    assert!(
        timeout(Duration::from_millis(100), rx.recv())
            .await
            .is_err(),
        "acked message should not redeliver after reopen"
    );
}

#[tokio::test]
async fn persistent_shared_ack_state_recovers_after_reopen() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("shared-cursor.db");
    let topic = "persistent://public/default/shared-cursor-recovery";
    let subscription_name = "sub";

    {
        let mut storage = Storage::new(&db_path).unwrap();
        storage.create_topic(topic).unwrap();
        open_earliest_cursor(&mut storage, topic, subscription_name);
        storage.append_message(topic, -1, b"first").unwrap();
        storage.append_message(topic, -1, b"second").unwrap();
        storage.append_message(topic, -1, b"acked-hole").unwrap();
    }

    {
        let storage = create_persistent_storage(&db_path);
        let subscription = create_persistent_subscription(
            storage.clone(),
            topic,
            subscription_name,
            SubscriptionType::Shared,
        );
        let (consumer, mut rx) = create_test_consumer_with_capacity(1, subscription.clone(), 8);
        {
            let mut sub = subscription.write().await;
            sub.add_consumer(consumer).unwrap();
        }
        let consumer = add_consumer_and_flow(&subscription, 1, 3).await;

        let first = rx.recv().await.expect("first dispatch");
        let second = rx.recv().await.expect("second dispatch");
        let acked_hole = rx.recv().await.expect("third dispatch");
        assert_eq!(first.1.payload, b"first".to_vec());
        assert_eq!(second.1.payload, b"second".to_vec());
        assert_eq!(acked_hole.1.payload, b"acked-hole".to_vec());

        assert!(consumer.remove_pending_ack(&acked_hole.1.message_id).await);
        storage
            .lock()
            .await
            .ack_message_shared(topic, subscription_name, acked_hole.1.message_id)
            .unwrap();
        assert!(subscription.write().await.remove_consumer(1).is_some());
    }

    let storage = create_persistent_storage(&db_path);
    let subscription =
        create_persistent_subscription(storage, topic, subscription_name, SubscriptionType::Shared);
    let (consumer, mut rx) = create_test_consumer_with_capacity(2, subscription.clone(), 8);
    {
        let mut sub = subscription.write().await;
        sub.add_consumer(consumer).unwrap();
    }
    add_consumer_and_flow(&subscription, 2, 3).await;

    let first = rx.recv().await.expect("unacked first should dispatch");
    let second = rx.recv().await.expect("unacked second should dispatch");
    assert_eq!(first.1.payload, b"first".to_vec());
    assert_eq!(second.1.payload, b"second".to_vec());
    assert!(
        timeout(Duration::from_millis(100), rx.recv())
            .await
            .is_err(),
        "shared ack hole should not redeliver after reopen"
    );
}
