use crate::broker::service::topic::SubscriptionType;
use crate::broker::service::Consumer;
use crate::storage::{MessageId, NonPersistentEntry};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
    Arc,
};

#[derive(Debug, Default)]
pub struct NonPersistentDispatcherMultipleConsumers {
    consumers_by_id: HashMap<u64, Arc<Consumer>>,
    ordered_consumers: Vec<Arc<Consumer>>,
    next_consumer_index: AtomicUsize,
    total_available_permits: AtomicU32,
    received_messages: AtomicU64,
    dropped_messages: AtomicU64,
    dispatched_messages: AtomicU64,
}

impl NonPersistentDispatcherMultipleConsumers {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_type(&self) -> SubscriptionType {
        SubscriptionType::Shared
    }

    pub fn is_consumer_connected(&self) -> bool {
        !self.consumers_by_id.is_empty()
    }

    pub fn get_consumers(&self) -> Vec<Arc<Consumer>> {
        self.consumers_by_id.values().cloned().collect()
    }

    pub fn get_consumer(&self, consumer_id: u64) -> Option<Arc<Consumer>> {
        self.consumers_by_id.get(&consumer_id).cloned()
    }

    fn rebuild_ordered_consumers(&mut self) {
        let mut consumers: Vec<_> = self.consumers_by_id.values().cloned().collect();
        consumers.sort_by_key(|consumer| (consumer.get_priority_level(), consumer.consumer_id));
        self.ordered_consumers = consumers;
    }

    pub fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String> {
        if self.consumers_by_id.contains_key(&consumer.consumer_id) {
            return Err(format!(
                "Consumer {} already exists in non-persistent shared dispatcher",
                consumer.consumer_id
            ));
        }
        self.consumers_by_id.insert(consumer.consumer_id, consumer);
        self.rebuild_ordered_consumers();
        Ok(())
    }

    pub fn remove_consumer(&mut self, consumer_id: u64) -> Option<Arc<Consumer>> {
        let consumer = self.consumers_by_id.remove(&consumer_id);
        if let Some(consumer) = &consumer {
            self.subtract_total_permits(consumer.available_permits_now());
        }
        self.rebuild_ordered_consumers();
        consumer
    }

    pub fn consumer_flow(&self, consumer_id: u64, additional_permits: u32) {
        self.total_available_permits
            .fetch_add(additional_permits, Ordering::Relaxed);

        log::debug!(
            "Non-persistent shared dispatcher received flow from consumer {}, permits={}, total={}",
            consumer_id,
            additional_permits,
            self.total_available_permits.load(Ordering::Relaxed)
        );
    }

    pub fn dropped_messages(&self) -> u64 {
        self.dropped_messages.load(Ordering::Relaxed)
    }

    pub fn received_messages(&self) -> u64 {
        self.received_messages.load(Ordering::Relaxed)
    }

    pub fn dispatched_messages(&self) -> u64 {
        self.dispatched_messages.load(Ordering::Relaxed)
    }

    pub fn record_drop(&self, count: u64) {
        self.dropped_messages.fetch_add(count, Ordering::Relaxed);
    }

    fn record_dispatched(&self, count: u64) {
        self.dispatched_messages.fetch_add(count, Ordering::Relaxed);
    }

    fn subtract_total_permits(&self, permits: u32) {
        let _ = self.total_available_permits.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current: u32| Some(current.saturating_sub(permits)),
        );
    }

    fn next_consumer_start_index(&self) -> Option<usize> {
        if self.ordered_consumers.is_empty() {
            return None;
        }

        Some(
            self.next_consumer_index.fetch_add(1, Ordering::Relaxed) % self.ordered_consumers.len(),
        )
    }

    pub async fn send_messages(
        &self,
        entries: Vec<NonPersistentEntry>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let entry_count = entries.len() as u64;
        self.received_messages
            .fetch_add(entry_count, Ordering::Relaxed);
        let mut pending_entries = entries.into_iter();
        while let Some(entry) = pending_entries.next() {
            if self.total_available_permits.load(Ordering::Relaxed) == 0 {
                log::debug!("Dropping non-persistent shared entry due to zero aggregate permits");
                self.record_drop(1);
                entry.release();
                while let Some(remaining) = pending_entries.next() {
                    self.record_drop(1);
                    remaining.release();
                }
                continue;
            }

            let Some(start) = self.next_consumer_start_index() else {
                log::debug!("Dropping non-persistent shared entry due to no connected consumer");
                self.record_drop(1);
                entry.release();
                continue;
            };

            let message_id = MessageId {
                ledger: entry.ledger_id(),
                entry: entry.entry_id(),
                partition: entry.partition(),
            };
            let metadata = entry.metadata_bytes();
            let payload = entry.payload_bytes();

            let mut delivered = false;
            for offset in 0..self.ordered_consumers.len() {
                let consumer =
                    self.ordered_consumers[(start + offset) % self.ordered_consumers.len()].clone();

                if let Some(reservation) = consumer
                    .try_reserve_dispatch(&message_id, metadata.clone(), payload.clone(), 0)
                    .await
                {
                    reservation.send();
                    self.record_dispatched(1);
                    self.subtract_total_permits(1);
                    consumer.record_message_dispatched(entry.len()).await;
                    delivered = true;
                    break;
                }
            }

            if !delivered {
                log::debug!("Dropping non-persistent shared entry due to no writable consumer");
                self.record_drop(1);
            }
            entry.release();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::service::topic::Subscription;
    use crate::broker::service::SharedStorage;
    use crate::storage::{NonPersistentEntry, Storage};
    use bytes::Bytes;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::sync::{mpsc, Mutex, RwLock};

    fn create_test_storage() -> SharedStorage {
        Arc::new(Mutex::new(
            Storage::new_memory(Path::new("/tmp/test-np-shared-dispatcher-storage")).unwrap(),
        ))
    }

    fn create_test_subscription() -> Arc<RwLock<Subscription>> {
        Arc::new(RwLock::new(Subscription::new(
            "test-sub".to_string(),
            "test-topic".to_string(),
            SubscriptionType::Shared,
            create_test_storage(),
        )))
    }

    fn create_test_consumer(
        id: u64,
        subscription: Arc<RwLock<Subscription>>,
    ) -> (
        Arc<Consumer>,
        mpsc::Receiver<(u64, crate::broker::service::PendingMessage)>,
    ) {
        create_test_consumer_with_capacity(id, subscription, 8192)
    }

    fn create_test_consumer_with_capacity(
        id: u64,
        subscription: Arc<RwLock<Subscription>>,
        capacity: usize,
    ) -> (
        Arc<Consumer>,
        mpsc::Receiver<(u64, crate::broker::service::PendingMessage)>,
    ) {
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

    #[tokio::test]
    async fn shared_dispatcher_skips_backpressured_consumer() {
        let subscription = create_test_subscription();
        let (consumer_a, _rx_a) = create_test_consumer_with_capacity(1, subscription.clone(), 1);
        let (consumer_b, mut rx_b) = create_test_consumer_with_capacity(2, subscription, 1);

        let mut dispatcher = NonPersistentDispatcherMultipleConsumers::new();
        dispatcher.add_consumer(consumer_a.clone()).unwrap();
        dispatcher.add_consumer(consumer_b.clone()).unwrap();

        consumer_a.add_permits(1).await;
        consumer_b.add_permits(1).await;
        dispatcher.consumer_flow(consumer_a.consumer_id, 1);
        dispatcher.consumer_flow(consumer_b.consumer_id, 1);

        let permit = consumer_a
            .try_acquire_send_permit()
            .expect("consumer A channel should have capacity");
        permit.send((
            consumer_a.consumer_id,
            crate::broker::service::PendingMessage {
                message_id: MessageId {
                    ledger: 9,
                    entry: 9,
                    partition: -1,
                },
                metadata: Bytes::new(),
                payload: Bytes::from_static(b"occupied"),
                redelivery_count: 0,
                wire_size: 8,
            },
        ));

        dispatcher
            .send_messages(vec![NonPersistentEntry::create(
                1,
                1,
                -1,
                Bytes::new(),
                Bytes::from_static(b"hello"),
            )])
            .await
            .unwrap();

        let delivered = rx_b
            .recv()
            .await
            .expect("consumer B should receive message");
        assert_eq!(delivered.0, consumer_b.consumer_id);
        assert_eq!(delivered.1.payload, Bytes::from_static(b"hello"));
        assert_eq!(dispatcher.dropped_messages(), 0);
    }

    fn sized_bytes(fill: u8, size: usize) -> Bytes {
        Bytes::from(vec![fill; size])
    }

    #[tokio::test]
    async fn shared_dispatcher_limits_batch_by_aggregate_permits() {
        let subscription = create_test_subscription();
        let (consumer, mut rx) = create_test_consumer(1, subscription);
        let mut dispatcher = NonPersistentDispatcherMultipleConsumers::new();
        dispatcher.add_consumer(consumer.clone()).unwrap();

        consumer.add_permits(2).await;
        dispatcher.consumer_flow(consumer.consumer_id, 1);

        dispatcher
            .send_messages(vec![
                NonPersistentEntry::create(0, 0, -1, Bytes::new(), Bytes::from_static(b"first")),
                NonPersistentEntry::create(0, 0, -1, Bytes::new(), Bytes::from_static(b"second")),
            ])
            .await
            .unwrap();

        let first = rx.recv().await.expect("first message should be delivered");
        assert_eq!(first.1.payload, b"first".to_vec());
        assert!(rx.try_recv().is_err());
        assert_eq!(dispatcher.dropped_messages(), 1);
        assert_eq!(consumer.get_available_permits().await, 1);
    }

    #[tokio::test]
    #[ignore]
    async fn perf_baseline_shared_dispatcher_32_consumers_10k_entries() {
        const CONSUMER_COUNT: usize = 32;
        const ENTRY_COUNT: usize = 10_000;

        let subscription = create_test_subscription();
        let mut dispatcher = NonPersistentDispatcherMultipleConsumers::new();
        let mut _receivers = Vec::with_capacity(CONSUMER_COUNT);

        for consumer_id in 0..CONSUMER_COUNT as u64 {
            let (consumer, rx) = create_test_consumer(consumer_id, subscription.clone());
            _receivers.push(rx);
            consumer.add_permits(ENTRY_COUNT as u32).await;
            dispatcher.consumer_flow(consumer.consumer_id, ENTRY_COUNT as u32);
            dispatcher.add_consumer(consumer).unwrap();
        }

        // 构造消息
        let entries: Vec<_> = (0..ENTRY_COUNT)
            .map(|entry_id| {
                NonPersistentEntry::create(
                    1,
                    entry_id as u64,
                    -1,
                    Bytes::new(),
                    Bytes::from(format!("shared-{entry_id}")),
                )
            })
            .collect();

        let start = Instant::now();
        dispatcher.send_messages(entries).await.unwrap();
        let elapsed = start.elapsed();

        println!(
            "PERF baseline shared dispatcher: consumers={CONSUMER_COUNT}, entries={ENTRY_COUNT}, elapsed_ms={}",
            elapsed.as_millis()
        );
        assert_eq!(dispatcher.dropped_messages(), 0);
    }

    #[tokio::test]
    #[ignore]
    async fn perf_copy_path_shared_dispatcher_32_consumers_10k_entries_4k_payload() {
        const CONSUMER_COUNT: usize = 32;
        const ENTRY_COUNT: usize = 10_000;
        const METADATA_SIZE: usize = 256;
        const PAYLOAD_SIZE: usize = 4096;

        let subscription = create_test_subscription();
        let mut dispatcher = NonPersistentDispatcherMultipleConsumers::new();
        let mut _receivers = Vec::with_capacity(CONSUMER_COUNT);

        for consumer_id in 0..CONSUMER_COUNT as u64 {
            let (consumer, rx) = create_test_consumer(consumer_id, subscription.clone());
            _receivers.push(rx);
            consumer.add_permits(ENTRY_COUNT as u32).await;
            dispatcher.consumer_flow(consumer.consumer_id, ENTRY_COUNT as u32);
            dispatcher.add_consumer(consumer).unwrap();
        }

        let metadata = sized_bytes(b'm', METADATA_SIZE);
        let payload = sized_bytes(b'p', PAYLOAD_SIZE);
        let entries: Vec<_> = (0..ENTRY_COUNT)
            .map(|entry_id| {
                NonPersistentEntry::create(
                    1,
                    entry_id as u64,
                    -1,
                    metadata.clone(),
                    payload.clone(),
                )
            })
            .collect();

        let start = Instant::now();
        dispatcher.send_messages(entries).await.unwrap();
        let elapsed = start.elapsed();

        println!(
            "PERF copy-path shared dispatcher: consumers={CONSUMER_COUNT}, entries={ENTRY_COUNT}, metadata_bytes={METADATA_SIZE}, payload_bytes={PAYLOAD_SIZE}, elapsed_ms={}",
            elapsed.as_millis()
        );
        assert_eq!(dispatcher.dropped_messages(), 0);
    }
}
