use crate::broker::service::topic::{
    KeySharedHashRange, KeySharedMode, KeySharedPolicy, SubscriptionType,
};
use crate::broker::service::Consumer;
use crate::protocol::codec::proto::pulsar::MessageMetadata;
use crate::storage::{MessageId, NonPersistentEntry};
use prost::Message;
use std::collections:: HashMap;
use std::sync::{
    atomic::{AtomicU32, AtomicU64, Ordering},
    Arc,
};

#[derive(Debug)]
pub struct NonPersistentStickyKeyDispatcher {
    consumers_by_id: HashMap<u64, Arc<Consumer>>,
    auto_split_assignments: Vec<(KeySharedHashRange, Arc<Consumer>)>,
    sticky_assignments: Vec<(KeySharedHashRange, Arc<Consumer>)>,
    key_shared_policy: KeySharedPolicy,
    total_available_permits: AtomicU32,
    dropped_messages: AtomicU64,
}

impl NonPersistentStickyKeyDispatcher {
    pub fn new(key_shared_policy: Option<KeySharedPolicy>) -> Self {
        Self {
            consumers_by_id: HashMap::new(),
            auto_split_assignments: Vec::new(),
            sticky_assignments: Vec::new(),
            key_shared_policy: key_shared_policy.unwrap_or(KeySharedPolicy {
                mode: KeySharedMode::AutoSplit,
                ranges: Vec::new(),
                allow_out_of_order_delivery: false,
            }),
            total_available_permits: AtomicU32::new(0),
            dropped_messages: AtomicU64::new(0),
        }
    }

    pub fn get_type(&self) -> SubscriptionType {
        SubscriptionType::KeyShared
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

    pub fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String> {
        if self.consumers_by_id.contains_key(&consumer.consumer_id) {
            return Err(format!(
                "Consumer {} already exists in non-persistent key-shared dispatcher",
                consumer.consumer_id
            ));
        }
        if !self.has_same_key_shared_policy(consumer.key_shared_policy().as_ref()) {
            return Err(
                "Consumer key shared policy is incompatible with the dispatcher".to_string(),
            );
        }
        self.consumers_by_id.insert(consumer.consumer_id, consumer);
        self.rebuild_assignments();
        Ok(())
    }

    pub fn remove_consumer(&mut self, consumer_id: u64) -> Option<Arc<Consumer>> {
        let consumer = self.consumers_by_id.remove(&consumer_id);
        if let Some(consumer) = &consumer {
            self.subtract_total_permits(consumer.available_permits_now());
        }
        self.rebuild_assignments();
        consumer
    }

    pub fn consumer_flow(&self, consumer_id: u64, additional_permits: u32) {
        self.total_available_permits
            .fetch_add(additional_permits, Ordering::Relaxed);

        log::debug!(
            "Non-persistent key-shared dispatcher received flow from consumer {}, permits={}, total={}",
            consumer_id,
            additional_permits,
            self.total_available_permits.load(Ordering::Relaxed)
        );
    }

    pub fn dropped_messages(&self) -> u64 {
        self.dropped_messages.load(Ordering::Relaxed)
    }

    pub fn has_same_key_shared_policy(&self, policy: Option<&KeySharedPolicy>) -> bool {
        let Some(policy) = policy else {
            return self.key_shared_policy.mode == KeySharedMode::AutoSplit;
        };
        policy.mode == self.key_shared_policy.mode
    }

    fn record_drop(&self, count: u64) {
        self.dropped_messages.fetch_add(count, Ordering::Relaxed);
    }

    fn subtract_total_permits(&self, permits: u32) {
        let _ = self.total_available_permits.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current: u32| Some(current.saturating_sub(permits)),
        );
    }

    fn resolve_sticky_key(entry: &NonPersistentEntry) -> Vec<u8> {
        let Ok(metadata) = MessageMetadata::decode(entry.metadata()) else {
            return Vec::new();
        };

        if let Some(ordering_key) = metadata.ordering_key {
            return ordering_key;
        }

        if let Some(partition_key) = metadata.partition_key {
            return partition_key.into_bytes();
        }

        if !metadata.producer_name.is_empty() {
            return format!("{}-{}", metadata.producer_name, metadata.sequence_id).into_bytes();
        }

        Vec::new()
    }

    fn murmur3_32(bytes: &[u8], seed: u32) -> u32 {
        const C1: u32 = 0xcc9e2d51;
        const C2: u32 = 0x1b873593;

        let mut hash = seed;
        let mut chunks = bytes.chunks_exact(4);

        for chunk in &mut chunks {
            let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
            k = k.wrapping_mul(C1);
            k = k.rotate_left(15);
            k = k.wrapping_mul(C2);

            hash ^= k;
            hash = hash.rotate_left(13);
            hash = hash.wrapping_mul(5).wrapping_add(0xe6546b64);
        }

        let tail = chunks.remainder();
        let mut k1 = 0u32;
        match tail.len() {
            3 => {
                k1 ^= (tail[2] as u32) << 16;
                k1 ^= (tail[1] as u32) << 8;
                k1 ^= tail[0] as u32;
            }
            2 => {
                k1 ^= (tail[1] as u32) << 8;
                k1 ^= tail[0] as u32;
            }
            1 => {
                k1 ^= tail[0] as u32;
            }
            _ => {}
        }
        if !tail.is_empty() {
            k1 = k1.wrapping_mul(C1);
            k1 = k1.rotate_left(15);
            k1 = k1.wrapping_mul(C2);
            hash ^= k1;
        }

        hash ^= bytes.len() as u32;
        hash ^= hash >> 16;
        hash = hash.wrapping_mul(0x85ebca6b);
        hash ^= hash >> 13;
        hash = hash.wrapping_mul(0xc2b2ae35);
        hash ^= hash >> 16;
        hash
    }

    fn sticky_key_hash(sticky_key: &[u8]) -> i32 {
        const RANGE_SIZE: u32 = 2 << 15;
        (Self::murmur3_32(sticky_key, 0) % RANGE_SIZE) as i32
    }

    fn sticky_ranges_for_consumer(consumer: &Arc<Consumer>) -> Vec<KeySharedHashRange> {
        consumer
            .key_shared_policy()
            .map(|policy| policy.ranges.clone())
            .unwrap_or_default()
    }

    fn rebuild_auto_split_assignments(&mut self) {
        const RANGE_SIZE: i32 = (2 << 15) - 1;
        let mut consumers: Vec<_> = self.consumers_by_id.values().cloned().collect();
        consumers.sort_by_key(|consumer| consumer.consumer_id);

        if consumers.is_empty() {
            self.auto_split_assignments.clear();
            return;
        }

        let total = consumers.len() as i32;
        let mut assignments = Vec::with_capacity(consumers.len());
        for (index, consumer) in consumers.into_iter().enumerate() {
            let index = index as i32;
            let start = (index * (RANGE_SIZE + 1)) / total;
            let end = (((index + 1) * (RANGE_SIZE + 1)) / total) - 1;
            assignments.push((KeySharedHashRange { start, end }, consumer));
        }
        self.auto_split_assignments = assignments;
    }

    fn rebuild_sticky_assignments(&mut self) {
        let mut assignments = Vec::new();
        for consumer in self.consumers_by_id.values() {
            for range in Self::sticky_ranges_for_consumer(consumer) {
                assignments.push((range, consumer.clone()))
            }
        }

        assignments.sort_by_key(|(range, consumer)| (range.start, range.end, consumer.consumer_id));
        self.sticky_assignments = assignments;
    }

    fn rebuild_assignments(&mut self) {
        self.rebuild_auto_split_assignments();
        self.rebuild_sticky_assignments();
    }

    fn select_consumer(&self, sticky_key: &[u8]) -> Option<Arc<Consumer>> {
        let sticky_key_hash = Self::sticky_key_hash(sticky_key);
        let assignments = match self.key_shared_policy.mode {
            KeySharedMode::AutoSplit => &self.auto_split_assignments,
            KeySharedMode::Sticky => &self.sticky_assignments,
        };

        for (range, consumer) in assignments {
            if sticky_key_hash >= range.start
            && sticky_key_hash <= range.end
            && consumer.available_permits_now() > 0
            {
                return Some(consumer.clone());
            }
        }

        None
    }
    

    pub async fn send_messages(
        &self,
        entries: Vec<NonPersistentEntry>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.total_available_permits.load(Ordering::Relaxed) == 0 {
            self.record_drop(entries.len() as u64);
            for entry in entries {
                entry.release();
            }
            return Ok(());
        }

        let mut grouped_entries: HashMap<u64, (Arc<Consumer>, Vec<NonPersistentEntry>)> =
            HashMap::new();
        for entry in entries {
            let sticky_key = Self::resolve_sticky_key(&entry);
            let Some(consumer) = self.select_consumer(&sticky_key) else {
                log::debug!(
                    "Dropping non-persistent key-shared entry due to no available consumer"
                );
                self.record_drop(1);
                entry.release();
                continue;
            };
            grouped_entries
                .entry(consumer.consumer_id)
                .or_insert_with(|| (consumer.clone(), Vec::new()))
                .1
                .push(entry);
        }

        for (_consumer_id, (consumer, entries_for_consumer)) in grouped_entries {
            let consumer_permits = consumer.get_available_permits().await as usize;
            let aggregate_permits = self.total_available_permits.load(Ordering::Relaxed) as usize;
            let dispatchable = consumer_permits.min(aggregate_permits);
            if dispatchable == 0 {
                self.record_drop(entries_for_consumer.len() as u64);
                for entry in entries_for_consumer {
                    entry.release();
                }
                continue;
            }

            let mut batch_entries = entries_for_consumer;
            let overflow = batch_entries.split_off(dispatchable.min(batch_entries.len()));

            let mut batch_messages = Vec::with_capacity(batch_entries.len());
            for entry in &batch_entries {
                let permit_acquired = consumer.use_permit().await;
                debug_assert!(
                    permit_acquired,
                    "key-shared dispatch window exceeded permits"
                );
                batch_messages.push((
                    MessageId {
                        ledger: entry.ledger_id(),
                        entry: entry.entry_id(),
                        partition: entry.partition(),
                    },
                    entry.metadata_bytes(),
                    entry.payload_bytes(),
                    0,
                ));
            }

            let attempted = batch_messages.len();
            let sent = consumer.send_messages_batch(batch_messages).await;
            if sent > 0 {
                self.subtract_total_permits(sent as u32);
                for entry in batch_entries.iter().take(sent) {
                    consumer.record_message_dispatched(entry.len()).await;
                }
            }
            if sent < attempted {
                consumer.add_permits((attempted - sent) as u32).await;
            }
            if sent < attempted {
                self.record_drop((attempted - sent) as u64);
            }
            self.record_drop(overflow.len() as u64);

            for entry in batch_entries.into_iter().chain(overflow.into_iter()) {
                entry.release();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::service::topic::{Subscription, SubscriptionRuntimeMode};
    use crate::storage::Storage;
    use bytes::Bytes;
    use std::path::Path;
    use std::time::Instant;
    use tokio::sync::{mpsc, Mutex, RwLock};

    fn create_test_storage() -> crate::broker::service::SharedStorage {
        Arc::new(Mutex::new(
            Storage::new(Path::new("/tmp/test-sticky-dispatcher-storage")).unwrap(),
        ))
    }

    fn create_consumer(
        consumer_id: u64,
        key_shared_policy: Option<KeySharedPolicy>,
    ) -> Arc<Consumer> {
        let (consumer, _rx) = create_consumer_with_rx(consumer_id, key_shared_policy);
        consumer
    }

    fn create_consumer_with_rx(
        consumer_id: u64,
        key_shared_policy: Option<KeySharedPolicy>,
    ) -> (
        Arc<Consumer>,
        mpsc::UnboundedReceiver<(u64, crate::broker::service::PendingMessage)>,
    ) {
        let subscription = Arc::new(RwLock::new(Subscription::new_with_options(
            "sub".to_string(),
            "non-persistent://public/default/key-shared".to_string(),
            SubscriptionType::KeyShared,
            SubscriptionRuntimeMode::NonPersistent,
            HashMap::new(),
            key_shared_policy.clone(),
            create_test_storage(),
        )));
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Arc::new(Consumer::new_with_options(
                consumer_id,
                format!("consumer-{consumer_id}"),
                subscription,
                "conn".to_string(),
                tx,
                0,
                key_shared_policy,
            )),
            rx,
        )
    }

    fn metadata_with_partition_key(key: &str) -> Vec<u8> {
        MessageMetadata {
            partition_key: Some(key.to_string()),
            ..Default::default()
        }
        .encode_to_vec()
    }

    fn metadata_with_ordering_key_padding(key: &str, padding: usize) -> Vec<u8> {
        MessageMetadata {
            producer_name: "m".repeat(padding),
            ordering_key: Some(key.as_bytes().to_vec()),
            ..Default::default()
        }
        .encode_to_vec()
    }

    #[tokio::test]
    async fn sticky_mode_uses_consumer_hash_ranges() {
        let mut dispatcher = NonPersistentStickyKeyDispatcher::new(Some(KeySharedPolicy {
            mode: KeySharedMode::Sticky,
            ranges: Vec::new(),
            allow_out_of_order_delivery: false,
        }));
        let consumer_a = create_consumer(
            1,
            Some(KeySharedPolicy {
                mode: KeySharedMode::Sticky,
                ranges: vec![KeySharedHashRange {
                    start: 0,
                    end: 32767,
                }],
                allow_out_of_order_delivery: false,
            }),
        );
        let consumer_b = create_consumer(
            2,
            Some(KeySharedPolicy {
                mode: KeySharedMode::Sticky,
                ranges: vec![KeySharedHashRange {
                    start: 32768,
                    end: 65535,
                }],
                allow_out_of_order_delivery: false,
            }),
        );
        dispatcher.add_consumer(consumer_a.clone()).unwrap();
        dispatcher.add_consumer(consumer_b.clone()).unwrap();
        consumer_a.add_permits(1).await;
        consumer_b.add_permits(1).await;
        dispatcher.consumer_flow(consumer_a.consumer_id, 1);
        dispatcher.consumer_flow(consumer_b.consumer_id, 1);

        let mut chosen = None;
        for i in 0..500 {
            let key = format!("key-{i}");
            let sticky_hash = NonPersistentStickyKeyDispatcher::sticky_key_hash(key.as_bytes());
            if sticky_hash <= 32767 {
                chosen = dispatcher.select_consumer(key.as_bytes());
                break;
            }
        }
        assert_eq!(chosen.expect("consumer").consumer_id, 1);

        let entry = NonPersistentEntry::create(
            0,
            0,
            -1,
            Bytes::from(metadata_with_partition_key("key-a")),
            Bytes::from_static(b"payload"),
        );
        let _ = entry;
    }

    fn sticky_ranges_for_slot(slot: usize, total: usize) -> Vec<KeySharedHashRange> {
        const RANGE_MAX: i32 = (2 << 15) - 1;
        let total = total as i32;
        let slot = slot as i32;
        let start = (slot * (RANGE_MAX + 1)) / total;
        let end = (((slot + 1) * (RANGE_MAX + 1)) / total) - 1;
        vec![KeySharedHashRange { start, end }]
    }

    #[tokio::test]
    #[ignore]
    async fn perf_baseline_key_shared_auto_split_32_consumers_10k_entries() {
        const CONSUMER_COUNT: usize = 32;
        const ENTRY_COUNT: usize = 10_000;

        let mut dispatcher = NonPersistentStickyKeyDispatcher::new(Some(KeySharedPolicy {
            mode: KeySharedMode::AutoSplit,
            ranges: Vec::new(),
            allow_out_of_order_delivery: false,
        }));
        let mut _receivers = Vec::with_capacity(CONSUMER_COUNT);

        for consumer_id in 0..CONSUMER_COUNT as u64 {
            let (consumer, rx) = create_consumer_with_rx(
                consumer_id,
                Some(KeySharedPolicy {
                    mode: KeySharedMode::AutoSplit,
                    ranges: Vec::new(),
                    allow_out_of_order_delivery: false,
                }),
            );
            _receivers.push(rx);
            consumer.add_permits(ENTRY_COUNT as u32).await;
            dispatcher.consumer_flow(consumer.consumer_id, ENTRY_COUNT as u32);
            dispatcher.add_consumer(consumer).unwrap();
        }

        let entries: Vec<_> = (0..ENTRY_COUNT)
            .map(|entry_id| {
                let key = format!("auto-split-key-{}", entry_id % 128);
                NonPersistentEntry::create(
                    1,
                    entry_id as u64,
                    -1,
                    Bytes::from(metadata_with_partition_key(&key)),
                    Bytes::from(format!("auto-split-{entry_id}")),
                )
            })
            .collect();

        let start = Instant::now();
        dispatcher.send_messages(entries).await.unwrap();
        let elapsed = start.elapsed();

        println!(
            "PERF baseline key-shared auto-split: consumers={CONSUMER_COUNT}, entries={ENTRY_COUNT}, elapsed_ms={}",
            elapsed.as_millis()
        );
        assert_eq!(dispatcher.dropped_messages(), 0);
    }

    #[tokio::test]
    #[ignore]
    async fn perf_baseline_key_shared_sticky_32_consumers_10k_entries() {
        const CONSUMER_COUNT: usize = 32;
        const ENTRY_COUNT: usize = 10_000;

        let mut dispatcher = NonPersistentStickyKeyDispatcher::new(Some(KeySharedPolicy {
            mode: KeySharedMode::Sticky,
            ranges: Vec::new(),
            allow_out_of_order_delivery: false,
        }));
        let mut _receivers = Vec::with_capacity(CONSUMER_COUNT);

        for consumer_id in 0..CONSUMER_COUNT as u64 {
            let (consumer, rx) = create_consumer_with_rx(
                consumer_id,
                Some(KeySharedPolicy {
                    mode: KeySharedMode::Sticky,
                    ranges: sticky_ranges_for_slot(consumer_id as usize, CONSUMER_COUNT),
                    allow_out_of_order_delivery: false,
                }),
            );
            _receivers.push(rx);
            consumer.add_permits(ENTRY_COUNT as u32).await;
            dispatcher.consumer_flow(consumer.consumer_id, ENTRY_COUNT as u32);
            dispatcher.add_consumer(consumer).unwrap();
        }

        let entries: Vec<_> = (0..ENTRY_COUNT)
            .map(|entry_id| {
                let key = format!("sticky-key-{}", entry_id % 128);
                NonPersistentEntry::create(
                    1,
                    entry_id as u64,
                    -1,
                    Bytes::from(metadata_with_partition_key(&key)),
                    Bytes::from(format!("sticky-{entry_id}")),
                )
            })
            .collect();

        let start = Instant::now();
        dispatcher.send_messages(entries).await.unwrap();
        let elapsed = start.elapsed();

        println!(
            "PERF baseline key-shared sticky: consumers={CONSUMER_COUNT}, entries={ENTRY_COUNT}, elapsed_ms={}",
            elapsed.as_millis()
        );
        assert_eq!(dispatcher.dropped_messages(), 0);
    }

    #[tokio::test]
    #[ignore]
    async fn perf_copy_path_key_shared_auto_split_32_consumers_10k_entries_4k_payload() {
        const CONSUMER_COUNT: usize = 32;
        const ENTRY_COUNT: usize = 10_000;
        const KEY_COUNT: usize = 128;
        const METADATA_PADDING: usize = 256;
        const PAYLOAD_SIZE: usize = 4096;

        let mut dispatcher = NonPersistentStickyKeyDispatcher::new(Some(KeySharedPolicy {
            mode: KeySharedMode::AutoSplit,
            ranges: Vec::new(),
            allow_out_of_order_delivery: false,
        }));
        let mut _receivers = Vec::with_capacity(CONSUMER_COUNT);

        for consumer_id in 0..CONSUMER_COUNT as u64 {
            let (consumer, rx) = create_consumer_with_rx(
                consumer_id,
                Some(KeySharedPolicy {
                    mode: KeySharedMode::AutoSplit,
                    ranges: Vec::new(),
                    allow_out_of_order_delivery: false,
                }),
            );
            _receivers.push(rx);
            consumer.add_permits(ENTRY_COUNT as u32).await;
            dispatcher.consumer_flow(consumer.consumer_id, ENTRY_COUNT as u32);
            dispatcher.add_consumer(consumer).unwrap();
        }

        let metadata_templates: Vec<_> = (0..KEY_COUNT)
            .map(|key_id| {
                let key = format!("copy-path-key-{key_id}");
                Bytes::from(metadata_with_ordering_key_padding(&key, METADATA_PADDING))
            })
            .collect();
        let payload = Bytes::from(vec![b'p'; PAYLOAD_SIZE]);
        let entries: Vec<_> = (0..ENTRY_COUNT)
            .map(|entry_id| {
                NonPersistentEntry::create(
                    1,
                    entry_id as u64,
                    -1,
                    metadata_templates[entry_id % KEY_COUNT].clone(),
                    payload.clone(),
                )
            })
            .collect();

        let start = Instant::now();
        dispatcher.send_messages(entries).await.unwrap();
        let elapsed = start.elapsed();

        println!(
            "PERF copy-path key-shared auto-split: consumers={CONSUMER_COUNT}, entries={ENTRY_COUNT}, metadata_padding={METADATA_PADDING}, payload_bytes={PAYLOAD_SIZE}, elapsed_ms={}",
            elapsed.as_millis()
        );
        assert_eq!(dispatcher.dropped_messages(), 0);
    }
}
