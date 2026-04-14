use crate::broker::service::topic::SubscriptionType;
use crate::broker::service::Consumer;
use crate::storage::{MessageId, NonPersistentEntry};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[derive(Debug, Default)]
pub struct NonPersistentDispatcherExclusive {
    consumer: Option<Arc<Consumer>>,
    dropped_messages: AtomicU64,
}

impl NonPersistentDispatcherExclusive {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_type(&self) -> SubscriptionType {
        SubscriptionType::Exclusive
    }

    pub fn is_consumer_connected(&self) -> bool {
        self.consumer.is_some()
    }

    pub fn get_consumers(&self) -> Vec<Arc<Consumer>> {
        self.consumer.iter().cloned().collect()
    }

    pub fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String> {
        if self.consumer.is_some() {
            return Err(
                "Non-persistent Exclusive subscription already has an active consumer".to_string(),
            );
        }
        self.consumer = Some(consumer);
        Ok(())
    }

    pub fn remove_consumer(&mut self, consumer_id: u64) -> Option<Arc<Consumer>> {
        if let Some(current) = &self.consumer {
            if current.consumer_id == consumer_id {
                return self.consumer.take();
            }
        }
        None
    }

    pub fn consumer_flow(&self, consumer_id: u64, additional_permits: u32) {
        log::debug!(
            "Non-persistent exclusive dispatcher received flow from consumer {}, permits={}",
            consumer_id,
            additional_permits
        );
    }

    pub fn dropped_messages(&self) -> u64 {
        self.dropped_messages.load(Ordering::Relaxed)
    }

    fn record_drop(&self, count: u64) {
        self.dropped_messages.fetch_add(count, Ordering::Relaxed);
    }

    pub async fn send_messages(
        &self,
        entries: Vec<NonPersistentEntry>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let Some(consumer) = &self.consumer else {
            for entry in entries {
                self.record_drop(1);
                entry.release();
            }
            return Ok(());
        };

        let dispatchable = consumer.get_available_permits().await as usize;
        if dispatchable == 0 {
            self.record_drop(entries.len() as u64);
            for entry in entries {
                entry.release();
            }
            return Ok(());
        }

        let mut batch_entries = entries;
        let overflow = batch_entries.split_off(dispatchable.min(batch_entries.len()));

        let mut batch_messages = Vec::with_capacity(batch_entries.len());
        for entry in &batch_entries {
            let permit_acquired = consumer.use_permit().await;
            debug_assert!(permit_acquired, "exclusive dispatch window exceeded permits");
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
        for entry in batch_entries.iter().take(sent) {
            consumer.record_message_dispatched(entry.len()).await;
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
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct NonPersistentDispatcherFailover {
    topic_name: String,
    partition_index: i32,
    consumers: Vec<Arc<Consumer>>,
    active_consumer_id: Option<u64>,
    dropped_messages: AtomicU64,
}

impl NonPersistentDispatcherFailover {
    pub fn new(topic_name: String, partition_index: i32) -> Self {
        Self {
            topic_name,
            partition_index,
            consumers: Vec::new(),
            active_consumer_id: None,
            dropped_messages: AtomicU64::new(0),
        }
    }

    pub fn get_type(&self) -> SubscriptionType {
        SubscriptionType::Failover
    }

    pub fn is_consumer_connected(&self) -> bool {
        !self.consumers.is_empty()
    }

    pub fn get_consumers(&self) -> Vec<Arc<Consumer>> {
        self.consumers.clone()
    }

    pub fn get_active_consumer(&self) -> Option<Arc<Consumer>> {
        let active_consumer_id = self.active_consumer_id?;
        self.consumers
            .iter()
            .find(|consumer| consumer.consumer_id == active_consumer_id)
            .cloned()
    }

    fn sort_consumers(&mut self) {
        self.consumers.sort_by(|left, right| {
            left.get_priority_level()
                .cmp(&right.get_priority_level())
                .then_with(|| left.get_consumer_name().cmp(right.get_consumer_name()))
        });
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

    fn select_active_consumer_id(&self) -> Option<u64> {
        if self.consumers.is_empty() {
            return None;
        }

        let highest_priority = self.consumers.first()?.get_priority_level();
        let highest_priority_consumers: Vec<_> = self
            .consumers
            .iter()
            .filter(|consumer| consumer.get_priority_level() == highest_priority)
            .cloned()
            .collect();
        if highest_priority_consumers.is_empty() {
            return None;
        }

        let selected_index = if self.partition_index >= 0 {
            (self.partition_index as usize) % highest_priority_consumers.len()
        } else {
            let mut hash_ring = Vec::with_capacity(highest_priority_consumers.len() * 100);
            for (consumer_index, consumer) in highest_priority_consumers.iter().enumerate() {
                for replica in 0..100 {
                    let key = format!("{}{}", consumer.get_consumer_name(), replica);
                    hash_ring.push((Self::murmur3_32(key.as_bytes(), 0), consumer_index));
                }
            }
            hash_ring.sort_by_key(|(hash, _)| *hash);
            let topic_hash = Self::murmur3_32(self.topic_name.as_bytes(), 0);
            let consumer_index = hash_ring
                .iter()
                .find(|(hash, _)| *hash >= topic_hash)
                .map(|(_, index)| *index)
                .unwrap_or_else(|| hash_ring.first().map(|(_, index)| *index).unwrap_or(0));
            consumer_index
        };

        highest_priority_consumers
            .get(selected_index)
            .map(|consumer| consumer.consumer_id)
    }

    fn notify_active_consumer_changed(&self, active_consumer_id: u64) {
        for consumer in &self.consumers {
            consumer.notify_active_consumer_change(active_consumer_id);
        }
    }

    fn pick_and_schedule_active_consumer(&mut self) -> bool {
        let selected_consumer_id = self.select_active_consumer_id();
        if selected_consumer_id == self.active_consumer_id {
            return false;
        }

        self.active_consumer_id = selected_consumer_id;
        if let Some(active_consumer_id) = selected_consumer_id {
            self.notify_active_consumer_changed(active_consumer_id);
        }
        true
    }

    pub fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String> {
        let added_consumer = consumer.clone();
        if self
            .consumers
            .iter()
            .any(|current| current.consumer_id == consumer.consumer_id)
        {
            return Err(format!(
                "Consumer {} already exists in non-persistent failover dispatcher",
                consumer.consumer_id
            ));
        }

        self.consumers.push(consumer);
        self.sort_consumers();
        if !self.pick_and_schedule_active_consumer() {
            if let Some(active_consumer_id) = self.active_consumer_id {
                added_consumer.notify_active_consumer_change(active_consumer_id);
            }
        }
        Ok(())
    }

    pub fn remove_consumer(&mut self, consumer_id: u64) -> Option<Arc<Consumer>> {
        let removed = self
            .consumers
            .iter()
            .position(|consumer| consumer.consumer_id == consumer_id)
            .map(|index| self.consumers.remove(index));

        if removed.is_some() {
            self.sort_consumers();
            if self.consumers.is_empty() {
                self.active_consumer_id = None;
            } else {
                self.pick_and_schedule_active_consumer();
            }
        }

        removed
    }

    pub fn consumer_flow(&self, consumer_id: u64, additional_permits: u32) {
        log::debug!(
            "Non-persistent failover dispatcher received flow from consumer {}, permits={}",
            consumer_id,
            additional_permits
        );
    }

    pub fn dropped_messages(&self) -> u64 {
        self.dropped_messages.load(Ordering::Relaxed)
    }

    fn record_drop(&self, count: u64) {
        self.dropped_messages.fetch_add(count, Ordering::Relaxed);
    }

    pub async fn send_messages(
        &self,
        entries: Vec<NonPersistentEntry>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let Some(active_consumer) = self.get_active_consumer() else {
            for entry in entries {
                self.record_drop(1);
                entry.release();
            }
            return Ok(());
        };

        let dispatchable = active_consumer.get_available_permits().await as usize;
        if dispatchable == 0 {
            self.record_drop(entries.len() as u64);
            for entry in entries {
                entry.release();
            }
            return Ok(());
        }

        let mut batch_entries = entries;
        let overflow = batch_entries.split_off(dispatchable.min(batch_entries.len()));

        let mut batch_messages = Vec::with_capacity(batch_entries.len());
        for entry in &batch_entries {
            let permit_acquired = active_consumer.use_permit().await;
            debug_assert!(permit_acquired, "failover dispatch window exceeded permits");
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
        let sent = active_consumer.send_messages_batch(batch_messages).await;
        for entry in batch_entries.iter().take(sent) {
            active_consumer.record_message_dispatched(entry.len()).await;
        }
        if sent < attempted {
            active_consumer.add_permits((attempted - sent) as u32).await;
        }
        if sent < attempted {
            self.record_drop((attempted - sent) as u64);
        }
        self.record_drop(overflow.len() as u64);

        for entry in batch_entries.into_iter().chain(overflow.into_iter()) {
            entry.release();
        }
        Ok(())
    }
}
