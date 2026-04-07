/*
 * Non-persistent runtime foundation
 *
 * This layer deliberately keeps only topic/subscription runtime state.
 * Dispatcher-driven delivery is added in a later step so this PR can stay
 * focused on domain and runtime split without pulling protocol wiring along.
 */

use crate::broker::service::topic::{KeySharedPolicy, SubscriptionType};
use crate::broker::service::Consumer;
use crate::storage::NonPersistentEntry;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct NonPersistentTopicRuntime {
    published_messages: VecDeque<NonPersistentEntry>,
}

impl NonPersistentTopicRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn publish_entry(&mut self, entry: NonPersistentEntry) {
        self.published_messages.push_back(entry);
    }

    pub fn pending_message_count(&self) -> usize {
        self.published_messages.len()
    }

    pub fn drain_published_messages(&mut self) -> Vec<NonPersistentEntry> {
        if self.published_messages.is_empty() {
            return Vec::new();
        }

        std::mem::take(&mut self.published_messages)
            .into_iter()
            .collect()
    }
}

#[derive(Debug)]
pub struct NonPersistentSubscriptionRuntime {
    topic_name: String,
    partition_index: i32,
    sub_type: SubscriptionType,
    properties: HashMap<String, String>,
    key_shared_policy: Option<KeySharedPolicy>,
    is_fenced: bool,
    consumers: BTreeMap<u64, Arc<Consumer>>,
    active_consumer_id: Option<u64>,
}

impl NonPersistentSubscriptionRuntime {
    pub fn new(
        topic_name: String,
        partition_index: i32,
        sub_type: SubscriptionType,
        properties: HashMap<String, String>,
        key_shared_policy: Option<KeySharedPolicy>,
    ) -> Self {
        Self {
            topic_name,
            partition_index,
            sub_type,
            properties,
            key_shared_policy,
            is_fenced: false,
            consumers: BTreeMap::new(),
            active_consumer_id: None,
        }
    }

    fn refresh_active_consumer(&mut self) {
        self.active_consumer_id = match self.sub_type {
            SubscriptionType::Failover => self.consumers.first_key_value().map(|(id, _)| *id),
            _ => None,
        };
    }

    pub fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String> {
        if self.is_fenced {
            return Err("Subscription is fenced".to_string());
        }

        if matches!(self.sub_type, SubscriptionType::Exclusive) && !self.consumers.is_empty() {
            return Err("Exclusive subscription already has a consumer".to_string());
        }

        self.consumers.insert(consumer.consumer_id, consumer);
        self.refresh_active_consumer();
        Ok(())
    }

    pub fn remove_consumer(&mut self, consumer_id: u64) -> Option<Arc<Consumer>> {
        let removed = self.consumers.remove(&consumer_id);
        self.refresh_active_consumer();
        removed
    }

    pub fn get_consumer(&self, consumer_id: u64) -> Option<Arc<Consumer>> {
        self.consumers.get(&consumer_id).cloned()
    }

    pub fn get_consumers(&self) -> Vec<Arc<Consumer>> {
        self.consumers.values().cloned().collect()
    }

    pub fn get_active_consumer(&self) -> Option<Arc<Consumer>> {
        self.active_consumer_id
            .and_then(|consumer_id| self.get_consumer(consumer_id))
    }

    pub fn has_consumers(&self) -> bool {
        !self.consumers.is_empty()
    }

    pub fn is_fenced(&self) -> bool {
        self.is_fenced
    }

    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    pub fn key_shared_policy(&self) -> Option<&KeySharedPolicy> {
        self.key_shared_policy.as_ref()
    }

    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    pub fn fence(&mut self) {
        self.is_fenced = true;
    }

    pub fn resume_after_fence(&mut self) {
        self.is_fenced = false;
    }

    pub fn dropped_messages(&self) -> u64 {
        0
    }

    pub fn consumer_flow(&self, _consumer_id: u64, _additional_permits: u32) {
        // Delivery wiring is intentionally deferred. Foundation PR only keeps
        // runtime state and capability boundaries in place.
    }

    pub async fn send_messages(
        &self,
        entries: Vec<NonPersistentEntry>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for entry in entries {
            entry.release();
        }
        Ok(())
    }
}
