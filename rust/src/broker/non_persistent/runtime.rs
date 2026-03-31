/*
 * Non-persistent runtime placeholders
 *
 * These placeholders allow the broker runtime to grow a dedicated
 * non-persistent path without changing the protocol/topic naming layer first.
 */

use crate::broker::non_persistent::NonPersistentDispatcherEnum;
use crate::broker::service::topic::{KeySharedPolicy, SubscriptionType};
use crate::broker::service::Consumer;
use crate::storage::NonPersistentEntry;
use std::collections::HashMap;
use std::collections::VecDeque;
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
        self.published_messages.drain(..).collect()
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
    dispatcher: Option<NonPersistentDispatcherEnum>,
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
            dispatcher: None,
        }
    }

    fn reuse_or_create_dispatcher(&mut self) {
        let requires_rebuild = self.dispatcher.as_ref().is_some_and(|dispatcher| {
            !dispatcher.is_consumer_connected()
                && (dispatcher.get_type() != self.sub_type
                    || !dispatcher.has_same_key_shared_policy(self.key_shared_policy.as_ref()))
        });

        if requires_rebuild {
            self.dispatcher = None;
        }

        if self.dispatcher.is_none() {
            self.dispatcher = Some(NonPersistentDispatcherEnum::new(
                self.sub_type,
                self.topic_name.clone(),
                self.partition_index,
                self.key_shared_policy.clone(),
            ));
        }
    }

    pub fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String> {
        if self.is_fenced {
            return Err("Subscription is fenced".to_string());
        }
        self.reuse_or_create_dispatcher();
        let dispatcher = self
            .dispatcher
            .as_mut()
            .ok_or_else(|| "Failed to create non-persistent dispatcher".to_string())?;
        if dispatcher.is_consumer_connected()
            && (dispatcher.get_type() != self.sub_type
                || !dispatcher.has_same_key_shared_policy(consumer.key_shared_policy().as_ref()))
        {
            return Err("Consumer is incompatible with the current dispatcher".to_string());
        }
        dispatcher.add_consumer(consumer)
    }

    pub fn remove_consumer(&mut self, consumer_id: u64) -> Option<Arc<Consumer>> {
        let consumer = self
            .dispatcher
            .as_mut()
            .and_then(|dispatcher| dispatcher.remove_consumer(consumer_id));

        if self
            .dispatcher
            .as_ref()
            .is_some_and(|dispatcher| !dispatcher.is_consumer_connected())
        {
            self.dispatcher = None;
        }

        consumer
    }

    pub fn get_consumer(&self, consumer_id: u64) -> Option<Arc<Consumer>> {
        self.dispatcher.as_ref()?.get_consumer(consumer_id)
    }

    pub fn get_consumers(&self) -> Vec<Arc<Consumer>> {
        self.dispatcher
            .as_ref()
            .map(|dispatcher| dispatcher.get_consumers())
            .unwrap_or_default()
    }

    pub fn get_active_consumer(&self) -> Option<Arc<Consumer>> {
        self.dispatcher
            .as_ref()
            .and_then(|dispatcher| dispatcher.get_active_consumer())
    }

    pub fn has_consumers(&self) -> bool {
        self.dispatcher
            .as_ref()
            .map(|dispatcher| dispatcher.is_consumer_connected())
            .unwrap_or(false)
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

    pub fn fence(&mut self) {
        self.is_fenced = true;
    }

    pub fn resume_after_fence(&mut self) {
        self.is_fenced = false;
    }

    pub fn dropped_messages(&self) -> u64 {
        self.dispatcher
            .as_ref()
            .map(|dispatcher| dispatcher.dropped_messages())
            .unwrap_or(0)
    }

    pub fn consumer_flow(&self, consumer_id: u64, additional_permits: u32) {
        if let Some(dispatcher) = &self.dispatcher {
            dispatcher.consumer_flow(consumer_id, additional_permits);
        }
    }

    pub async fn send_messages(
        &self,
        entries: Vec<NonPersistentEntry>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(dispatcher) = &self.dispatcher {
            dispatcher.send_messages(entries).await?;
        } else {
            for entry in entries {
                entry.release();
            }
        }
        Ok(())
    }
}
