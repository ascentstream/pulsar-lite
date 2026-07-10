/*
 * Subscription Management
 * Manages consumers for a specific subscription on a topic
 * Inspired by Apache Pulsar's PersistentSubscription
 */

use std::collections::HashMap;
use std::sync::Arc;

/// Forward declaration for Consumer type
use super::super::{Consumer, SharedStorage};
use crate::broker::dispatcher::redelivery_controller::RedeliveryEntry;
use crate::broker::non_persistent::NonPersistentSubscriptionRuntime;
use crate::broker::service::persistent::PersistentSubscriptionRuntime;
use crate::storage::{ManagedLedgerPosition, MessageId, NonPersistentEntry, StorageSeekExt};

/// Subscription type (matches Pulsar protocol)
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum SubscriptionType {
    #[default]
    Exclusive = 0,
    Shared = 1,
    Failover = 2,
    KeyShared = 3,
}

/// Mirrors `CommandAck.AckType` in PulsarApi.proto (Individual = 0, Cumulative = 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckCommandType {
    Individual,
    Cumulative,
}

impl AckCommandType {
    pub fn from_proto(value: i32) -> Self {
        if value == 1 {
            Self::Cumulative
        } else {
            Self::Individual
        }
    }
}

/// Internal runtime mode for a subscription.
///
/// This is a transitional split point that lets the broker keep the current
/// protocol/topic entry path unchanged while the runtime gradually diverges
/// into persistent and non-persistent implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SubscriptionRuntimeMode {
    #[default]
    Persistent,
    NonPersistent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeySharedMode {
    AutoSplit,
    Sticky,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeySharedHashRange {
    pub start: i32,
    pub end: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeySharedPolicy {
    pub mode: KeySharedMode,
    pub ranges: Vec<KeySharedHashRange>,
    pub allow_out_of_order_delivery: bool,
}

/// Subscription represents a named subscription on a topic
/// It manages the dispatcher which handles consumers (Apache Pulsar style)
pub struct Subscription {
    /// Subscription name
    pub name: String,
    /// Topic this subscription belongs to
    pub topic: String,
    /// Subscription type (Exclusive, Shared, Failover)
    pub sub_type: SubscriptionType,
    /// Internal runtime mode for this subscription.
    runtime_mode: SubscriptionRuntimeMode,
    properties: HashMap<String, String>,
    key_shared_policy: Option<KeySharedPolicy>,
    /// Dedicated non-persistent runtime path.
    non_persistent_runtime: Option<NonPersistentSubscriptionRuntime>,
    /// Persistent runtime path.
    persistent_runtime: Option<PersistentSubscriptionRuntime>,
    /// Storage backend for reading messages
    storage: SharedStorage,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("name", &self.name)
            .field("topic", &self.topic)
            .field("sub_type", &self.sub_type)
            .field("runtime_mode", &self.runtime_mode)
            .field("properties", &self.properties)
            .field("key_shared_policy", &self.key_shared_policy)
            .field(
                "non_persistent_runtime",
                &self.non_persistent_runtime.as_ref().map(|_| "initialized"),
            )
            .field(
                "persistent_runtime",
                &self.persistent_runtime.as_ref().map(|_| "initialized"),
            )
            .finish()
    }
}

impl Subscription {
    /// Create a new subscription
    pub fn new(
        name: String,
        topic: String,
        sub_type: SubscriptionType,
        storage: SharedStorage,
    ) -> Self {
        Self::new_with_options(
            name,
            topic,
            sub_type,
            SubscriptionRuntimeMode::Persistent,
            HashMap::new(),
            None,
            storage,
        )
    }

    /// Create a new subscription with an explicit runtime mode.
    pub fn new_with_runtime_mode(
        name: String,
        topic: String,
        sub_type: SubscriptionType,
        runtime_mode: SubscriptionRuntimeMode,
        storage: SharedStorage,
    ) -> Self {
        Self::new_with_options(
            name,
            topic,
            sub_type,
            runtime_mode,
            HashMap::new(),
            None,
            storage,
        )
    }

    pub fn new_with_options(
        name: String,
        topic: String,
        sub_type: SubscriptionType,
        runtime_mode: SubscriptionRuntimeMode,
        properties: HashMap<String, String>,
        key_shared_policy: Option<KeySharedPolicy>,
        storage: SharedStorage,
    ) -> Self {
        let persistent_runtime = if runtime_mode == SubscriptionRuntimeMode::Persistent {
            Some(PersistentSubscriptionRuntime::new(
                topic.clone(),
                name.clone(),
                sub_type,
                key_shared_policy.clone(),
                storage.clone(),
            ))
        } else {
            None
        };

        Self {
            name,
            topic,
            sub_type,
            runtime_mode,
            properties,
            key_shared_policy,
            non_persistent_runtime: None,
            persistent_runtime,
            storage,
        }
    }

    /// Get subscription type
    pub fn get_sub_type(&self) -> SubscriptionType {
        self.sub_type
    }

    /// Get the internal runtime mode.
    pub fn runtime_mode(&self) -> SubscriptionRuntimeMode {
        self.runtime_mode
    }

    /// Check whether this subscription still uses the current persistent runtime.
    pub fn is_persistent(&self) -> bool {
        self.runtime_mode == SubscriptionRuntimeMode::Persistent
    }

    /// Check whether this subscription uses the non-persistent runtime.
    pub fn is_non_persistent(&self) -> bool {
        self.runtime_mode == SubscriptionRuntimeMode::NonPersistent
    }

    /// Update runtime mode. This is a small transitional hook until topic runtime
    /// selection is split into separate runtime implementations.
    pub fn set_runtime_mode(&mut self, mode: SubscriptionRuntimeMode) {
        self.runtime_mode = mode;
    }

    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    pub fn key_shared_policy(&self) -> Option<&KeySharedPolicy> {
        self.key_shared_policy.as_ref()
    }

    pub fn set_pending_first_unacked(&mut self, pos: Option<ManagedLedgerPosition>) {
        if let Some(runtime) = self.persistent_runtime.as_mut() {
            runtime.set_pending_first_unacked(pos);
        }
    }

    pub fn is_fenced(&self) -> bool {
        match self.runtime_mode {
            SubscriptionRuntimeMode::Persistent => false,
            SubscriptionRuntimeMode::NonPersistent => self
                .non_persistent_runtime
                .as_ref()
                .map(|runtime| runtime.is_fenced())
                .unwrap_or(false),
        }
    }

    pub fn fence(&mut self) {
        if self.runtime_mode == SubscriptionRuntimeMode::NonPersistent {
            self.reuse_or_create_non_persistent_runtime();
            if let Some(runtime) = self.non_persistent_runtime.as_mut() {
                runtime.fence();
            }
        }
    }

    pub fn close(&mut self) {
        self.fence();
    }

    pub fn delete(&mut self) {
        self.fence();
    }

    fn reuse_or_create_non_persistent_runtime(&mut self) {
        if self.non_persistent_runtime.is_none() {
            log::info!(
                "Creating non-persistent runtime for subscription '{}' on topic '{}' (subType={:?})",
                self.name,
                self.topic,
                self.sub_type
            );
            let partition_index = self
                .topic
                .rsplit_once("-partition-")
                .and_then(|(_, suffix)| suffix.parse::<i32>().ok())
                .unwrap_or(-1);
            self.non_persistent_runtime = Some(NonPersistentSubscriptionRuntime::new(
                self.topic.clone(),
                partition_index,
                self.sub_type,
                self.properties.clone(),
                self.key_shared_policy.clone(),
            ));
        }
    }

    /// Add a consumer to this subscription.
    pub fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String> {
        match self.runtime_mode {
            SubscriptionRuntimeMode::Persistent => self
                .persistent_runtime
                .as_mut()
                .ok_or_else(|| "persistent runtime is not initialized".to_string())?
                .add_consumer(consumer),
            SubscriptionRuntimeMode::NonPersistent => {
                self.reuse_or_create_non_persistent_runtime();
                self.non_persistent_runtime
                    .as_mut()
                    .ok_or_else(|| "Failed to create non-persistent runtime".to_string())?
                    .add_consumer(consumer)
            }
        }
    }

    /// Remove a consumer from this subscription
    pub fn remove_consumer(&mut self, consumer_id: u64) -> Option<Arc<Consumer>> {
        match self.runtime_mode {
            SubscriptionRuntimeMode::Persistent => self
                .persistent_runtime
                .as_mut()
                .and_then(|runtime| runtime.remove_consumer(consumer_id)),
            SubscriptionRuntimeMode::NonPersistent => {
                let consumer = self
                    .non_persistent_runtime
                    .as_mut()
                    .and_then(|runtime| runtime.remove_consumer(consumer_id));

                if self
                    .non_persistent_runtime
                    .as_ref()
                    .is_some_and(|runtime| !runtime.has_consumers())
                {
                    self.non_persistent_runtime = None;
                }

                consumer
            }
        }
    }

    pub async fn remove_consumer_with_recovery(
        &mut self,
        consumer_id: u64,
    ) -> Option<Arc<Consumer>> {
        match self.runtime_mode {
            SubscriptionRuntimeMode::Persistent => {
                if let Some(runtime) = self.persistent_runtime.as_mut() {
                    runtime.remove_consumer_with_recovery(consumer_id).await
                } else {
                    None
                }
            }
            SubscriptionRuntimeMode::NonPersistent => self.remove_consumer(consumer_id),
        }
    }

    pub async fn unsubscribe_consumer(&mut self, consumer_id: u64) -> Result<(), String> {
        self.remove_consumer_with_recovery(consumer_id).await;

        if self.is_persistent() {
            let mut guard = self.storage.lock().await;
            guard
                .delete_cursor(&self.topic, &self.name)
                .map_err(|e| e.to_string())?;
        }

        Ok(())
    }

    pub async fn seek_to_message_id(&mut self, message_id: &MessageId) -> Result<(), String> {
        if !self.is_persistent() {
            return Err("seek is not supported for non-persistent subscriptions".to_string());
        }

        let shared_cursor = matches!(
            self.sub_type,
            SubscriptionType::Shared | SubscriptionType::KeyShared
        );
        let first_unacked = {
            let mut guard = self.storage.lock().await;
            guard
                .seek_cursor(&self.topic, &self.name, message_id, shared_cursor)
                .await
                .map_err(|e| e.to_string())?;
            guard
                .first_unacked_position(&self.topic, &self.name)
                .map_err(|e| e.to_string())?
        };

        if let Some(runtime) = self.persistent_runtime.as_ref() {
            runtime.reset_after_seek(first_unacked);

            for consumer in runtime.get_consumers() {
                let drained = consumer.drain_pending_acks().await;
                if !drained.is_empty() {
                    log::debug!(
                        "Seek cleared {} pending acks for consumer {} on subscription '{}'",
                        drained.len(),
                        consumer.consumer_id,
                        self.name
                    );
                }
            }
        }

        Ok(())
    }

    pub async fn seek_by_publish_time(&mut self, publish_time: u64) -> Result<(), String> {
        if !self.is_persistent() {
            return Err("seek is not supported for non-persistent subscriptions".to_string());
        }
        let message_id = {
            let guard = self.storage.lock().await;
            guard
                .find_message_id_by_publish_time(&self.topic, publish_time)
                .map_err(|e| e.to_string())?
        };
        match message_id {
            Some(id) => self.seek_to_message_id(&id).await,
            None => Err(format!(
                "no message at or after publish_time {} on topic '{}'",
                publish_time, self.topic
            )),
        }
    }

    pub async fn get_last_message_id(&self) -> Result<Option<MessageId>, String> {
        if !self.is_persistent() {
            return Err(
                "getLastMessageId is unsupported for non-persistent subscriptions".to_string(),
            );
        }

        let guard = self.storage.lock().await;
        guard
            .get_last_position(&self.topic)
            .map(|position| position.map(MessageId::from))
            .map_err(|e| e.to_string())
    }

    /// Persist cursor updates and notify the dispatcher after a message was acked.
    ///
    /// The caller (`Consumer::message_acked`) must handle Shared ownership resolution
    /// and clear `pending_acks` before invoking this method.
    pub async fn acknowledge_message(
        &mut self,
        message_ids: &[MessageId],
        ack_type: AckCommandType,
    ) -> Result<(), String> {
        if !self.is_persistent() || message_ids.is_empty() {
            return Ok(());
        }

        match (self.sub_type, ack_type) {
            (
                SubscriptionType::Shared | SubscriptionType::KeyShared,
                AckCommandType::Cumulative,
            ) => {
                log::warn!(
                    "Ignoring cumulative ack on Shared subscription '{}' for topic '{}'",
                    self.name,
                    self.topic
                );
                Ok(())
            }
            (
                SubscriptionType::Shared | SubscriptionType::KeyShared,
                AckCommandType::Individual,
            ) => {
                {
                    let mut guard = self.storage.lock().await;
                    for message_id in message_ids {
                        guard
                            .ack_message_shared(&self.topic, &self.name, message_id.clone())
                            .map_err(|e| e.to_string())?;
                    }
                }
                for message_id in message_ids {
                    self.notify_dispatcher_message_acked(message_id);
                }
                self.notify_dispatcher_ack_state_updated().await?;
                Ok(())
            }
            (
                SubscriptionType::Exclusive | SubscriptionType::Failover,
                AckCommandType::Individual,
            ) => {
                {
                    let mut guard = self.storage.lock().await;
                    for message_id in message_ids {
                        guard
                            .ack_message_shared(&self.topic, &self.name, message_id.clone())
                            .map_err(|e| e.to_string())?;
                    }
                }
                for message_id in message_ids {
                    self.notify_dispatcher_message_acked(message_id);
                }
                Ok(())
            }
            (
                SubscriptionType::Exclusive | SubscriptionType::Failover,
                AckCommandType::Cumulative,
            ) => {
                let message_id = message_ids
                    .last()
                    .cloned()
                    .ok_or_else(|| "cumulative ack requires a message id".to_string())?;
                {
                    let mut guard = self.storage.lock().await;
                    guard
                        .ack_message(&self.topic, &self.name, message_id.clone())
                        .map_err(|e| e.to_string())?;
                }
                self.notify_dispatcher_message_acked(&message_id);
                Ok(())
            }
        }
    }

    fn notify_dispatcher_message_acked(&mut self, message_id: &MessageId) {
        if let Some(runtime) = self.persistent_runtime.as_mut() {
            runtime.on_message_acknowledged(message_id);
        }
    }

    async fn notify_dispatcher_ack_state_updated(&mut self) -> Result<(), String> {
        if let Some(runtime) = self.persistent_runtime.as_mut() {
            runtime.on_ack_state_updated().await
        } else {
            Ok(())
        }
    }

    pub async fn redeliver_unacknowledged_messages(
        &mut self,
        consumer_id: u64,
        message_ids: Vec<MessageId>,
    ) -> Result<(), String> {
        if !self.is_persistent() {
            log::warn!(
                "Ignoring redelivery command for non-persistent subscription '{}'",
                self.name
            );
            return Ok(());
        }

        if !matches!(
            self.sub_type,
            SubscriptionType::Shared | SubscriptionType::KeyShared
        ) {
            log::warn!(
                "Ignoring redelivery command for non-shared subscription '{}'",
                self.name
            );
            return Ok(());
        }

        let Some(consumer) = self.get_consumer(consumer_id) else {
            return Err(format!("Unknown consumer ID: {}", consumer_id));
        };

        let mut redeliver = Vec::new();
        if message_ids.is_empty() {
            redeliver.extend(consumer.drain_pending_acks().await.into_iter().map(
                |(message_id, pending_ack)| RedeliveryEntry {
                    message_id,
                    redelivery_count: pending_ack.redelivery_count + 1,
                    sticky_key_hash: pending_ack.sticky_key_hash,
                },
            ));
        } else {
            for message_id in message_ids {
                if let Some(pending_ack) =
                    consumer.take_pending_ack_for_redelivery(&message_id).await
                {
                    redeliver.push(RedeliveryEntry {
                        message_id,
                        redelivery_count: pending_ack.redelivery_count + 1,
                        sticky_key_hash: pending_ack.sticky_key_hash,
                    });
                } else {
                    log::debug!(
                        "Consumer {} requested redelivery for non-pending message {}:{}",
                        consumer_id,
                        message_id.ledger,
                        message_id.entry
                    );
                }
            }
        }

        if redeliver.is_empty() {
            return Ok(());
        }

        let mut dispatchable = Vec::with_capacity(redeliver.len());
        {
            let guard = self.storage.lock().await;
            for entry in redeliver {
                let acknowledged = guard
                    .is_acknowledged(&self.topic, &self.name, &entry.message_id)
                    .map_err(|e| e.to_string())?;
                if !acknowledged {
                    dispatchable.push(entry);
                }
            }
        }

        if dispatchable.is_empty() {
            return Ok(());
        }

        if let Some(runtime) = self.persistent_runtime.as_mut() {
            runtime.redeliver_messages(dispatchable).await;
        }

        Ok(())
    }

    /// Get a consumer by ID
    pub fn get_consumer(&self, consumer_id: u64) -> Option<Arc<Consumer>> {
        match self.runtime_mode {
            SubscriptionRuntimeMode::Persistent => {
                self.persistent_runtime.as_ref()?.get_consumer(consumer_id)
            }

            SubscriptionRuntimeMode::NonPersistent => self
                .non_persistent_runtime
                .as_ref()?
                .get_consumer(consumer_id),
        }
    }

    /// Get all consumers
    pub fn get_consumers(&self) -> Vec<Arc<Consumer>> {
        match self.runtime_mode {
            SubscriptionRuntimeMode::Persistent => self
                .persistent_runtime
                .as_ref()
                .map(|runtime| runtime.get_consumers())
                .unwrap_or_default(),
            SubscriptionRuntimeMode::NonPersistent => self
                .non_persistent_runtime
                .as_ref()
                .map(|runtime| runtime.get_consumers())
                .unwrap_or_default(),
        }
    }

    /// Get active consumers (for Failover, only the primary consumer)
    pub fn get_active_consumers(&self) -> Vec<Arc<Consumer>> {
        match self.runtime_mode {
            SubscriptionRuntimeMode::Persistent => match self.sub_type {
                SubscriptionType::Failover => self
                    .persistent_runtime
                    .as_ref()
                    .and_then(|runtime| runtime.get_active_consumer())
                    .into_iter()
                    .collect(),
                _ => self.get_consumers(),
            },
            SubscriptionRuntimeMode::NonPersistent => match self.sub_type {
                SubscriptionType::Failover => self
                    .non_persistent_runtime
                    .as_ref()
                    .and_then(|runtime| runtime.get_active_consumer())
                    .into_iter()
                    .collect(),
                _ => self.get_consumers(),
            },
        }
    }

    /// Get consumer count
    pub fn get_consumer_count(&self) -> usize {
        self.get_consumers().len()
    }

    /// Check if subscription has any consumers
    pub fn has_consumers(&self) -> bool {
        match self.runtime_mode {
            SubscriptionRuntimeMode::Persistent => self
                .persistent_runtime
                .as_ref()
                .map(|runtime| runtime.has_consumers())
                .unwrap_or(false),
            SubscriptionRuntimeMode::NonPersistent => self
                .non_persistent_runtime
                .as_ref()
                .map(|runtime| runtime.has_consumers())
                .unwrap_or(false),
        }
    }

    /// Get total available permits across all consumers
    pub async fn get_total_permits(&self) -> u32 {
        let mut total = 0;
        for consumer in self.get_consumers() {
            total += consumer.get_available_permits().await;
        }
        total
    }

    /// Get subscription statistics
    pub async fn get_stats(&self) -> SubscriptionStats {
        let consumers = self.get_consumers();
        let consumer_count = consumers.len();
        let mut total_permits = 0;
        for consumer in consumers {
            total_permits += consumer.get_available_permits().await;
        }

        SubscriptionStats {
            name: self.name.clone(),
            topic: self.topic.clone(),
            sub_type: self.sub_type,
            consumer_count,
            total_permits,
            received_messages: match self.runtime_mode {
                SubscriptionRuntimeMode::Persistent => 0,
                SubscriptionRuntimeMode::NonPersistent => self
                    .non_persistent_runtime
                    .as_ref()
                    .map(|runtime| runtime.received_messages())
                    .unwrap_or(0),
            },
            dispatched_messages: match self.runtime_mode {
                SubscriptionRuntimeMode::Persistent => 0,
                SubscriptionRuntimeMode::NonPersistent => self
                    .non_persistent_runtime
                    .as_ref()
                    .map(|runtime| runtime.dispatched_messages())
                    .unwrap_or(0),
            },
            dropped_messages: match self.runtime_mode {
                SubscriptionRuntimeMode::Persistent => 0,
                SubscriptionRuntimeMode::NonPersistent => self
                    .non_persistent_runtime
                    .as_ref()
                    .map(|runtime| runtime.dropped_messages())
                    .unwrap_or(0),
            },
        }
    }

    // ==================== Message Dispatch (Push mode) ====================

    /// Dispatch messages to consumers (Push mode)
    ///
    /// This is called by Topic.dispatch_to_subscriptions() when a new message is published.
    pub async fn dispatch_messages(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        log::debug!(
            "Dispatching messages for subscription '{}', consumers={}, permits={}",
            self.name,
            self.get_consumer_count(),
            self.get_total_permits().await
        );

        match self.runtime_mode {
            SubscriptionRuntimeMode::Persistent => {
                if let Some(runtime) = self.persistent_runtime.as_ref() {
                    runtime.dispatch_messages().await?;
                }
                Ok(())
            }
            SubscriptionRuntimeMode::NonPersistent => Ok(()),
        }
    }

    pub async fn send_non_persistent_entries(
        &self,
        entries: Vec<NonPersistentEntry>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self.runtime_mode {
            SubscriptionRuntimeMode::Persistent => {
                for entry in entries {
                    entry.release();
                }
                Ok(())
            }
            SubscriptionRuntimeMode::NonPersistent => {
                if let Some(ref runtime) = self.non_persistent_runtime {
                    let result = runtime.send_messages(entries).await;
                    let recv = runtime.received_messages();
                    let dispatched = runtime.dispatched_messages();
                    let dropped = runtime.dropped_messages();
                    if recv > 0 && recv % 100_000 < 50 {
                        log::info!(
                            "[dispatch-metrics] sub='{}' received={} dispatched={} dropped={} drop_rate={:.1}%",
                            self.name, recv, dispatched, dropped,
                            if recv > 0 { dropped as f64 / recv as f64 * 100.0 } else { 0.0 }
                        );
                    }
                    result
                } else {
                    for entry in entries {
                        entry.release();
                    }
                    log::debug!(
                        "No non-persistent runtime found for subscription '{}'",
                        self.name
                    );
                    Ok(())
                }
            }
        }
    }

    pub fn record_non_persistent_drop(&self, count: u64) {
        if self.runtime_mode == SubscriptionRuntimeMode::NonPersistent {
            if let Some(runtime) = self.non_persistent_runtime.as_ref() {
                runtime.record_drop(count);
            }
        }
    }

    /// Handle consumer flow command (Apache Pulsar style)
    pub async fn consumer_flow(&self, consumer_id: u64, additional_permits: u32) {
        match self.runtime_mode {
            SubscriptionRuntimeMode::Persistent => {
                if let Some(runtime) = self.persistent_runtime.as_ref() {
                    runtime.consumer_flow(consumer_id, additional_permits).await;
                } else {
                    log::warn!(
                        "No persistent runtime available for subscription '{}'",
                        self.name
                    );
                }
            }
            SubscriptionRuntimeMode::NonPersistent => {
                if let Some(ref runtime) = self.non_persistent_runtime {
                    log::debug!(
                        "Non-persistent subscription '{}' received flow from consumer {}, permits={}",
                        self.name, consumer_id, additional_permits
                    );

                    runtime.consumer_flow(consumer_id, additional_permits);
                } else {
                    log::warn!(
                        "No non-persistent runtime available for subscription '{}'",
                        self.name
                    );
                }
            }
        }
    }

    pub async fn clear_backlog(&self) -> Result<(), String> {
        match self.runtime_mode {
            SubscriptionRuntimeMode::Persistent => {
                Err("clearBacklog is not implemented for persistent runtime".to_string())
            }
            SubscriptionRuntimeMode::NonPersistent => Ok(()),
        }
    }

    pub async fn skip_messages(&self, _count: u64) -> Result<(), String> {
        match self.runtime_mode {
            SubscriptionRuntimeMode::Persistent => {
                Err("skipMessages is not implemented for persistent runtime".to_string())
            }
            SubscriptionRuntimeMode::NonPersistent => Ok(()),
        }
    }

    pub async fn reset_cursor(&self) -> Result<(), String> {
        match self.runtime_mode {
            SubscriptionRuntimeMode::Persistent => {
                Err("resetCursor is not implemented for persistent runtime".to_string())
            }
            SubscriptionRuntimeMode::NonPersistent => Ok(()),
        }
    }

    pub async fn backlog_size(&self) -> Result<usize, String> {
        match self.runtime_mode {
            SubscriptionRuntimeMode::Persistent => {
                Err("backlog inspection is not implemented for persistent runtime".to_string())
            }
            SubscriptionRuntimeMode::NonPersistent => Ok(0),
        }
    }
}

/// Statistics for a subscription
#[derive(Debug, Clone)]
pub struct SubscriptionStats {
    pub name: String,
    pub topic: String,
    pub sub_type: SubscriptionType,
    pub consumer_count: usize,
    pub total_permits: u32,
    pub received_messages: u64,
    pub dispatched_messages: u64,
    pub dropped_messages: u64,
}
