/*
 * Subscription Management
 * Manages consumers for a specific subscription on a topic
 * Inspired by Apache Pulsar's PersistentSubscription
 */

use std::collections::HashMap;
use std::sync::Arc;

/// Forward declaration for Consumer type
use super::super::{Consumer, SharedStorage};
use crate::broker::dispatcher::DispatcherEnum;
use crate::broker::non_persistent::NonPersistentSubscriptionRuntime;
use crate::storage::NonPersistentEntry;

/// Subscription type (matches Pulsar protocol)
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SubscriptionType {
    Exclusive = 0,
    Shared = 1,
    Failover = 2,
    KeyShared = 3,
}

impl Default for SubscriptionType {
    fn default() -> Self {
        SubscriptionType::Exclusive
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
    /// Dispatcher for this subscription (created on first consumer)
    /// Apache Pulsar style - subscription holds dispatcher, not consumers directly
    dispatcher: Option<DispatcherEnum>,
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
                "dispatcher",
                &self.dispatcher.as_ref().map(|d| d.get_type()),
            )
            .field(
                "non_persistent_runtime",
                &self.non_persistent_runtime.as_ref().map(|_| "initialized"),
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
        Self {
            name,
            topic,
            sub_type,
            runtime_mode,
            properties,
            key_shared_policy,
            non_persistent_runtime: None,
            dispatcher: None,
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

    /// Create or reuse persistent dispatcher based on subscription type.
    fn reuse_or_create_dispatcher(&mut self) {
        let needs_recreate = self.dispatcher.as_ref().is_some_and(|dispatcher| {
            !dispatcher.is_consumer_connected() && dispatcher.get_type() != self.sub_type
        });
        if needs_recreate {
            self.dispatcher = None;
        }
        if self.dispatcher.is_none() {
            log::info!(
                "Creating {} dispatcher for subscription '{}' on topic '{}'",
                match self.sub_type {
                    SubscriptionType::Exclusive => "Exclusive",
                    SubscriptionType::Shared => "Shared",
                    SubscriptionType::Failover => "Failover",
                    SubscriptionType::KeyShared => "KeyShared (fallback to Shared)",
                },
                self.name,
                self.topic
            );
            self.dispatcher = Some(DispatcherEnum::new(self.sub_type));
        }
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
            SubscriptionRuntimeMode::Persistent => {
                self.reuse_or_create_dispatcher();
                self.dispatcher
                    .as_mut()
                    .ok_or_else(|| "Failed to create dispatcher".to_string())?
                    .add_consumer(consumer)
            }
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
            SubscriptionRuntimeMode::Persistent => {
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
                let consumer = if let Some(ref mut dispatcher) = self.dispatcher {
                    dispatcher
                        .remove_consumer_with_recovery(
                            consumer_id,
                            self.storage.clone(),
                            &self.topic,
                            &self.name,
                        )
                        .await
                } else {
                    None
                };

                if let Some(ref dispatcher) = self.dispatcher {
                    if let Err(e) = dispatcher
                        .dispatch_messages(
                            self.storage.clone(),
                            self.topic.clone(),
                            self.name.clone(),
                        )
                        .await
                    {
                        log::error!(
                            "Failed to dispatch replay messages for subscription '{}': {}",
                            self.name,
                            e
                        );
                    }

                    if !dispatcher.is_consumer_connected() {
                        self.dispatcher = None;
                    }
                }

                consumer
            }
            SubscriptionRuntimeMode::NonPersistent => self.remove_consumer(consumer_id),
        }
    }

    /// Get a consumer by ID
    pub fn get_consumer(&self, consumer_id: u64) -> Option<Arc<Consumer>> {
        match self.runtime_mode {
            SubscriptionRuntimeMode::Persistent => {
                self.dispatcher.as_ref()?.get_consumer(consumer_id)
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
                .dispatcher
                .as_ref()
                .map(|d| d.get_consumers())
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
                    .get_consumers()
                    .into_iter()
                    .next()
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
                .dispatcher
                .as_ref()
                .map(|d| d.is_consumer_connected())
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
                if let Some(ref dispatcher) = self.dispatcher {
                    dispatcher
                        .dispatch_messages(
                            self.storage.clone(),
                            self.topic.clone(),
                            self.name.clone(),
                        )
                        .await
                } else {
                    log::warn!("No dispatcher found for subscription '{}'", self.name);
                    Ok(())
                }
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
                if let Some(ref dispatcher) = self.dispatcher {
                    log::debug!(
                        "Subscription '{}' received flow from consumer {}, permits={}",
                        self.name,
                        consumer_id,
                        additional_permits
                    );

                    dispatcher.consumer_flow(consumer_id, additional_permits);

                    if let Err(e) = dispatcher
                        .dispatch_messages(
                            self.storage.clone(),
                            self.topic.clone(),
                            self.name.clone(),
                        )
                        .await
                    {
                        log::error!(
                            "Failed to dispatch messages for subscription '{}': {}",
                            self.name,
                            e
                        );
                    }
                } else {
                    log::warn!("No dispatcher available for subscription '{}'", self.name);
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
