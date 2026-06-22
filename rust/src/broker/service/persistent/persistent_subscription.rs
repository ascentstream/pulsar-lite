use crate::broker::dispatcher::redelivery_controller::RedeliveryEntry;
use crate::broker::dispatcher::DispatcherEnum;
use crate::broker::service::topic::{KeySharedPolicy, SubscriptionType};
use crate::broker::service::{Consumer, SharedStorage};
use crate::storage::{ManagedLedgerPosition, MessageId};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct PersistentSubscriptionRuntime {
    topic: String,
    name: String,
    sub_type: SubscriptionType,
    key_shared_policy: Option<KeySharedPolicy>,
    storage: SharedStorage,
    dispatcher: Option<DispatcherEnum>,
    pending_first_unacked: Option<Option<ManagedLedgerPosition>>,
}

impl PersistentSubscriptionRuntime {
    pub(crate) fn new(
        topic: String,
        name: String,
        sub_type: SubscriptionType,
        key_shared_policy: Option<KeySharedPolicy>,
        storage: SharedStorage,
    ) -> Self {
        Self {
            topic,
            name,
            sub_type,
            key_shared_policy,
            storage,
            dispatcher: None,
            pending_first_unacked: None,
        }
    }

    pub(crate) fn set_pending_first_unacked(&mut self, pos: Option<ManagedLedgerPosition>) {
        self.pending_first_unacked = Some(pos);
    }

    fn reuse_or_create_dispatcher(&mut self) {
        let needs_recreate = self.dispatcher.as_ref().is_some_and(|dispatcher| {
            !dispatcher.is_consumer_connected() && dispatcher.get_type() != self.sub_type
        });

        if needs_recreate {
            self.dispatcher = None;
        }

        if self.dispatcher.is_none() {
            log::info!(
                "Creating '{:?}' dispatcher for subscription '{}' on topic '{}'",
                self.sub_type,
                self.name,
                self.topic,
            );
            self.dispatcher = Some(DispatcherEnum::new_with_key_shared_policy(
                self.sub_type,
                self.key_shared_policy.clone(),
            ));
        }
    }

    pub(crate) fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String> {
        self.reuse_or_create_dispatcher();

        if let Some(pos) = self.pending_first_unacked.take() {
            if let Some(dispatcher) = self.dispatcher.as_ref() {
                dispatcher.init_read_position(pos);
            }
        }

        self.dispatcher
            .as_mut()
            .ok_or_else(|| "Failed to create dispatcher".to_string())?
            .add_consumer(consumer)
    }

    pub(crate) fn remove_consumer(&mut self, consumer_id: u64) -> Option<Arc<Consumer>> {
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

    pub(crate) async fn remove_consumer_with_recovery(
        &mut self,
        consumer_id: u64,
    ) -> Option<Arc<Consumer>> {
        let consumer = if let Some(dispatcher) = self.dispatcher.as_mut() {
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
                .dispatch_messages(self.storage.clone(), self.topic.clone(), self.name.clone())
                .await
            {
                log::error!(
                    "Failed to dispatch replay message for subscribe '{}':'{}'",
                    self.name,
                    e
                );
            }
            if !dispatcher.is_consumer_connected()
                && !matches!(
                    dispatcher.get_type(),
                    SubscriptionType::Shared | SubscriptionType::KeyShared
                )
            {
                self.dispatcher = None;
            }
        }
        consumer
    }

    pub(crate) fn get_consumer(&self, consumer_id: u64) -> Option<Arc<Consumer>> {
        self.dispatcher.as_ref()?.get_consumer(consumer_id)
    }

    pub(crate) fn get_consumers(&self) -> Vec<Arc<Consumer>> {
        self.dispatcher
            .as_ref()
            .map(|dispatcher| dispatcher.get_consumers())
            .unwrap_or_default()
    }

    pub(crate) fn get_active_consumer(&self) -> Option<Arc<Consumer>> {
        self.dispatcher
            .as_ref()
            .and_then(|dispatcher| dispatcher.get_active_consumer())
    }

    pub(crate) fn has_consumers(&self) -> bool {
        self.dispatcher
            .as_ref()
            .is_some_and(|dispatcher| dispatcher.is_consumer_connected())
    }

    pub(crate) fn init_read_position(&self, first_unacked: Option<ManagedLedgerPosition>) {
        if let Some(dispatcher) = self.dispatcher.as_ref() {
            dispatcher.init_read_position(first_unacked);
        }
    }

    pub(crate) fn on_message_acknowledged(&mut self, message_id: &MessageId) {
        if let Some(dispatcher) = self.dispatcher.as_mut() {
            dispatcher.on_message_acknowledged(message_id);
        }
    }

    pub(crate) async fn on_ack_state_updated(&mut self) -> Result<(), String> {
        if let Some(dispatcher) = self.dispatcher.as_mut() {
            dispatcher
                .on_ack_state_updated(self.storage.clone(), &self.topic, &self.name)
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    pub(crate) async fn redeliver_messages(&mut self, dispatchable: Vec<RedeliveryEntry>) {
        if let Some(dispatcher) = self.dispatcher.as_mut() {
            let queued = dispatchable.len();
            dispatcher.redeliver_messages(dispatchable);
            log::info!(
                "Queued {} messages for redelivery on subscription '{}'",
                queued,
                self.name
            );

            if let Err(e) = dispatcher
                .dispatch_messages(self.storage.clone(), self.topic.clone(), self.name.clone())
                .await
            {
                log::error!(
                    "Failed to dispatch redelivered messages for subscription '{}': {}",
                    self.name,
                    e
                );
            }
        }
    }

    pub(crate) async fn dispatch_messages(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(dispatcher) = self.dispatcher.as_ref() {
            dispatcher
                .dispatch_messages(self.storage.clone(), self.topic.clone(), self.name.clone())
                .await
        } else {
            log::warn!("No dispatcher found for subscription '{}'", self.name);
            Ok(())
        }
    }

    pub(crate) async fn consumer_flow(&self, consumer_id: u64, additional_permits: u32) {
        if let Some(dispatcher) = self.dispatcher.as_ref() {
            log::debug!(
                "Subscription '{}' received flow from consumer {}, permits={}",
                self.name,
                consumer_id,
                additional_permits
            );
            dispatcher.consumer_flow(consumer_id, additional_permits);

            if let Err(e) = dispatcher
                .dispatch_messages(self.storage.clone(), self.topic.clone(), self.name.clone())
                .await
            {
                log::error!(
                    "Failed to dispatch messages for subscription '{}': {}",
                    self.name,
                    e
                );
            }
        } else {
            log::warn!("No dispatcher found for subscription '{}'", self.name);
        }
    }
}
