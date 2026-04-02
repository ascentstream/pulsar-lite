/*
 * Dispatcher Trait
 * Defines the interface for message dispatchers
 * Each subscription type (Exclusive, Shared, Failover) implements this trait
 */

use crate::broker::service::topic::SubscriptionType;
use crate::broker::service::{Consumer, SharedStorage};
use std::future::Future;
use std::sync::Arc;

/// Dispatcher trait - interface for message dispatchers
pub trait Dispatcher: Send + Sync {
    /// Get the subscription type for this dispatcher
    fn get_type(&self) -> SubscriptionType;

    /// Check if there's at least one consumer connected
    fn is_consumer_connected(&self) -> bool;

    /// Get all consumers managed by this dispatcher
    fn get_consumers(&self) -> Vec<Arc<Consumer>>;

    /// Add a consumer to this dispatcher
    fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String>;

    /// Remove a consumer from this dispatcher
    fn remove_consumer(&mut self, consumer_id: u64) -> Option<Arc<Consumer>>;

    // ==================== Flow Control ====================

    /// Handle flow command - update permits (Push mode)
    ///
    /// This is called when a consumer sends Flow command.
    /// It updates the available permits for the consumer.
    fn consumer_flow(&self, consumer_id: u64, additional_permits: u32);

    // ==================== Message Dispatch (Push mode) ====================

    /// Dispatch messages to consumers (Push mode - Apache Pulsar style)
    ///
    /// Called when:
    /// 1. Consumer sends Flow command (permits increased)
    /// 2. Producer sends new message (message available)
    ///
    /// This method should:
    /// 1. Check if permits are available
    /// 2. Use appropriate algorithm (Round-Robin for Shared, etc.)
    /// 3. Dispatch messages up to batch size
    /// 4. Send messages via BrokerService consumer senders
    fn dispatch_messages(
        &self,
        storage: SharedStorage,
        topic: String,
        subscription: String,
    ) -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send;
}
