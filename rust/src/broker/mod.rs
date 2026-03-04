/*
 * Broker Module
 * Core broker functionality organized into submodules
 */

pub mod service;
pub mod handler;
pub mod dispatcher;
pub mod stats;
pub mod broker_service;

// Re-export main types and functions for convenience
pub use service::SharedStorage;
pub use service::topic::{Topic, TopicStats, Subscription, SubscriptionStats, SubscriptionType};
pub use service::{handle_connection, ConnectionState};
pub use stats::{BrokerMetrics, SharedMetrics};
pub use broker_service::{BrokerService, SharedTopic, SharedBrokerService};
