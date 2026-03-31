/*
 * Broker Module
 * Core broker functionality organized into submodules
 */

pub mod broker_service;
pub mod connection_limiter;
pub mod dispatcher;
pub mod handler;
pub mod non_persistent;
pub mod service;
pub mod stats;

// Re-export main types and functions for convenience
pub use broker_service::{BrokerService, SharedBrokerService, SharedTopic};
pub use connection_limiter::{ConnectionLimiter, ConnectionPermit};
pub use service::topic::{
    KeySharedHashRange, KeySharedMode, KeySharedPolicy, Subscription, SubscriptionRuntimeMode,
    SubscriptionStats, SubscriptionType, Topic, TopicRuntimeMode, TopicStats,
};
pub use service::SharedStorage;
pub use service::{handle_connection, ConnectionState};
pub use stats::{BrokerMetrics, SharedMetrics};
