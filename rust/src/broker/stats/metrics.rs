/*
 * Broker Metrics
 * Provides metrics collection and reporting for Pulsar Lite broker
 */

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Broker metrics container
#[derive(Debug)]
pub struct BrokerMetrics {
    // Connection metrics
    pub total_connections: AtomicU64,
    pub active_connections: AtomicU64,

    // Producer metrics
    pub total_producers: AtomicU64,
    pub messages_published: AtomicU64,
    pub bytes_published: AtomicU64,

    // Consumer metrics
    pub total_consumers: AtomicU64,
    pub messages_delivered: AtomicU64,
    pub bytes_delivered: AtomicU64,
    pub messages_acked: AtomicU64,

    // Topic metrics
    pub total_topics: AtomicU64,
    pub total_subscriptions: AtomicU64,

    // Error metrics
    pub errors: AtomicU64,

    // Performance metrics
    pub start_time: Instant,
}

impl Default for BrokerMetrics {
    fn default() -> Self {
        Self {
            total_connections: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            total_producers: AtomicU64::new(0),
            messages_published: AtomicU64::new(0),
            bytes_published: AtomicU64::new(0),
            total_consumers: AtomicU64::new(0),
            messages_delivered: AtomicU64::new(0),
            bytes_delivered: AtomicU64::new(0),
            messages_acked: AtomicU64::new(0),
            total_topics: AtomicU64::new(0),
            total_subscriptions: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }
}

impl BrokerMetrics {
    /// Create a new metrics instance
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment connection count
    pub fn inc_connections(&self) {
        self.total_connections.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement active connection count
    pub fn dec_active_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// Increment producer count
    pub fn inc_producers(&self) {
        self.total_producers.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement producer count
    pub fn dec_producers(&self) {
        self.total_producers.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a published message
    pub fn record_message_published(&self, size: usize) {
        self.messages_published.fetch_add(1, Ordering::Relaxed);
        self.bytes_published
            .fetch_add(size as u64, Ordering::Relaxed);
    }

    /// Increment consumer count
    pub fn inc_consumers(&self) {
        self.total_consumers.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement consumer count
    pub fn dec_consumers(&self) {
        self.total_consumers.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a delivered message
    pub fn record_message_delivered(&self, size: usize) {
        self.messages_delivered.fetch_add(1, Ordering::Relaxed);
        self.bytes_delivered
            .fetch_add(size as u64, Ordering::Relaxed);
    }

    /// Record an acknowledged message
    pub fn record_message_acked(&self) {
        self.messages_acked.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment topic count
    pub fn inc_topics(&self) {
        self.total_topics.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment subscription count
    pub fn inc_subscriptions(&self) {
        self.total_subscriptions.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an error
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Get uptime in seconds
    pub fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    /// Get messages published per second (since start)
    pub fn messages_published_rate(&self) -> f64 {
        let uptime = self.uptime_secs();
        if uptime == 0 {
            return 0.0;
        }
        let total = self.messages_published.load(Ordering::Relaxed);
        total as f64 / uptime as f64
    }

    /// Get messages delivered per second (since start)
    pub fn messages_delivered_rate(&self) -> f64 {
        let uptime = self.uptime_secs();
        if uptime == 0 {
            return 0.0;
        }
        let total = self.messages_delivered.load(Ordering::Relaxed);
        total as f64 / uptime as f64
    }

    /// Format metrics as a human-readable string
    pub fn to_string(&self) -> String {
        format!(
            "Broker Metrics:\n\
             ===============\n\
             Uptime: {}s\n\
             \n\
             Connections:\n\
             - Total: {}\n\
             - Active: {}\n\
             \n\
             Producers:\n\
             - Total: {}\n\
             - Messages Published: {} ({:.2}/s)\n\
             - Bytes Published: {} ({:.2} MB)\n\
             \n\
             Consumers:\n\
             - Total: {}\n\
             - Messages Delivered: {} ({:.2}/s)\n\
             - Bytes Delivered: {} ({:.2} MB)\n\
             - Messages Acked: {}\n\
             \n\
             Topics:\n\
             - Total: {}\n\
             - Subscriptions: {}\n\
             \n\
             Errors: {}",
            self.uptime_secs(),
            self.total_connections.load(Ordering::Relaxed),
            self.active_connections.load(Ordering::Relaxed),
            self.total_producers.load(Ordering::Relaxed),
            self.messages_published.load(Ordering::Relaxed),
            self.messages_published_rate(),
            self.bytes_published.load(Ordering::Relaxed),
            self.bytes_published.load(Ordering::Relaxed) as f64 / 1_048_576.0,
            self.total_consumers.load(Ordering::Relaxed),
            self.messages_delivered.load(Ordering::Relaxed),
            self.messages_delivered_rate(),
            self.bytes_delivered.load(Ordering::Relaxed),
            self.bytes_delivered.load(Ordering::Relaxed) as f64 / 1_048_576.0,
            self.messages_acked.load(Ordering::Relaxed),
            self.total_topics.load(Ordering::Relaxed),
            self.total_subscriptions.load(Ordering::Relaxed),
            self.errors.load(Ordering::Relaxed),
        )
    }
}

/// Shared metrics instance
pub type SharedMetrics = Arc<BrokerMetrics>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_basic() {
        let metrics = BrokerMetrics::new();

        metrics.inc_connections();
        metrics.inc_producers();
        metrics.record_message_published(100);
        metrics.inc_consumers();
        metrics.record_message_delivered(100);
        metrics.record_message_acked();

        assert_eq!(metrics.total_connections.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.active_connections.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.total_producers.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.messages_published.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.bytes_published.load(Ordering::Relaxed), 100);
        assert_eq!(metrics.total_consumers.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.messages_delivered.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.bytes_delivered.load(Ordering::Relaxed), 100);
        assert_eq!(metrics.messages_acked.load(Ordering::Relaxed), 1);

        // Test decrement
        metrics.dec_active_connections();
        assert_eq!(metrics.active_connections.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_metrics_rates() {
        let metrics = BrokerMetrics::new();

        // Initially zero
        assert_eq!(metrics.messages_published_rate(), 0.0);
        assert_eq!(metrics.messages_delivered_rate(), 0.0);

        // Record some messages
        for _ in 0..100 {
            metrics.record_message_published(50);
        }

        // Rate should be calculated based on uptime
        let rate = metrics.messages_published_rate();
        assert!(rate >= 0.0);
    }
}
