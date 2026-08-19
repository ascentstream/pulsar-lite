/*
 * Broker Prometheus Metric Families
 *
 * Families are created once at startup and registered into the shared
 * registry from `pulsar_lite_storage::metrics`. Hot paths only ever touch
 * pre-resolved handles (plain atomic adds); label resolution happens once
 * at entity creation, never per message.
 *
 * Naming contract: families that reproduce native Pulsar semantics keep
 * the exact `pulsar_*` names and label sets; extensions with no native
 * counterpart use the `pulsar_lite_*` prefix.
 */

use std::sync::{Arc, OnceLock};

use prometheus::{Gauge, GaugeVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts};
use crate::global_registry;

/// Topic-labeled families shared by every `TopicMetrics` handle.
#[derive(Debug, Clone)]
pub struct TopicFamilies {
    /// pulsar_in_messages_total{cluster, namespace, topic, partition}
    pub in_messages: IntCounterVec,
    /// pulsar_in_bytes_total{...}
    pub in_bytes: IntCounterVec,
    /// pulsar_publish_rate_limit_times{...}
    pub publish_rate_limit: IntCounterVec,
    /// pulsar_rate_in{...} (scrape-derived gauge)
    pub rate_in: GaugeVec,
    /// pulsar_throughput_in{...}
    pub throughput_in: GaugeVec,
    /// pulsar_average_msg_size{...}
    pub average_msg_size: IntGaugeVec,
    /// pulsar_storage_size{...}
    pub storage_size: IntGaugeVec,
    /// pulsar_subscriptions_count{...}
    pub subscriptions_count: IntGaugeVec,
    /// pulsar_producers_count{...}
    pub producers_count: IntGaugeVec,
    /// pulsar_consumers_count{...}
    pub consumers_count: IntGaugeVec,
}

/// Subscription-labeled families shared by every `SubscriptionMetrics`.
#[derive(Debug, Clone)]
pub struct SubscriptionFamilies {
    /// pulsar_out_messages_total{cluster, namespace, topic, partition, subscription}
    pub out_messages: IntCounterVec,
    /// pulsar_out_bytes_total{...}
    pub out_bytes: IntCounterVec,
    /// pulsar_lite_subscription_redelivered_total{...}
    pub redelivered: IntCounterVec,
    /// pulsar_lite_subscription_dropped_messages_total{...}
    pub dropped: IntCounterVec,
    /// pulsar_lite_subscription_acked_messages_total{...}
    pub acked: IntCounterVec,
    /// pulsar_subscription_msg_rate_out{...}
    pub msg_rate_out: GaugeVec,
    /// pulsar_subscription_msg_throughput_out{...}
    pub msg_throughput_out: GaugeVec,
    /// pulsar_subscription_msg_ack_rate{...}
    pub msg_ack_rate: GaugeVec,
    /// pulsar_subscription_msg_rate_redeliver{...}
    pub msg_rate_redeliver: GaugeVec,
    /// pulsar_subscription_msg_drop_rate{...}
    pub msg_drop_rate: GaugeVec,
    /// pulsar_subscription_back_log{...}
    pub back_log: IntGaugeVec,
    /// pulsar_subscription_unacked_messages{...}
    pub unacked_messages: IntGaugeVec,
    /// pulsar_subscription_blocked_on_unacked_messages{...}
    pub blocked_on_unacked: IntGaugeVec,
    /// pulsar_subscription_consumers_count{...}
    pub consumers_count: IntGaugeVec,
    /// pulsar_subscription_last_acked_timestamp{...} (unix seconds)
    pub last_acked_timestamp: GaugeVec,
    /// pulsar_subscription_last_consumed_timestamp{...} (unix seconds)
    pub last_consumed_timestamp: GaugeVec,
}

impl SubscriptionFamilies {
    fn new(registry: &prometheus::Registry) -> Result<Self, prometheus::Error> {
        let out_messages = IntCounterVec::new(
            Opts::new(
                "pulsar_out_messages_total",
                "Total messages delivered to consumers",
            ),
            SUBSCRIPTION_LABELS,
        )?;
        let out_bytes = IntCounterVec::new(
            Opts::new(
                "pulsar_out_bytes_total",
                "Total bytes delivered to consumers",
            ),
            SUBSCRIPTION_LABELS,
        )?;
        let redelivered = IntCounterVec::new(
            Opts::new(
                "pulsar_lite_subscription_redelivered_total",
                "Messages queued for redelivery",
            ),
            SUBSCRIPTION_LABELS,
        )?;
        let dropped = IntCounterVec::new(
            Opts::new(
                "pulsar_lite_subscription_dropped_messages_total",
                "Non-persistent messages dropped with no writable consumer",
            ),
            SUBSCRIPTION_LABELS,
        )?;
        let acked = IntCounterVec::new(
            Opts::new(
                "pulsar_lite_subscription_acked_messages_total",
                "Messages acknowledged by consumers",
            ),
            SUBSCRIPTION_LABELS,
        )?;
        let msg_rate_out = GaugeVec::new(
            Opts::new(
                "pulsar_subscription_msg_rate_out",
                "Delivered message rate (window average)",
            ),
            SUBSCRIPTION_LABELS,
        )?;
        let msg_throughput_out = GaugeVec::new(
            Opts::new(
                "pulsar_subscription_msg_throughput_out",
                "Delivered byte rate (window average)",
            ),
            SUBSCRIPTION_LABELS,
        )?;
        let msg_ack_rate = GaugeVec::new(
            Opts::new(
                "pulsar_subscription_msg_ack_rate",
                "Acknowledge rate (window average)",
            ),
            SUBSCRIPTION_LABELS,
        )?;
        let msg_rate_redeliver = GaugeVec::new(
            Opts::new(
                "pulsar_subscription_msg_rate_redeliver",
                "Redelivery rate (window average)",
            ),
            SUBSCRIPTION_LABELS,
        )?;
        let msg_drop_rate = GaugeVec::new(
            Opts::new(
                "pulsar_subscription_msg_drop_rate",
                "Non-persistent drop rate (window average)",
            ),
            SUBSCRIPTION_LABELS,
        )?;
        let back_log = IntGaugeVec::new(
            Opts::new(
                "pulsar_subscription_back_log",
                "Unacknowledged stored entries",
            ),
            SUBSCRIPTION_LABELS,
        )?;
        let unacked_messages = IntGaugeVec::new(
            Opts::new(
                "pulsar_subscription_unacked_messages",
                "Dispatched-but-unacknowledged messages",
            ),
            SUBSCRIPTION_LABELS,
        )?;
        let blocked_on_unacked = IntGaugeVec::new(
            Opts::new(
                "pulsar_subscription_blocked_on_unacked_messages",
                "1 while dispatch is blocked by the unacked gate",
            ),
            SUBSCRIPTION_LABELS,
        )?;
        let consumers_count = IntGaugeVec::new(
            Opts::new("pulsar_subscription_consumers_count", "Connected consumers"),
            SUBSCRIPTION_LABELS,
        )?;
        let last_acked_timestamp = GaugeVec::new(
            Opts::new(
                "pulsar_subscription_last_acked_timestamp",
                "Last acknowledge time (unix seconds)",
            ),
            SUBSCRIPTION_LABELS,
        )?;
        let last_consumed_timestamp = GaugeVec::new(
            Opts::new(
                "pulsar_subscription_last_consumed_timestamp",
                "Last dispatch time (unix seconds)",
            ),
            SUBSCRIPTION_LABELS,
        )?;

        registry.register(Box::new(out_messages.clone()))?;
        registry.register(Box::new(out_bytes.clone()))?;
        registry.register(Box::new(redelivered.clone()))?;
        registry.register(Box::new(dropped.clone()))?;
        registry.register(Box::new(acked.clone()))?;
        registry.register(Box::new(msg_rate_out.clone()))?;
        registry.register(Box::new(msg_throughput_out.clone()))?;
        registry.register(Box::new(msg_ack_rate.clone()))?;
        registry.register(Box::new(msg_rate_redeliver.clone()))?;
        registry.register(Box::new(msg_drop_rate.clone()))?;
        registry.register(Box::new(back_log.clone()))?;
        registry.register(Box::new(unacked_messages.clone()))?;
        registry.register(Box::new(blocked_on_unacked.clone()))?;
        registry.register(Box::new(consumers_count.clone()))?;
        registry.register(Box::new(last_acked_timestamp.clone()))?;
        registry.register(Box::new(last_consumed_timestamp.clone()))?;

        Ok(Self {
            out_messages,
            out_bytes,
            redelivered,
            dropped,
            acked,
            msg_rate_out,
            msg_throughput_out,
            msg_ack_rate,
            msg_rate_redeliver,
            msg_drop_rate,
            back_log,
            unacked_messages,
            blocked_on_unacked,
            consumers_count,
            last_acked_timestamp,
            last_consumed_timestamp,
        })
    }
}

const SUBSCRIPTION_LABELS: &[&str] =
    &["cluster", "namespace", "topic", "partition", "subscription"];
const TOPIC_LABELS: &[&str] = &["cluster", "namespace", "topic", "partition"];

impl TopicFamilies {
    fn new(registry: &prometheus::Registry) -> Result<Self, prometheus::Error> {
        let in_messages = IntCounterVec::new(
            Opts::new(
                "pulsar_in_messages_total",
                "Total messages accepted for publish",
            ),
            TOPIC_LABELS,
        )?;
        let in_bytes = IntCounterVec::new(
            Opts::new("pulsar_in_bytes_total", "Total bytes accepted for publish"),
            TOPIC_LABELS,
        )?;
        let publish_rate_limit = IntCounterVec::new(
            Opts::new(
                "pulsar_publish_rate_limit_times",
                "Publishes rejected by the topic rate limiter",
            ),
            TOPIC_LABELS,
        )?;
        let rate_in = GaugeVec::new(
            Opts::new("pulsar_rate_in", "Accepted message rate (window average)"),
            TOPIC_LABELS,
        )?;
        let throughput_in = GaugeVec::new(
            Opts::new(
                "pulsar_throughput_in",
                "Accepted byte rate (window average)",
            ),
            TOPIC_LABELS,
        )?;
        let average_msg_size = IntGaugeVec::new(
            Opts::new(
                "pulsar_average_msg_size",
                "Average accepted message size in bytes",
            ),
            TOPIC_LABELS,
        )?;
        let storage_size = IntGaugeVec::new(
            Opts::new("pulsar_storage_size", "Bytes stored for the topic"),
            TOPIC_LABELS,
        )?;
        let subscriptions_count = IntGaugeVec::new(
            Opts::new(
                "pulsar_subscriptions_count",
                "Active subscriptions on the topic",
            ),
            TOPIC_LABELS,
        )?;
        let producers_count = IntGaugeVec::new(
            Opts::new("pulsar_producers_count", "Connected producers on the topic"),
            TOPIC_LABELS,
        )?;
        let consumers_count = IntGaugeVec::new(
            Opts::new("pulsar_consumers_count", "Connected consumers on the topic"),
            TOPIC_LABELS,
        )?;

        registry.register(Box::new(in_messages.clone()))?;
        registry.register(Box::new(in_bytes.clone()))?;
        registry.register(Box::new(publish_rate_limit.clone()))?;
        registry.register(Box::new(rate_in.clone()))?;
        registry.register(Box::new(throughput_in.clone()))?;
        registry.register(Box::new(average_msg_size.clone()))?;
        registry.register(Box::new(storage_size.clone()))?;
        registry.register(Box::new(subscriptions_count.clone()))?;
        registry.register(Box::new(producers_count.clone()))?;
        registry.register(Box::new(consumers_count.clone()))?;

        Ok(Self {
            in_messages,
            in_bytes,
            publish_rate_limit,
            rate_in,
            throughput_in,
            average_msg_size,
            storage_size,
            subscriptions_count,
            producers_count,
            consumers_count,
        })
    }
}

/// Broker-scoped metric families.
#[derive(Debug)]
pub struct BrokerMetrics {
    /// `cluster` label value applied to every family (config-injected).
    cluster: String,

    /// Topic-labeled families (per-`TopicMetrics` handles resolve from these).
    pub topics: TopicFamilies,
    /// Subscription-labeled families (per-`SubscriptionMetrics` handles).
    pub subscriptions: SubscriptionFamilies,

    /// pulsar_active_connections{cluster}
    pub active_connections: IntGauge,
    /// pulsar_connection_created_total_count{cluster}
    pub connection_created: IntCounter,
    /// pulsar_connection_closed_total_count{cluster}
    pub connection_closed: IntCounter,
    /// pulsar_lite_broker_errors_total{cluster, reason}
    pub errors: IntCounterVec,
    /// pulsar_broker_in_messages_total{cluster}
    pub broker_in_messages: IntCounter,
    /// pulsar_broker_in_bytes_total{cluster}
    pub broker_in_bytes: IntCounter,
    /// pulsar_broker_out_messages_total{cluster}
    pub broker_out_messages: IntCounter,
    /// pulsar_broker_out_bytes_total{cluster}
    pub broker_out_bytes: IntCounter,
    /// pulsar_broker_topics_count{cluster} (scrape-set)
    pub broker_topics_count: IntGauge,
    /// pulsar_broker_subscriptions_count{cluster} (scrape-set)
    pub broker_subscriptions_count: IntGauge,
    /// pulsar_broker_producers_count{cluster} (scrape-set)
    pub broker_producers_count: IntGauge,
    /// pulsar_broker_consumers_count{cluster} (scrape-set)
    pub broker_consumers_count: IntGauge,
    /// pulsar_broker_rate_in{cluster} (scrape-derived)
    pub broker_rate_in: Gauge,
    /// pulsar_broker_throughput_in{cluster}
    pub broker_throughput_in: Gauge,
    /// pulsar_broker_rate_out{cluster}
    pub broker_rate_out: Gauge,
    /// pulsar_broker_throughput_out{cluster}
    pub broker_throughput_out: Gauge,
    /// pulsar_broker_msg_backlog{cluster}
    pub broker_msg_backlog: IntGauge,
    /// pulsar_broker_storage_size{cluster}
    pub broker_storage_size: IntGauge,
}

impl BrokerMetrics {
    fn new(cluster: &str, registry: &prometheus::Registry) -> Result<Self, prometheus::Error> {
        let topics = TopicFamilies::new(registry)?;
        let subscriptions = SubscriptionFamilies::new(registry)?;

        let active_connections = IntGauge::new(
            "pulsar_active_connections",
            "Currently open client connections",
        )?;
        let connection_created = IntCounter::new(
            "pulsar_connection_created_total_count",
            "Total accepted connections",
        )?;
        let connection_closed = IntCounter::new(
            "pulsar_connection_closed_total_count",
            "Total closed connections",
        )?;
        let errors = IntCounterVec::new(
            Opts::new(
                "pulsar_lite_broker_errors_total",
                "Rejected or failed operations by reason",
            ),
            &["cluster", "reason"],
        )?;
        let version_info = IntGaugeVec::new(
            Opts::new("pulsar_version_info", "Broker version constant"),
            &["cluster", "version"],
        )?;
        let broker_in_messages = IntCounter::new(
            "pulsar_broker_in_messages_total",
            "Total messages accepted for publish",
        )?;
        let broker_in_bytes = IntCounter::new(
            "pulsar_broker_in_bytes_total",
            "Total bytes accepted for publish",
        )?;
        let broker_out_messages = IntCounter::new(
            "pulsar_broker_out_messages_total",
            "Total messages delivered to consumers",
        )?;
        let broker_out_bytes = IntCounter::new(
            "pulsar_broker_out_bytes_total",
            "Total bytes delivered to consumers",
        )?;
        let broker_topics_count =
            IntGauge::new("pulsar_broker_topics_count", "Topics hosted by the broker")?;
        let broker_subscriptions_count = IntGauge::new(
            "pulsar_broker_subscriptions_count",
            "Subscriptions hosted by the broker",
        )?;
        let broker_producers_count = IntGauge::new(
            "pulsar_broker_producers_count",
            "Connected producers on the broker",
        )?;
        let broker_consumers_count = IntGauge::new(
            "pulsar_broker_consumers_count",
            "Connected consumers on the broker",
        )?;
        let broker_rate_in = Gauge::new(
            "pulsar_broker_rate_in",
            "Accepted message rate across all topics (window average)",
        )?;
        let broker_throughput_in = Gauge::new(
            "pulsar_broker_throughput_in",
            "Accepted byte rate across all topics (window average)",
        )?;
        let broker_rate_out = Gauge::new(
            "pulsar_broker_rate_out",
            "Delivered message rate across all topics (window average)",
        )?;
        let broker_throughput_out = Gauge::new(
            "pulsar_broker_throughput_out",
            "Delivered byte rate across all topics (window average)",
        )?;
        let broker_msg_backlog =
            IntGauge::new("pulsar_broker_msg_backlog", "Total backlog entries")?;
        let broker_storage_size =
            IntGauge::new("pulsar_broker_storage_size", "Total stored bytes")?;

        registry.register(Box::new(active_connections.clone()))?;
        registry.register(Box::new(connection_created.clone()))?;
        registry.register(Box::new(connection_closed.clone()))?;
        registry.register(Box::new(errors.clone()))?;
        registry.register(Box::new(version_info.clone()))?;
        registry.register(Box::new(broker_in_messages.clone()))?;
        registry.register(Box::new(broker_in_bytes.clone()))?;
        registry.register(Box::new(broker_out_messages.clone()))?;
        registry.register(Box::new(broker_out_bytes.clone()))?;
        registry.register(Box::new(broker_topics_count.clone()))?;
        registry.register(Box::new(broker_subscriptions_count.clone()))?;
        registry.register(Box::new(broker_producers_count.clone()))?;
        registry.register(Box::new(broker_consumers_count.clone()))?;
        registry.register(Box::new(broker_rate_in.clone()))?;
        registry.register(Box::new(broker_throughput_in.clone()))?;
        registry.register(Box::new(broker_rate_out.clone()))?;
        registry.register(Box::new(broker_throughput_out.clone()))?;
        registry.register(Box::new(broker_msg_backlog.clone()))?;
        registry.register(Box::new(broker_storage_size.clone()))?;
        // The registry keeps the gauge alive; we only pin the value once.
        version_info
            .with_label_values(&[cluster, env!("CARGO_PKG_VERSION")])
            .set(1);
        Ok(Self {
            cluster: cluster.to_string(),
            topics,
            subscriptions,
            active_connections,
            connection_created,
            connection_closed,
            errors,
            broker_in_messages,
            broker_in_bytes,
            broker_out_messages,
            broker_out_bytes,
            broker_topics_count,
            broker_subscriptions_count,
            broker_producers_count,
            broker_consumers_count,
            broker_rate_in,
            broker_throughput_in,
            broker_rate_out,
            broker_throughput_out,
            broker_msg_backlog,
            broker_storage_size,
        })
    }

    /// The configured `cluster` label value.
    pub fn cluster(&self) -> &str {
        &self.cluster
    }

    /// Pre-resolves an error counter for `reason` (one label lookup, then
    /// stored by the caller; `inc()` is a single atomic add).
    pub fn error_counter(&self, reason: &str) -> IntCounter {
        self.errors
            .with_label_values(&[self.cluster.as_str(), reason])
    }

    /// Fallback constructor used only when registration failed: identical
    /// handles, not attached to the served registry.
    fn unregistered(cluster: &str) -> Self {
        // Static family names are proven valid by tests; registration into a
        // fresh registry cannot fail for any other reason.
        Self::new(cluster, &prometheus::Registry::new())
            .expect("static broker metric definitions are valid")
    }
}

static BROKER_METRICS: OnceLock<Arc<BrokerMetrics>> = OnceLock::new();

/// Initializes broker metric families into the shared global registry.
///
/// Idempotent: the first call wins, so unit tests and production startup
/// share one code path. Construction only fails on invalid static names
/// (a programming error caught by tests); in that case we log once and
/// fall back to unregistered families so counters stay callable.
/// Initializes broker metric families into the shared global registry.
///
/// Idempotent: the first call wins, so unit tests and production startup
/// share one code path.
pub fn init(cluster: &str) -> Arc<BrokerMetrics> {
    BROKER_METRICS
        .get_or_init(|| match BrokerMetrics::new(cluster, &global_registry()) {
            Ok(metrics) => Arc::new(metrics),
            Err(error) => {
                log::error!("Failed to register broker metrics: {}", error);
                Arc::new(BrokerMetrics::unregistered(cluster))
            }
        })
        .clone()
}

/// Returns the initialized metrics, auto-initializing with the default
/// cluster label for callers (tests, library embedders) that skipped
/// explicit startup wiring.
pub fn get() -> Arc<BrokerMetrics> {
    init("pulsar-lite")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_is_idempotent_and_counters_tick() {
        let metrics = init("test-cluster");
        assert!(Arc::ptr_eq(&metrics, &init("other-cluster")));

        let before = metrics.connection_created.get();
        metrics.connection_created.inc();
        // Unique label: the served registry is process-global and other
        // tests (oversized-message handling) also bump error counters.
        let rejected = metrics.error_counter("registry_unit_test");
        rejected.inc();
        assert!(metrics.connection_created.get() > before);
        assert_eq!(rejected.get(), 1);
        // Cluster label itself is racy under parallel tests (first init
        // wins process-wide); ptr_eq above already proves idempotency.
    }

    #[test]
    fn topic_families_resolve_same_cell_for_same_labels() {
        let families = &get().topics;
        let a = families
            .in_messages
            .with_label_values(&["c", "public/default", "t", "-1"]);
        let b = families
            .in_messages
            .with_label_values(&["c", "public/default", "t", "-1"]);
        a.inc();
        assert_eq!(a.get(), b.get());
    }
}
