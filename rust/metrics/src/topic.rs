/*
 * Topic-level metric handles
 *
 * One `TopicMetrics` per Topic entity, created at `Topic::new` time. All
 * label resolution happens exactly once here; hot paths afterwards only
 * touch the resolved `IntCounter`/`IntGauge` handles (plain atomic ops).
 *
 * Label conventions mirror native Pulsar: `{cluster, namespace, topic,
 * partition}`, with `partition="-1"` for non-partitioned topics and the
 * `-partition-N` suffix stripped from the exported topic label.
 */

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use prometheus::{Gauge, IntCounter, IntGauge};

use crate::broker::BrokerMetrics;

/// Native-style decomposition of a full topic URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicLabels {
    /// `tenant/namespace` (native namespace label format).
    pub namespace: String,
    /// Local topic name without domain or partition suffix.
    pub topic: String,
    /// Partition index, `-1` for non-partitioned topics.
    pub partition: i32,
}

/// Splits `persistent://tenant/ns/local[-partition-N]` into label values.
///
/// Malformed names degrade to `namespace="unknown"` instead of failing:
/// metrics must never reject traffic the broker already accepted.
pub fn parse_topic_labels(name: &str) -> TopicLabels {
    let body = match name.split_once("://") {
        Some((_, rest)) => rest,
        None => name,
    };

    let mut parts = body.splitn(3, '/');
    let tenant = parts.next().unwrap_or_default();
    let namespace_part = parts.next().unwrap_or_default();
    let local = parts.next().unwrap_or(body);

    let namespace = if tenant.is_empty() || namespace_part.is_empty() {
        "unknown".to_string()
    } else {
        format!("{}/{}", tenant, namespace_part)
    };

    let (topic, partition) = match local.rsplit_once("-partition-") {
        Some((base, digits))
            if !base.is_empty()
                && !digits.is_empty()
                && digits.bytes().all(|b| b.is_ascii_digit()) =>
        {
            (base.to_string(), digits.parse::<i32>().unwrap_or(-1))
        }
        _ => (local.to_string(), -1),
    };

    TopicLabels {
        namespace,
        topic,
        partition,
    }
}

/// Pre-resolved per-topic handles (counters) + scrape-set gauges.
pub struct TopicMetrics {
    in_messages: IntCounter,
    in_bytes: IntCounter,
    publish_rate_limit: IntCounter,
    rate_in: Gauge,
    throughput_in: Gauge,
    average_msg_size: IntGauge,
    storage_size: IntGauge,
    subscriptions_count: IntGauge,
    producers_count: IntGauge,
    consumers_count: IntGauge,
    broker: Arc<BrokerMetrics>,
    /// (timestamp, cumulative messages, cumulative bytes) samples for the
    /// scrape-derived rate gauges; only the scrape task touches this.
    /// Seeded with a zero baseline so the first update already reports
    /// the full window delta.
    rate_samples: Mutex<VecDeque<(Instant, u64, u64)>>,
}

impl TopicMetrics {
    /// Resolves all handles for `topic_name` from the global families.
    pub fn new(topic_name: &str) -> Self {
        let broker = crate::get();
        let labels = parse_topic_labels(topic_name);
        let cluster = broker.cluster();
        let values = [cluster, labels.namespace.as_str(), labels.topic.as_str()];
        let partition = labels.partition.to_string();
        let mut with_partition = values.to_vec();
        with_partition.push(partition.as_str());
        let vals: Vec<&str> = with_partition;

        let families = &broker.topics;
        Self {
            in_messages: families.in_messages.with_label_values(&vals),
            in_bytes: families.in_bytes.with_label_values(&vals),
            publish_rate_limit: families.publish_rate_limit.with_label_values(&vals),
            rate_in: families.rate_in.with_label_values(&vals),
            throughput_in: families.throughput_in.with_label_values(&vals),
            average_msg_size: families.average_msg_size.with_label_values(&vals),
            storage_size: families.storage_size.with_label_values(&vals),
            subscriptions_count: families.subscriptions_count.with_label_values(&vals),
            producers_count: families.producers_count.with_label_values(&vals),
            consumers_count: families.consumers_count.with_label_values(&vals),
            broker,
            rate_samples: {
                let mut samples = VecDeque::new();
                samples.push_back((Instant::now(), 0, 0));
                Mutex::new(samples)
            },
        }
    }

    /// Records accepted publishes (messages and payload+metadata bytes).
    ///
    /// Called from single-writer contexts (non-persistent fan-out worker,
    /// in-process publish); the durable write-queue path goes through the
    /// `PublishCommitObserver` impl below instead.
    pub fn record_publish(&self, messages: u64, bytes: u64) {
        self.in_messages.inc_by(messages);
        self.in_bytes.inc_by(bytes);
        self.broker.broker_in_messages.inc_by(messages);
        self.broker.broker_in_bytes.inc_by(bytes);
    }

    /// Records a publish-rate-limit rejection.
    pub fn record_rate_limit_reject(&self) {
        self.publish_rate_limit.inc();
    }

    /// Scrape task: refreshes derived rate gauges over `window_secs`.
    ///
    /// Returns the window delta (messages, bytes) so the caller can fold
    /// broker-level rate gauges in the same pass.
    pub fn update_rates(&self, window_secs: u64) -> (u64, u64) {
        let (messages, bytes) = (self.in_messages.get(), self.in_bytes.get());
        let now = Instant::now();
        let window = std::time::Duration::from_secs(window_secs.max(1));

        let mut samples = match self.rate_samples.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        samples.push_back((now, messages, bytes));
        while let Some((t, _, _)) = samples.front() {
            if now.duration_since(*t) > window {
                samples.pop_front();
            } else {
                break;
            }
        }

        let (d_messages, d_bytes, elapsed) = match (samples.front(), samples.back()) {
            (Some((t0, m0, b0)), Some((t1, m1, b1))) => {
                let elapsed = t1.duration_since(*t0).as_secs_f64();
                (m1.saturating_sub(*m0), b1.saturating_sub(*b0), elapsed)
            }
            _ => (0, 0, 0.0),
        };

        if elapsed >= 1.0 {
            self.rate_in.set(d_messages as f64 / elapsed);
            self.throughput_in.set(d_bytes as f64 / elapsed);
            self.average_msg_size
                .set(d_bytes.checked_div(d_messages).unwrap_or(0) as i64);
        }
        (d_messages, d_bytes)
    }

    /// Scrape task: entity count gauges for this topic.
    pub fn set_entity_counts(&self, subscriptions: i64, producers: i64, consumers: i64) {
        self.subscriptions_count.set(subscriptions);
        self.producers_count.set(producers);
        self.consumers_count.set(consumers);
    }

    /// Scrape task: stored bytes for this topic.
    pub fn set_storage_size(&self, bytes: i64) {
        self.storage_size.set(bytes);
    }
}

impl std::fmt::Debug for TopicMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicMetrics").finish_non_exhaustive()
    }
}
/// Durable-batch accounting for the write-queue worker (single writer).
impl crate::observer::PublishCommitObserver for TopicMetrics {
    fn on_commit(&self, messages: u64, bytes: u64) {
        self.in_messages.inc_by(messages);
        self.in_bytes.inc_by(bytes);
        self.broker.broker_in_messages.inc_by(messages);
        self.broker.broker_in_bytes.inc_by(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_persistent_topic_uri() {
        let labels = parse_topic_labels("persistent://public/default/orders");
        assert_eq!(labels.namespace, "public/default");
        assert_eq!(labels.topic, "orders");
        assert_eq!(labels.partition, -1);
    }

    #[test]
    fn parses_partition_suffix() {
        let labels = parse_topic_labels("non-persistent://tenant/ns/queue-partition-7");
        assert_eq!(labels.namespace, "tenant/ns");
        assert_eq!(labels.topic, "queue");
        assert_eq!(labels.partition, 7);
    }

    #[test]
    fn malformed_names_degrade_to_unknown_namespace() {
        let labels = parse_topic_labels("bare-topic");
        assert_eq!(labels.namespace, "unknown");
        assert_eq!(labels.topic, "bare-topic");
        assert_eq!(labels.partition, -1);
    }

    #[test]
    fn non_numeric_partition_suffix_is_kept_in_topic() {
        let labels = parse_topic_labels("persistent://t/n/v2-partition-x");
        assert_eq!(labels.topic, "v2-partition-x");
        assert_eq!(labels.partition, -1);
    }

    #[test]
    fn topic_metrics_counters_and_rates_tick() {
        let metrics = TopicMetrics::new("persistent://public/tickns/metrics-tick");
        metrics.record_publish(3, 300);
        metrics.record_rate_limit_reject();

        // First update reports the full delta from the zero baseline
        // (the elapsed guard only suppresses the rate gauges, not the
        // returned delta used for broker-level folding).
        let (d_msgs, d_bytes) = metrics.update_rates(60);
        assert_eq!(d_msgs, 3);
        assert_eq!(d_bytes, 300);

        let labels = parse_topic_labels("persistent://public/tickns/metrics-tick");
        assert_eq!(labels.namespace, "public/tickns");
        assert_eq!(labels.topic, "metrics-tick");
    }
}
