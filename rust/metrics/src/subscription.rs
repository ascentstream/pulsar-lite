/*
 * Subscription-level metric handles
 *
 * One `SubscriptionMetrics` per Subscription entity, resolved at creation
 * from the global families (labels `{cluster, namespace, topic, partition,
 * subscription}`). Hot paths touch pre-resolved atomic handles only;
 * gauges are set by the scrape aggregation loop.
 */

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use prometheus::{Gauge, IntCounter, IntGauge};

use crate::broker::BrokerMetrics;
use crate::topic::parse_topic_labels;

/// (timestamp, out_messages, out_bytes, acked, redelivered, dropped).
type RateSample = (Instant, u64, u64, u64, u64, u64);
type RateSampleWindow = VecDeque<RateSample>;

/// Pre-resolved per-subscription handles.
pub struct SubscriptionMetrics {
    out_messages: IntCounter,
    out_bytes: IntCounter,
    redelivered: IntCounter,
    dropped: IntCounter,
    acked: IntCounter,
    msg_rate_out: Gauge,
    msg_throughput_out: Gauge,
    msg_ack_rate: Gauge,
    msg_rate_redeliver: Gauge,
    msg_drop_rate: Gauge,
    back_log: IntGauge,
    consumers_count: IntGauge,
    unacked_messages: IntGauge,
    blocked_on_unacked: IntGauge,
    last_acked_timestamp: Gauge,
    last_consumed_timestamp: Gauge,
    broker: Arc<BrokerMetrics>,
    /// Samples for derived rates; touched only by the scrape task. Seeded
    /// with a zero baseline.
    rate_samples: Mutex<RateSampleWindow>,
}

impl SubscriptionMetrics {
    /// Resolves all handles for `(topic_name, subscription_name)`.
    pub fn new(topic_name: &str, subscription_name: &str) -> Self {
        let broker = crate::get();
        let labels = parse_topic_labels(topic_name);
        let cluster = broker.cluster();
        let partition = labels.partition.to_string();
        let values: Vec<&str> = vec![
            cluster,
            labels.namespace.as_str(),
            labels.topic.as_str(),
            partition.as_str(),
            subscription_name,
        ];

        let families = &broker.subscriptions;
        let mut samples: RateSampleWindow = VecDeque::new();
        samples.push_back((Instant::now(), 0, 0, 0, 0, 0));
        Self {
            out_messages: families.out_messages.with_label_values(&values),
            out_bytes: families.out_bytes.with_label_values(&values),
            redelivered: families.redelivered.with_label_values(&values),
            dropped: families.dropped.with_label_values(&values),
            acked: families.acked.with_label_values(&values),
            msg_rate_out: families.msg_rate_out.with_label_values(&values),
            msg_throughput_out: families.msg_throughput_out.with_label_values(&values),
            msg_ack_rate: families.msg_ack_rate.with_label_values(&values),
            msg_rate_redeliver: families.msg_rate_redeliver.with_label_values(&values),
            msg_drop_rate: families.msg_drop_rate.with_label_values(&values),
            back_log: families.back_log.with_label_values(&values),
            consumers_count: families.consumers_count.with_label_values(&values),
            unacked_messages: families.unacked_messages.with_label_values(&values),
            blocked_on_unacked: families.blocked_on_unacked.with_label_values(&values),
            last_acked_timestamp: families.last_acked_timestamp.with_label_values(&values),
            last_consumed_timestamp: families.last_consumed_timestamp.with_label_values(&values),
            broker,
            rate_samples: Mutex::new(samples),
        }
    }

    /// Records dispatched messages (batch-aware count) and their bytes.
    pub fn record_dispatched(&self, messages: u64, bytes: u64) {
        self.out_messages.inc_by(messages);
        self.out_bytes.inc_by(bytes);
        self.broker.broker_out_messages.inc_by(messages);
        self.broker.broker_out_bytes.inc_by(bytes);
        self.last_consumed_timestamp.set(unix_epoch_seconds());
    }

    /// Records an acknowledged message.
    pub fn record_acked(&self) {
        self.acked.inc();
        self.last_acked_timestamp.set(unix_epoch_seconds());
    }

    /// Records messages queued for redelivery.
    pub fn record_redelivered(&self, messages: u64) {
        self.redelivered.inc_by(messages);
    }

    /// Records a non-persistent message dropped (no writable consumer).
    pub fn record_dropped(&self) {
        self.dropped.inc();
    }

    /// Records `count` non-persistent drops (batch drop accounting).
    pub fn record_dropped_n(&self, count: u64) {
        self.dropped.inc_by(count);
    }

    /// Scrape task: refreshes derived rate gauges over `window_secs`.
    /// Returns (out_messages, out_bytes) deltas for broker-level folding.
    pub fn update_rates(&self, window_secs: u64) -> (u64, u64) {
        let now = Instant::now();
        let window = std::time::Duration::from_secs(window_secs.max(1));
        let current = (
            self.out_messages.get(),
            self.out_bytes.get(),
            self.acked.get(),
            self.redelivered.get(),
            self.dropped.get(),
        );

        let mut samples = match self.rate_samples.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        samples.push_back((now, current.0, current.1, current.2, current.3, current.4));
        while let Some((t, ..)) = samples.front() {
            if now.duration_since(*t) > window {
                samples.pop_front();
            } else {
                break;
            }
        }

        let deltas = match (samples.front(), samples.back()) {
            (Some((t0, m0, b0, a0, r0, d0)), Some((t1, m1, b1, a1, r1, d1))) => {
                let elapsed = t1.duration_since(*t0).as_secs_f64();
                (
                    m1.saturating_sub(*m0),
                    b1.saturating_sub(*b0),
                    a1.saturating_sub(*a0),
                    r1.saturating_sub(*r0),
                    d1.saturating_sub(*d0),
                    elapsed,
                )
            }
            _ => (0, 0, 0, 0, 0, 0.0),
        };
        let (d_out, d_bytes, d_acked, d_redelivered, d_dropped, elapsed) = deltas;

        if elapsed >= 1.0 {
            self.msg_rate_out.set(d_out as f64 / elapsed);
            self.msg_throughput_out.set(d_bytes as f64 / elapsed);
            self.msg_ack_rate.set(d_acked as f64 / elapsed);
            self.msg_rate_redeliver.set(d_redelivered as f64 / elapsed);
            self.msg_drop_rate.set(d_dropped as f64 / elapsed);
        }
        (d_out, d_bytes)
    }

    /// Scrape task: state gauges for this subscription.
    pub fn set_state(&self, back_log: i64, unacked: i64, blocked: bool, consumers: i64) {
        self.back_log.set(back_log);
        self.unacked_messages.set(unacked);
        self.blocked_on_unacked.set(blocked as i64);
        self.consumers_count.set(consumers);
    }
}

impl std::fmt::Debug for SubscriptionMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionMetrics")
            .finish_non_exhaustive()
    }
}

fn unix_epoch_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatched_acked_and_liveness_tick() {
        let metrics =
            SubscriptionMetrics::new("persistent://public/subns/sub-topic", "sub-unit-test");
        metrics.record_dispatched(1, 120);
        metrics.record_dispatched(1, 30);
        metrics.record_acked();
        metrics.record_redelivered(2);
        metrics.record_dropped();

        let (d_out, d_bytes) = metrics.update_rates(60);
        assert_eq!(d_out, 2);
        assert_eq!(d_bytes, 150);

        metrics.set_state(3, 1, true, 2);
        // State gauges are read through the registry in integration checks;
        // here we only assert the counters did not panic and rates reported.
    }
}
