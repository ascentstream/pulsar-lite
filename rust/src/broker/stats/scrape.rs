/*
 * Scrape-time aggregation task
 *
 * A 5s background task that walks broker state and sets the gauge half of
 * the exported metrics: entity counts, backlog, unacked state, and
 * scrape-derived rates. Monotonic counters are maintained on hot paths
 * and never touched here.
 *
 * Lock discipline: broker/topic/subscription locks are acquired with
 * `try_read`; a busy lock skips that entity for this round (gauges keep
 * their previous value) so aggregation can never block the
 * publish/dispatch paths. The storage lock is `try_lock` for the same
 * reason.
 */

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, RwLock};
use tokio::time::{interval, MissedTickBehavior};

use crate::broker::connection_limiter::ConnectionLimiter;
use crate::broker::dispatcher::DEFAULT_MAX_UNACKED_MESSAGES_PER_CONSUMER;
use crate::broker::service::topic::{SubscriptionRuntimeMode, Topic};
use crate::broker::service::Consumer;
use crate::broker::stats;
use crate::broker::BrokerService;

use pulsar_lite_storage::Storage;

/// Aggregation cadence; rate windows are computed from samples, not from
/// this interval, so changing one does not change the other.
const AGGREGATION_INTERVAL: Duration = Duration::from_secs(5);

type SharedBrokerService = Arc<RwLock<BrokerService>>;
type SharedStorage = Arc<Mutex<Storage>>;

/// Spawns the aggregation loop. Owns clones of broker-wide shared handles;
/// aborts with the process.
pub fn spawn(
    broker_service: SharedBrokerService,
    storage: SharedStorage,
    connection_limiter: ConnectionLimiter,
    rate_window_secs: u64,
) {
    tokio::spawn(async move {
        let mut ticker = interval(AGGREGATION_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        // First tick fires immediately, which publishes an initial zero
        // snapshot before any traffic arrives.
        loop {
            ticker.tick().await;
            aggregate(
                &broker_service,
                &storage,
                &connection_limiter,
                rate_window_secs,
            )
            .await;
        }
    });
}

async fn aggregate(
    broker_service: &SharedBrokerService,
    storage: &SharedStorage,
    connection_limiter: &ConnectionLimiter,
    rate_window_secs: u64,
) {
    let metrics = stats::get();
    metrics
        .active_connections
        .set(connection_limiter.active_connections() as i64);

    let broker = match broker_service.try_read() {
        Ok(guard) => guard,
        Err(_) => return,
    };

    let mut totals = WalkTotals::default();

    for topic in broker.get_all_topics().values() {
        walk_topic(topic, storage, rate_window_secs, &mut totals).await;
    }
    for partitioned in broker.get_all_partitioned_topics().values() {
        let guard = match partitioned.try_read() {
            Ok(guard) => guard,
            Err(_) => continue,
        };
        for partition in guard.get_all_partitions() {
            walk_topic(partition, storage, rate_window_secs, &mut totals).await;
        }
    }

    metrics.broker_topics_count.set(totals.topics);
    metrics.broker_subscriptions_count.set(totals.subscriptions);
    metrics.broker_producers_count.set(totals.producers);
    metrics.broker_consumers_count.set(totals.consumers);
    metrics.broker_msg_backlog.set(totals.backlog);
    metrics.broker_storage_size.set(totals.stored_bytes as i64);
    let window_secs = rate_window_secs.max(1) as f64;
    metrics
        .broker_rate_in
        .set(totals.window_messages as f64 / window_secs);
    metrics
        .broker_throughput_in
        .set(totals.window_bytes as f64 / window_secs);
    metrics
        .broker_rate_out
        .set(totals.window_out_messages as f64 / window_secs);
    metrics
        .broker_throughput_out
        .set(totals.window_out_bytes as f64 / window_secs);
}

#[derive(Default)]
struct WalkTotals {
    topics: i64,
    subscriptions: i64,
    producers: i64,
    consumers: i64,
    backlog: i64,
    stored_bytes: u64,
    window_messages: u64,
    window_bytes: u64,
    window_out_messages: u64,
    window_out_bytes: u64,
}

async fn walk_topic(
    topic: &Arc<RwLock<Topic>>,
    storage: &SharedStorage,
    rate_window_secs: u64,
    totals: &mut WalkTotals,
) {
    // Phase 1 (holding short-lived try_read guards, fully synchronous):
    // snapshot everything needed, then drop the guards before any await so
    // aggregation never extends broker lock hold times.
    let snapshot = match topic.try_read() {
        Ok(guard) => {
            let producers = guard.get_producer_count() as i64;
            let consumers = guard.total_consumer_count_snapshot();
            let subscriptions = guard.get_subscription_count() as i64;
            let (d_msgs, d_bytes) = guard.metrics.update_rates(rate_window_secs);
            guard
                .metrics
                .set_entity_counts(subscriptions, producers, consumers);
            let topic_metrics = Arc::clone(&guard.metrics);
            let topic_name = guard.name.clone();
            let subscriptions: Vec<SubscriptionSnapshot> = guard
                .get_all_subscriptions()
                .into_iter()
                .filter_map(|subscription| {
                    let sub_guard = subscription.try_read().ok()?;
                    Some(SubscriptionSnapshot {
                        metrics: Arc::clone(&sub_guard.metrics),
                        name: sub_guard.name.clone(),
                        persistent: sub_guard.runtime_mode() == SubscriptionRuntimeMode::Persistent,
                        consumers: sub_guard.get_consumers(),
                    })
                })
                .collect();
            let subscription_count = subscriptions.len() as i64;
            (
                topic_metrics,
                topic_name,
                subscriptions,
                producers,
                consumers,
                subscription_count,
                d_msgs,
                d_bytes,
            )
        }
        Err(_) => return,
    };
    let (
        topic_metrics,
        topic_name,
        subscriptions,
        producers,
        consumers,
        subscription_count,
        d_msgs,
        d_bytes,
    ) = snapshot;

    totals.topics += 1;
    totals.subscriptions += subscription_count;
    totals.producers += producers;
    totals.consumers += consumers;
    totals.window_messages += d_msgs;
    totals.window_bytes += d_bytes;

    // Phase 2 (no broker locks held): storage queries and per-consumer
    // awaits. Storage uses try_lock per query so a busy backend skips the
    // round instead of queueing behind publish paths.
    let stored_bytes = match storage.try_lock() {
        Ok(storage_guard) => storage_guard.stored_bytes(&topic_name),
        Err(_) => 0,
    };
    topic_metrics.set_storage_size(stored_bytes as i64);
    totals.stored_bytes += stored_bytes;

    for subscription in subscriptions {
        let mut unacked: i64 = 0;
        let mut blocked = false;
        for consumer in &subscription.consumers {
            let pending = consumer.pending_ack_count().await as i64;
            unacked += pending;
            if pending >= DEFAULT_MAX_UNACKED_MESSAGES_PER_CONSUMER as i64 {
                blocked = true;
            }
        }

        let backlog = if subscription.persistent {
            match storage.try_lock() {
                Ok(storage_guard) => storage_guard
                    .backlog_entries(&topic_name, &subscription.name)
                    .unwrap_or(0) as i64,
                Err(_) => 0,
            }
        } else {
            0
        };

        let (d_out, d_out_bytes) = subscription.metrics.update_rates(rate_window_secs);
        subscription.metrics.set_state(
            backlog,
            unacked,
            blocked,
            subscription.consumers.len() as i64,
        );

        totals.backlog += backlog;
        totals.window_out_messages += d_out;
        totals.window_out_bytes += d_out_bytes;
    }
}

/// Synchronous per-subscription snapshot taken under the subscription's
/// try_read guard.
struct SubscriptionSnapshot {
    metrics: Arc<crate::broker::stats::SubscriptionMetrics>,
    name: String,
    persistent: bool,
    consumers: Vec<Arc<Consumer>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::service::topic::{Subscription, SubscriptionType, Topic};
    use pulsar_lite_storage::Storage;

    #[cfg(feature = "rocksdb-storage")]
    #[tokio::test]
    async fn backlog_reflects_partial_acks_through_broker_subscription() {
        let dir = tempfile::tempdir().unwrap();
        let storage: SharedStorage = Arc::new(Mutex::new(
            Storage::new_rocksdb(dir.path()).expect("open rocksdb"),
        ));
        let topic: Arc<RwLock<Topic>> = Arc::new(RwLock::new(Topic::new(
            "persistent://public/default/scrape-backlog".to_string(),
            storage.clone(),
        )));

        {
            let mut guard = topic.write().await;
            for i in 0..10u64 {
                guard
                    .publish_message(None, bytes::Bytes::from(format!("m{i}")))
                    .await
                    .unwrap();
            }
        }

        let subscription: Arc<RwLock<Subscription>> = Arc::new(RwLock::new(Subscription::new(
            "sub".to_string(),
            "persistent://public/default/scrape-backlog".to_string(),
            SubscriptionType::Exclusive,
            storage.clone(),
        )));

        // Ack the first four entries through the broker ack path.
        {
            let mut sub = subscription.write().await;
            let ids: Vec<_> = (0..4u64)
                .map(|entry| pulsar_lite_storage_managed_ledger::MessageId {
                    ledger: 0,
                    entry,
                    partition: -1,
                })
                .collect();
            sub.acknowledge_message(
                &ids,
                crate::broker::service::topic::AckCommandType::Individual,
            )
            .await
            .unwrap();
        }

        let direct = {
            let guard = storage.lock().await;
            guard.backlog_entries("persistent://public/default/scrape-backlog", "sub")
        };
        assert_eq!(direct, Some(6), "storage-level backlog must be 6");
    }
}
