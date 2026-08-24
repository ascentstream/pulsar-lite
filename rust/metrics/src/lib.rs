//! Pulsar Lite metrics crate.
//!
//! One home for every Prometheus family this broker exports:
//!
//! - [`global_registry`] — the process-wide registry served on
//!   `GET /metrics` (via `prometheus-hyper` in the broker binary);
//! - [`storage`] — families observed from the managed-ledger write path
//!   (`pulsar_storage_write_latency`, `pulsar_entry_size`,
//!   write-queue batch metrics);
//! - [`broker`] — broker-scoped families and the topic/subscription
//!   label families (`pulsar_broker_*`, `pulsar_in/out_*`,
//!   `pulsar_subscription_*`);
//! - [`topic`] / [`subscription`] — pre-resolved per-entity handles whose
//!   label lookup happens exactly once at entity creation;
//! - [`observer`] — the `PublishCommitObserver` hook the RocksDB
//!   write-queue worker invokes per committed batch.
//!
//! Naming contract: families reproducing native Pulsar semantics keep the
//! exact `pulsar_*` names and label sets; extensions use `pulsar_lite_*`.
//!
//! Hot paths only ever touch pre-resolved handles (plain atomic adds).
//! [`init`] registers everything once; before it runs, accessors return
//! no-op handles so unit tests never panic.

use std::sync::{Arc, LazyLock};

use prometheus::Registry;

pub mod broker;
pub mod observer;
pub mod storage;
pub mod subscription;
pub mod topic;

pub use broker::{get, BrokerMetrics};
pub use observer::PublishCommitObserver;
pub use storage::{storage_metrics, StorageMetrics, StorageMetricsHandle};
pub use subscription::SubscriptionMetrics;
pub use topic::{parse_topic_labels, TopicLabels, TopicMetrics};

static GLOBAL: LazyLock<Arc<Registry>> = LazyLock::new(|| {
    let registry = Registry::new();
    // The process collector (RSS / CPU gauges) is registered best-effort: a
    // missing /proc mount only drops those gauges, never breaks collection.
    match registry.register(Box::new(
        prometheus::process_collector::ProcessCollector::for_self(),
    )) {
        Ok(()) => {}
        Err(error) => log::warn!("Failed to register process collector: {}", error),
    }
    Arc::new(registry)
});

/// Returns the process-wide metrics registry, creating it on first use.
pub fn global_registry() -> Arc<Registry> {
    GLOBAL.clone()
}

/// Registers broker families (idempotent) and storage families.
///
/// `broker::init` is separately idempotent so it can also serve as the
/// lazy entry point for embedders that never call this function.
pub fn init(cluster: &str) -> Arc<BrokerMetrics> {
    let metrics = broker::init(cluster);
    let _ = storage::init(cluster);
    metrics
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_registry_is_stable_across_calls() {
        assert!(Arc::ptr_eq(&global_registry(), &global_registry()));
    }

    #[test]
    fn init_registers_all_families_once() {
        let metrics = init("crate-test-cluster");
        // Unique labels: the registry is process-global and sibling tests
        // also bump shared counters.
        let cell = metrics
            .topics
            .in_messages
            .with_label_values(&["c", "ns", "lib-init-check", "-1"]);
        cell.inc();
        let storage = storage::storage_metrics();
        storage.observe_entry_size(10.0);
        assert_eq!(cell.get(), 1);
    }
}
