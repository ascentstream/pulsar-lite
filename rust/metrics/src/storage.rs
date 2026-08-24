//! Storage-side metric families observed from the managed-ledger write path.

use std::sync::{Arc, LazyLock, OnceLock};

use prometheus::{Histogram, HistogramOpts, IntCounter, Registry};

use crate::global_registry;

/// Native Pulsar's addEntry latency bucket layout, in seconds
/// ({0.5, 1, 5, 10, 20, 50, 100, 200, 1000} ms).
const WRITE_LATENCY_BUCKETS: &[f64] = &[
    0.0005, 0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 1.0,
];

/// Native Pulsar's entry-size bucket layout, in bytes
/// ({128, 512, 1K, 2K, 4K, 16K, 100K, 1M}).
const ENTRY_SIZE_BUCKETS: &[f64] = &[
    128.0, 512.0, 1024.0, 2048.0, 4096.0, 16384.0, 102400.0, 1048576.0,
];

/// Write-queue batch sizes (MAX_BATCH today is 64).
const BATCH_SIZE_BUCKETS: &[f64] = &[1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0];

/// Storage-side histogram/counter families.
///
/// `pulsar_storage_write_latency` is the end-to-end durable-publish view
/// (enqueue → committed batch), the equivalent of native addEntry latency
/// including queue wait. `pulsar_storage_ledger_write_latency` covers only
/// the ledger append (native bookie-write view). Write-queue families
/// replace the removed 1 Hz summary log.
#[derive(Debug)]
pub struct StorageMetrics {
    write_latency: Histogram,
    ledger_write_latency: Histogram,
    entry_size: Histogram,
    wq_batches: IntCounter,
    wq_batch_messages: IntCounter,
    wq_batch_size: Histogram,
}

static STORAGE_METRICS: OnceLock<Arc<StorageMetrics>> = OnceLock::new();

impl StorageMetrics {
    fn new(cluster: &str, registry: &Registry) -> Result<Self, prometheus::Error> {
        let write_latency = Histogram::with_opts(
            HistogramOpts::new(
                "pulsar_storage_write_latency",
                "End-to-end durable publish latency (enqueue to committed batch)",
            )
            .buckets(WRITE_LATENCY_BUCKETS.to_vec()),
        )?;
        let ledger_write_latency = Histogram::with_opts(
            HistogramOpts::new(
                "pulsar_storage_ledger_write_latency",
                "Managed-ledger append latency per committed group",
            )
            .buckets(WRITE_LATENCY_BUCKETS.to_vec()),
        )?;
        let entry_size = Histogram::with_opts(
            HistogramOpts::new("pulsar_entry_size", "Accepted entry size in bytes")
                .buckets(ENTRY_SIZE_BUCKETS.to_vec()),
        )?;
        let wq_batches = IntCounter::new(
            "pulsar_lite_write_queue_batches_total",
            "Write-queue batches drained",
        )?;
        let wq_batch_messages = IntCounter::new(
            "pulsar_lite_write_queue_batch_messages_total",
            "Messages passed through the write queue",
        )?;
        let wq_batch_size = Histogram::with_opts(
            HistogramOpts::new(
                "pulsar_lite_write_queue_batch_size",
                "Write-queue batch size in messages",
            )
            .buckets(BATCH_SIZE_BUCKETS.to_vec()),
        )?;

        registry.register(Box::new(write_latency.clone()))?;
        registry.register(Box::new(ledger_write_latency.clone()))?;
        registry.register(Box::new(entry_size.clone()))?;
        registry.register(Box::new(wq_batches.clone()))?;
        registry.register(Box::new(wq_batch_messages.clone()))?;
        registry.register(Box::new(wq_batch_size.clone()))?;
        let _ = cluster; // families are broker-global; reserved for future labels

        Ok(Self {
            write_latency,
            ledger_write_latency,
            entry_size,
            wq_batches,
            wq_batch_messages,
            wq_batch_size,
        })
    }
}

impl StorageMetrics {
    /// End-to-end durable-publish latency (seconds).
    pub fn observe_write_latency(&self, seconds: f64) {
        self.write_latency.observe(seconds);
    }

    /// Ledger-append latency (seconds).
    pub fn observe_ledger_write_latency(&self, seconds: f64) {
        self.ledger_write_latency.observe(seconds);
    }

    /// Accepted entry size (metadata + payload bytes).
    pub fn observe_entry_size(&self, bytes: f64) {
        self.entry_size.observe(bytes);
    }

    /// One drained write-queue batch of `messages` entries.
    pub fn observe_batch(&self, messages: u64) {
        self.wq_batches.inc();
        self.wq_batch_messages.inc_by(messages);
        self.wq_batch_size.observe(messages as f64);
    }
}

/// A no-op stand-in used before [`init`]; keeps call sites branch-free.
#[derive(Debug)]
pub struct DisabledStorageMetrics;

impl DisabledStorageMetrics {
    fn observe_write_latency(&self, _seconds: f64) {}
    fn observe_ledger_write_latency(&self, _seconds: f64) {}
    fn observe_entry_size(&self, _bytes: f64) {}
    fn observe_batch(&self, _messages: u64) {}
}

/// Unified accessor so workers can record without checking initialization.
#[derive(Debug, Clone)]
pub enum StorageMetricsHandle {
    Enabled(Arc<StorageMetrics>),
    Disabled(Arc<DisabledStorageMetrics>),
}

impl StorageMetricsHandle {
    pub fn observe_write_latency(&self, seconds: f64) {
        match self {
            Self::Enabled(metrics) => metrics.observe_write_latency(seconds),
            Self::Disabled(metrics) => metrics.observe_write_latency(seconds),
        }
    }

    pub fn observe_ledger_write_latency(&self, seconds: f64) {
        match self {
            Self::Enabled(metrics) => metrics.observe_ledger_write_latency(seconds),
            Self::Disabled(metrics) => metrics.observe_ledger_write_latency(seconds),
        }
    }

    pub fn observe_entry_size(&self, bytes: f64) {
        match self {
            Self::Enabled(metrics) => metrics.observe_entry_size(bytes),
            Self::Disabled(metrics) => metrics.observe_entry_size(bytes),
        }
    }

    pub fn observe_batch(&self, messages: u64) {
        match self {
            Self::Enabled(metrics) => metrics.observe_batch(messages),
            Self::Disabled(metrics) => metrics.observe_batch(messages),
        }
    }
}

static HANDLE: LazyLock<StorageMetricsHandle> = LazyLock::new(|| {
    match STORAGE_METRICS.get() {
        Some(metrics) => StorageMetricsHandle::Enabled(Arc::clone(metrics)),
        None => StorageMetricsHandle::Disabled(Arc::new(DisabledStorageMetrics)),
    }
});

/// Returns the storage metrics handle (no-op before [`init`]).
pub fn storage_metrics() -> StorageMetricsHandle {
    HANDLE.clone()
}

/// Registers storage metric families into the shared global registry.
///
/// Idempotent: the first call wins. Called by broker startup; unit tests
/// that never call it get no-op handles.
pub fn init(cluster: &str) -> Arc<StorageMetrics> {
    STORAGE_METRICS
        .get_or_init(|| match StorageMetrics::new(cluster, &global_registry()) {
            Ok(metrics) => Arc::new(metrics),
            Err(error) => {
                log::error!("Failed to register storage metrics: {}", error);
                // Names are static and test-proven; this branch is unreachable
                // unless the definitions themselves are invalid.
                Arc::new(
                    StorageMetrics::new(cluster, &Registry::new())
                        .expect("static storage metric definitions are valid"),
                )
            }
        })
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_registry_is_stable_across_calls() {
        assert!(Arc::ptr_eq(&global_registry(), &global_registry()));
    }

    #[test]
    fn handle_before_init_is_noop() {
        // Do not call init(): the disabled handle must swallow observations.
        let handle = storage_metrics();
        handle.observe_write_latency(0.1);
        handle.observe_ledger_write_latency(0.2);
        handle.observe_entry_size(42.0);
        handle.observe_batch(7);
    }

    #[test]
    fn init_registers_enabled_handle() {
        let metrics = init("test-cluster");
        metrics.observe_write_latency(0.001);
        assert_eq!(metrics.write_latency.get_sample_count(), 1);
    }
}
