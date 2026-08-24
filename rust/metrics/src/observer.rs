//! Publish accounting hook implemented by the broker.

use std::sync::Arc;

/// Per-topic publish accounting hook implemented by the broker.
///
/// The write-queue worker calls it once per successfully committed group
/// with the group's message and byte totals, so broker counters fold whole
/// batches into single atomic updates while storage crates stay free of
/// any metrics-family knowledge.
pub trait PublishCommitObserver: Send + Sync {
    fn on_commit(&self, messages: u64, bytes: u64);
}

/// Convenience alias for pre-resolved observer handles carried per request.
pub type SharedObserver = Arc<dyn PublishCommitObserver>;
