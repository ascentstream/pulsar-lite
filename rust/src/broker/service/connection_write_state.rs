use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Byte budget for the per-connection message channel (dispatcher -> socket).
/// Dispatch pauses when buffered-but-unencoded message bytes exceed this,
/// so fan-out scenarios (e.g. 8 subscriptions) cannot flood the channel
/// faster than the client can drain it. Roughly 64 entries of ~1MB each.
const DEFAULT_MAX_CHANNEL_BYTES: usize = 64 * 1024 * 1024;

/// Shared connection-level outbound write state.
///
/// This approximates Pulsar/Netty channel writability semantics: once the
/// amount of outbound data pending flush crosses the high watermark, the
/// connection becomes non-writable; it becomes writable again only after the
/// pending bytes fall below the low watermark.
#[derive(Debug)]
pub struct ConnectionWriteState {
    pending_bytes: AtomicUsize,
    writable: AtomicBool,
    high_watermark_bytes: usize,
    low_watermark_bytes: usize,
    /// Unencoded message bytes sitting in the mpsc channel (dispatcher side).
    channel_pending_bytes: AtomicUsize,
    max_channel_bytes: usize,
}

impl ConnectionWriteState {
    pub fn new(high_watermark_bytes: usize, low_watermark_bytes: usize) -> Self {
        assert!(high_watermark_bytes > 0, "high watermark must be positive");
        assert!(
            low_watermark_bytes <= high_watermark_bytes,
            "low watermark must be <= high watermark"
        );
        Self {
            pending_bytes: AtomicUsize::new(0),
            writable: AtomicBool::new(true),
            high_watermark_bytes,
            low_watermark_bytes,
            channel_pending_bytes: AtomicUsize::new(0),
            max_channel_bytes: DEFAULT_MAX_CHANNEL_BYTES,
        }
    }

    pub fn is_writable(&self) -> bool {
        self.writable.load(Ordering::Acquire)
    }

    pub fn pending_bytes(&self) -> usize {
        self.pending_bytes.load(Ordering::Acquire)
    }

    /// Mirror the bytes currently buffered by the connection's outbound write buffer.
    pub fn observe_buffered_bytes(&self, bytes: usize) {
        self.pending_bytes.store(bytes, Ordering::Release);
        let currently_writable = self.writable.load(Ordering::Acquire);
        let next_writable = if bytes >= self.high_watermark_bytes {
            false
        } else if bytes <= self.low_watermark_bytes {
            true
        } else {
            currently_writable
        };
        self.writable.store(next_writable, Ordering::Release);
    }

    /// Try to reserve `bytes` of channel budget. Returns false (and reserves
    /// nothing) when the channel would exceed the byte cap.
    pub fn try_reserve_channel_bytes(&self, bytes: usize) -> bool {
        let current = self.channel_pending_bytes.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_add(bytes);
            if next > self.max_channel_bytes {
                return false;
            }
            match self.channel_pending_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => {
                    let current = actual;
                    let _ = current;
                }
            }
        }
    }

    /// Release channel budget after the run loop dequeues a message.
    pub fn release_channel_bytes(&self, bytes: usize) {
        self.channel_pending_bytes
            .fetch_sub(bytes, Ordering::Relaxed);
    }

    pub fn high_watermark_bytes(&self) -> usize {
        self.high_watermark_bytes
    }

    pub fn low_watermark_bytes(&self) -> usize {
        self.low_watermark_bytes
    }

    /// Compatibility helper for tests that want to simulate a buffered-bytes snapshot.
    pub fn release_bytes(&self, bytes: usize) {
        let next = self
            .pending_bytes
            .load(Ordering::Acquire)
            .saturating_sub(bytes);
        if next <= self.low_watermark_bytes {
            self.writable.store(true, Ordering::Release);
        }
        self.pending_bytes.store(next, Ordering::Release);
    }
}
