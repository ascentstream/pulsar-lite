use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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
