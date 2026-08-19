//! RocksDB backend for managed-ledger storage.
mod cursor;
mod entrylog;
mod factory;
mod keys;
mod ledger;
mod metadata;
mod store;
mod write_queue;

pub use store::{ConcurrentAppender, RocksDbManagedLedgerStorage};
pub use write_queue::ConnAppendResult;
pub use pulsar_lite_metrics::PublishCommitObserver;

/// Internal types exposed for integration tests in `tests/`.
#[doc(hidden)]
pub mod test_support {
    pub use crate::cursor::{
        ack_managed_cursor_shared, is_managed_position_acknowledged, RocksDBManagedCursor,
    };
    pub use crate::entrylog::{EntryLogStore, EntryToAppend};
    pub use crate::factory::RocksDBManagedLedgerFactory;
    pub use crate::ledger::RocksDBManagedLedger;
    pub use crate::metadata::{proto, StoredEntryLocation, StoredManagedLedgerInfo};

    pub mod keys {
        pub use crate::keys::{
            encode_cursor_name, managed_cursor_key, managed_entry_key, managed_ledger_key,
            managed_ledger_name,
        };
    }

    use anyhow::Result;
    use pulsar_lite_storage_managed_ledger::ManagedLedgerPosition;

    /// Test-only durable append (crate-private API). Production must use WriteQueue.
    pub fn append_payload(
        ledger: &RocksDBManagedLedger,
        payload: &[u8],
    ) -> Result<ManagedLedgerPosition> {
        append_with_partition(ledger, -1, payload)
    }

    /// Test-only durable append with partition.
    pub fn append_with_partition(
        ledger: &RocksDBManagedLedger,
        partition: i32,
        payload: &[u8],
    ) -> Result<ManagedLedgerPosition> {
        let mut positions = ledger.add_entries_with_partition_and_metadata(&[(
            partition,
            &[] as &[u8],
            payload,
        )])?;
        positions
            .pop()
            .ok_or_else(|| anyhow::anyhow!("add_entries returned empty positions"))
    }
}
