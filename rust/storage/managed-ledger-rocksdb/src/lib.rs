//! RocksDB backend for managed-ledger storage.
mod cursor;
mod entrylog;
mod factory;
mod keys;
mod ledger;
mod metadata;
mod store;

pub use store::RocksDbManagedLedgerStorage;

/// Internal types exposed for integration tests in `tests/`.
#[doc(hidden)]
pub mod test_support {
    pub use crate::cursor::{ack_managed_cursor_shared, RocksDBManagedCursor};
    pub use crate::entrylog::EntryLogStore;
    pub use crate::factory::RocksDBManagedLedgerFactory;
    pub use crate::ledger::RocksDBManagedLedger;
    pub use crate::metadata::{proto, StoredEntryLocation, StoredManagedLedgerInfo};

    pub mod keys {
        pub use crate::keys::{
            encode_cursor_name, managed_cursor_key, managed_entry_key, managed_ledger_key,
            managed_ledger_name,
        };
    }
}
