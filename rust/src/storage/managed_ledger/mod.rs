//! Managed-ledger module: re-exports the new managed-ledger crate + keeps the
//! transitional `ManagedLedgerStore` enum (store.rs) until Phase 7 moves it to
//! storage/core.
mod store;

pub use pulsar_lite_storage_managed_ledger::{
    CursorInitOptions, CursorOpenResult, InitialPosition, InMemoryManagedCursor,
    InMemoryManagedLedger, InMemoryManagedLedgerFactory, InMemoryManagedLedgerStorage,
    ManagedCursor, ManagedCursorState, ManagedLedger, ManagedLedgerConfig, ManagedLedgerFactory,
    ManagedLedgerPosition, ManagedLedgerStorage, MessageId, NonPersistentEntry, StoredMessage,
    SubscriptionCursor,
};
pub use store::ManagedLedgerStore;
