//! Managed-ledger module: re-exports the managed-ledger crate types.
pub use pulsar_lite_storage_managed_ledger::{
    CursorInitOptions, CursorOpenResult, InitialPosition, InMemoryManagedCursor,
    InMemoryManagedLedger, InMemoryManagedLedgerFactory, InMemoryManagedLedgerStorage,
    ManagedCursor, ManagedCursorState, ManagedLedger, ManagedLedgerConfig, ManagedLedgerFactory,
    ManagedLedgerPosition, ManagedLedgerStorage, MessageId, NonPersistentEntry, StoredMessage,
    SubscriptionCursor,
};
