//! Managed-ledger module: re-exports the managed-ledger crate types.
pub use pulsar_lite_storage_managed_ledger::{
    CursorInitOptions, CursorOpenResult, InMemoryManagedCursor, InMemoryManagedLedger,
    InMemoryManagedLedgerFactory, InMemoryManagedLedgerStorage, InitialPosition, ManagedCursor,
    ManagedCursorState, ManagedLedger, ManagedLedgerConfig, ManagedLedgerFactory,
    ManagedLedgerPosition, ManagedLedgerStorage, MessageId, NonPersistentEntry, StoredMessage,
    SubscriptionCursor,
};
