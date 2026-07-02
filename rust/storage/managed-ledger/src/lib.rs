//! Managed-ledger core: traits, positions, messages, and in-memory backend.
mod config;
mod cursor;
mod cursor_init;
mod cursor_read;
mod factory;
mod ledger;
mod legacy_storage;
mod memory;
mod position;

pub use config::ManagedLedgerConfig;
pub use cursor::{
    ack_shared, advance_mark_delete, is_message_acknowledged, ManagedCursor, ManagedCursorState,
    SubscriptionCursor,
};
pub use cursor_init::{CursorInitOptions, CursorOpenResult, InitialPosition};
pub use cursor_read::{
    cursor_subscription_key, first_unacked_from_messages, last_position_from_messages,
    next_position_single_ledger, position_at_or_after, read_from_messages,
};
pub use factory::ManagedLedgerFactory;
pub use ledger::ManagedLedger;
pub use legacy_storage::ManagedLedgerStorage;
pub use memory::{
    InMemoryManagedCursor, InMemoryManagedLedger, InMemoryManagedLedgerFactory,
    InMemoryManagedLedgerStorage,
};
pub use position::{ManagedLedgerPosition, MessageId, NonPersistentEntry, StoredMessage};
