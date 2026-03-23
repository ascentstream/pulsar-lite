//! Managed-ledger style persistence skeleton.
//!
//! This module is the future landing zone for durable message persistence,
//! ledger/cursor abstractions, and factory/config wiring. The current runtime
//! message state still lives in `storage::Storage`; these types only define the
//! target shape for that later migration.

mod config;
mod cursor;
mod factory;
mod ledger;
mod memory;
mod types;

pub use config::ManagedLedgerConfig;
pub use cursor::{ManagedCursor, ManagedCursorState};
pub use factory::ManagedLedgerFactory;
pub use ledger::ManagedLedger;
pub use memory::{
    InMemoryManagedCursor, InMemoryManagedLedger, InMemoryManagedLedgerFactory,
    InMemoryManagedLedgerStorage,
};
pub use types::ManagedLedgerPosition;
