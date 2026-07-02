//! Storage composition entry: combines metadata resources and managed-ledger backends.

mod backend;
mod config;
mod error;
mod service;

pub use backend::ManagedLedgerStore;
pub use config::{ManagedLedgerBackendConfig, StorageConfig};
pub use error::StorageResult;
pub use service::Storage;
