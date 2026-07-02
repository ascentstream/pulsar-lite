use std::path::{Path, PathBuf};

/// Managed-ledger backend selection for `Storage`.
#[derive(Debug, Clone)]
pub enum ManagedLedgerBackendConfig {
    Memory,
    #[cfg(feature = "rocksdb-storage")]
    RocksDb { path: PathBuf },
}

/// Storage construction config: metadata file path + managed-ledger backend.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub metadata_path: PathBuf,
    pub managed_ledger: ManagedLedgerBackendConfig,
}

impl StorageConfig {
    pub fn memory(metadata_path: impl AsRef<Path>) -> Self {
        Self {
            metadata_path: metadata_path.as_ref().to_path_buf(),
            managed_ledger: ManagedLedgerBackendConfig::Memory,
        }
    }

    #[cfg(feature = "rocksdb-storage")]
    pub fn rocksdb(metadata_path: impl AsRef<Path>) -> Self {
        let metadata_path = metadata_path.as_ref().to_path_buf();
        Self {
            managed_ledger: ManagedLedgerBackendConfig::RocksDb {
                path: metadata_path.with_extension("rocksdb"),
            },
            metadata_path,
        }
    }
}
