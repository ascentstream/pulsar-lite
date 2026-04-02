/// Transitional in-memory managed-ledger storage shell.
///
/// This is intentionally kept as a pure skeleton. Runtime message and cursor
/// behavior still lives in `storage::Storage`; this type only reserves the
/// eventual integration point.
#[derive(Debug, Default)]
pub struct InMemoryManagedLedgerStorage;

impl InMemoryManagedLedgerStorage {
    pub fn new() -> Self {
        Self
    }
}

/// Placeholder factory type for the managed-ledger migration.
#[derive(Debug, Default)]
pub struct InMemoryManagedLedgerFactory;

impl InMemoryManagedLedgerFactory {
    pub fn new() -> Self {
        Self
    }
}

/// Placeholder ledger type for the managed-ledger migration.
#[derive(Debug, Clone, Default)]
pub struct InMemoryManagedLedger;

impl InMemoryManagedLedger {
    pub fn new(_name: &str) -> Self {
        Self
    }
}

/// Placeholder cursor type for the managed-ledger migration.
#[derive(Debug, Clone, Default)]
pub struct InMemoryManagedCursor;

impl InMemoryManagedCursor {
    pub fn new(_name: &str) -> Self {
        Self
    }
}
