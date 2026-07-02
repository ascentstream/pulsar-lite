/// Managed-ledger configuration skeleton.
///
/// This intentionally stays small for now. The goal is to reserve a stable
/// location for future ledger/cursor retention and factory-level settings.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManagedLedgerConfig {
    pub storage_class: Option<String>,
    pub max_entries_per_ledger: Option<u64>,
    pub retention_time_secs: Option<u64>,
}
