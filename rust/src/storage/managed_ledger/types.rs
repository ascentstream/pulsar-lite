/// Position inside a managed-ledger style append-only log.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct ManagedLedgerPosition {
    pub ledger_id: u64,
    pub entry_id: u64,
    pub partition: i32,
}
