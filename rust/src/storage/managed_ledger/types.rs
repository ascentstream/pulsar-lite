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

/// Message id used by broker/runtime APIs.
///
/// This remains the public message identity type for `pulsar-lite`, while the
/// managed-ledger line uses `ManagedLedgerPosition` as its structural analogue.
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
pub struct MessageId {
    pub ledger: u64,
    pub entry: u64,
    pub partition: i32,
}

impl From<&MessageId> for ManagedLedgerPosition {
    fn from(value: &MessageId) -> Self {
        Self {
            ledger_id: value.ledger,
            entry_id: value.entry,
            partition: value.partition,
        }
    }
}

impl From<MessageId> for ManagedLedgerPosition {
    fn from(value: MessageId) -> Self {
        Self::from(&value)
    }
}

impl From<&ManagedLedgerPosition> for MessageId {
    fn from(value: &ManagedLedgerPosition) -> Self {
        Self {
            ledger: value.ledger_id,
            entry: value.entry_id,
            partition: value.partition,
        }
    }
}

impl From<ManagedLedgerPosition> for MessageId {
    fn from(value: ManagedLedgerPosition) -> Self {
        Self::from(&value)
    }
}
