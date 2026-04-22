use bytes::Bytes;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// Position inside a managed-ledger style append-only log.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
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
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
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

/// Runtime entry aligned with Pulsar's managed-ledger `Entry` shape.
///
/// This type is used by the non-persistent runtime as a transient message
/// carrier. It keeps ledger/entry ids, partition, ref-counted message bytes,
/// and an explicit per-instance release marker.
#[derive(Debug, Clone)]
pub struct NonPersistentEntry {
    position: ManagedLedgerPosition,
    metadata: Bytes,
    payload: Bytes,
    released: Arc<AtomicBool>,
}

impl NonPersistentEntry {
    pub fn create(
        ledger_id: u64,
        entry_id: u64,
        partition: i32,
        metadata: Bytes,
        payload: Bytes,
    ) -> Self {
        Self {
            position: ManagedLedgerPosition {
                ledger_id,
                entry_id,
                partition,
            },
            metadata,
            payload,
            released: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn create_with_vecs(
        ledger_id: u64,
        entry_id: u64,
        partition: i32,
        metadata: Vec<u8>,
        payload: Vec<u8>,
    ) -> Self {
        Self::create(
            ledger_id,
            entry_id,
            partition,
            Bytes::from(metadata),
            Bytes::from(payload),
        )
    }

    pub fn retained_duplicate(&self) -> Self {
        Self::create(
            self.position.ledger_id,
            self.position.entry_id,
            self.position.partition,
            self.metadata.clone(),
            self.payload.clone(),
        )
    }

    pub fn position(&self) -> ManagedLedgerPosition {
        self.position.clone()
    }

    pub fn ledger_id(&self) -> u64 {
        self.position.ledger_id
    }

    pub fn entry_id(&self) -> u64 {
        self.position.entry_id
    }

    pub fn partition(&self) -> i32 {
        self.position.partition
    }

    pub fn metadata(&self) -> &[u8] {
        self.metadata.as_ref()
    }

    pub fn metadata_bytes(&self) -> Bytes {
        self.metadata.clone()
    }

    pub fn payload(&self) -> &[u8] {
        self.payload.as_ref()
    }

    pub fn payload_bytes(&self) -> Bytes {
        self.payload.clone()
    }

    pub fn len(&self) -> usize {
        self.metadata.len() + self.payload.len()
    }

    pub fn is_empty(&self) -> bool {
        self.metadata.is_empty() && self.payload.is_empty()
    }

    pub fn is_released(&self) -> bool {
        self.released.load(Ordering::Relaxed)
    }

    pub fn release(&self) -> bool {
        !self.released.swap(true, Ordering::Relaxed)
    }
}
