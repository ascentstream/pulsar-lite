use super::{CursorInitOptions, CursorOpenResult, ManagedLedgerPosition, MessageId};
use anyhow::Result;

/// Storage-level abstraction mirroring the role of Pulsar's managed-ledger
/// storage integration, while remaining in-memory for now.
pub trait ManagedLedgerStorage: Send + Sync {
    fn create_topic(&mut self, name: &str) -> Result<()>;

    fn append_message(&mut self, topic: &str, partition: i32, data: &[u8]) -> Result<MessageId>;

    fn subscribe(&mut self, topic: &str, subscription: &str) -> Result<()>;

    fn initialize_or_open_cursor(
        &mut self,
        topic: &str,
        subscription: &str,
        options: CursorInitOptions,
    ) -> Result<CursorOpenResult> {
        let _ = (topic, subscription, options);
        anyhow::bail!("initialize_or_open_cursor is not implemented for this managed-ledger store")
    }

    fn first_unacked_position(
        &self,
        topic: &str,
        subscription: &str,
    ) -> Result<Option<ManagedLedgerPosition>> {
        let _ = (topic, subscription);
        anyhow::bail!("first_unacked_position is not implemented for this managed-ledger store")
    }

    fn read_from(
        &self,
        topic: &str,
        from: &ManagedLedgerPosition,
        limit: usize,
    ) -> Result<Vec<(MessageId, Vec<u8>)>> {
        let _ = (topic, from, limit);
        anyhow::bail!("read_from is not implemented for this managed-ledger store")
    }

    fn get_last_position(&self, topic: &str) -> Result<Option<ManagedLedgerPosition>> {
        let _ = topic;
        anyhow::bail!("get_last_position is not implemented for this managed-ledger store")
    }

    fn get_next_position(
        &self,
        topic: &str,
        current: &ManagedLedgerPosition,
    ) -> Result<Option<ManagedLedgerPosition>> {
        let _ = (topic, current);
        anyhow::bail!("get_next_position is not implemented for this managed-ledger store")
    }

    fn is_acknowledged(
        &self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
    ) -> Result<bool> {
        let _ = (topic, subscription, message_id);
        anyhow::bail!("is_acknowledged is not implemented for this managed-ledger store")
    }

    fn get_next_unassigned_message(
        &mut self,
        topic: &str,
        subscription: &str,
        consumer_id: u64,
    ) -> Result<Option<(MessageId, Vec<u8>)>>;

    fn ack_message(&mut self, topic: &str, subscription: &str, message_id: MessageId)
        -> Result<()>;

    fn ack_message_shared(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: MessageId,
    ) -> Result<()>;

    fn get_message_by_id(
        &self,
        topic: &str,
        message_id: &MessageId,
    ) -> Option<(MessageId, Vec<u8>)>;

    fn get_messages(&self, topic: &str) -> Vec<(MessageId, Vec<u8>)>;

    fn is_acknowledged_shared(
        &self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
    ) -> bool;

    fn get_mark_delete_position(&self, topic: &str, subscription: &str) -> Option<u64>;
}
