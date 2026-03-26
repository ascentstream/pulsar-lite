use super::MessageId;
use anyhow::Result;

/// Storage-level abstraction mirroring the role of Pulsar's managed-ledger
/// storage integration, while remaining in-memory for now.
pub trait ManagedLedgerStorage: Send + Sync {
    fn create_topic(&mut self, name: &str) -> Result<()>;

    fn append_message(&mut self, topic: &str, partition: i32, data: &[u8]) -> Result<MessageId>;

    fn subscribe(&mut self, topic: &str, subscription: &str) -> Result<()>;

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
