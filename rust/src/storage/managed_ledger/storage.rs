use super::{CursorInitOptions, CursorOpenResult, ManagedLedgerPosition, MessageId, StoredMessage};
use anyhow::Result;
use std::future::Future;

/// Storage-level abstraction mirroring the role of Pulsar's managed-ledger
/// storage integration, while remaining in-memory for now.
pub trait ManagedLedgerStorage: Send + Sync {
    fn create_topic(&mut self, name: &str) -> Result<()>;

    fn append_message(&mut self, topic: &str, partition: i32, data: &[u8]) -> Result<MessageId>;

    fn append_message_with_metadata(
        &mut self,
        topic: &str,
        partition: i32,
        metadata: &[u8],
        payload: &[u8],
    ) -> Result<MessageId> {
        let _ = metadata;
        self.append_message(topic, partition, payload)
    }

    fn initialize_or_open_cursor(
        &mut self,
        topic: &str,
        subscription: &str,
        options: CursorInitOptions,
    ) -> Result<CursorOpenResult> {
        let _ = (topic, subscription, options);
        anyhow::bail!("initialize_or_open_cursor is not implemented for this managed-ledger store")
    }

    fn delete_cursor(&mut self, topic: &str, subscription: &str) -> Result<()> {
        let _ = (topic, subscription);
        anyhow::bail!("delete_cursor is not implemented for this managed-ledger store")
    }

    fn seek_cursor(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
        shared: bool,
    ) -> impl Future<Output = Result<()>> + Send {
        async move {
            let _ = (topic, subscription, message_id, shared);
            anyhow::bail!("seek_cursor is not implemented for this managed-ledger store")
        }
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

    fn read_entries_from(
        &self,
        topic: &str,
        from: &ManagedLedgerPosition,
        limit: usize,
    ) -> Result<Vec<StoredMessage>> {
        Ok(self
            .read_from(topic, from, limit)?
            .into_iter()
            .map(|(message_id, payload)| StoredMessage::from_payload(message_id, payload))
            .collect())
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

    fn get_message_entry_by_id(
        &self,
        topic: &str,
        message_id: &MessageId,
    ) -> Option<StoredMessage> {
        self.get_message_by_id(topic, message_id)
            .map(|(message_id, payload)| StoredMessage::from_payload(message_id, payload))
    }

    fn get_messages(&self, topic: &str) -> Vec<(MessageId, Vec<u8>)>;

    fn get_message_entries(&self, topic: &str) -> Vec<StoredMessage> {
        self.get_messages(topic)
            .into_iter()
            .map(|(message_id, payload)| StoredMessage::from_payload(message_id, payload))
            .collect()
    }

    fn is_acknowledged_shared(
        &self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
    ) -> bool;

    fn get_mark_delete_position(&self, topic: &str, subscription: &str) -> Option<u64>;

    fn find_message_id_by_publish_time(
        &self,
        topic: &str,
        publish_time: u64,
    ) -> Result<Option<MessageId>> {
        let entries = self.get_message_entries(topic);
        if entries.is_empty() {
            return Ok(None);
        }
        let mut last_earlier: Option<usize> = None;
        for (i, entry) in entries.iter().enumerate() {
            match super::super::decode_publish_time(&entry.metadata) {
                Some(pt) if pt < publish_time => last_earlier = Some(i),
                Some(_) => break,
                None => {}
            }
        }
        let target = match last_earlier {
            None => Some(entries[0].message_id.clone()),
            Some(i) => entries.get(i + 1).map(|e| e.message_id.clone()),
        };
        Ok(target)
    }
}
