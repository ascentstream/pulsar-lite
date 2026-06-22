use super::cursor_init::{CursorInitOptions, CursorOpenResult, InitialPosition};
use super::cursor_read::{
    cursor_subscription_key, first_unacked_from_messages, last_position_from_messages,
    next_position_single_ledger, read_from_messages,
};
use super::{
    ack_shared, is_message_acknowledged, ManagedCursor, ManagedCursorState, ManagedLedger,
    ManagedLedgerConfig, ManagedLedgerFactory, ManagedLedgerPosition, ManagedLedgerStorage,
    MessageId, StoredMessage, SubscriptionCursor,
};
use anyhow::Result;
use log::debug;
use std::collections::HashMap;

/// In-memory managed-ledger style storage used by the current runtime.
#[derive(Debug, Default)]
pub struct InMemoryManagedLedgerStorage {
    factory: InMemoryManagedLedgerFactory,
    cursors: HashMap<String, u64>,
    subscription_cursors: HashMap<String, SubscriptionCursor>,
    entry_metadata: HashMap<MessageId, Vec<u8>>,
}

impl InMemoryManagedLedgerStorage {
    pub fn new() -> Self {
        Self::default()
    }

    fn cursor_key(topic: &str, subscription: &str) -> String {
        cursor_subscription_key(topic, subscription)
    }

    fn cursor_exists(&self, key: &str) -> bool {
        self.cursors.contains_key(key) || self.subscription_cursors.contains_key(key)
    }

    fn messages(&self, topic: &str) -> Vec<(MessageId, Vec<u8>)> {
        self.factory.messages(topic).cloned().unwrap_or_default()
    }

    fn stored_message(&self, message_id: MessageId, payload: Vec<u8>) -> StoredMessage {
        let metadata = self
            .entry_metadata
            .get(&message_id)
            .cloned()
            .unwrap_or_default();
        StoredMessage::new(message_id, metadata, payload)
    }

    fn is_acknowledged_inner(
        &self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
    ) -> bool {
        let key = Self::cursor_key(topic, subscription);

        if let Some(shared) = self.subscription_cursors.get(&key) {
            return is_message_acknowledged(Some(shared), message_id.entry);
        }

        let mark_delete = self.cursors.get(&key).copied().unwrap_or(u64::MAX);
        mark_delete != u64::MAX && message_id.entry <= mark_delete
    }

    fn ensure_exclusive_cursor(&mut self, key: &str) {
        self.cursors.entry(key.to_string()).or_insert(u64::MAX);
    }

    fn apply_latest_exclusive(&mut self, key: &str, topic: &str) {
        if let Some(last) = last_position_from_messages(&self.messages(topic)) {
            self.cursors.insert(key.to_string(), last.entry_id);
        } else {
            self.ensure_exclusive_cursor(key);
        }
    }

    fn apply_start_message_id_exclusive(&mut self, key: &str, start: &MessageId) {
        if start.entry > 0 {
            self.cursors.insert(key.to_string(), start.entry - 1);
        } else {
            self.ensure_exclusive_cursor(key);
        }
    }
}

impl ManagedLedgerStorage for InMemoryManagedLedgerStorage {
    fn create_topic(&mut self, name: &str) -> Result<()> {
        self.factory.ensure_ledger(name);
        Ok(())
    }

    fn append_message(&mut self, topic: &str, partition: i32, data: &[u8]) -> Result<MessageId> {
        Ok(self.factory.append_entry(topic, partition, data))
    }

    fn append_message_with_metadata(
        &mut self,
        topic: &str,
        partition: i32,
        metadata: &[u8],
        payload: &[u8],
    ) -> Result<MessageId> {
        let message_id = self.factory.append_entry(topic, partition, payload);
        if !metadata.is_empty() {
            self.entry_metadata
                .insert(message_id.clone(), metadata.to_vec());
        }
        Ok(message_id)
    }

    fn initialize_or_open_cursor(
        &mut self,
        topic: &str,
        subscription: &str,
        options: CursorInitOptions,
    ) -> Result<CursorOpenResult> {
        let key = Self::cursor_key(topic, subscription);

        if self.cursor_exists(&key) {
            return Ok(CursorOpenResult {
                created: false,
                first_unacked: self.first_unacked_position(topic, subscription)?,
            });
        }

        if let Some(start_id) = options.start_message_id.as_ref() {
            self.apply_start_message_id_exclusive(&key, start_id);
        } else if options.initial_position == InitialPosition::Latest {
            self.apply_latest_exclusive(&key, topic);
        } else {
            self.ensure_exclusive_cursor(&key);
        }

        Ok(CursorOpenResult {
            created: true,
            first_unacked: self.first_unacked_position(topic, subscription)?,
        })
    }

    fn delete_cursor(&mut self, topic: &str, subscription: &str) -> Result<()> {
        let key = Self::cursor_key(topic, subscription);
        self.cursors.remove(&key);
        self.subscription_cursors.remove(&key);
        Ok(())
    }

    fn seek_cursor(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
        shared: bool,
    ) -> Result<()> {
        let key = Self::cursor_key(topic, subscription);
        let target = ManagedLedgerPosition::from(message_id);
        let previous = self
            .messages(topic)
            .into_iter()
            .map(|(stored_id, _)| ManagedLedgerPosition::from(stored_id))
            .take_while(|position| position < &target)
            .last();

        if shared {
            self.cursors.remove(&key);
            self.subscription_cursors.insert(
                key,
                SubscriptionCursor {
                    mark_delete: previous.map(|position| position.entry_id),
                    acked_holes: Default::default(),
                },
            );
        } else {
            self.subscription_cursors.remove(&key);
            self.cursors
                .insert(key, previous.map_or(u64::MAX, |position| position.entry_id));
        }

        Ok(())
    }

    fn first_unacked_position(
        &self,
        topic: &str,
        subscription: &str,
    ) -> Result<Option<ManagedLedgerPosition>> {
        let messages = self.messages(topic);
        Ok(first_unacked_from_messages(&messages, |id| {
            self.is_acknowledged_inner(topic, subscription, id)
        }))
    }

    fn read_from(
        &self,
        topic: &str,
        from: &ManagedLedgerPosition,
        limit: usize,
    ) -> Result<Vec<(MessageId, Vec<u8>)>> {
        let messages = self.messages(topic);
        Ok(read_from_messages(&messages, from, limit))
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
            .map(|(message_id, payload)| self.stored_message(message_id, payload))
            .collect())
    }

    fn get_last_position(&self, topic: &str) -> Result<Option<ManagedLedgerPosition>> {
        Ok(last_position_from_messages(&self.messages(topic)))
    }

    fn get_next_position(
        &self,
        topic: &str,
        current: &ManagedLedgerPosition,
    ) -> Result<Option<ManagedLedgerPosition>> {
        let _ = topic;
        Ok(next_position_single_ledger(current))
    }

    fn is_acknowledged(
        &self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
    ) -> Result<bool> {
        Ok(self.is_acknowledged_inner(topic, subscription, message_id))
    }

    fn ack_message(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: MessageId,
    ) -> Result<()> {
        let cursor_key = format!("{}:{}", topic, subscription);
        self.cursors.insert(cursor_key, message_id.entry);
        Ok(())
    }

    fn ack_message_shared(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: MessageId,
    ) -> Result<()> {
        let cursor_key = format!("{}:{}", topic, subscription);
        let (mark_delete, holes_count) = {
            let cursor = self.subscription_cursors.entry(cursor_key).or_default();
            ack_shared(cursor, message_id.entry)
        };

        debug!(
            "Shared ack: topic={}, sub={}, entry={}, mark_delete={:?}, holes_count={}",
            topic, subscription, message_id.entry, mark_delete, holes_count
        );

        Ok(())
    }

    fn get_message_by_id(
        &self,
        topic: &str,
        message_id: &MessageId,
    ) -> Option<(MessageId, Vec<u8>)> {
        self.factory.get_message_by_id(topic, message_id)
    }

    fn get_message_entry_by_id(
        &self,
        topic: &str,
        message_id: &MessageId,
    ) -> Option<StoredMessage> {
        self.get_message_by_id(topic, message_id)
            .map(|(message_id, payload)| self.stored_message(message_id, payload))
    }

    fn get_messages(&self, topic: &str) -> Vec<(MessageId, Vec<u8>)> {
        self.factory.messages(topic).cloned().unwrap_or_default()
    }

    fn get_message_entries(&self, topic: &str) -> Vec<StoredMessage> {
        self.get_messages(topic)
            .into_iter()
            .map(|(message_id, payload)| self.stored_message(message_id, payload))
            .collect()
    }

    fn is_acknowledged_shared(
        &self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
    ) -> bool {
        let cursor_key = format!("{}:{}", topic, subscription);
        is_message_acknowledged(self.subscription_cursors.get(&cursor_key), message_id.entry)
    }

    fn get_mark_delete_position(&self, topic: &str, subscription: &str) -> Option<u64> {
        let cursor_key = format!("{}:{}", topic, subscription);
        self.subscription_cursors.get(&cursor_key)?.mark_delete
    }
}

#[derive(Debug, Default)]
pub struct InMemoryManagedLedgerFactory {
    ledgers: HashMap<String, InMemoryManagedLedger>,
    next_ledger_id: u64,
}

impl InMemoryManagedLedgerFactory {
    pub fn new() -> Self {
        Self::default()
    }

    fn ensure_ledger(&mut self, name: &str) -> &mut InMemoryManagedLedger {
        self.ledgers.entry(name.to_string()).or_insert_with(|| {
            let ledger_id = self.next_ledger_id;
            self.next_ledger_id += 1;
            InMemoryManagedLedger::new(name, ledger_id)
        })
    }

    fn append_entry(&mut self, topic: &str, partition: i32, data: &[u8]) -> MessageId {
        self.ensure_ledger(topic)
            .add_entry_in_place(partition, data)
    }

    fn messages(&self, topic: &str) -> Option<&Vec<(MessageId, Vec<u8>)>> {
        self.ledgers.get(topic).map(|ledger| &ledger.entries)
    }

    fn get_message_by_id(
        &self,
        topic: &str,
        message_id: &MessageId,
    ) -> Option<(MessageId, Vec<u8>)> {
        let messages = self.messages(topic)?;
        if let Some((stored_id, data)) = messages.get(message_id.entry as usize) {
            if stored_id == message_id {
                return Some((stored_id.clone(), data.clone()));
            }
        }

        messages
            .iter()
            .find(|(stored_id, _)| stored_id == message_id)
            .map(|(stored_id, data)| (stored_id.clone(), data.clone()))
    }
}

impl ManagedLedgerFactory for InMemoryManagedLedgerFactory {
    type Ledger = InMemoryManagedLedger;

    fn open(&mut self, name: &str, _config: &ManagedLedgerConfig) -> Result<Self::Ledger> {
        Ok(self.ensure_ledger(name).clone())
    }
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryManagedLedger {
    name: String,
    ledger_id: u64,
    entries: Vec<(MessageId, Vec<u8>)>,
}

impl InMemoryManagedLedger {
    pub fn new(name: &str, ledger_id: u64) -> Self {
        Self {
            name: name.to_string(),
            ledger_id,
            entries: Vec::new(),
        }
    }

    fn add_entry_in_place(&mut self, partition: i32, payload: &[u8]) -> MessageId {
        let message_id = MessageId {
            ledger: self.ledger_id,
            entry: self.entries.len() as u64,
            partition,
        };
        self.entries.push((message_id.clone(), payload.to_vec()));
        message_id
    }
}

impl ManagedLedger for InMemoryManagedLedger {
    type Cursor = InMemoryManagedCursor;

    fn name(&self) -> &str {
        &self.name
    }

    fn add_entry(&mut self, payload: &[u8]) -> Result<ManagedLedgerPosition> {
        let message_id = self.add_entry_in_place(-1, payload);
        Ok(ManagedLedgerPosition::from(message_id))
    }

    fn open_cursor(&mut self, name: &str) -> Result<Self::Cursor> {
        Ok(InMemoryManagedCursor::new(name))
    }

    fn read_entry(&self, position: &ManagedLedgerPosition) -> Option<Vec<u8>> {
        self.entries
            .iter()
            .find(|(message_id, _)| {
                message_id.ledger == position.ledger_id && message_id.entry == position.entry_id
            })
            .map(|(_, payload)| payload.clone())
    }
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryManagedCursor {
    name: String,
    state: ManagedCursorState,
}

impl InMemoryManagedCursor {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            state: ManagedCursorState::default(),
        }
    }
}

impl ManagedCursor for InMemoryManagedCursor {
    fn name(&self) -> &str {
        &self.name
    }

    fn state(&self) -> &ManagedCursorState {
        &self.state
    }

    fn mark_delete(&mut self, position: ManagedLedgerPosition) -> Result<()> {
        self.state.mark_delete = Some(position);
        Ok(())
    }

    fn delete_individual(&mut self, position: ManagedLedgerPosition) -> Result<()> {
        self.state.individually_deleted_entries.insert(position);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_lookup_message_by_id() {
        let mut storage = InMemoryManagedLedgerStorage::new();
        storage
            .create_topic("persistent://public/default/test")
            .unwrap();

        let msg0 = storage
            .append_message("persistent://public/default/test", -1, b"0")
            .unwrap();
        let msg1 = storage
            .append_message("persistent://public/default/test", -1, b"1")
            .unwrap();

        let found = storage
            .get_message_by_id("persistent://public/default/test", &msg1)
            .unwrap();
        assert_eq!(found.0, msg1);
        assert_eq!(found.1, b"1".to_vec());
        assert_eq!(msg0.entry, 0);
    }

    #[test]
    fn first_unacked_skips_individual_deleted_hole() {
        let mut storage = InMemoryManagedLedgerStorage::new();
        let topic = "persistent://public/default/hole";
        let sub = "sub";

        storage.create_topic(topic).unwrap();
        storage
            .initialize_or_open_cursor(
                topic,
                sub,
                CursorInitOptions {
                    initial_position: InitialPosition::Earliest,
                    start_message_id: None,
                },
            )
            .unwrap();

        let msg0 = storage.append_message(topic, -1, b"0").unwrap();
        storage.append_message(topic, -1, b"1").unwrap();
        let msg2 = storage.append_message(topic, -1, b"2").unwrap();

        storage.ack_message_shared(topic, sub, msg0).unwrap();
        storage.ack_message_shared(topic, sub, msg2).unwrap();

        let first = storage.first_unacked_position(topic, sub).unwrap().unwrap();
        assert_eq!(first.entry_id, 1);
    }

    #[test]
    fn latest_skips_existing_backlog_on_new_cursor() {
        let mut storage = InMemoryManagedLedgerStorage::new();
        let topic = "persistent://public/default/latest";
        let sub = "sub";

        storage.create_topic(topic).unwrap();
        storage.append_message(topic, -1, b"old").unwrap();

        let result = storage
            .initialize_or_open_cursor(
                topic,
                sub,
                CursorInitOptions {
                    initial_position: InitialPosition::Latest,
                    start_message_id: None,
                },
            )
            .unwrap();

        assert!(result.created);
        assert!(result.first_unacked.is_none());
    }

    #[test]
    fn start_message_id_positions_new_cursor_at_requested_message() {
        let mut storage = InMemoryManagedLedgerStorage::new();
        let topic = "persistent://public/default/start-message-id";
        let sub = "sub";

        storage.create_topic(topic).unwrap();
        storage.append_message(topic, -1, b"before").unwrap();
        let start = storage.append_message(topic, -1, b"start").unwrap();
        storage.append_message(topic, -1, b"after").unwrap();

        let result = storage
            .initialize_or_open_cursor(
                topic,
                sub,
                CursorInitOptions {
                    initial_position: InitialPosition::Latest,
                    start_message_id: Some(start.clone()),
                },
            )
            .unwrap();

        assert!(result.created);
        assert_eq!(result.first_unacked.unwrap().entry_id, start.entry);
    }
}
