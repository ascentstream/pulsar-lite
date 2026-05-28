use super::{
    ManagedCursor, ManagedCursorState, ManagedLedger, ManagedLedgerConfig, ManagedLedgerFactory,
    ManagedLedgerPosition, ManagedLedgerStorage, MessageId,
};
use anyhow::Result;
use rocksdb::{Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_MAX_ENTRIES_PER_LEDGER: u64 = 50_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredEntry {
    partition: i32,
    payload: Vec<u8>,
}

/// RocksDB-backed managed-ledger store for persistent topics.
#[derive(Debug)]
pub struct RocksDbManagedLedgerStorage {
    db: DB,
}

impl RocksDbManagedLedgerStorage {
    pub fn open(path: &Path) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        Ok(Self {
            db: DB::open(&options, path)?,
        })
    }

    fn managed_cursor_key(ledger_name: &str, cursor_name: &str) -> Vec<u8> {
        format!("/managed-ledgers/{ledger_name}/{cursor_name}").into_bytes()
    }

    fn managed_ledger_key(ledger_name: &str) -> Vec<u8> {
        format!("/managed-ledgers/{ledger_name}").into_bytes()
    }

    fn managed_entry_key(ledger_name: &str, ledger_id: u64, entry_id: u64) -> Vec<u8> {
        format!("managed_entry|{ledger_name}|{ledger_id:020}|{entry_id:020}").into_bytes()
    }

    fn managed_entry_prefix(ledger_name: &str) -> Vec<u8> {
        format!("managed_entry|{ledger_name}|").into_bytes()
    }

    fn managed_ledger_name(topic: &str) -> String {
        if let Some((domain, rest)) = topic.split_once("://") {
            let mut parts = rest.splitn(3, '/');
            if let (Some(tenant), Some(namespace), Some(local_name)) =
                (parts.next(), parts.next(), parts.next())
            {
                return format!("{tenant}/{namespace}/{domain}/{local_name}");
            }
        }

        topic.to_string()
    }

    fn encode_cursor_name(name: &str) -> String {
        const HEX: &[u8; 16] = b"0123456789ABCDEF";
        let mut encoded = String::with_capacity(name.len());

        for byte in name.bytes() {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
                encoded.push(byte as char);
            } else {
                encoded.push('%');
                encoded.push(HEX[(byte >> 4) as usize] as char);
                encoded.push(HEX[(byte & 0x0f) as usize] as char);
            }
        }

        encoded
    }
}

impl ManagedLedgerStorage for RocksDbManagedLedgerStorage {
    fn create_topic(&mut self, name: &str) -> Result<()> {
        self.ensure_ledger_state(name)?;
        Ok(())
    }

    fn append_message(&mut self, topic: &str, partition: i32, data: &[u8]) -> Result<MessageId> {
        let mut state = self.ensure_ledger_state(topic)?;
        let message_id = MessageId {
            ledger: state.ledger_id,
            entry: state.next_entry_id,
            partition,
        };
        state.next_entry_id += 1;

        let stored_entry = StoredEntry {
            partition,
            payload: data.to_vec(),
        };

        let mut batch = WriteBatch::default();
        batch.put(
            Self::entry_key(topic, message_id.ledger, message_id.entry),
            bincode::serialize(&stored_entry)?,
        );
        batch.put(Self::ledger_key(topic), bincode::serialize(&state)?);
        self.db.write(batch)?;

        Ok(message_id)
    }

    fn subscribe(&mut self, topic: &str, _subscription: &str) -> Result<()> {
        self.ensure_ledger_state(topic)?;
        Ok(())
    }

    fn get_next_unassigned_message(
        &mut self,
        topic: &str,
        subscription: &str,
        _consumer_id: u64,
    ) -> Result<Option<(MessageId, Vec<u8>)>> {
        let cursor = self.read_cursor(topic, subscription)?;
        for (message_id, payload) in self.get_messages(topic) {
            if is_message_acknowledged(Some(&cursor), message_id.entry) {
                continue;
            }
            return Ok(Some((message_id, payload)));
        }
        Ok(None)
    }

    fn ack_message(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: MessageId,
    ) -> Result<()> {
        self.db.put(
            Self::cursor_key(topic, subscription),
            bincode::serialize(&Some(message_id.entry))?,
        )?;
        Ok(())
    }

    fn ack_message_shared(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: MessageId,
    ) -> Result<()> {
        let before = self.read_cursor(topic, subscription)?;
        let mut after = before.clone();
        ack_shared(&mut after, message_id.entry);
        self.write_cursor(topic, subscription, &before, &after)
    }

    fn get_message_by_id(
        &self,
        topic: &str,
        message_id: &MessageId,
    ) -> Option<(MessageId, Vec<u8>)> {
        let bytes = self
            .db
            .get(Self::entry_key(topic, message_id.ledger, message_id.entry))
            .ok()
            .flatten()?;
        let stored_entry: StoredEntry = bincode::deserialize(&bytes).ok()?;
        if stored_entry.partition != message_id.partition {
            return None;
        }
        Some((message_id.clone(), stored_entry.payload))
    }

    fn get_messages(&self, topic: &str) -> Vec<(MessageId, Vec<u8>)> {
        let prefix = Self::entry_prefix(topic);
        let mut messages = Vec::new();
        for item in self.db.prefix_iterator(&prefix) {
            let Ok((key, value)) = item else {
                continue;
            };
            let Some(suffix) = key.strip_prefix(prefix.as_slice()) else {
                continue;
            };
            let Ok(suffix) = std::str::from_utf8(suffix) else {
                continue;
            };
            let mut parts = suffix.split('|');
            let Some(ledger) = parts.next().and_then(|value| value.parse::<u64>().ok()) else {
                continue;
            };
            let Some(entry) = parts.next().and_then(|value| value.parse::<u64>().ok()) else {
                continue;
            };
            let Ok(stored_entry) = bincode::deserialize::<StoredEntry>(&value) else {
                continue;
            };
            messages.push((
                MessageId {
                    ledger,
                    entry,
                    partition: stored_entry.partition,
                },
                stored_entry.payload,
            ));
        }
        messages
    }

    fn is_acknowledged_shared(
        &self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
    ) -> bool {
        self.read_cursor(topic, subscription)
            .map(|cursor| is_message_acknowledged(Some(&cursor), message_id.entry))
            .unwrap_or(false)
    }

    fn get_mark_delete_position(&self, topic: &str, subscription: &str) -> Option<u64> {
        self.read_cursor(topic, subscription)
            .ok()
            .and_then(|cursor| cursor.mark_delete)
    }
}
