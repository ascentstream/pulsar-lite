use super::{
    ack_shared, is_message_acknowledged, ManagedLedgerStorage, MessageId, SubscriptionCursor,
};
use anyhow::Result;
use rocksdb::{Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LedgerState {
    ledger_id: u64,
    next_entry_id: u64,
}

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

    fn ledger_key(topic: &str) -> Vec<u8> {
        format!("ledger|{topic}").into_bytes()
    }

    fn entry_key(topic: &str, ledger_id: u64, entry_id: u64) -> Vec<u8> {
        format!("entry|{topic}|{ledger_id:020}|{entry_id:020}").into_bytes()
    }

    fn entry_prefix(topic: &str) -> Vec<u8> {
        format!("entry|{topic}|").into_bytes()
    }

    fn cursor_key(topic: &str, subscription: &str) -> Vec<u8> {
        format!("cursor|{topic}|{subscription}").into_bytes()
    }

    fn ack_hole_key(topic: &str, subscription: &str, entry_id: u64) -> Vec<u8> {
        format!("hole|{topic}|{subscription}|{entry_id:020}").into_bytes()
    }

    fn ack_hole_prefix(topic: &str, subscription: &str) -> Vec<u8> {
        format!("hole|{topic}|{subscription}|").into_bytes()
    }

    fn read_ledger_state(&self, topic: &str) -> Result<Option<LedgerState>> {
        self.db
            .get(Self::ledger_key(topic))?
            .map(|bytes| bincode::deserialize(&bytes).map_err(Into::into))
            .transpose()
    }

    fn ensure_ledger_state(&self, topic: &str) -> Result<LedgerState> {
        if let Some(state) = self.read_ledger_state(topic)? {
            return Ok(state);
        }

        let state = LedgerState {
            ledger_id: 0,
            next_entry_id: 0,
        };
        self.db
            .put(Self::ledger_key(topic), bincode::serialize(&state)?)?;
        Ok(state)
    }

    fn read_cursor(&self, topic: &str, subscription: &str) -> Result<SubscriptionCursor> {
        let mark_delete = self
            .db
            .get(Self::cursor_key(topic, subscription))?
            .map(|bytes| {
                if bytes.is_empty() {
                    Ok(None)
                } else {
                    bincode::deserialize(&bytes)
                }
            })
            .transpose()?
            .flatten();

        let prefix = Self::ack_hole_prefix(topic, subscription);
        let mut acked_holes = BTreeSet::new();
        for item in self.db.prefix_iterator(&prefix) {
            let (key, _) = item?;
            let Some(suffix) = key.strip_prefix(prefix.as_slice()) else {
                break;
            };
            let entry = std::str::from_utf8(suffix)?.parse::<u64>()?;
            acked_holes.insert(entry);
        }

        Ok(SubscriptionCursor {
            mark_delete,
            acked_holes,
        })
    }

    fn write_cursor(
        &self,
        topic: &str,
        subscription: &str,
        before: &SubscriptionCursor,
        after: &SubscriptionCursor,
    ) -> Result<()> {
        let mut batch = WriteBatch::default();
        match after.mark_delete {
            Some(mark_delete) => batch.put(
                Self::cursor_key(topic, subscription),
                bincode::serialize(&Some(mark_delete))?,
            ),
            None => batch.delete(Self::cursor_key(topic, subscription)),
        }

        for entry in before.acked_holes.difference(&after.acked_holes) {
            batch.delete(Self::ack_hole_key(topic, subscription, *entry));
        }
        for entry in after.acked_holes.difference(&before.acked_holes) {
            batch.put(Self::ack_hole_key(topic, subscription, *entry), []);
        }

        self.db.write(batch)?;
        Ok(())
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
