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
    factory: RocksDBManagedLedgerFactory,
}

impl RocksDbManagedLedgerStorage {
    pub fn open(path: &Path) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = Arc::new(DB::open(&options, path)?);
        Ok(Self {
            factory: RocksDBManagedLedgerFactory::new(db),
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
        let ledger_name = RocksDbManagedLedgerStorage::managed_ledger_name(name);
        self.factory.open_ledger(&ledger_name)?;
        Ok(())
    }

    fn append_message(&mut self, topic: &str, partition: i32, data: &[u8]) -> Result<MessageId> {
        let ledger_name = RocksDbManagedLedgerStorage::managed_ledger_name(topic);
        let mut ledger = self.factory.open_ledger(&ledger_name)?;
        let position = ledger.add_entry_with_partition(partition, data)?;
        Ok(MessageId::from(position))
    }

    fn subscribe(&mut self, topic: &str, subscription: &str) -> Result<()> {
        let ledger_name = RocksDbManagedLedgerStorage::managed_ledger_name(topic);
        let cursor_name = RocksDbManagedLedgerStorage::encode_cursor_name(subscription);
        let mut ledger = self.factory.open_ledger(&ledger_name)?;
        ledger.open_cursor(&cursor_name)?;
        Ok(())
    }

    fn get_next_unassigned_message(
        &mut self,
        topic: &str,
        subscription: &str,
        _consumer_id: u64,
    ) -> Result<Option<(MessageId, Vec<u8>)>> {
        let ledger_name = RocksDbManagedLedgerStorage::managed_ledger_name(topic);
        let cursor_name = RocksDbManagedLedgerStorage::encode_cursor_name(subscription);
        let mut ledger = self.factory.open_ledger(&ledger_name)?;
        let cursor = ledger.open_cursor(&cursor_name)?;
        for (message_id, payload) in self.get_messages(topic) {
            let position = ManagedLedgerPosition::from(&message_id);
            if is_managed_position_acknowledged(cursor.state(), &position) {
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
        let ledger_name = RocksDbManagedLedgerStorage::managed_ledger_name(topic);
        let cursor_name = RocksDbManagedLedgerStorage::encode_cursor_name(subscription);
        let mut ledger = self.factory.open_ledger(&ledger_name)?;
        let mut cursor = ledger.open_cursor(&cursor_name)?;
        cursor.mark_delete(ManagedLedgerPosition::from(message_id))
    }

    fn ack_message_shared(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: MessageId,
    ) -> Result<()> {
        let ledger_name = RocksDbManagedLedgerStorage::managed_ledger_name(topic);
        let cursor_name = RocksDbManagedLedgerStorage::encode_cursor_name(subscription);
        let mut ledger = self.factory.open_ledger(&ledger_name)?;
        let mut cursor = ledger.open_cursor(&cursor_name)?;
        ack_managed_cursor_shared(
            &mut cursor,
            ManagedLedgerPosition::from(message_id),
            &ledger.info,
        )
    }

    fn get_message_by_id(
        &self,
        topic: &str,
        message_id: &MessageId,
    ) -> Option<(MessageId, Vec<u8>)> {
        let ledger_name = RocksDbManagedLedgerStorage::managed_ledger_name(topic);
        self.factory
            .open_ledger(&ledger_name)
            .ok()?
            .get_message_by_id(message_id)
    }

    fn get_messages(&self, topic: &str) -> Vec<(MessageId, Vec<u8>)> {
        let ledger_name = RocksDbManagedLedgerStorage::managed_ledger_name(topic);
        self.factory
            .open_ledger(&ledger_name)
            .map(|ledger| ledger.messages())
            .unwrap_or_default()
    }

    fn is_acknowledged_shared(
        &self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
    ) -> bool {
        let ledger_name = RocksDbManagedLedgerStorage::managed_ledger_name(topic);
        let cursor_name = RocksDbManagedLedgerStorage::encode_cursor_name(subscription);
        let mut ledger = match self.factory.open_ledger(&ledger_name) {
            Ok(ledger) => ledger,
            Err(_) => return false,
        };
        ledger
            .open_cursor(&cursor_name)
            .map(|cursor| {
                is_managed_position_acknowledged(
                    cursor.state(),
                    &ManagedLedgerPosition::from(message_id),
                )
            })
            .unwrap_or(false)
    }

    fn get_mark_delete_position(&self, topic: &str, subscription: &str) -> Option<u64> {
        let ledger_name = RocksDbManagedLedgerStorage::managed_ledger_name(topic);
        let cursor_name = RocksDbManagedLedgerStorage::encode_cursor_name(subscription);
        let mut ledger = self.factory.open_ledger(&ledger_name).ok()?;
        let cursor = ledger.open_cursor(&cursor_name).ok()?;
        cursor
            .state()
            .mark_delete
            .as_ref()
            .map(|position| position.entry_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredManagedCursorState {
    mark_delete: Option<ManagedLedgerPosition>,
    individually_deleted_entries: BTreeSet<ManagedLedgerPosition>,
}

impl From<ManagedCursorState> for StoredManagedCursorState {
    fn from(value: ManagedCursorState) -> Self {
        Self {
            mark_delete: value.mark_delete,
            individually_deleted_entries: value.individually_deleted_entries,
        }
    }
}

impl From<StoredManagedCursorState> for ManagedCursorState {
    fn from(value: StoredManagedCursorState) -> Self {
        Self {
            mark_delete: value.mark_delete,
            individually_deleted_entries: value.individually_deleted_entries,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RocksDBManagedCursor {
    ledger_name: String,
    name: String,
    db: Arc<DB>,
    state: ManagedCursorState,
}

impl RocksDBManagedCursor {
    pub fn open(ledger_name: &str, name: &str, db: Arc<DB>) -> Result<Self> {
        let key = RocksDbManagedLedgerStorage::managed_cursor_key(ledger_name, name);
        let state = db
            .get(key)?
            .map(|bytes| bincode::deserialize::<StoredManagedCursorState>(&bytes))
            .transpose()?
            .map(ManagedCursorState::from)
            .unwrap_or_default();

        Ok(Self {
            ledger_name: ledger_name.to_string(),
            name: name.to_string(),
            db,
            state,
        })
    }

    fn persist_state(&self) -> Result<()> {
        let key = RocksDbManagedLedgerStorage::managed_cursor_key(&self.ledger_name, &self.name);
        let stored = StoredManagedCursorState::from(self.state.clone());
        self.db.put(key, bincode::serialize(&stored)?)?;
        Ok(())
    }
}

impl ManagedCursor for RocksDBManagedCursor {
    fn name(&self) -> &str {
        &self.name
    }

    fn state(&self) -> &ManagedCursorState {
        &self.state
    }

    fn mark_delete(&mut self, position: ManagedLedgerPosition) -> Result<()> {
        self.state.mark_delete = Some(position);
        self.persist_state()
    }

    fn delete_individual(&mut self, position: ManagedLedgerPosition) -> Result<()> {
        self.state.individually_deleted_entries.insert(position);
        self.persist_state()
    }
}

fn is_managed_position_acknowledged(
    cursor: &ManagedCursorState,
    position: &ManagedLedgerPosition,
) -> bool {
    cursor
        .mark_delete
        .as_ref()
        .is_some_and(|mark_delete| position <= mark_delete)
        || cursor.individually_deleted_entries.contains(position)
}

fn first_position(info: &StoredManagedLedgerInfo, partition: i32) -> Option<ManagedLedgerPosition> {
    info.ledgers
        .iter()
        .find(|ledger| ledger.entries > 0)
        .map(|ledger| ManagedLedgerPosition {
            ledger_id: ledger.ledger_id,
            entry_id: 0,
            partition,
        })
}

fn next_position(
    position: &ManagedLedgerPosition,
    info: &StoredManagedLedgerInfo,
) -> Option<ManagedLedgerPosition> {
    let current_ledger = info
        .ledgers
        .iter()
        .find(|ledger| ledger.ledger_id == position.ledger_id)?;

    if position.entry_id + 1 < current_ledger.entries {
        return Some(ManagedLedgerPosition {
            ledger_id: position.ledger_id,
            entry_id: position.entry_id + 1,
            partition: position.partition,
        });
    }

    info.ledgers
        .iter()
        .find(|ledger| ledger.ledger_id > position.ledger_id && ledger.entries > 0)
        .map(|ledger| ManagedLedgerPosition {
            ledger_id: ledger.ledger_id,
            entry_id: 0,
            partition: position.partition,
        })
}

fn ack_managed_cursor_shared(
    cursor: &mut RocksDBManagedCursor,
    position: ManagedLedgerPosition,
    info: &StoredManagedLedgerInfo,
) -> Result<()> {
    if is_managed_position_acknowledged(cursor.state(), &position) {
        return Ok(());
    }

    match cursor.state().mark_delete.as_ref() {
        None if Some(position.clone()) == first_position(info, position.partition) => {
            cursor.mark_delete(position)?
        }
        None => cursor.delete_individual(position)?,
        Some(mark_delete) if Some(position.clone()) == next_position(mark_delete, info) => {
            cursor.mark_delete(position)?
        }
        Some(mark_delete) if position > *mark_delete => cursor.delete_individual(position)?,
        Some(_) => {}
    }

    while let Some(mark_delete) = cursor.state().mark_delete.clone() {
        let Some(next) = next_position(&mark_delete, info) else {
            break;
        };
        if cursor.state.individually_deleted_entries.remove(&next) {
            cursor.mark_delete(next)?;
        } else {
            break;
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredLedgerInfo {
    ledger_id: u64,
    entries: u64,
    size: u64,
    timestamp: u64,
    is_offloaded: bool,
    offloaded_context_uuid: Option<String>,
}

impl StoredLedgerInfo {
    fn new(ledger_id: u64) -> Self {
        Self {
            ledger_id,
            entries: 0,
            size: 0,
            timestamp: 0,
            is_offloaded: false,
            offloaded_context_uuid: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredManagedLedgerInfo {
    ledgers: Vec<StoredLedgerInfo>,
}

impl StoredManagedLedgerInfo {
    fn new() -> Self {
        Self {
            ledgers: vec![StoredLedgerInfo::new(0)],
        }
    }

    fn current_ledger_mut(&mut self) -> &mut StoredLedgerInfo {
        if self.ledgers.is_empty() {
            self.ledgers.push(StoredLedgerInfo::new(0));
        }
        self.ledgers.last_mut().expect("ledger info is initialized")
    }

    fn ensure_writable_ledger(&mut self, max_entries_per_ledger: u64) {
        let current_ledger = self.current_ledger_mut();
        if current_ledger.entries >= max_entries_per_ledger {
            let next_ledger_id = current_ledger.ledger_id + 1;
            self.ledgers.push(StoredLedgerInfo::new(next_ledger_id));
        }
    }

    fn roll_over_current_ledger_if_full(&mut self, max_entries_per_ledger: u64) {
        let current_ledger = self.current_ledger_mut();
        if current_ledger.entries >= max_entries_per_ledger {
            current_ledger.timestamp = current_time_millis();
            let next_ledger_id = current_ledger.ledger_id + 1;
            self.ledgers.push(StoredLedgerInfo::new(next_ledger_id));
        }
    }
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[derive(Debug, Clone)]
pub struct RocksDBManagedLedger {
    name: String,
    db: Arc<DB>,
    info: StoredManagedLedgerInfo,
    entries: Vec<(ManagedLedgerPosition, Vec<u8>)>,
    max_entries_per_ledger: u64,
}

impl RocksDBManagedLedger {
    pub fn open(name: &str, db: Arc<DB>) -> Result<Self> {
        Self::open_with_config(name, db, &ManagedLedgerConfig::default())
    }

    fn open_with_config(name: &str, db: Arc<DB>, config: &ManagedLedgerConfig) -> Result<Self> {
        let key = RocksDbManagedLedgerStorage::managed_ledger_key(name);
        let mut info = db
            .get(&key)?
            .map(|bytes| bincode::deserialize::<StoredManagedLedgerInfo>(&bytes))
            .transpose()?
            .unwrap_or_else(StoredManagedLedgerInfo::new);
        let max_entries_per_ledger = config
            .max_entries_per_ledger
            .unwrap_or(DEFAULT_MAX_ENTRIES_PER_LEDGER)
            .max(1);

        info.ensure_writable_ledger(max_entries_per_ledger);

        db.put(&key, bincode::serialize(&info)?)?;

        Ok(Self {
            name: name.to_string(),
            entries: Self::load_entries(name, &db)?,
            db,
            info,
            max_entries_per_ledger,
        })
    }

    fn load_entries(name: &str, db: &DB) -> Result<Vec<(ManagedLedgerPosition, Vec<u8>)>> {
        let prefix = RocksDbManagedLedgerStorage::managed_entry_prefix(name);
        let mut entries = Vec::new();

        for item in db.prefix_iterator(&prefix) {
            let (key, value) = item?;
            let Some(suffix) = key.strip_prefix(prefix.as_slice()) else {
                break;
            };
            let suffix = std::str::from_utf8(suffix)?;
            let mut parts = suffix.split('|');
            let Some(ledger_id) = parts.next().and_then(|value| value.parse::<u64>().ok()) else {
                continue;
            };
            let Some(entry_id) = parts.next().and_then(|value| value.parse::<u64>().ok()) else {
                continue;
            };
            let stored_entry: StoredEntry = bincode::deserialize(&value)?;

            entries.push((
                ManagedLedgerPosition {
                    ledger_id,
                    entry_id,
                    partition: stored_entry.partition,
                },
                stored_entry.payload,
            ));
        }

        entries.sort_by_key(|(position, _)| {
            (position.ledger_id, position.entry_id, position.partition)
        });
        Ok(entries)
    }

    fn add_entry_with_partition(
        &mut self,
        partition: i32,
        payload: &[u8],
    ) -> Result<ManagedLedgerPosition> {
        let mut next_info = self.info.clone();
        next_info.ensure_writable_ledger(self.max_entries_per_ledger);
        let current_ledger = next_info.current_ledger_mut();
        let position = ManagedLedgerPosition {
            ledger_id: current_ledger.ledger_id,
            entry_id: current_ledger.entries,
            partition,
        };
        current_ledger.entries += 1;
        current_ledger.size += payload.len() as u64;
        next_info.roll_over_current_ledger_if_full(self.max_entries_per_ledger);

        let stored_entry = StoredEntry {
            partition,
            payload: payload.to_vec(),
        };

        let mut batch = WriteBatch::default();
        batch.put(
            RocksDbManagedLedgerStorage::managed_entry_key(
                &self.name,
                position.ledger_id,
                position.entry_id,
            ),
            bincode::serialize(&stored_entry)?,
        );
        batch.put(
            RocksDbManagedLedgerStorage::managed_ledger_key(&self.name),
            bincode::serialize(&next_info)?,
        );
        self.db.write(batch)?;

        self.info = next_info;
        self.entries.push((position.clone(), payload.to_vec()));

        Ok(position)
    }

    fn get_message_by_id(&self, message_id: &MessageId) -> Option<(MessageId, Vec<u8>)> {
        self.entries
            .iter()
            .find(|(position, _)| {
                position.ledger_id == message_id.ledger
                    && position.entry_id == message_id.entry
                    && position.partition == message_id.partition
            })
            .map(|(_, payload)| (message_id.clone(), payload.clone()))
    }

    fn messages(&self) -> Vec<(MessageId, Vec<u8>)> {
        self.entries
            .iter()
            .map(|(position, payload)| (MessageId::from(position), payload.clone()))
            .collect()
    }
}

impl ManagedLedger for RocksDBManagedLedger {
    type Cursor = RocksDBManagedCursor;

    fn name(&self) -> &str {
        &self.name
    }

    fn add_entry(&mut self, payload: &[u8]) -> Result<ManagedLedgerPosition> {
        self.add_entry_with_partition(-1, payload)
    }

    fn open_cursor(&mut self, name: &str) -> Result<Self::Cursor> {
        RocksDBManagedCursor::open(&self.name, name, Arc::clone(&self.db))
    }

    fn read_entry(&self, position: &ManagedLedgerPosition) -> Option<&[u8]> {
        self.entries
            .iter()
            .find(|(stored_position, _)| stored_position == position)
            .map(|(_, payload)| payload.as_slice())
    }
}

#[derive(Debug, Clone)]
pub struct RocksDBManagedLedgerFactory {
    db: Arc<DB>,
}

impl RocksDBManagedLedgerFactory {
    pub fn new(db: Arc<DB>) -> Self {
        Self { db }
    }

    fn open_ledger(&self, name: &str) -> Result<RocksDBManagedLedger> {
        RocksDBManagedLedger::open(name, Arc::clone(&self.db))
    }

    fn open_ledger_with_config(
        &self,
        name: &str,
        config: &ManagedLedgerConfig,
    ) -> Result<RocksDBManagedLedger> {
        RocksDBManagedLedger::open_with_config(name, Arc::clone(&self.db), config)
    }
}

impl ManagedLedgerFactory for RocksDBManagedLedgerFactory {
    type Ledger = RocksDBManagedLedger;

    fn open(&mut self, name: &str, config: &ManagedLedgerConfig) -> Result<Self::Ledger> {
        self.open_ledger_with_config(name, config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn open_test_db(path: &Path) -> Arc<DB> {
        let mut options = Options::default();
        options.create_if_missing(true);
        Arc::new(DB::open(&options, path).unwrap())
    }

    fn position(ledger_id: u64, entry_id: u64) -> ManagedLedgerPosition {
        ManagedLedgerPosition {
            ledger_id,
            entry_id,
            partition: -1,
        }
    }

    fn read_managed_ledger_info(db: &DB, ledger_name: &str) -> StoredManagedLedgerInfo {
        let bytes = db
            .get(RocksDbManagedLedgerStorage::managed_ledger_key(ledger_name))
            .unwrap()
            .expect("managed ledger info should exist");
        bincode::deserialize(&bytes).unwrap()
    }

    #[test]
    fn managed_metadata_keys_follow_pulsar_path_shape() {
        assert_eq!(
            RocksDbManagedLedgerStorage::managed_ledger_key("tenant/namespace/persistent/topic"),
            b"/managed-ledgers/tenant/namespace/persistent/topic".to_vec()
        );
        assert_eq!(
            RocksDbManagedLedgerStorage::managed_cursor_key(
                "tenant/namespace/persistent/topic",
                "sub-a",
            ),
            b"/managed-ledgers/tenant/namespace/persistent/topic/sub-a".to_vec()
        );
    }

    #[test]
    fn managed_ledger_name_normalizes_pulsar_urls_and_keeps_plain_names() {
        assert_eq!(
            RocksDbManagedLedgerStorage::managed_ledger_name("persistent://public/default/test"),
            "public/default/persistent/test"
        );
        assert_eq!(
            RocksDbManagedLedgerStorage::managed_ledger_name(
                "persistent://public/default/test-partition-0",
            ),
            "public/default/persistent/test-partition-0"
        );
        assert_eq!(
            RocksDbManagedLedgerStorage::managed_ledger_name("test-topic"),
            "test-topic"
        );
    }

    #[test]
    fn cursor_name_percent_encoding_preserves_path_segment_boundary() {
        assert_eq!(
            RocksDbManagedLedgerStorage::encode_cursor_name("sub-a"),
            "sub-a"
        );
        assert_eq!(
            RocksDbManagedLedgerStorage::encode_cursor_name("team/a"),
            "team%2Fa"
        );
        assert_eq!(
            RocksDbManagedLedgerStorage::encode_cursor_name("team%2Fa"),
            "team%252Fa"
        );
    }

    #[test]
    fn managed_cursor_mark_delete_recovers_after_reopen() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("cursor-mark-delete");
        let mark_delete = position(3, 11);

        {
            let db = open_test_db(&db_path);
            let mut cursor = RocksDBManagedCursor::open("ledger-a", "sub-a", db).unwrap();
            cursor.mark_delete(mark_delete.clone()).unwrap();
        }

        let db = open_test_db(&db_path);
        let cursor = RocksDBManagedCursor::open("ledger-a", "sub-a", db).unwrap();

        assert_eq!(cursor.state().mark_delete, Some(mark_delete));
    }

    #[test]
    fn managed_cursor_individual_delete_recovers_after_reopen() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("cursor-individual-delete");
        let deleted = position(5, 17);

        {
            let db = open_test_db(&db_path);
            let mut cursor = RocksDBManagedCursor::open("ledger-a", "sub-a", db).unwrap();
            cursor.delete_individual(deleted.clone()).unwrap();
        }

        let db = open_test_db(&db_path);
        let cursor = RocksDBManagedCursor::open("ledger-a", "sub-a", db).unwrap();

        assert!(cursor
            .state()
            .individually_deleted_entries
            .contains(&deleted));
    }

    #[test]
    fn managed_cursor_state_is_isolated_by_ledger_and_cursor_name() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("cursor-isolation");
        let ledger_a_sub_a = position(1, 2);
        let ledger_a_sub_b = position(1, 7);

        {
            let db = open_test_db(&db_path);
            let mut cursor_a =
                RocksDBManagedCursor::open("ledger-a", "sub-a", Arc::clone(&db)).unwrap();
            let mut cursor_b =
                RocksDBManagedCursor::open("ledger-a", "sub-b", Arc::clone(&db)).unwrap();

            cursor_a.mark_delete(ledger_a_sub_a.clone()).unwrap();
            cursor_b.mark_delete(ledger_a_sub_b.clone()).unwrap();
        }

        let db = open_test_db(&db_path);
        let cursor_a = RocksDBManagedCursor::open("ledger-a", "sub-a", Arc::clone(&db)).unwrap();
        let cursor_b = RocksDBManagedCursor::open("ledger-a", "sub-b", Arc::clone(&db)).unwrap();
        let cursor_c = RocksDBManagedCursor::open("ledger-b", "sub-a", db).unwrap();

        assert_eq!(cursor_a.state().mark_delete, Some(ledger_a_sub_a));
        assert_eq!(cursor_b.state().mark_delete, Some(ledger_a_sub_b));
        assert_eq!(cursor_c.state().mark_delete, None);
    }

    #[test]
    fn managed_ledger_entry_recovers_after_reopen() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("ledger-entry-recovery");

        let first_position = {
            let db = open_test_db(&db_path);
            let mut ledger = RocksDBManagedLedger::open("ledger-a", db).unwrap();
            ledger.add_entry(b"first").unwrap()
        };

        let db = open_test_db(&db_path);
        let ledger = RocksDBManagedLedger::open("ledger-a", Arc::clone(&db)).unwrap();

        assert_eq!(first_position.ledger_id, 0);
        assert_eq!(first_position.entry_id, 0);
        assert_eq!(
            ledger.read_entry(&first_position),
            Some(b"first".as_slice())
        );
    }

    #[test]
    fn managed_ledger_next_entry_id_is_derived_from_last_ledger_entries() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("ledger-next-entry");

        {
            let db = open_test_db(&db_path);
            let mut ledger = RocksDBManagedLedger::open("ledger-a", db).unwrap();
            assert_eq!(ledger.add_entry(b"first").unwrap().entry_id, 0);
            assert_eq!(ledger.add_entry(b"second").unwrap().entry_id, 1);
        }

        let db = open_test_db(&db_path);
        let mut ledger = RocksDBManagedLedger::open("ledger-a", db).unwrap();
        let third_position = ledger.add_entry(b"third").unwrap();

        assert_eq!(third_position.ledger_id, 0);
        assert_eq!(third_position.entry_id, 2);
        assert_eq!(
            ledger.read_entry(&third_position),
            Some(b"third".as_slice())
        );
    }

    #[test]
    fn managed_ledger_rolls_over_after_max_entries_like_pulsar() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("ledger-rollover");
        let db = open_test_db(&db_path);
        let config = ManagedLedgerConfig {
            max_entries_per_ledger: Some(2),
            ..ManagedLedgerConfig::default()
        };
        let mut factory = RocksDBManagedLedgerFactory::new(Arc::clone(&db));

        {
            let mut ledger = factory.open("ledger-a", &config).unwrap();
            assert_eq!(ledger.add_entry(b"first").unwrap(), position(0, 0));
            assert_eq!(ledger.add_entry(b"second").unwrap(), position(0, 1));
            assert_eq!(ledger.add_entry(b"third").unwrap(), position(1, 0));
        }

        let ledger = factory.open("ledger-a", &config).unwrap();

        assert_eq!(ledger.info.ledgers.len(), 2);
        assert_eq!(ledger.info.ledgers[0].ledger_id, 0);
        assert_eq!(ledger.info.ledgers[0].entries, 2);
        assert_eq!(ledger.info.ledgers[1].ledger_id, 1);
        assert_eq!(ledger.info.ledgers[1].entries, 1);
        assert_eq!(
            ledger.read_entry(&position(0, 0)),
            Some(b"first".as_slice())
        );
        assert_eq!(
            ledger.read_entry(&position(0, 1)),
            Some(b"second".as_slice())
        );
        assert_eq!(
            ledger.read_entry(&position(1, 0)),
            Some(b"third".as_slice())
        );
    }

    #[test]
    fn managed_ledger_rollover_metadata_is_persisted_in_rocksdb() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("ledger-rollover-metadata");
        let db = open_test_db(&db_path);
        let config = ManagedLedgerConfig {
            max_entries_per_ledger: Some(2),
            ..ManagedLedgerConfig::default()
        };
        let mut factory = RocksDBManagedLedgerFactory::new(Arc::clone(&db));

        {
            let mut ledger = factory.open("ledger-a", &config).unwrap();
            ledger.add_entry(b"first").unwrap();
            ledger.add_entry(b"second").unwrap();
            ledger.add_entry(b"third").unwrap();
        }

        let info = read_managed_ledger_info(&db, "ledger-a");

        assert_eq!(info.ledgers.len(), 2);
        assert_eq!(info.ledgers[0].ledger_id, 0);
        assert_eq!(info.ledgers[0].entries, 2);
        assert_eq!(
            info.ledgers[0].size,
            b"first".len() as u64 + b"second".len() as u64
        );
        assert!(info.ledgers[0].timestamp > 0);
        assert_eq!(info.ledgers[1].ledger_id, 1);
        assert_eq!(info.ledgers[1].entries, 1);
        assert_eq!(info.ledgers[1].size, b"third".len() as u64);
        assert_eq!(info.ledgers[1].timestamp, 0);
    }

    #[test]
    fn managed_ledger_reopen_continues_from_persisted_rollover_metadata() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("ledger-rollover-reopen");
        let config = ManagedLedgerConfig {
            max_entries_per_ledger: Some(2),
            ..ManagedLedgerConfig::default()
        };

        {
            let db = open_test_db(&db_path);
            let mut factory = RocksDBManagedLedgerFactory::new(db);
            let mut ledger = factory.open("ledger-a", &config).unwrap();
            assert_eq!(ledger.add_entry(b"first").unwrap(), position(0, 0));
            assert_eq!(ledger.add_entry(b"second").unwrap(), position(0, 1));
        }

        {
            let db = open_test_db(&db_path);
            let mut factory = RocksDBManagedLedgerFactory::new(Arc::clone(&db));
            let mut ledger = factory.open("ledger-a", &config).unwrap();
            assert_eq!(ledger.add_entry(b"third").unwrap(), position(1, 0));
            assert_eq!(ledger.add_entry(b"fourth").unwrap(), position(1, 1));
            assert_eq!(ledger.add_entry(b"fifth").unwrap(), position(2, 0));

            let info = read_managed_ledger_info(&db, "ledger-a");
            assert_eq!(info.ledgers.len(), 3);
            assert_eq!(info.ledgers[0].entries, 2);
            assert_eq!(info.ledgers[1].entries, 2);
            assert_eq!(info.ledgers[2].entries, 1);
        }

        let db = open_test_db(&db_path);
        let ledger = RocksDBManagedLedger::open_with_config("ledger-a", db, &config).unwrap();
        assert_eq!(
            ledger.read_entry(&position(0, 0)),
            Some(b"first".as_slice())
        );
        assert_eq!(
            ledger.read_entry(&position(1, 0)),
            Some(b"third".as_slice())
        );
        assert_eq!(
            ledger.read_entry(&position(2, 0)),
            Some(b"fifth".as_slice())
        );
    }

    #[test]
    fn shared_ack_advances_contiguously_across_rolled_ledgers() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("ledger-rollover-shared-ack");
        let db = open_test_db(&db_path);
        let config = ManagedLedgerConfig {
            max_entries_per_ledger: Some(2),
            ..ManagedLedgerConfig::default()
        };
        let mut factory = RocksDBManagedLedgerFactory::new(Arc::clone(&db));
        let mut ledger = factory.open("ledger-a", &config).unwrap();
        let first = ledger.add_entry(b"first").unwrap();
        let second = ledger.add_entry(b"second").unwrap();
        let third = ledger.add_entry(b"third").unwrap();
        let mut cursor = ledger.open_cursor("sub-a").unwrap();

        ack_managed_cursor_shared(&mut cursor, third.clone(), &ledger.info).unwrap();
        assert_eq!(cursor.state().mark_delete, None);
        assert!(cursor.state().individually_deleted_entries.contains(&third));

        ack_managed_cursor_shared(&mut cursor, first, &ledger.info).unwrap();
        ack_managed_cursor_shared(&mut cursor, second, &ledger.info).unwrap();

        assert_eq!(cursor.state().mark_delete, Some(third));
        assert!(cursor.state().individually_deleted_entries.is_empty());
    }

    #[test]
    fn managed_ledger_open_cursor_recovers_cursor_state() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("ledger-cursor-recovery");
        let mark_delete = position(0, 3);

        {
            let db = open_test_db(&db_path);
            let mut ledger = RocksDBManagedLedger::open("ledger-a", db).unwrap();
            let mut cursor = ledger.open_cursor("sub-a").unwrap();
            cursor.mark_delete(mark_delete.clone()).unwrap();
        }

        let db = open_test_db(&db_path);
        let mut ledger = RocksDBManagedLedger::open("ledger-a", db).unwrap();
        let cursor = ledger.open_cursor("sub-a").unwrap();

        assert_eq!(cursor.state().mark_delete, Some(mark_delete));
    }

    #[test]
    fn storage_writes_managed_ledger_keys_instead_of_legacy_keys() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("storage-managed-keyspace");
        let topic = "tenant/namespace/persistent/topic";
        let subscription = "sub-a";

        let message_id = {
            let mut storage = RocksDbManagedLedgerStorage::open(&db_path).unwrap();
            let message_id = storage.append_message(topic, 7, b"payload").unwrap();
            storage
                .ack_message_shared(topic, subscription, message_id.clone())
                .unwrap();
            message_id
        };

        let db = open_test_db(&db_path);

        assert!(db
            .get(RocksDbManagedLedgerStorage::managed_ledger_key(topic))
            .unwrap()
            .is_some());
        assert!(db
            .get(RocksDbManagedLedgerStorage::managed_cursor_key(
                topic,
                subscription
            ))
            .unwrap()
            .is_some());
        assert!(db
            .get(RocksDbManagedLedgerStorage::managed_entry_key(
                topic,
                message_id.ledger,
                message_id.entry
            ))
            .unwrap()
            .is_some());

        assert!(db.get(format!("ledger|{topic}")).unwrap().is_none());
        assert!(db
            .get(format!(
                "entry|{topic}|{:020}|{:020}",
                message_id.ledger, message_id.entry
            ))
            .unwrap()
            .is_none());
        assert!(db
            .get(format!("cursor|{topic}|{subscription}"))
            .unwrap()
            .is_none());
        assert!(db
            .get(format!(
                "hole|{topic}|{subscription}|{:020}",
                message_id.entry
            ))
            .unwrap()
            .is_none());
    }

    #[test]
    fn storage_normalizes_topic_url_and_encodes_cursor_name() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("storage-normalized-keyspace");
        let topic = "persistent://public/default/test";
        let ledger_name = "public/default/persistent/test";
        let subscription = "team/a";
        let cursor_name = "team%2Fa";

        let message_id = {
            let mut storage = RocksDbManagedLedgerStorage::open(&db_path).unwrap();
            let message_id = storage.append_message(topic, -1, b"payload").unwrap();
            storage
                .ack_message_shared(topic, subscription, message_id.clone())
                .unwrap();
            message_id
        };

        let db = open_test_db(&db_path);

        assert!(db
            .get(RocksDbManagedLedgerStorage::managed_ledger_key(ledger_name))
            .unwrap()
            .is_some());
        assert!(db
            .get(RocksDbManagedLedgerStorage::managed_cursor_key(
                ledger_name,
                cursor_name
            ))
            .unwrap()
            .is_some());
        assert!(db
            .get(RocksDbManagedLedgerStorage::managed_entry_key(
                ledger_name,
                message_id.ledger,
                message_id.entry
            ))
            .unwrap()
            .is_some());

        assert!(db
            .get(RocksDbManagedLedgerStorage::managed_ledger_key(topic))
            .unwrap()
            .is_none());
        assert!(db
            .get(RocksDbManagedLedgerStorage::managed_cursor_key(
                ledger_name,
                subscription
            ))
            .unwrap()
            .is_none());
    }
}
