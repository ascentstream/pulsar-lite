mod managed_ledger;
mod metadata;
mod resources;
#[cfg(feature = "rocksdb-storage")]
pub(crate) mod rocksdb;

use anyhow::Result;
use log::{debug, info, warn};
use std::path::Path;

#[cfg(feature = "rocksdb-storage")]
pub(crate) use managed_ledger::{
    first_unacked_from_messages, last_position_from_messages, read_from_messages,
};
pub use managed_ledger::{
    CursorInitOptions, CursorOpenResult, InMemoryManagedCursor, InMemoryManagedLedger,
    InMemoryManagedLedgerFactory, InMemoryManagedLedgerStorage, InitialPosition, ManagedCursor,
    ManagedCursorState, ManagedLedger, ManagedLedgerConfig, ManagedLedgerFactory,
    ManagedLedgerPosition, ManagedLedgerStorage, ManagedLedgerStore, MessageId, NonPersistentEntry,
    SubscriptionCursor,
};
pub use metadata::{
    DomainNode, JsonFileMetadataStore, MetadataBackend, MetadataDocument, MetadataFileNode,
    MetadataStore, NamespaceMetadata, NamespaceNode, ParsedTopicName, PartitionedTopicNode,
    SubscriptionMetadata, SubscriptionNode, TenantMetadata, TenantNode, TopicMetadata, TopicNode,
};
pub use resources::{
    BaseResources, NamespaceResources, PulsarResources, TenantResources, TopicResources,
};

/// Broker storage facade.
///
/// Topic runtime selection happens above this layer from the topic URL domain.
/// Persistent topics use this facade's managed-ledger store, while
/// non-persistent topics use their in-memory runtime path.
#[derive(Debug)]
pub struct Storage {
    // Aggregated broker resource access aligned with PulsarResources.
    resources: PulsarResources,
    // Persistent-topic managed-ledger state.
    managed_ledger: ManagedLedgerStore,
}

impl Storage {
    pub(crate) const METADATA_VERSION: u32 = 2;

    /// Create a new storage instance.
    #[cfg(feature = "rocksdb-storage")]
    pub fn new(path: &Path) -> Result<Self> {
        Self::new_rocksdb(path)
    }

    /// Create a new storage instance.
    #[cfg(not(feature = "rocksdb-storage"))]
    pub fn new(path: &Path) -> Result<Self> {
        Self::new_memory(path)
    }

    /// Create a new storage instance backed by the in-memory managed-ledger store.
    pub fn new_memory(path: &Path) -> Result<Self> {
        info!("In-memory storage initialized (MVP version)");
        let storage = Self {
            resources: PulsarResources::new(path)?,
            managed_ledger: ManagedLedgerStore::memory(),
        };
        Ok(storage)
    }

    /// Create a new storage instance backed by RocksDB managed-ledger state.
    #[cfg(feature = "rocksdb-storage")]
    pub fn new_rocksdb(path: &Path) -> Result<Self> {
        info!("RocksDB managed-ledger storage initialized");
        let rocksdb_path = path.with_extension("rocksdb");
        let storage = Self {
            resources: PulsarResources::new(path)?,
            managed_ledger: ManagedLedgerStore::rocksdb(&rocksdb_path)?,
        };
        Ok(storage)
    }

    /// RocksDB storage requires the `rocksdb-storage` feature.
    #[cfg(not(feature = "rocksdb-storage"))]
    pub fn new_rocksdb(_path: &Path) -> Result<Self> {
        Err(anyhow::anyhow!(
            "managed ledger store 'rocksdb' requires the rocksdb-storage feature"
        ))
    }

    pub fn resources(&self) -> &PulsarResources {
        &self.resources
    }

    pub fn resources_mut(&mut self) -> &mut PulsarResources {
        &mut self.resources
    }

    /// Create a topic.
    pub fn create_topic(&mut self, name: &str) -> Result<()> {
        self.managed_ledger.create_topic(name)
    }

    /// Append a message using the current Pulsar-compatible message id layout.
    pub fn append_message(
        &mut self,
        topic: &str,
        partition: i32,
        data: &[u8],
    ) -> Result<MessageId> {
        let message_id = self.managed_ledger.append_message(topic, partition, data)?;
        debug!(
            "Message appended to {}: ledger={}, entry={}, partition={}",
            topic, message_id.ledger, message_id.entry, message_id.partition
        );
        Ok(message_id)
    }

    /// Create or reuse a subscription for a topic.
    pub fn subscribe(&mut self, topic: &str, subscription: &str) -> Result<()> {
        if let Err(error) =
            self.resources_mut()
                .ensure_subscription(topic, subscription, Self::METADATA_VERSION)
        {
            warn!(
                "Skipping metadata persistence for subscription '{}' on topic '{}': {}",
                subscription, topic, error
            );
        }

        let key = format!("{}:{}", topic, subscription);
        if self.managed_ledger.subscribe(topic, subscription).is_ok() {
            info!(
                "Subscribed to topic {} with subscription {}",
                topic, subscription
            );
        } else {
            info!("Subscription {} already exists for topic {}", key, topic);
        }

        Ok(())
    }

    pub fn initialize_or_open_cursor(
        &mut self,
        topic: &str,
        subscription: &str,
        options: CursorInitOptions,
    ) -> Result<CursorOpenResult> {
        self.managed_ledger
            .initialize_or_open_cursor(topic, subscription, options)
    }

    pub fn first_unacked_position(
        &self,
        topic: &str,
        subscription: &str,
    ) -> Result<Option<ManagedLedgerPosition>> {
        self.managed_ledger
            .first_unacked_position(topic, subscription)
    }

    pub fn read_from(
        &self,
        topic: &str,
        from: &ManagedLedgerPosition,
        limit: usize,
    ) -> Result<Vec<(MessageId, Vec<u8>)>> {
        self.managed_ledger.read_from(topic, from, limit)
    }

    pub fn get_last_position(&self, topic: &str) -> Result<Option<ManagedLedgerPosition>> {
        self.managed_ledger.get_last_position(topic)
    }

    pub fn get_next_position(
        &self,
        topic: &str,
        current: &ManagedLedgerPosition,
    ) -> Result<Option<ManagedLedgerPosition>> {
        self.managed_ledger.get_next_position(topic, current)
    }

    pub fn is_acknowledged(
        &self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
    ) -> Result<bool> {
        self.managed_ledger
            .is_acknowledged(topic, subscription, message_id)
    }

    /// Return the next deliverable message for the current in-memory flow.
    pub fn get_next_unassigned_message(
        &mut self,
        topic: &str,
        subscription: &str,
        consumer_id: u64,
    ) -> Result<Option<(MessageId, Vec<u8>)>> {
        self.managed_ledger
            .get_next_unassigned_message(topic, subscription, consumer_id)
    }

    /// Acknowledge a message under cumulative-style cursor semantics.
    pub fn ack_message(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: MessageId,
    ) -> Result<()> {
        info!(
            "Message acknowledged for topic {} subscription {}: ledger={}, entry={}",
            topic, subscription, message_id.ledger, message_id.entry
        );
        self.managed_ledger
            .ack_message(topic, subscription, message_id)
    }

    // ==================== Shared Ack Frontier ====================

    /// Acknowledge a message under Shared subscription semantics.
    ///
    /// The current in-memory frontier uses a `mark_delete + acked_holes` model:
    /// - `mark_delete`: current contiguous acknowledged frontier
    /// - `acked_holes`: acknowledged entries beyond the frontier that are not
    ///   yet part of a contiguous range
    ///
    /// When an entry is acknowledged:
    /// 1. Non-contiguous entries are recorded in `acked_holes`
    /// 2. The frontier advances once the next contiguous range is complete
    pub fn ack_message_shared(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: MessageId,
    ) -> Result<()> {
        self.managed_ledger
            .ack_message_shared(topic, subscription, message_id)
    }

    /// Look up a message by its full `MessageId`.
    pub fn get_message_by_id(
        &self,
        topic: &str,
        message_id: &MessageId,
    ) -> Option<(MessageId, Vec<u8>)> {
        self.managed_ledger.get_message_by_id(topic, message_id)
    }

    /// Return the current in-memory message list for a topic.
    pub fn get_messages(&self, topic: &str) -> Vec<(MessageId, Vec<u8>)> {
        self.managed_ledger.get_messages(topic)
    }

    /// Check whether a message is already covered by the Shared ack frontier.
    pub fn is_acknowledged_shared(
        &self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
    ) -> bool {
        self.managed_ledger
            .is_acknowledged_shared(topic, subscription, message_id)
    }

    /// Return the current Shared `mark_delete` frontier for a subscription.
    pub fn get_mark_delete_position(&self, topic: &str, subscription: &str) -> Option<u64> {
        self.managed_ledger
            .get_mark_delete_position(topic, subscription)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::tempdir;

    fn create_storage() -> Storage {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("test-storage-{unique}.db"));
        Storage::new_memory(&path).unwrap()
    }

    #[test]
    fn new_memory_uses_memory_managed_ledger_store() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("storage.db");
        let mut storage = Storage::new_memory(&db_path).unwrap();

        let topic = "persistent://public/default/memory-store-test";
        storage.create_topic(topic).unwrap();
        let message_id = storage.append_message(topic, -1, b"memory").unwrap();

        assert_eq!(message_id.entry, 0);
        assert_eq!(
            storage.get_message_by_id(topic, &message_id).unwrap().1,
            b"memory".to_vec()
        );
    }

    #[cfg(feature = "rocksdb-storage")]
    #[test]
    fn new_uses_rocksdb_managed_ledger_store_when_feature_enabled() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("storage.db");
        let topic = "persistent://public/default/default-rocksdb-store-test";

        let message_id = {
            let mut storage = Storage::new(&db_path).unwrap();
            storage.create_topic(topic).unwrap();
            storage
                .append_message(topic, -1, b"default-durable")
                .unwrap()
        };

        let storage = Storage::new(&db_path).unwrap();
        assert_eq!(
            storage.get_message_by_id(topic, &message_id).unwrap().1,
            b"default-durable".to_vec()
        );
    }

    #[cfg(feature = "rocksdb-storage")]
    #[test]
    fn new_rocksdb_persists_messages_across_reopen() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("storage.db");
        let topic = "persistent://public/default/rocksdb-store-test";

        let message_id = {
            let mut storage = Storage::new_rocksdb(&db_path).unwrap();
            storage.create_topic(topic).unwrap();
            storage.append_message(topic, -1, b"durable").unwrap()
        };

        let storage = Storage::new_rocksdb(&db_path).unwrap();
        assert_eq!(
            storage.get_message_by_id(topic, &message_id).unwrap().1,
            b"durable".to_vec()
        );
    }

    #[cfg(feature = "rocksdb-storage")]
    #[test]
    fn rocksdb_persists_cumulative_cursor_across_reopen() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("storage.db");
        let topic = "persistent://public/default/rocksdb-cursor-test";
        let subscription = "sub";

        let acked = {
            let mut storage = Storage::new_rocksdb(&db_path).unwrap();
            storage.create_topic(topic).unwrap();
            storage.subscribe(topic, subscription).unwrap();
            storage.append_message(topic, -1, b"0").unwrap();
            let msg1 = storage.append_message(topic, -1, b"1").unwrap();
            storage
                .ack_message(topic, subscription, msg1.clone())
                .unwrap();
            msg1
        };

        let mut storage = Storage::new_rocksdb(&db_path).unwrap();
        assert_eq!(
            storage
                .get_next_unassigned_message(topic, subscription, 1)
                .unwrap(),
            None
        );
        assert_eq!(
            storage.get_mark_delete_position(topic, subscription),
            Some(acked.entry)
        );
    }

    #[cfg(feature = "rocksdb-storage")]
    #[test]
    fn rocksdb_persists_shared_ack_holes_across_reopen() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("storage.db");
        let topic = "persistent://public/default/rocksdb-shared-holes-test";
        let subscription = "sub";

        let (msg0, msg1, msg2) = {
            let mut storage = Storage::new_rocksdb(&db_path).unwrap();
            storage.create_topic(topic).unwrap();
            storage.subscribe(topic, subscription).unwrap();
            let msg0 = storage.append_message(topic, -1, b"0").unwrap();
            let msg1 = storage.append_message(topic, -1, b"1").unwrap();
            let msg2 = storage.append_message(topic, -1, b"2").unwrap();

            storage
                .ack_message_shared(topic, subscription, msg2.clone())
                .unwrap();
            storage
                .ack_message_shared(topic, subscription, msg1.clone())
                .unwrap();
            assert_eq!(storage.get_mark_delete_position(topic, subscription), None);
            (msg0, msg1, msg2)
        };

        let mut storage = Storage::new_rocksdb(&db_path).unwrap();
        assert!(storage.is_acknowledged_shared(topic, subscription, &msg1));
        assert!(storage.is_acknowledged_shared(topic, subscription, &msg2));
        assert_eq!(storage.get_mark_delete_position(topic, subscription), None);

        storage
            .ack_message_shared(topic, subscription, msg0)
            .unwrap();
        assert_eq!(
            storage.get_mark_delete_position(topic, subscription),
            Some(2)
        );
    }

    #[test]
    fn shared_ack_out_of_order_advances_only_when_contiguous() {
        let mut storage = create_storage();
        let topic = "persistent://public/default/test";
        let sub = "sub";

        storage.create_topic(topic).unwrap();
        storage.subscribe(topic, sub).unwrap();

        let msg0 = storage.append_message(topic, -1, b"0").unwrap();
        let msg1 = storage.append_message(topic, -1, b"1").unwrap();
        let msg2 = storage.append_message(topic, -1, b"2").unwrap();

        storage
            .ack_message_shared(topic, sub, msg2.clone())
            .unwrap();
        assert_eq!(storage.get_mark_delete_position(topic, sub), None);
        assert!(storage.is_acknowledged_shared(topic, sub, &msg2));

        storage
            .ack_message_shared(topic, sub, msg1.clone())
            .unwrap();
        assert_eq!(storage.get_mark_delete_position(topic, sub), None);
        assert!(storage.is_acknowledged_shared(topic, sub, &msg1));

        storage.ack_message_shared(topic, sub, msg0).unwrap();
        assert_eq!(storage.get_mark_delete_position(topic, sub), Some(2));
    }

    #[test]
    fn shared_first_ack_non_zero_does_not_jump_frontier() {
        let mut storage = create_storage();
        let topic = "persistent://public/default/test";
        let sub = "sub";

        storage.create_topic(topic).unwrap();
        storage.subscribe(topic, sub).unwrap();
        for i in 0..6u8 {
            storage.append_message(topic, -1, &[i]).unwrap();
        }

        let msg5 = MessageId {
            ledger: 0,
            entry: 5,
            partition: -1,
        };
        storage
            .ack_message_shared(topic, sub, msg5.clone())
            .unwrap();

        assert_eq!(storage.get_mark_delete_position(topic, sub), None);
        let next = storage
            .get_next_unassigned_message(topic, sub, 1)
            .unwrap()
            .unwrap();
        assert_eq!(next.0.entry, 0);
        assert!(storage.is_acknowledged_shared(topic, sub, &msg5));
    }

    #[test]
    fn parse_topic_name_accepts_standard_pulsar_names() {
        let parsed = Storage::parse_topic_name("persistent://public/default/test").unwrap();
        assert_eq!(parsed.domain, "persistent");
        assert_eq!(parsed.tenant, "public");
        assert_eq!(parsed.namespace, "default");
        assert_eq!(parsed.local_name, "test");
    }

    #[test]
    fn parse_topic_name_accepts_non_persistent_names() {
        let parsed = Storage::parse_topic_name("non-persistent://public/default/test").unwrap();
        assert_eq!(parsed.domain, "non-persistent");
        assert_eq!(parsed.tenant, "public");
        assert_eq!(parsed.namespace, "default");
        assert_eq!(parsed.local_name, "test");
    }

    #[test]
    fn parse_topic_name_rejects_invalid_names() {
        assert!(Storage::parse_topic_name("public/default/test").is_err());
        assert!(Storage::parse_topic_name("persistent://public/default").is_err());
        assert!(Storage::parse_topic_name("other://public/default/test").is_err());
    }

    #[test]
    fn metadata_ensure_is_idempotent_and_persists_partitioned_topics() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("storage.db");
        let mut storage = Storage::new_memory(&db_path).unwrap();

        let topic = "persistent://public/default/test";
        storage
            .resources_mut()
            .ensure_topic(topic, true, 3, Storage::METADATA_VERSION)
            .unwrap();
        storage
            .resources_mut()
            .ensure_topic(topic, true, 3, Storage::METADATA_VERSION)
            .unwrap();
        storage
            .resources_mut()
            .ensure_subscription(topic, "sub", Storage::METADATA_VERSION)
            .unwrap();
        storage
            .resources_mut()
            .ensure_subscription(topic, "sub", Storage::METADATA_VERSION)
            .unwrap();

        assert!(storage.resources().has_tenant("public"));
        assert!(storage.resources().has_namespace("public", "default"));
        assert!(storage.resources().has_subscription(topic, "sub"));
        let metadata = storage.resources().get_topic_metadata(topic).unwrap();
        assert!(metadata.partitioned);
        assert_eq!(metadata.partition_count, 3);

        let document = storage.build_metadata_document();
        let path_key = storage.metadata_path().display().to_string();
        let topic_node = &document.resource_files[&path_key].tenants["public"].namespaces
            ["default"]
            .domains["persistent"]
            .topics["test"];
        assert!(topic_node.subscriptions.contains_key("sub"));
        assert_eq!(
            document.partitioned_topics["persistent://public/default/test"].partitions,
            3
        );

        let reloaded = Storage::new_memory(&db_path).unwrap();
        let metadata = reloaded.resources().get_topic_metadata(topic).unwrap();
        assert!(metadata.partitioned);
        assert_eq!(metadata.partition_count, 3);
        assert!(reloaded.resources().has_subscription(topic, "sub"));
    }

    #[test]
    fn partition_topics_are_persisted_as_concrete_topics() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("storage.db");
        let mut storage = Storage::new_memory(&db_path).unwrap();

        let base_topic = "persistent://public/default/test";
        let partition_topic = "persistent://public/default/test-partition-0";
        storage
            .resources_mut()
            .ensure_topic(base_topic, true, 3, Storage::METADATA_VERSION)
            .unwrap();
        storage
            .resources_mut()
            .ensure_topic(partition_topic, false, 0, Storage::METADATA_VERSION)
            .unwrap();
        storage
            .resources_mut()
            .ensure_subscription(partition_topic, "sub", Storage::METADATA_VERSION)
            .unwrap();

        assert!(storage.resources().get_topic_metadata(base_topic).is_some());
        assert!(storage
            .resources()
            .get_topic_metadata(partition_topic)
            .is_some());
        assert!(!storage.resources().has_subscription(base_topic, "sub"));
        assert!(storage.resources().has_subscription(partition_topic, "sub"));

        let document = storage.build_metadata_document();
        let path_key = storage.metadata_path().display().to_string();
        let topics = &document.resource_files[&path_key].tenants["public"].namespaces["default"]
            .domains["persistent"]
            .topics;
        assert!(!topics.contains_key("test"));
        assert!(topics.contains_key("test-partition-0"));
        assert!(topics["test-partition-0"].subscriptions.contains_key("sub"));
        assert_eq!(
            document.partitioned_topics["persistent://public/default/test"].partitions,
            3
        );
        assert!(!document
            .partitioned_topics
            .contains_key("persistent://public/default/test-partition-0"));
    }

    #[test]
    fn metadata_file_corruption_returns_error() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("storage.db");
        let metadata_path = db_path.with_extension("metadata.json");
        fs::write(&metadata_path, "{not-json").unwrap();

        let error = Storage::new_memory(&db_path).unwrap_err();
        assert!(error.to_string().contains("Failed to parse metadata file"));
    }

    #[test]
    fn old_flat_metadata_snapshot_format_is_rejected() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("storage.db");
        let metadata_path = db_path.with_extension("metadata.json");
        fs::write(
            &metadata_path,
            serde_json::json!({
                "version": 1,
                "tenants": [{"name": "public"}],
                "namespaces": [{"tenant": "public", "name": "default"}],
                "topics": [],
                "subscriptions": [],
            })
            .to_string(),
        )
        .unwrap();

        let error = Storage::new_memory(&db_path).unwrap_err();
        assert!(error
            .to_string()
            .contains("old flat MetadataSnapshot format is no longer supported"));
    }
}
