//! Metadata resources integration tests for `Storage`.

use pulsar_lite_storage::Storage;
use pulsar_lite_storage_metadata::parse_topic_name;
use std::fs;
use tempfile::tempdir;

    #[test]
    fn parse_topic_name_accepts_standard_pulsar_names() {
        let parsed = parse_topic_name("persistent://public/default/test").unwrap();
        assert_eq!(parsed.domain, "persistent");
        assert_eq!(parsed.tenant, "public");
        assert_eq!(parsed.namespace, "default");
        assert_eq!(parsed.local_name, "test");
    }

    #[test]
    fn parse_topic_name_accepts_non_persistent_names() {
        let parsed = parse_topic_name("non-persistent://public/default/test").unwrap();
        assert_eq!(parsed.domain, "non-persistent");
        assert_eq!(parsed.tenant, "public");
        assert_eq!(parsed.namespace, "default");
        assert_eq!(parsed.local_name, "test");
    }

    #[test]
    fn parse_topic_name_rejects_invalid_names() {
        assert!(parse_topic_name("public/default/test").is_err());
        assert!(parse_topic_name("persistent://public/default").is_err());
        assert!(parse_topic_name("other://public/default/test").is_err());
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

        let document = storage
            .resources()
            .build_metadata_document(Storage::METADATA_VERSION);
        let path_key = storage.resources().metadata_path().display().to_string();
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

        let document = storage
            .resources()
            .build_metadata_document(Storage::METADATA_VERSION);
        let path_key = storage.resources().metadata_path().display().to_string();
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
