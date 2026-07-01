use pulsar_lite_storage_metadata::{FileMetadataStore, TopicMetadata};
use tempfile::tempdir;

#[test]
fn file_store_roundtrips_metadata_across_reopen() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("storage.db");
    let topic = "persistent://public/default/roundtrip";

    {
        let mut store = FileMetadataStore::new(&db_path).unwrap();
        store.insert_tenant_metadata("public");
        store.insert_namespace_metadata("public", "default");
        store.upsert_topic_metadata(TopicMetadata {
            full_name: topic.to_string(),
            domain: "persistent".to_string(),
            tenant: "public".to_string(),
            namespace: "default".to_string(),
            local_name: "roundtrip".to_string(),
            partitioned: false,
            partition_count: 0,
        });
        store.insert_subscription_metadata(topic, "sub");
        store.persist_document(2).unwrap();
    }

    let reopened = FileMetadataStore::new(&db_path).unwrap();
    assert!(reopened.has_tenant_metadata("public"));
    assert!(reopened.has_namespace_metadata("public", "default"));
    assert!(reopened.get_topic_metadata(topic).is_some());
    assert!(reopened.has_subscription_metadata(topic, "sub"));
}
