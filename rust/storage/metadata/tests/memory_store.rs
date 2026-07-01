use pulsar_lite_storage_metadata::{InMemoryMetadataStore, MetadataStore, TopicMetadata};

#[test]
fn in_memory_store_inserts_and_queries_metadata() {
    let mut store = InMemoryMetadataStore::new();
    assert!(store.insert_tenant_metadata("public"));
    assert!(!store.insert_tenant_metadata("public"));
    assert!(store.has_tenant_metadata("public"));

    assert!(store.insert_namespace_metadata("public", "default"));
    assert!(store.has_namespace_metadata("public", "default"));

    store.upsert_topic_metadata(TopicMetadata {
        full_name: "persistent://public/default/t".to_string(),
        domain: "persistent".to_string(),
        tenant: "public".to_string(),
        namespace: "default".to_string(),
        local_name: "t".to_string(),
        partitioned: false,
        partition_count: 0,
    });
    assert!(store
        .get_topic_metadata("persistent://public/default/t")
        .is_some());

    assert!(store.insert_subscription_metadata("persistent://public/default/t", "sub"));
    assert!(store.has_subscription_metadata("persistent://public/default/t", "sub"));
}

#[test]
fn in_memory_store_load_and_persist_are_noop() {
    let mut store = InMemoryMetadataStore::new();
    assert!(store.load().is_ok());
    assert!(store.persist_document(2).is_ok());
}
