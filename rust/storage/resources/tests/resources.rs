use pulsar_lite_storage_metadata::{FileMetadataStore, InMemoryMetadataStore, MetadataStore};
use pulsar_lite_storage_resources::{
    NamespaceResources, PulsarResources, TenantResources, TopicResources,
};
use tempfile::tempdir;

const VERSION: u32 = 2;

#[test]
fn tenant_resource_ensures_tenant() {
    let mut store = InMemoryMetadataStore::new();
    let resources = TenantResources::new();

    resources
        .ensure_tenant(&mut store, "public", VERSION)
        .unwrap();

    assert!(store.state().has_tenant_metadata("public"));
}

#[test]
fn namespace_resource_ensures_tenant_parent_and_namespace() {
    let mut store = InMemoryMetadataStore::new();
    let resources = NamespaceResources::new();

    resources
        .ensure_namespace(&mut store, "public", "default", VERSION)
        .unwrap();

    assert!(store.state().has_tenant_metadata("public"));
    assert!(store.state().has_namespace_metadata("public", "default"));
}

#[test]
fn topic_resource_ensures_topic_with_parents() {
    let mut store = InMemoryMetadataStore::new();
    let mut resources = TopicResources::new();
    let topic = "persistent://public/default/t1";

    resources
        .ensure_topic(&mut store, topic, false, 0, VERSION)
        .unwrap();

    assert!(store.state().has_tenant_metadata("public"));
    assert!(store.state().has_namespace_metadata("public", "default"));

    let metadata = store.state().get_topic_metadata(topic).unwrap();
    assert_eq!(metadata.full_name, topic);
    assert_eq!(metadata.domain, "persistent");
    assert_eq!(metadata.tenant, "public");
    assert_eq!(metadata.namespace, "default");
    assert_eq!(metadata.local_name, "t1");
    assert!(!metadata.partitioned);
    assert_eq!(metadata.partition_count, 0);
}

#[test]
fn topic_resource_normalizes_partitioned_topic_count() {
    let mut store = InMemoryMetadataStore::new();
    let mut resources = TopicResources::new();
    let topic = "persistent://public/default/partitioned";

    resources
        .ensure_topic(&mut store, topic, true, 0, VERSION)
        .unwrap();

    let metadata = store.state().get_topic_metadata(topic).unwrap();
    assert!(metadata.partitioned);
    assert_eq!(metadata.partition_count, 1);
}

#[test]
fn topic_resource_ensures_subscription_and_topic() {
    let mut store = InMemoryMetadataStore::new();
    let mut resources = TopicResources::new();
    let topic = "persistent://public/default/t2";

    resources
        .ensure_subscription(&mut store, topic, "sub", VERSION)
        .unwrap();

    assert!(store.state().get_topic_metadata(topic).is_some());
    assert!(store.state().has_subscription_metadata(topic, "sub"));
}

#[test]
fn pulsar_resources_supports_in_memory_metadata_store() {
    let store = InMemoryMetadataStore::new();
    let mut resources = PulsarResources::from_metadata_store(store);
    let topic = "persistent://public/default/t3";

    resources.ensure_topic(topic, true, 3, VERSION).unwrap();
    resources
        .ensure_subscription(topic, "sub", VERSION)
        .unwrap();

    assert!(resources.has_tenant("public"));
    assert!(resources.has_namespace("public", "default"));
    assert!(resources.get_topic_metadata(topic).is_some());
    assert!(resources.has_subscription(topic, "sub"));
    assert_eq!(
        resources
            .get_partitioned_topic_metadata()
            .get(topic)
            .copied(),
        Some(3)
    );
}

#[test]
fn pulsar_resources_file_store_roundtrips_metadata() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("storage.db");
    let topic = "persistent://public/default/roundtrip";

    {
        let mut resources = PulsarResources::<FileMetadataStore>::new(&db_path).unwrap();

        resources.ensure_topic(topic, true, 4, VERSION).unwrap();
        resources
            .ensure_subscription(topic, "sub", VERSION)
            .unwrap();
    }

    let reopened = PulsarResources::<FileMetadataStore>::new(&db_path).unwrap();

    assert!(reopened.has_tenant("public"));
    assert!(reopened.has_namespace("public", "default"));
    assert!(reopened.get_topic_metadata(topic).is_some());
    assert!(reopened.has_subscription(topic, "sub"));
    assert_eq!(
        reopened
            .get_partitioned_topic_metadata()
            .get(topic)
            .copied(),
        Some(4)
    );
}
