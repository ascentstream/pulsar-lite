use anyhow::Result;
use pulsar_lite_storage_metadata::{
    parse_topic_name, MetadataStore, TopicMetadata,
};
use std::collections::HashMap;

/// Topic resource accessor skeleton.
#[derive(Debug, Clone, Default)]
pub struct TopicResources;

impl TopicResources {
    pub fn new() -> Self {
        Self
    }

    pub fn ensure_topic<S: MetadataStore>(
        &mut self,
        metadata: &mut S,
        topic: &str,
        partitioned: bool,
        partition_count: usize,
        version: u32,
    ) -> Result<()> {
        let parsed = parse_topic_name(topic)?;

        let mut changed = metadata
            .state_mut()
            .insert_tenant_metadata(&parsed.tenant);

        changed |= metadata
            .state_mut()
            .insert_namespace_metadata(&parsed.tenant,
            &parsed.namespace);

        changed |=
        metadata.state_mut().upsert_topic_metadata(TopicMetadata {
            full_name: topic.to_string(),
            domain: parsed.domain,
            tenant: parsed.tenant,
            namespace: parsed.namespace,
            local_name: parsed.local_name,
            partitioned,
            partition_count: if partitioned {
                partition_count.max(1)
            } else {
                0
            },
        });

        if changed {
            metadata.persist_document(version)?;
        }

        Ok(())
    }

    pub fn ensure_subscription<S: MetadataStore>(
        &mut self,
        metadata: &mut S,
        topic: &str,
        subscription: &str,
        version: u32,
    ) -> Result<()> {
        let parsed = parse_topic_name(topic)?;

        let mut changed = metadata
            .state_mut()
            .insert_tenant_metadata(&parsed.tenant);

        changed |= metadata
            .state_mut()
            .insert_namespace_metadata(&parsed.tenant,
            &parsed.namespace);

        changed |=
        metadata.state_mut().upsert_topic_metadata(TopicMetadata {
            full_name: topic.to_string(),
            domain: parsed.domain,
            tenant: parsed.tenant,
            namespace: parsed.namespace,
            local_name: parsed.local_name,
            partitioned: false,
            partition_count: 0,
        });

        changed |= metadata
            .state_mut()
            .insert_subscription_metadata(topic, subscription);

        if changed {
            metadata.persist_document(version)?;
        }

        Ok(())
    }

    pub fn get_topic_metadata<'a, S: MetadataStore>(
        &self,
        metadata: &'a S,
        topic: &str,
    ) -> Option<&'a TopicMetadata> {
        metadata.state().get_topic_metadata(topic)
    }

    pub fn has_subscription<S: MetadataStore>(
        &self,
        metadata: &S,
        topic: &str,
        subscription: &str,
    ) -> bool {
        metadata
            .state()
            .has_subscription_metadata(topic, subscription)
    }

    pub fn get_partitioned_topic_metadata<S: MetadataStore>(
        &self,
        metadata: &S,
    ) -> HashMap<String, usize> {
        metadata.state().get_partitioned_topic_metadata()
    }
}