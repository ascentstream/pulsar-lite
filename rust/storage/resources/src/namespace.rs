use anyhow::Result;
use pulsar_lite_storage_metadata::MetadataStore;

/// Namespace resource accessor skeleton.
#[derive(Debug, Clone, Default)]
pub struct NamespaceResources;

impl NamespaceResources {
    pub fn new() -> Self {
        Self
    }

    pub fn ensure_namespace<S: MetadataStore>(
        &self,
        metadata: &mut S,
        tenant: &str,
        namespace: &str,
        version: u32,
    ) -> Result<()> {
        let mut changed = metadata.state_mut().insert_tenant_metadata(tenant);
        changed |= metadata
            .state_mut()
            .insert_namespace_metadata(tenant, namespace);
        if changed {
            metadata.persist_document(version)?;
        }
        Ok(())
    }

    pub fn has_namespace<S: MetadataStore>(
        &self,
        metadata: &S,
        tenant: &str,
        namespace: &str,
    ) -> bool {
        metadata.state().has_namespace_metadata(tenant, namespace)
    }
}
