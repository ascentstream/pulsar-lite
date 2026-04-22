use super::BaseResources;
use crate::storage::MetadataStore;
use anyhow::Result;

/// Namespace resource accessor skeleton.
#[derive(Debug, Clone, Default)]
pub struct NamespaceResources {
    _base: BaseResources,
}

impl NamespaceResources {
    pub fn new() -> Self {
        Self {
            _base: BaseResources::new(),
        }
    }

    pub fn ensure_namespace(
        &mut self,
        metadata: &mut MetadataStore,
        tenant: &str,
        namespace: &str,
        version: u32,
    ) -> Result<()> {
        let changed = self
            ._base
            .ensure_namespace_parents(metadata, tenant, namespace);
        self._base.persist_if_changed(metadata, version, changed)
    }

    pub fn has_namespace(&self, metadata: &MetadataStore, tenant: &str, namespace: &str) -> bool {
        self._base.has_namespace(metadata, tenant, namespace)
    }
}
