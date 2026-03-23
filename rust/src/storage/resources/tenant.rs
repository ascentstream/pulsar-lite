use super::BaseResources;
use crate::storage::MetadataStore;
use anyhow::Result;

/// Tenant resource accessor skeleton.
#[derive(Debug, Clone, Default)]
pub struct TenantResources {
    _base: BaseResources,
}

impl TenantResources {
    pub fn new() -> Self {
        Self {
            _base: BaseResources::new(),
        }
    }

    pub fn ensure_tenant(
        &mut self,
        metadata: &mut MetadataStore,
        tenant: &str,
        version: u32,
    ) -> Result<()> {
        let changed = self._base.ensure_tenant_parent(metadata, tenant);
        self._base.persist_if_changed(metadata, version, changed)
    }

    pub fn has_tenant(&self, metadata: &MetadataStore, tenant: &str) -> bool {
        self._base.has_tenant(metadata, tenant)
    }
}
