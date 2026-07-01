use pulsar_lite_storage_metadata::MetadataStore;
use anyhow::Result;

#[derive(Debug,Clone,Default)]
pub struct TenantResources;

impl TenantResources {
    pub fn new() -> Self {
        Self
    }

    pub fn ensure_tenant<S: MetadataStore>(&self, metadata:&mut S,tenant:&str,version: u32) -> Result<()> {
        let changed =
          metadata.state_mut().insert_tenant_metadata(tenant);

          if changed {
              metadata.persist_document(version)?;
          }

          Ok(())
    }

    pub fn has_tenant<S: MetadataStore>(
        &self,
        metadata:&S,
        tenant:&str,
    ) -> bool {
        metadata.state().has_tenant_metadata(tenant)
    }
}   
