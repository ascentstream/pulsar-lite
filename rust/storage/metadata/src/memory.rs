use crate::store::{MetadataState, MetadataStore};
use anyhow::Result;
use std::path::PathBuf;

/// In-memory metadata store: no persistence, used for tests and ephemeral runs.
///
/// HashMap key/value examples:
///
/// - `tenants["public"] = TenantMetadata { name: "public" }`
/// - `namespaces["public/default"] = NamespaceMetadata { tenant: "public", name: "default" }`
/// - `topics["persistent://public/default/my-topic"] = TopicMetadata { full_name: "persistent://public/default/my-topic", ... }`
/// - `subscriptions["persistent://public/default/my-topic:sub"] = SubscriptionMetadata { topic: "persistent://public/default/my-topic", name: "sub" }`
#[derive(Debug, Default)]
pub struct InMemoryMetadataStore {
    state: MetadataState,
}

impl InMemoryMetadataStore {
    pub fn new() -> Self {
        Self {
            state: MetadataState::new(PathBuf::new()),
        }
    }
}

impl MetadataStore for InMemoryMetadataStore {
    fn state(&self) -> &MetadataState {
        &self.state
    }
    fn state_mut(&mut self) -> &mut MetadataState {
        &mut self.state
    }

    fn load(&mut self) -> Result<()> {
        Ok(())
    }

    fn persist_document(&self, _version: u32) -> Result<()> {
        Ok(())
    }
}
