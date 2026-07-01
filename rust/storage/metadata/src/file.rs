use crate::model::MetadataDocument;
use crate::store::{MetadataState, MetadataStore};
use anyhow::{anyhow, Result};
use std::fs;
use std::path::{Path, PathBuf};

/// File-backed metadata store: persists tenant/namespace/topic/subscription
/// state to `<db_path>.metadata.json` using a `MetadataDocument` snapshot.
#[derive(Debug, Default)]
pub struct FileMetadataStore {
    state: MetadataState,
}

impl FileMetadataStore {
    pub fn new(db_path: &Path) -> Result<Self> {
        let mut store = Self {
            state: MetadataState::new(db_path.with_extension("metadata.json")),
        };
        store.load()?;
        Ok(store)
    }

    fn load_document(&self) -> Result<Option<MetadataDocument>> {
        if !self.state.metadata_path().exists() {
            return Ok(None);
        }
        let content = fs::read_to_string(&self.state.metadata_path()).map_err(|error| {
            anyhow!(
                "Failed to read metadata file '{}':{error}",
                self.state.metadata_path().display()
            )
        })?;
        let raw: serde_json::Value = serde_json::from_str(&content).map_err(|error| {
            anyhow!(
                "Failed to parse metadata file '{}':{error}",
                self.state.metadata_path().display()
            )
        })?;
        if raw.get("tenants").is_some()
            || raw.get("namespaces").is_some()
            || raw.get("topics").is_some()
            || raw.get("subscriptions").is_some()
        {
            return Err(anyhow!(
                "Failed to parse metadata file '{}': old flat MetadataSnapshot format is no longer supported; delete the metadata file and recreate resources",
                self.state.metadata_path().display()
            ));
        }
        let document: MetadataDocument = serde_json::from_value(raw).map_err(|error| {
            anyhow!(
                "Failed to parse metadata file '{}': {error}",
                self.state.metadata_path().display()
            )
        })?;
        Ok(Some(document))
    }

    fn save_document(&self, document: &MetadataDocument) -> Result<()> {
        if let Some(parent) = self.state.metadata_path().parent() {
            fs::create_dir_all(parent).map_err(|error| {
                anyhow!(
                    "Failed to create metadata directory '{}':{error}",
                    parent.display()
                )
            })?;
        }

        let serialized = serde_json::to_string_pretty(document)?;
        let tmp_path = PathBuf::from(format!("{}.tmp", self.state.metadata_path().display()));
        fs::write(&tmp_path, serialized).map_err(|error| {
            anyhow!(
                "Failed to write temporary metadata file '{}':{error}",
                tmp_path.display()
            )
        })?;
        fs::rename(&tmp_path, &self.state.metadata_path()).map_err(|error| {
            anyhow!(
                "Failed to replace metadata file '{}' with '{}': {error}",
                self.state.metadata_path().display(),
                tmp_path.display()
            )
        })?;
        Ok(())
    }
}

impl MetadataStore for FileMetadataStore {
    fn state(&self) -> &MetadataState {
        &self.state
    }
    fn state_mut(&mut self) -> &mut MetadataState {
        &mut self.state
    }

    fn load(&mut self) -> Result<()> {
        if let Some(document) = self.load_document()? {
            self.state.apply_metadata_document(document)?;
        }
        Ok(())
    }

    fn persist_document(&self, version: u32) -> Result<()> {
        let document = self.state.build_metadata_document(version);
        self.save_document(&document)
    }
}
