use super::traits::MetadataBackend;
use super::types::MetadataDocument;
use anyhow::{anyhow, Result};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Default)]
pub struct JsonFileMetadataStore {
    metadata_path: PathBuf,
}

impl JsonFileMetadataStore {
    pub fn new(db_path: &Path) -> Self {
        Self {
            metadata_path: Self::metadata_path_from_db_path(db_path),
        }
    }

    pub(crate) fn metadata_path_from_db_path(path: &Path) -> PathBuf {
        path.with_extension("metadata.json")
    }

    pub fn metadata_path(&self) -> &Path {
        &self.metadata_path
    }

    pub fn load_document(&self) -> Result<Option<MetadataDocument>> {
        if !self.metadata_path.exists() {
            return Ok(None);
        }

        let content = fs::read_to_string(&self.metadata_path).map_err(|error| {
            anyhow!(
                "Failed to read metadata file '{}': {error}",
                self.metadata_path.display()
            )
        })?;
        let raw: serde_json::Value = serde_json::from_str(&content).map_err(|error| {
            anyhow!(
                "Failed to parse metadata file '{}': {error}",
                self.metadata_path.display()
            )
        })?;

        if raw.get("tenants").is_some()
            || raw.get("namespaces").is_some()
            || raw.get("topics").is_some()
            || raw.get("subscriptions").is_some()
        {
            return Err(anyhow!(
                "Failed to parse metadata file '{}': old flat MetadataSnapshot format is no longer supported; delete the metadata file and recreate resources",
                self.metadata_path.display()
            ));
        }

        let document: MetadataDocument = serde_json::from_value(raw).map_err(|error| {
            anyhow!(
                "Failed to parse metadata file '{}': {error}",
                self.metadata_path.display()
            )
        })?;
        Ok(Some(document))
    }

    pub fn save_document(&self, document: &MetadataDocument) -> Result<()> {
        if let Some(parent) = self.metadata_path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                anyhow!(
                    "Failed to create metadata directory '{}': {error}",
                    parent.display()
                )
            })?;
        }

        let serialized = serde_json::to_string_pretty(document)?;
        let tmp_path = self.metadata_path.with_extension("metadata.json.tmp");
        fs::write(&tmp_path, serialized).map_err(|error| {
            anyhow!(
                "Failed to write temporary metadata file '{}': {error}",
                tmp_path.display()
            )
        })?;
        fs::rename(&tmp_path, &self.metadata_path).map_err(|error| {
            anyhow!(
                "Failed to replace metadata file '{}' with '{}': {error}",
                self.metadata_path.display(),
                tmp_path.display()
            )
        })?;
        Ok(())
    }
}

impl MetadataBackend for JsonFileMetadataStore {
    fn metadata_path(&self) -> &Path {
        JsonFileMetadataStore::metadata_path(self)
    }

    fn load_document(&self) -> Result<Option<MetadataDocument>> {
        JsonFileMetadataStore::load_document(self)
    }

    fn save_document(&self, document: &MetadataDocument) -> Result<()> {
        JsonFileMetadataStore::save_document(self, document)
    }
}
