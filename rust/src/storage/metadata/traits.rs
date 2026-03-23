use super::types::MetadataDocument;
use anyhow::Result;
use std::path::Path;

/// Metadata backend skeleton.
///
/// This trait only models document-level persistence and intentionally does not
/// include broker resource semantics such as tenant/topic/subscription helpers.
pub trait MetadataBackend {
    fn metadata_path(&self) -> &Path;
    fn load_document(&self) -> Result<Option<MetadataDocument>>;
    fn save_document(&self, document: &MetadataDocument) -> Result<()>;
}
