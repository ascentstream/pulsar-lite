/// Core storage result alias. Detailed error taxonomy can be added in later phases.
pub type StorageResult<T> = anyhow::Result<T>;
