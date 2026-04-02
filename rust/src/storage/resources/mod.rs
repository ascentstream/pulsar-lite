//! Skeleton broker resource layer aligned with Pulsar's `broker/resources`
//! split. Real resource semantics still live behind `Storage` for now.

mod base;
mod namespace;
mod pulsar;
mod tenant;
mod topic;

pub use base::BaseResources;
pub use namespace::NamespaceResources;
pub use pulsar::PulsarResources;
pub use tenant::TenantResources;
pub use topic::TopicResources;
