//! Broker resource API translating tenant/namespace/topic/subscription ops onto a metadata store.

mod namespace;
mod pulsar;
mod tenant;
mod topic;

pub use namespace::NamespaceResources;
pub use pulsar::PulsarResources;
pub use tenant::TenantResources;
pub use topic::TopicResources;