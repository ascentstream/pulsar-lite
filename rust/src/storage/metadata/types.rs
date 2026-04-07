use anyhow::{anyhow, Result};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TenantMetadata {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NamespaceMetadata {
    pub tenant: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TopicMetadata {
    pub full_name: String,
    pub domain: String,
    pub tenant: String,
    pub namespace: String,
    pub local_name: String,
    pub partitioned: bool,
    pub partition_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubscriptionMetadata {
    pub topic: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedTopicName {
    pub domain: String,
    pub tenant: String,
    pub namespace: String,
    pub local_name: String,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct MetadataDocument {
    pub version: u32,
    #[serde(flatten)]
    pub resource_files: BTreeMap<String, MetadataFileNode>,
    pub partitioned_topics: BTreeMap<String, PartitionedTopicNode>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct MetadataFileNode {
    #[serde(flatten)]
    pub tenants: BTreeMap<String, TenantNode>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct TenantNode {
    #[serde(flatten)]
    pub namespaces: BTreeMap<String, NamespaceNode>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct NamespaceNode {
    #[serde(flatten)]
    pub domains: BTreeMap<String, DomainNode>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct DomainNode {
    #[serde(flatten)]
    pub topics: BTreeMap<String, TopicNode>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct TopicNode {
    #[serde(default)]
    pub subscriptions: BTreeMap<String, SubscriptionNode>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SubscriptionNode {}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionedTopicNode {
    pub partitions: usize,
}

pub fn parse_topic_name(topic: &str) -> Result<ParsedTopicName> {
    let (domain, rest) = topic
        .split_once("://")
        .ok_or_else(|| anyhow!("Invalid topic name '{}': missing domain", topic))?;

    if domain != "persistent" && domain != "non-persistent" {
        return Err(anyhow!(
            "Invalid topic name '{}': only persistent:// and non-persistent:// topics are supported",
            topic
        ));
    }

    let mut parts = rest.splitn(3, '/');
    let tenant = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("Invalid topic name '{}': missing tenant", topic))?;
    let namespace = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("Invalid topic name '{}': missing namespace", topic))?;
    let local_name = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("Invalid topic name '{}': missing local topic name", topic))?;

    Ok(ParsedTopicName {
        domain: domain.to_string(),
        tenant: tenant.to_string(),
        namespace: namespace.to_string(),
        local_name: local_name.to_string(),
    })
}

pub fn namespace_key(tenant: &str, namespace: &str) -> String {
    format!("{tenant}/{namespace}")
}

pub fn subscription_key(topic: &str, subscription: &str) -> String {
    format!("{topic}:{subscription}")
}

pub fn logical_topic_name(topic: &str) -> String {
    let Ok(parsed) = parse_topic_name(topic) else {
        return topic.to_string();
    };

    let Some((base_local_name, suffix)) = parsed.local_name.rsplit_once("-partition-") else {
        return topic.to_string();
    };
    if suffix.parse::<usize>().is_err() {
        return topic.to_string();
    }

    format!(
        "{}://{}/{}/{}",
        parsed.domain, parsed.tenant, parsed.namespace, base_local_name
    )
}
