use anyhow::{anyhow, Result};
use std::collections::BTreeMap;

/// Memory Metadata
//  Example: topics_meta["persistent://public/default/my-topic-partition-0"] = TopicMetadata {
//     full_name: "persistent://public/default/my-topic-partition-0",
//     domain: "persistent",
//     tenant: "public",
//     namespace: "default",
//     local_name: "my-topic-partition-0",
//     partitioned: false,
//     partition_count: 0,
// }

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

/// File json Metadata
//  Example: {
//          "version": 2,
//          "/data/storage.db.metadata.json": {
//               "public": {
//                 "default": {
//                   "persistent": {
//                     "my-topic-partition-0": { "subscriptions": { "sub": {} } },
//                     "my-topic-partition-1": { "subscriptions": { "sub": {} } },
//                     "my-topic-partition-2": { "subscriptions": { "sub": {} } }
//                   }
//              }
//             }
//            },
//            "partitioned_topics": {
//              "persistent://public/default/my-topic": { "partitions": 3 }
//            }
//       }

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

/// persistent://public/default/my-topic-partition-0 -> ParsedTopicName
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
        .ok_or_else(|| anyhow!("Invalid topic name '{}': missing local name", topic))?;

    Ok(ParsedTopicName {
        domain: domain.to_string(),
        tenant: tenant.to_string(),
        namespace: namespace.to_string(),
        local_name: local_name.to_string(),
    })
}

/// restore the "physical full name of the partitioned topic" to its "logical topic name".
/// Examples:
///     persistent://public/default/my-topic -> persistent://public/default/my-topic
///     persistent://public/default/my-topic-partition-0 -> persistent://public/default/my-topic
///     persistent://public/default/my-topic-partition-2 -> persistent://public/default/my-topic
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
