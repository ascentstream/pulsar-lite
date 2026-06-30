use std::fmt;

/// Type-safe wrapper around a metadata storage key string.
#[derive(Debug,Clone,PartialEq,Eq,Hash)]
pub struct MetadataKey(String);

impl MetadataKey {
    pub fn tenant(tenant: &str) -> Self {
        Self(tenant.to_string())
    }

    pub fn namespace(tenant: &str, namespace: &str) -> Self {
        Self(namespace_key(tenant,namespace))
    }

    pub fn topic(topic: &str) -> Self {
        Self(topic.to_string())
    }

    pub fn subscription(topic: &str,subscription: &str) -> Self {
        Self(subscription_key(topic, subscription))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for MetadataKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<MetadataKey> for String {
    fn from(value: MetadataKey) -> Self {
        value.0
    }
}

pub fn namespace_key(tenant: &str, namespace: &str) -> String {
    format!("{tenant}/{namespace}")
}

pub fn subscription_key(topic: &str, subscription: &str) -> String {
    format!("{topic}:{subscription}")
}