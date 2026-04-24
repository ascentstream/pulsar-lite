/*
 * Producer - represents a producer connection to a topic
 * Inspired by Apache Pulsar's Producer design
 */

use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Producer statistics
#[derive(Debug, Default, Clone)]
pub struct ProducerStats {
    /// Total number of messages sent
    pub messages_sent: u64,
    /// Total bytes sent
    pub bytes_sent: u64,
    /// Average message size
    pub avg_message_size: f64,
}

/// Forward declaration for Topic type
use super::topic::Topic;

/// Producer - represents a producer connection
/// Similar to Apache Pulsar's org.apache.pulsar.broker.service.Producer
#[derive(Debug, Clone)]
pub struct Producer {
    /// Producer ID (unique per connection)
    pub producer_id: u64,

    /// Producer name
    pub producer_name: String,

    /// Topic reference (Apache Pulsar style - Producer directly holds Topic)
    pub topic: Arc<RwLock<Topic>>,

    /// Connection ID (for tracking which connection this producer belongs to)
    pub connection_id: String,

    /// Statistics
    stats: Arc<RwLock<ProducerStats>>,
}

impl Producer {
    /// Create a new Producer (Apache Pulsar style - receives Topic reference)
    pub fn new(
        producer_id: u64,
        producer_name: String,
        topic: Arc<RwLock<Topic>>,
        connection_id: String,
    ) -> Self {
        Self {
            producer_id,
            producer_name,
            topic,
            connection_id,
            stats: Arc::new(RwLock::new(ProducerStats::default())),
        }
    }

    /// Update statistics when a message is sent
    pub async fn record_message_sent(&self, message_size: usize) {
        let mut stats = self.stats.write().await;
        stats.messages_sent += 1;
        stats.bytes_sent += message_size as u64;

        // Update average message size
        if stats.messages_sent > 0 {
            stats.avg_message_size = stats.bytes_sent as f64 / stats.messages_sent as f64;
        }
    }

    /// Publish a message to the topic
    ///
    /// This method:
    /// 1. (Future) Validates the message (checksum, encryption, etc.)
    /// 2. Records statistics
    /// 3. Delegates to Topic.publish_message()
    pub async fn publish_message(
        &self,
        metadata: Option<Bytes>,
        payload: Bytes,
    ) -> Result<crate::storage::MessageId, Box<dyn std::error::Error + Send + Sync>> {
        log::debug!(
            "Producer {} publishing message (metadata={} bytes, payload={} bytes)",
            self.producer_id,
            metadata.as_ref().map(|value| value.len()).unwrap_or(0),
            payload.len()
        );

        // TODO: Add validation logic here (Apache Pulsar style):
        // - Checksum verification
        // - Encryption validation
        // - Rate limiting
        // - Authorization checks
        // - Producer state validation (is_closed)

        // Record statistics before publishing
        self.record_message_sent(payload.len()).await;

        // Lock topic and publish (Apache Pulsar style)
        let mut topic = self.topic.write().await;
        let message_id = topic.publish_message(metadata, payload).await?;

        log::debug!(
            "Producer {} published message {}:{} to topic '{}'",
            self.producer_id,
            message_id.ledger,
            message_id.entry,
            topic.name
        );

        Ok(message_id)
    }

    /// Get current statistics
    pub async fn get_stats(&self) -> ProducerStats {
        self.stats.read().await.clone()
    }

    /// Get producer ID
    pub fn get_producer_id(&self) -> u64 {
        self.producer_id
    }

    /// Get producer name
    pub fn get_producer_name(&self) -> &str {
        &self.producer_name
    }

    /// Get topic reference
    pub fn get_topic(&self) -> Arc<RwLock<Topic>> {
        self.topic.clone()
    }

    /// Get topic name (convenience method)
    pub fn get_topic_name(&self) -> String {
        // Use try_read to avoid blocking, fallback to empty string if locked
        self.topic
            .try_read()
            .map(|t| t.name.clone())
            .unwrap_or_default()
    }
}

impl PartialEq for Producer {
    fn eq(&self, other: &Self) -> bool {
        self.producer_id == other.producer_id && self.connection_id == other.connection_id
    }
}

impl Eq for Producer {}

impl std::hash::Hash for Producer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.producer_id.hash(state);
        self.connection_id.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::super::topic::Topic;
    use super::*;
    use crate::storage::Storage;
    use std::path::Path;
    use std::sync::Arc as StdArc;
    use tokio::sync::Mutex;

    fn create_test_topic() -> Arc<RwLock<Topic>> {
        let storage = StdArc::new(Mutex::new(
            Storage::new(Path::new("/tmp/test-producer-storage")).unwrap(),
        ));
        Arc::new(RwLock::new(Topic::new("test-topic".to_string(), storage)))
    }

    #[tokio::test]
    async fn test_producer_stats() {
        let topic = create_test_topic();
        let producer = Producer::new(1, "test-producer".to_string(), topic, "conn-1".to_string());

        // Record some messages
        producer.record_message_sent(100).await;
        producer.record_message_sent(200).await;
        producer.record_message_sent(300).await;

        let stats = producer.get_stats().await;
        assert_eq!(stats.messages_sent, 3);
        assert_eq!(stats.bytes_sent, 600);
        assert_eq!(stats.avg_message_size, 200.0);
    }

    #[tokio::test]
    async fn test_producer_equality() {
        let topic = create_test_topic();
        let p1 = Producer::new(1, "p1".to_string(), topic.clone(), "conn-1".to_string());
        let p2 = Producer::new(1, "p1".to_string(), topic.clone(), "conn-1".to_string());
        let p3 = Producer::new(1, "p1".to_string(), topic, "conn-2".to_string());

        assert_eq!(p1, p2);
        assert_ne!(p1, p3);
    }
}
