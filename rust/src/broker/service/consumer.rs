/*
 * Consumer - represents a consumer connection to a subscription
 * Inspired by Apache Pulsar's Consumer design
 */

use std::sync::{
    atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering},
    Arc,
};

use super::topic::SubscriptionType;
use super::{PendingAck, PendingAcksMap};
use tokio::sync::{mpsc, RwLock};

/// Forward declaration for Subscription type
use super::topic::Subscription;
use super::topic::KeySharedPolicy;
use crate::storage::MessageId;

/// Consumer statistics
#[derive(Debug, Default, Clone)]
pub struct ConsumerStats {
    /// Total number of messages received
    pub messages_received: u64,
    /// Total bytes received
    pub bytes_received: u64,
    /// Total number of messages acknowledged
    pub messages_acked: u64,
    /// Available permits (flow control)
    pub available_permits: u32,
    /// Current active consumer id observed by this consumer for failover subscriptions.
    pub active_consumer_id: Option<u64>,
    /// Whether this consumer is currently the active consumer.
    pub is_active_consumer: bool,
}

/// Pending message waiting to be sent to consumer
#[derive(Debug, Clone)]
pub struct PendingMessage {
    /// Message ID
    pub message_id: MessageId,
    /// Encoded Pulsar MessageMetadata
    pub metadata: Vec<u8>,
    /// Message payload
    pub payload: Vec<u8>,
}

/// Consumer - represents a consumer connection
/// Similar to Apache Pulsar's org.apache.pulsar.broker.service.Consumer
pub struct Consumer {
    /// Consumer ID (unique per connection)
    pub consumer_id: u64,

    /// Consumer name
    pub consumer_name: String,

    /// Subscription reference (Apache Pulsar style - Consumer directly holds Subscription)
    pub subscription: Arc<RwLock<Subscription>>,

    /// Connection ID (for tracking which connection this consumer belongs to)
    pub connection_id: String,

    /// Statistics
    stats: Arc<RwLock<ConsumerStats>>,
    available_permits: AtomicU32,

    /// Message sender channel - sends messages to ServerCnx for delivery
    /// Format: (consumer_id, PendingMessage)
    /// This avoids circular dependency between Consumer and ServerCnx
    message_tx: mpsc::UnboundedSender<(u64, PendingMessage)>,

    ///  Pending message tracking
    pending_acks: Arc<PendingAcksMap>,

    /// Lower value means higher priority, consistent with native Pulsar.
    priority_level: i32,
    key_shared_policy: Option<KeySharedPolicy>,
    /// Failover active-consumer view, updated by dispatcher notifications.
    active_consumer_id: AtomicI64,
    is_active_consumer: AtomicBool,
}

impl std::fmt::Debug for Consumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("consumer_id", &self.consumer_id)
            .field("consumer_name", &self.consumer_name)
            .field("connection_id", &self.connection_id)
            .field("priority_level", &self.priority_level)
            .field(
                "subscription",
                &self.subscription.try_read().map(|s| s.name.clone()),
            )
            .finish()
    }
}

impl Consumer {
    /// Create a new Consumer (Apache Pulsar style - receives Subscription reference)
    pub fn new(
        consumer_id: u64,
        consumer_name: String,
        subscription: Arc<RwLock<Subscription>>,
        connection_id: String,
        message_tx: mpsc::UnboundedSender<(u64, PendingMessage)>,
        priority_level: i32,
    ) -> Self {
        Self::new_with_options(
            consumer_id,
            consumer_name,
            subscription,
            connection_id,
            message_tx,
            priority_level,
            None,
        )
    }

    pub fn new_with_options(
        consumer_id: u64,
        consumer_name: String,
        subscription: Arc<RwLock<Subscription>>,
        connection_id: String,
        message_tx: mpsc::UnboundedSender<(u64, PendingMessage)>,
        priority_level: i32,
        key_shared_policy: Option<KeySharedPolicy>,
    ) -> Self {
        Self {
            consumer_id,
            consumer_name,
            subscription,
            connection_id,
            stats: Arc::new(RwLock::new(ConsumerStats::default())),
            available_permits: AtomicU32::new(0),
            message_tx,
            pending_acks: Arc::new(PendingAcksMap::new()),
            priority_level,
            key_shared_policy,
            active_consumer_id: AtomicI64::new(-1),
            is_active_consumer: AtomicBool::new(false),
        }
    }

    /// Update permits (flow control)
    pub async fn add_permits(&self, permits: u32) {
        self.available_permits.fetch_add(permits, Ordering::Relaxed);
    }

    /// Use one permit when dispatching a message
    pub async fn use_permit(&self) -> bool {
        let mut current = self.available_permits.load(Ordering::Relaxed);
        while current > 0 {   
            match self.available_permits.compare_exchange(
                current,
                current - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
        false
    }

    /// Get available permits
    pub async fn get_available_permits(&self) -> u32 {
        self.available_permits.load(Ordering::Relaxed)
    }

    /// Record message dispatched to this consumer
    pub async fn record_message_dispatched(&self, message_size: usize) {
        let mut stats = self.stats.write().await;
        stats.messages_received += 1;
        stats.bytes_received += message_size as u64;
    }

    /// Record message acknowledged
    pub async fn record_message_acked(&self) {
        let mut stats = self.stats.write().await;
        stats.messages_acked += 1;
    }

    /// Get current statistics
    pub async fn get_stats(&self) -> ConsumerStats {
        let mut stats = self.stats.read().await.clone();
        stats.available_permits = self.available_permits.load(Ordering::Relaxed);
        let active_consumer_id = self.active_consumer_id.load(Ordering::Relaxed);
        stats.active_consumer_id = (active_consumer_id >= 0).then_some(active_consumer_id as u64);
        stats.is_active_consumer = self.is_active_consumer.load(Ordering::Relaxed);
        stats
    }

    /// Get consumer ID
    pub fn get_consumer_id(&self) -> u64 {
        self.consumer_id
    }

    /// Get consumer name
    pub fn get_consumer_name(&self) -> &str {
        &self.consumer_name
    }

    /// Get subscription reference
    pub fn get_subscription(&self) -> Arc<RwLock<Subscription>> {
        self.subscription.clone()
    }

    /// Get subscription name (convenience method)
    pub fn get_subscription_name(&self) -> String {
        // Use try_read to avoid blocking, fallback to empty string if locked
        self.subscription
            .try_read()
            .map(|s| s.name.clone())
            .unwrap_or_default()
    }

    /// Get topic name (convenience method)
    pub fn get_topic_name(&self) -> String {
        // Use try_read to avoid blocking, fallback to empty string if locked
        self.subscription
            .try_read()
            .map(|s| s.topic.clone())
            .unwrap_or_default()
    }

    /// Get subscription type (convenience method)
    pub fn get_sub_type(&self) -> SubscriptionType {
        self.subscription
            .try_read()
            .map(|s| s.sub_type)
            .unwrap_or(SubscriptionType::Exclusive)
    }

    /// Check if consumer has available permits
    pub async fn has_permits(&self) -> bool {
        self.available_permits.load(Ordering::Relaxed) > 0
    }

    // ========================================
    // Message Sending (Channel-based)
    // ========================================

    /// Send a message to this consumer via channel
    ///
    /// Called by Dispatcher to send messages for delivery.
    /// The message is sent through the channel to ServerCnx which will
    /// serialize and send it to the client.
    pub async fn send_message(
        &self,
        message_id: MessageId,
        metadata: Vec<u8>,
        payload: Vec<u8>,
        redelivery_count: u32,
    ) -> bool {
        let msg = PendingMessage {
            message_id: message_id.clone(),
            metadata,
            payload,
        };

        // Native Pulsar writes pending acks before the message is written so that
        // disconnect/close races cannot lose ownership bookkeeping.
        if matches!(
            self.get_sub_type(),
            SubscriptionType::Shared | SubscriptionType::KeyShared
        )
            && !self
                .track_message_dispatched(&message_id, redelivery_count)
                .await
        {
            log::debug!(
                "Skipping send of message {}:{} to closing consumer {}",
                message_id.ledger,
                message_id.entry,
                self.consumer_id
            );
            return false;
        }

        if let Err(e) = self.message_tx.send((self.consumer_id, msg)) {
            log::error!(
                "Failed to send message {}:{} to consumer {}: {}",
                message_id.ledger,
                message_id.entry,
                self.consumer_id,
                e
            );
            if matches!(
                self.get_sub_type(),
                SubscriptionType::Shared | SubscriptionType::KeyShared
            ) {
                self.remove_pending_ack(&message_id).await;
            }
            return false;
        }

        log::debug!(
            "Sent message {}:{} to consumer {} via channel",
            message_id.ledger,
            message_id.entry,
            self.consumer_id
        );
        true
    }

    pub async fn send_messages_batch(
        &self,
        messages: Vec<(MessageId, Vec<u8>, Vec<u8>, u32)>,
    ) -> usize {
        let mut sent = 0;
        for (message_id, metadata, payload, redelivery_count) in messages {
            if self
                .send_message(message_id, metadata, payload, redelivery_count)
                .await
            {
                sent += 1;
            } else {
                break;
            }
        }
        sent
    }

    /// Legacy method - now just calls send_message
    /// Kept for backward compatibility with dispatcher
    pub async fn enqueue_message(
        &self,
        message_id: MessageId,
        metadata: Vec<u8>,
        payload: Vec<u8>,
    ) -> bool {
        self.send_message(message_id, metadata, payload, 0).await
    }

    // ========================================
    // Pending Acks Tracking (Shared Mode)
    // ========================================

    /// Track pending ack ownership before the message is handed off to the connection.
    pub async fn track_message_dispatched(
        &self,
        message_id: &MessageId,
        redelivery_count: u32,
    ) -> bool {
        let tracked = self
            .pending_acks
            .add_pending_ack(message_id.clone(), redelivery_count)
            .await;

        if tracked {
            log::debug!(
                "Tracked message {}:{} for consumer {}",
                message_id.ledger,
                message_id.entry,
                self.consumer_id
            );
        }

        tracked
    }

    /// 确认消息并移除跟踪
    ///
    /// 返回：
    /// - true: 消息确实由该 Consumer 持有并成功移除
    /// - false: 消息不属于该 Consumer（可能是别的 Consumer 的消息或已重投递）
    pub async fn remove_pending_ack(&self, message_id: &MessageId) -> bool {
        if self.pending_acks.remove(message_id).await.is_some() {
            log::debug!(
                "Acked tracked message {}:{} for consumer {}",
                message_id.ledger,
                message_id.entry,
                self.consumer_id
            );
            true
        } else {
            log::warn!(
                "Consumer {} attempted to ack message {}:{} not in pending_acks",
                self.consumer_id,
                message_id.ledger,
                message_id.entry
            );
            false
        }
    }

    pub async fn has_pending_ack(&self, message_id: &MessageId) -> bool {
        self.pending_acks.contains(message_id).await
    }

    pub async fn find_pending_ack_by_position(&self, ledger: u64, entry: u64) -> Option<MessageId> {
        self.pending_acks.find_by_position(ledger, entry).await
    }

    pub fn close_pending_acks(&self) {
        self.pending_acks.close();
    }

    /// 获取所有待确认消息 (用于 disconnect recovery)
    ///
    /// 返回所有 pending messages 的 ID 和信息
    pub async fn drain_pending_acks(&self) -> Vec<(MessageId, PendingAck)> {
        self.pending_acks.drain().await
    }

    /// 获取待确认消息数量
    pub async fn pending_ack_count(&self) -> usize {
        self.pending_acks.len().await
    }

    pub fn get_priority_level(&self) -> i32 {
        self.priority_level
    }

    pub fn key_shared_policy(&self) -> Option<KeySharedPolicy> {
        self.key_shared_policy.clone()
    }

    pub fn available_permits_now(&self) -> u32 {
        self.available_permits.load(Ordering::Relaxed)
    }

    pub fn notify_active_consumer_change(&self, active_consumer_id: u64) {
        self.active_consumer_id
            .store(active_consumer_id as i64, Ordering::Relaxed);
        self.is_active_consumer
            .store(self.consumer_id == active_consumer_id, Ordering::Relaxed);
    }

    pub fn clear_active_consumer(&self) {
        self.active_consumer_id.store(-1, Ordering::Relaxed);
        self.is_active_consumer.store(false, Ordering::Relaxed);
    }

    // ========================================
    // Flow Control and Acknowledgment
    // ========================================

    /// Flow command - add permits for message delivery
    ///
    /// This method:
    /// 1. Updates consumer permits
    /// 2. (Future) Triggers message dispatch if permits available
    pub async fn flow_message(&self, permits: u32) {
        log::debug!("Consumer {} flowing {} permits", self.consumer_id, permits);
        self.add_permits(permits).await;

        // TODO: Trigger message dispatch (Apache Pulsar style)
        // In Pulsar, this would call subscription.dispatchMessages()
        // For now, the dispatcher will be called from the handler
    }

    /// Acknowledge a message
    ///
    /// This method:
    /// 1. Records the acknowledgment in stats
    /// 2. (Future) Updates subscription cursor
    pub async fn ack_message(&self, message_id: crate::storage::MessageId) {
        log::debug!(
            "Consumer {} acking message {}:{}",
            self.consumer_id,
            message_id.ledger,
            message_id.entry
        );

        // Record in stats
        self.record_message_acked().await;

        // TODO: Update subscription cursor (Apache Pulsar style)
        // In Pulsar, this would call subscription.acknowledgeMessage()
        // For now, the actual ack is handled in the handler through storage
    }
}

impl PartialEq for Consumer {
    fn eq(&self, other: &Self) -> bool {
        self.consumer_id == other.consumer_id && self.connection_id == other.connection_id
    }
}

impl Eq for Consumer {}

impl std::hash::Hash for Consumer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.consumer_id.hash(state);
        self.connection_id.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::super::topic::Subscription;
    use super::super::SharedStorage;
    use super::*;
    use crate::storage::Storage;
    use std::path::Path;
    use tokio::sync::Mutex;

    fn create_test_storage() -> SharedStorage {
        Arc::new(Mutex::new(
            Storage::new(Path::new("/tmp/test-consumer-storage")).unwrap(),
        ))
    }

    fn create_test_subscription() -> Arc<RwLock<Subscription>> {
        Arc::new(RwLock::new(Subscription::new(
            "test-sub".to_string(),
            "test-topic".to_string(),
            SubscriptionType::Shared,
            create_test_storage(),
        )))
    }

    fn create_test_consumer(
        id: u64,
        name: &str,
        subscription: Arc<RwLock<Subscription>>,
        conn_id: &str,
    ) -> Consumer {
        let (tx, _rx) = mpsc::unbounded_channel();
        Consumer::new(
            id,
            name.to_string(),
            subscription,
            conn_id.to_string(),
            tx,
            0,
        )
    }

    #[tokio::test]
    async fn test_consumer_permits() {
        let subscription = create_test_subscription();
        let consumer = create_test_consumer(1, "test-consumer", subscription, "conn-1");

        // Add permits
        consumer.add_permits(5).await;
        assert_eq!(consumer.get_available_permits().await, 5);

        // Use permits
        assert!(consumer.use_permit().await);
        assert!(consumer.use_permit().await);
        assert_eq!(consumer.get_available_permits().await, 3);

        // Record messages
        consumer.record_message_dispatched(100).await;
        consumer.record_message_acked().await;

        let stats = consumer.get_stats().await;
        assert_eq!(stats.messages_received, 1);
        assert_eq!(stats.bytes_received, 100);
        assert_eq!(stats.messages_acked, 1);
    }

    #[tokio::test]
    async fn test_consumer_equality() {
        let subscription = create_test_subscription();
        let c1 = create_test_consumer(1, "c1", subscription.clone(), "conn-1");
        let c2 = create_test_consumer(1, "c1", subscription.clone(), "conn-1");
        let c3 = create_test_consumer(1, "c1", subscription, "conn-2");

        assert_eq!(c1, c2);
        assert_ne!(c1, c3);
    }

    #[tokio::test]
    async fn test_consumer_getters() {
        let subscription = create_test_subscription();
        let consumer = create_test_consumer(42, "my-consumer", subscription, "conn-123");

        assert_eq!(consumer.get_consumer_id(), 42);
        assert_eq!(consumer.get_consumer_name(), "my-consumer");
        assert_eq!(consumer.get_subscription_name(), "test-sub");
        assert_eq!(consumer.get_topic_name(), "test-topic");
        assert_eq!(consumer.get_sub_type(), SubscriptionType::Shared);
    }

    #[tokio::test]
    async fn test_consumer_flow_and_ack() {
        let subscription = create_test_subscription();
        let consumer = create_test_consumer(1, "test-consumer", subscription, "conn-1");

        // Test flow
        consumer.flow_message(10).await;
        assert_eq!(consumer.get_available_permits().await, 10);

        // Test ack
        let msg_id = crate::storage::MessageId {
            ledger: 1,
            entry: 1,
            partition: -1,
        };
        consumer.ack_message(msg_id).await;

        let stats = consumer.get_stats().await;
        assert_eq!(stats.messages_acked, 1);
    }

    #[tokio::test]
    async fn test_consumer_send_message() {
        let subscription = create_test_subscription();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let consumer = Consumer::new(
            42,
            "test-consumer".to_string(),
            subscription,
            "conn-1".to_string(),
            tx,
            0,
        );

        // Send message via channel
        let msg_id = MessageId {
            ledger: 1,
            entry: 1,
            partition: -1,
        };
        assert!(
            consumer
                .send_message(msg_id.clone(), vec![9, 9], b"test-payload".to_vec(), 0)
                .await
        );

        // Verify message was sent through channel with consumer_id
        let (consumer_id, received) = rx.recv().await.unwrap();
        assert_eq!(consumer_id, 42);
        assert_eq!(received.message_id, msg_id);
        assert_eq!(received.metadata, vec![9, 9]);
        assert_eq!(received.payload, b"test-payload");
        assert_eq!(consumer.pending_ack_count().await, 1);
    }

    #[tokio::test]
    async fn test_pending_ack_tracking_and_drain() {
        let subscription = create_test_subscription();
        let consumer = create_test_consumer(7, "test-consumer", subscription, "conn-7");
        let msg1 = MessageId {
            ledger: 1,
            entry: 1,
            partition: -1,
        };
        let msg2 = MessageId {
            ledger: 1,
            entry: 2,
            partition: -1,
        };

        consumer.track_message_dispatched(&msg1, 0).await;
        consumer.track_message_dispatched(&msg2, 2).await;

        assert!(consumer.has_pending_ack(&msg1).await);
        assert_eq!(consumer.pending_ack_count().await, 2);
        assert!(consumer.remove_pending_ack(&msg1).await);
        assert!(!consumer.has_pending_ack(&msg1).await);
        assert_eq!(consumer.pending_ack_count().await, 1);

        let drained = consumer.drain_pending_acks().await;
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].0, msg2);
        assert_eq!(drained[0].1.redelivery_count, 2);
        assert_eq!(consumer.pending_ack_count().await, 0);
    }
}
