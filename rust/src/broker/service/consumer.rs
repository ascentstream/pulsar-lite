/*
 * Consumer - represents a consumer connection to a subscription
 * Inspired by Apache Pulsar's Consumer design
 */

use bytes::Bytes;
use std::sync::{
    atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering},
    Arc,
};

use super::ConnectionWriteState;
use super::{PendingAck, PendingAcksMap};
use tokio::sync::{mpsc, RwLock};

use super::topic::KeySharedPolicy;
use super::topic::{AckCommandType, Subscription, SubscriptionType};
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
    pub metadata: Bytes,
    /// Message payload
    pub payload: Bytes,
    /// Estimated encoded bytes currently pending on the connection.
    pub wire_size: usize,
}

/// RAII dispatch reservation holding all resources for one message dispatch.
///
/// Created by `Consumer::try_reserve_dispatch()` after atomically acquiring:
/// - flow-control permit
/// - outbound bytes reservation
/// - channel slot
/// - pending ack entry (Shared/KeyShared only)
///
/// Call `send()` to commit the dispatch. If dropped without sending, all
/// resources are automatically rolled back (dispatch-or-drop semantics).
pub struct DispatchReservation {
    available_permits: Arc<AtomicU32>,
    pending_acks: Arc<PendingAcksMap>,
    owned_permit: Option<mpsc::OwnedPermit<(u64, PendingMessage)>>,
    consumer_id: u64,
    message: Option<PendingMessage>,
    pending_ack_message_id: Option<MessageId>,
    committed: bool,
}

impl DispatchReservation {
    /// Commit the dispatch: send the message through the channel.
    /// Consumes self. After this call, resources belong to the connection write path.
    pub fn send(mut self) {
        self.committed = true;
        let permit = self
            .owned_permit
            .take()
            .expect("owned_permit always present");
        let message = self.message.take().expect("message always present");
        permit.send((self.consumer_id, message));
    }
}

impl Drop for DispatchReservation {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        // Sync rollback: permit + channel slot
        self.available_permits.fetch_add(1, Ordering::Relaxed);
        // owned_permit (Some) drops here, releasing the channel slot

        // Pending ack: fire-and-forget async cleanup (rare safety-net path)
        if let Some(msg_id) = self.pending_ack_message_id.take() {
            let pending_acks = self.pending_acks.clone();
            tokio::spawn(async move {
                pending_acks.remove(&msg_id).await;
            });
        }
    }
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
    available_permits: Arc<AtomicU32>,

    /// Message sender channel - sends messages to ServerCnx for delivery
    /// Format: (consumer_id, PendingMessage)
    /// This avoids circular dependency between Consumer and ServerCnx
    message_tx: mpsc::Sender<(u64, PendingMessage)>,
    /// Connection-level outbound write state sourced from ServerCnx.
    connection_write_state: Arc<ConnectionWriteState>,

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
        message_tx: mpsc::Sender<(u64, PendingMessage)>,
        priority_level: i32,
    ) -> Self {
        Self::new_with_options(
            consumer_id,
            consumer_name,
            subscription,
            connection_id,
            message_tx,
            Arc::new(ConnectionWriteState::new(64 * 1024, 32 * 1024)),
            priority_level,
            None,
        )
    }

    pub fn new_with_options(
        consumer_id: u64,
        consumer_name: String,
        subscription: Arc<RwLock<Subscription>>,
        connection_id: String,
        message_tx: mpsc::Sender<(u64, PendingMessage)>,
        connection_write_state: Arc<ConnectionWriteState>,
        priority_level: i32,
        key_shared_policy: Option<KeySharedPolicy>,
    ) -> Self {
        Self {
            consumer_id,
            consumer_name,
            subscription,
            connection_id,
            stats: Arc::new(RwLock::new(ConsumerStats::default())),
            available_permits: Arc::new(AtomicU32::new(0)),
            message_tx,
            connection_write_state,
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

    /// Atomically acquire a channel slot without blocking.
    /// Success = channel has capacity, subsequent permit.send() is guaranteed.
    /// Failure = channel full (consumer backpressure) or closed (consumer disconnected).
    pub fn try_acquire_send_permit(
        &self,
    ) -> Option<tokio::sync::mpsc::Permit<'_, (u64, PendingMessage)>> {
        self.message_tx.try_reserve().ok()
    }

    pub fn is_writable(&self) -> bool {
        self.connection_write_state.is_writable()
    }

    pub fn has_send_capacity(&self) -> bool {
        self.is_writable() && self.try_acquire_send_permit().is_some()
    }

    /// Send a message to this consumer via channel
    ///
    /// Called by Dispatcher to send messages for delivery.
    /// The message is sent through the channel to ServerCnx which will
    /// serialize and send it to the client.
    pub async fn send_message<M, P>(
        &self,
        message_id: MessageId,
        metadata: M,
        payload: P,
        redelivery_count: u32,
    ) -> bool
    where
        M: Into<Bytes>,
        P: Into<Bytes>,
    {
        let metadata = metadata.into();
        let payload = payload.into();
        let wire_size = crate::protocol::codec::estimate_message_parts_size(
            self.consumer_id,
            message_id.ledger,
            message_id.entry,
            message_id.partition,
            &metadata,
            &payload,
        );
        let msg = PendingMessage {
            message_id: message_id.clone(),
            metadata,
            payload,
            wire_size,
        };

        // Native Pulsar writes pending acks before the message is written so that
        // disconnect/close races cannot lose ownership bookkeeping.
        if matches!(
            self.get_sub_type(),
            SubscriptionType::Shared | SubscriptionType::KeyShared
        ) && !self
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

        // Atomically reserve a channel slot (non-async, non-blocking).
        // Failure = channel full (consumer backpressure) → return false,
        // dispatcher batch interrupts and handles partial success.
        let Some(permit) = self.try_acquire_send_permit() else {
            if matches!(
                self.get_sub_type(),
                SubscriptionType::Shared | SubscriptionType::KeyShared
            ) {
                self.remove_pending_ack(&message_id).await;
            }
            return false;
        };

        // Send the message (non-async, guaranteed success once permit is held).
        permit.send((self.consumer_id, msg));

        log::debug!(
            "Sent message {}:{} to consumer {} via channel",
            message_id.ledger,
            message_id.entry,
            self.consumer_id
        );
        true
    }

    /// Atomically reserve all resources needed to dispatch one message.
    ///
    /// Checks and acquires (in order): flow-control permit
    /// → connection writability → channel slot → pending ack entry (Shared/KeyShared only).
    ///
    /// Returns `Some(DispatchReservation)` on success, `None` if any check
    /// fails (dispatch-or-drop: the dispatcher should immediately drop the
    /// message and record the loss).
    pub async fn try_reserve_dispatch(
        &self,
        message_id: &MessageId,
        metadata: Bytes,
        payload: Bytes,
        redelivery_count: u32,
    ) -> Option<DispatchReservation> {
        // Step 1: Acquire flow-control permit
        if !self.use_permit().await {
            return None;
        }

        // Step 2: Calculate wire size
        let wire_size = crate::protocol::codec::estimate_message_parts_size(
            self.consumer_id,
            message_id.ledger,
            message_id.entry,
            message_id.partition,
            &metadata,
            &payload,
        );

        // Step 3: Connection must currently be writable.
        if !self.is_writable() {
            self.available_permits.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        // Step 4: Acquire owned channel slot
        let owned_permit = match self.message_tx.clone().try_reserve_owned() {
            Ok(p) => p,
            Err(_) => {
                self.available_permits.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };

        // Step 5: Track pending ack (Shared/KeyShared only)
        let mut pending_ack_message_id = None;
        if matches!(
            self.get_sub_type(),
            SubscriptionType::Shared | SubscriptionType::KeyShared
        ) {
            if !self
                .track_message_dispatched(message_id, redelivery_count)
                .await
            {
                self.available_permits.fetch_add(1, Ordering::Relaxed);
                // owned_permit drops here, releasing channel slot
                return None;
            }
            pending_ack_message_id = Some(message_id.clone());
        }

        // Step 6: Build reservation
        let message = PendingMessage {
            message_id: message_id.clone(),
            metadata,
            payload,
            wire_size,
        };

        Some(DispatchReservation {
            available_permits: self.available_permits.clone(),
            pending_acks: self.pending_acks.clone(),
            owned_permit: Some(owned_permit),
            consumer_id: self.consumer_id,
            message: Some(message),
            pending_ack_message_id,
            committed: false,
        })
    }

    pub async fn send_messages_batch(
        &self,
        messages: Vec<(MessageId, Bytes, Bytes, u32)>,
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
    pub async fn enqueue_message<M, P>(
        &self,
        message_id: MessageId,
        metadata: M,
        payload: P,
    ) -> bool
    where
        M: Into<Bytes>,
        P: Into<Bytes>,
    {
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

    /// Record consumer-level ack stats (Exclusive/Failover path).
    pub async fn ack_message(&self, message_id: crate::storage::MessageId) {
        log::debug!(
            "Consumer {} acking message {}:{}",
            self.consumer_id,
            message_id.ledger,
            message_id.entry
        );
        self.record_message_acked().await;
    }

    /// Handle `CommandAck` at the consumer layer
    ///
    /// - Shared/KeyShared: resolve pending owner, update stats, then cursor ack on subscription.
    /// - Exclusive/Failover: stats here; cursor ack on subscription for persistent topics.
    pub async fn message_acked(
        self: Arc<Self>,
        ack_type: AckCommandType,
        message_ids: Vec<MessageId>,
    ) -> Result<(), String> {
        if message_ids.is_empty() {
            return Ok(());
        }

        let (sub_type, is_persistent) = {
            let sub = self.subscription.read().await;
            (sub.get_sub_type(), sub.is_persistent())
        };

        match sub_type {
            SubscriptionType::Shared | SubscriptionType::KeyShared => {
                if ack_type == AckCommandType::Cumulative {
                    log::warn!(
                        "Consumer {} sent cumulative ack on Shared subscription; ignoring",
                        self.consumer_id
                    );
                    return Ok(());
                }

                let mut acked_for_storage = Vec::with_capacity(message_ids.len());
                for message_id in &message_ids {
                    let Some(owner) = self.resolve_ack_owner(message_id).await else {
                        log::warn!(
                            "Consumer {} attempted to ack message {}:{} without ownership; ignoring",
                            self.consumer_id,
                            message_id.ledger,
                            message_id.entry
                        );
                        continue;
                    };

                    if !owner.remove_pending_ack(message_id).await {
                        log::warn!(
                            "Consumer {} found owner {} for message {}:{} but pending ack removal failed; ignoring",
                            self.consumer_id,
                            owner.consumer_id,
                            message_id.ledger,
                            message_id.entry
                        );
                        continue;
                    }

                    owner.record_message_acked().await;
                    acked_for_storage.push(message_id.clone());
                }

                if is_persistent && !acked_for_storage.is_empty() {
                    self.subscription
                        .write()
                        .await
                        .acknowledge_message(&acked_for_storage, ack_type)
                        .await?;
                }
            }
            SubscriptionType::Exclusive | SubscriptionType::Failover => {
                for message_id in &message_ids {
                    self.ack_message(message_id.clone()).await;
                }

                if is_persistent {
                    self.subscription
                        .write()
                        .await
                        .acknowledge_message(&message_ids, ack_type)
                        .await?;
                }
            }
        }

        Ok(())
    }

    async fn resolve_ack_owner(self: &Arc<Self>, message_id: &MessageId) -> Option<Arc<Consumer>> {
        if self.has_pending_ack(message_id).await {
            return Some(Arc::clone(self));
        }

        let sub = self.subscription.read().await;
        for candidate in sub.get_consumers() {
            if candidate.consumer_id != self.consumer_id
                && candidate.has_pending_ack(message_id).await
            {
                return Some(candidate);
            }
        }

        None
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
    use super::super::{ConnectionWriteState, SharedStorage};
    use super::*;
    use crate::storage::Storage;
    use std::path::Path;
    use tokio::sync::Mutex;

    fn create_test_storage() -> SharedStorage {
        Arc::new(Mutex::new(
            Storage::new_memory(Path::new("/tmp/test-consumer-storage")).unwrap(),
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
        let (tx, _rx) = mpsc::channel(8192);
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
        let (tx, mut rx) = mpsc::channel(8192);
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
                .send_message(
                    msg_id.clone(),
                    Bytes::from_static(&[9, 9]),
                    Bytes::from_static(b"test-payload"),
                    0,
                )
                .await
        );

        // Verify message was sent through channel with consumer_id
        let (consumer_id, received) = rx.recv().await.unwrap();
        assert_eq!(consumer_id, 42);
        assert_eq!(received.message_id, msg_id);
        assert_eq!(received.metadata.as_ref(), &[9, 9]);
        assert_eq!(received.payload.as_ref(), b"test-payload");
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

    #[tokio::test]
    async fn test_consumer_writable_follows_pending_outbound_bytes_watermarks() {
        let subscription = create_test_subscription();
        let (tx, mut rx) = mpsc::channel(8);
        let write_state = Arc::new(ConnectionWriteState::new(64, 32));
        let consumer = Consumer::new_with_options(
            88,
            "watermark-consumer".to_string(),
            subscription,
            "conn-watermark".to_string(),
            tx,
            write_state.clone(),
            0,
            None,
        );

        consumer.add_permits(2).await;

        assert!(
            consumer
                .send_message(
                    MessageId {
                        ledger: 1,
                        entry: 1,
                        partition: -1,
                    },
                    Bytes::new(),
                    Bytes::from(vec![7u8; 80]),
                    0,
                )
                .await
        );

        let expected_wire_size = crate::protocol::codec::estimate_message_parts_size(
            consumer.consumer_id,
            1,
            1,
            -1,
            &Bytes::new(),
            &Bytes::from(vec![7u8; 80]),
        );
        write_state.observe_buffered_bytes(expected_wire_size);
        assert!(
            !consumer.is_writable(),
            "pending outbound bytes above the high watermark should flip writable false"
        );

        let (_cid, _pending) = rx.recv().await.expect("queued message");
        write_state.observe_buffered_bytes(0);

        assert!(
            consumer.is_writable(),
            "releasing bytes below the low watermark should restore writable"
        );
    }
}
