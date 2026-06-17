/*
 * Server Connection Handler
 * Handles individual client connections, inspired by Apache Pulsar's ServerCnx
 */

use futures::future::pending;
use futures::{SinkExt, StreamExt};
use prost::Message;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant, MissedTickBehavior};
use tokio_util::codec::Framed;

use super::consumer::PendingMessage;
use super::{ConnectionWriteState, Consumer, Producer, SharedStorage};
use crate::broker::broker_service::SharedBrokerService;
use crate::broker::handler;
use crate::broker::service::topic::TopicPublishRateExceeded;
use crate::protocol::codec::{
    proto::pulsar::{base_command, BaseCommand, ProtocolVersion, ServerError},
    PulsarFrame, PulsarFrameCodec,
};
use crate::protocol::ServerCommand;

type CnxError = Box<dyn Error + Send + Sync>;
type CnxResult<T> = Result<T, CnxError>;

fn to_cnx_error(error: impl ToString) -> CnxError {
    Box::new(std::io::Error::other(error.to_string()))
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum State {
    // TCP is established, but the Pulsar Connect handshake has not completed yet.
    Start,
    // Connect has been received and the broker-side handshake is in progress.
    Connecting,
    // Handshake completed and broker commands can now be processed.
    Connected,
    // The connection has failed and will only continue through cleanup.
    Failed,
    Closing,
    Closed,
}

#[derive(Debug, Clone)]
pub enum CloseReason {
    ClientClosed,
    HandshakeTimeout,
    KeepAliveTimeout,
    LivenessCheckTimeout,
    KeepAliveSendFailed,
    ProtocolError(String),
}

#[derive(Debug, Clone, Copy)]
enum CloseReasonKind {
    KeepAliveTimeout,
    LivenessCheckTimeout,
}

#[derive(Debug, Clone, Copy)]
struct ConnectionCheck {
    deadline: Instant,
    timeout_reason: CloseReasonKind,
}

/// Runtime context for a single broker connection.
/// This owns protocol I/O, connection state, keep-alive handling, and cleanup/recovery entry points.
pub struct ServerCnx<T>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    framed: Framed<T, PulsarFrameCodec>,
    state: State,
    handshake_completed: bool,
    last_activity: Instant,
    waiting_for_pong: bool,
    keep_alive_interval: Duration,
    handshake_timeout: Duration,
    remote_protocol_version: i32,
    connection_check_in_progress: Option<ConnectionCheck>,
    connection_liveness_check_timeout: Duration,
    close_reason: Option<CloseReason>,
    producers: HashMap<u64, Arc<Producer>>,

    /// Consumers on this connection (consumer_id -> Consumer) (Apache Pulsar style)
    consumers: HashMap<u64, Arc<Consumer>>,

    /// Message channel receiver - receives messages from consumers to send to client
    /// All consumers on this connection share the same channel
    message_rx: mpsc::Receiver<(u64, PendingMessage)>,

    /// Message channel sender - cloned and
    message_tx: mpsc::Sender<(u64, PendingMessage)>,
    /// Shared connection-level write-buffer state exposed to consumers.
    connection_write_state: Arc<ConnectionWriteState>,

    /// Connection ID (unique identifier for this connection)
    connection_id: String,

    /// Topic manager reference
    topic_manager: SharedBrokerService,
    /// Number of in-flight non-persistent publish tasks spawned from this connection.
    pending_send_requests: usize,
    /// Drop gate: messages are silently dropped with fake receipt when this is exceeded.
    /// Maps to Pulsar's maxConcurrentNonPersistentMessagePerConnection.
    max_concurrent_non_persistent: usize,
    /// TCP throttle gate: reading from framed stops when pending reaches this limit.
    /// Maps to Pulsar's maxPendingPublishRequestsPerConnection.
    max_pending_publish_requests: usize,
    /// Resume reading threshold (hysteresis = max_pending_publish_requests / 2).
    resume_threshold: usize,
    /// When true, the event loop skips framed.next(), equivalent to Netty auto-read = false.
    read_paused: bool,
    /// Maximum message size accepted by the broker.
    max_message_size: usize,

    /// Advertised broker service url returned from Lookup.
    broker_service_url: String,
}

impl<T> ServerCnx<T>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    fn sync_connection_writable_from_framed_buffer(&self) {
        self.connection_write_state
            .observe_buffered_bytes(self.framed.write_buffer().len());
    }

    async fn write_message_batch(&mut self, batch: Vec<(u64, PendingMessage)>) -> CnxResult<()> {
        for item in batch {
            self.sync_connection_writable_from_framed_buffer();
            self.framed.feed(item).await.map_err(to_cnx_error)?;
            self.sync_connection_writable_from_framed_buffer();
        }

        futures::sink::SinkExt::<(u64, PendingMessage)>::flush(&mut self.framed)
            .await
            .map_err(to_cnx_error)?;
        self.sync_connection_writable_from_framed_buffer();
        Ok(())
    }

    /// Create a new ServerCnx
    pub fn new(
        socket: T,
        _storage: SharedStorage,
        topic_manager: SharedBrokerService,
        connection_id: String,
        keep_alive_interval: Duration,
        handshake_timeout: Duration,
        connection_liveness_check_timeout: Duration,
        max_concurrent_non_persistent: usize,
        max_pending_publish_requests: usize,
        max_message_size: usize,
        broker_service_url: String,
        channel_write_buffer_high_water_mark_bytes: usize,
        channel_write_buffer_low_water_mark_bytes: usize,
    ) -> Self {
        let mut framed = Framed::new(socket, PulsarFrameCodec::new());
        framed.set_backpressure_boundary(channel_write_buffer_high_water_mark_bytes);
        let (message_tx, message_rx) = mpsc::channel(8192);
        let connection_write_state = Arc::new(ConnectionWriteState::new(
            channel_write_buffer_high_water_mark_bytes,
            channel_write_buffer_low_water_mark_bytes,
        ));
        Self {
            framed,
            state: State::Start,
            handshake_completed: false,
            last_activity: Instant::now(),
            waiting_for_pong: false,
            keep_alive_interval,
            handshake_timeout,
            remote_protocol_version: ProtocolVersion::V0 as i32,
            connection_check_in_progress: None,
            connection_liveness_check_timeout,
            close_reason: None,
            producers: HashMap::new(),
            consumers: HashMap::new(),
            message_rx,
            message_tx,
            connection_write_state,
            connection_id,
            topic_manager,
            pending_send_requests: 0,
            max_concurrent_non_persistent,
            max_pending_publish_requests,
            resume_threshold: max_pending_publish_requests / 2,
            read_paused: false,
            max_message_size,
            broker_service_url,
        }
    }

    pub fn get_message_sender(&self) -> mpsc::Sender<(u64, PendingMessage)> {
        self.message_tx.clone()
    }

    fn close_reason_message(&self) -> String {
        match self.close_reason.as_ref() {
            Some(CloseReason::ClientClosed) => "client closed connection".to_string(),
            Some(CloseReason::HandshakeTimeout) => "handshake timed out".to_string(),
            Some(CloseReason::KeepAliveTimeout) => "keep-alive timed out".to_string(),
            Some(CloseReason::LivenessCheckTimeout) => {
                "connection liveness check timed out".to_string()
            }
            Some(CloseReason::KeepAliveSendFailed) => "failed to send keep-alive ping".to_string(),
            Some(CloseReason::ProtocolError(message)) => format!("protocol error: {message}"),
            None => "normal shutdown".to_string(),
        }
    }

    pub async fn run(&mut self) -> CnxResult<()> {
        let mut keep_alive_tick = tokio::time::interval(self.keep_alive_interval);
        keep_alive_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        keep_alive_tick.tick().await;

        let loop_result: CnxResult<()> = loop {
            let connection_check_deadline = self.connection_check_in_progress;
            tokio::select! {
                // Inbound protocol commands — skipped when read_paused (TCP backpressure).
                frame_result = self.framed.next(), if !self.read_paused && self.connection_write_state.is_writable() => {
                    match frame_result {
                        Some(frame) => {
                            let frame = frame.map_err(to_cnx_error)?;
                            let base_command = BaseCommand::decode(&frame.command[..]).map_err(to_cnx_error)?;
                            log::debug!("Received command: {:?}", base_command.r#type);

                            self.mark_inbound_activity();

                            if let Err(e) = self.handle_command(base_command, frame).await {
                                self.set_failed(CloseReason::ProtocolError(e.to_string()));
                                log::error!("Error handling command: {}", e);
                                break Err(e);
                            }
                            self.sync_connection_writable_from_framed_buffer();
                        }
                        None => {
                            self.close_reason.get_or_insert(CloseReason::ClientClosed);
                            break Ok(());
                        }
                    }
                }

                // Outbound broker messages are batched: drain all pending, encode
                // into the write buffer with feed(), then flush once for a single
                // TCP write.  This amortises syscall overhead across many messages.
                Some((consumer_id, pending_msg)) = self.message_rx.recv() => {
                    let mut batch = vec![(consumer_id, pending_msg)];
                    while let Ok(next) = self.message_rx.try_recv() {
                        batch.push(next);
                        if batch.len() >= 128 {
                            break;
                        }
                    }

                    let mut send_error: Option<CnxError> = None;

                    if let Err(e) = self.write_message_batch(batch).await {
                        send_error = Some(e);
                    }

                    match send_error {
                        Some(e) => {
                            self.set_failed(CloseReason::ProtocolError(e.to_string()));
                            log::error!("Error sending message batch to client: {}", e);
                            break Err(e);
                        }
                        None => {}
                    }
                }

                // Periodic keep-alive handles handshake timeout, active Ping, and liveness probing.
                _ = keep_alive_tick.tick() => {
                    match self.handle_keep_alive_tick().await {
                        Ok(true) => break Ok(()),
                        Ok(false) => {}
                        Err(e) => {
                            self.set_failed(CloseReason::ProtocolError(e.to_string()));
                            log::error!("Error during keep-alive processing: {}", e);
                            break Err(e);
                        }
                    }
                }

                // A one-shot liveness check timeout provides a faster dead-connection path than waiting for the next keep-alive cycle.
                _ = async {
                    if let Some(check) = connection_check_deadline {
                        tokio::time::sleep_until(check.deadline).await;
                    } else {
                        pending::<()>().await;
                    }
                } => {
                    if self.handle_connection_check_timeout() {
                        break Ok(());
                    }
                }
            }
        };

        if self.state != State::Failed {
            self.state = State::Closing;
        }
        log::info!(
            "Closing connection {} with reason {}",
            self.connection_id,
            self.close_reason_message()
        );

        let cleanup_result = self.cleanup().await;
        self.state = State::Closed;

        match (loop_result, cleanup_result) {
            (Err(run_err), Ok(())) => Err(run_err),
            (Ok(()), Err(cleanup_err)) => Err(cleanup_err),
            (Err(run_err), Err(cleanup_err)) => {
                log::error!("Cleanup failed after connection error: {}", cleanup_err);
                Err(run_err)
            }
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    fn supports_keep_alive(&self) -> bool {
        self.remote_protocol_version >= ProtocolVersion::V1 as i32
    }

    fn mark_inbound_activity(&mut self) {
        // Any valid inbound command proves the peer is still alive, so both the keep-alive wait and the one-shot liveness probe can be cleared.
        self.last_activity = Instant::now();
        self.waiting_for_pong = false;
        self.connection_check_in_progress = None;
    }

    fn set_failed(&mut self, reason: CloseReason) {
        self.state = State::Failed;
        self.close_reason = Some(reason);
    }

    async fn start_connection_liveness_check(
        &mut self,
        timeout_reason: CloseReasonKind,
    ) -> CnxResult<()> {
        // Keep-alive and explicit liveness checks reuse the same Ping path; only the timeout reason differs.
        log::info!(
            "Starting connection liveness check on {} using protocol version {}",
            self.connection_id,
            self.remote_protocol_version
        );
        self.framed.send(ServerCommand::Ping).await.map_err(|e| {
            self.set_failed(CloseReason::KeepAliveSendFailed);
            to_cnx_error(e)
        })?;
        self.waiting_for_pong = true;
        self.connection_check_in_progress = Some(ConnectionCheck {
            deadline: Instant::now() + self.connection_liveness_check_timeout,
            timeout_reason,
        });
        Ok(())
    }

    pub async fn start_explicit_liveness_check(&mut self) -> CnxResult<()> {
        if !self.handshake_completed || !self.supports_keep_alive() {
            return Ok(());
        }
        // This is kept as a separate entry point so future broker-driven health checks do not need to piggyback on the periodic keep-alive path.
        self.start_connection_liveness_check(CloseReasonKind::LivenessCheckTimeout)
            .await
    }

    fn handle_connection_check_timeout(&mut self) -> bool {
        let Some(check) = self.connection_check_in_progress else {
            return false;
        };

        let reason = match check.timeout_reason {
            CloseReasonKind::KeepAliveTimeout => CloseReason::KeepAliveTimeout,
            CloseReasonKind::LivenessCheckTimeout => CloseReason::LivenessCheckTimeout,
        };

        log::warn!(
            "Connection {} liveness check timed out after {:?}",
            self.connection_id,
            self.connection_liveness_check_timeout
        );
        self.set_failed(reason);
        true
    }

    async fn handle_command(
        &mut self,
        base_command: BaseCommand,
        frame: PulsarFrame,
    ) -> CnxResult<()> {
        let command_type = base_command.r#type;
        let is_connect = command_type == base_command::Type::Connect as i32;
        let is_ping = command_type == base_command::Type::Ping as i32;
        let is_pong = command_type == base_command::Type::Pong as i32;

        if !self.handshake_completed && !is_connect && !is_ping && !is_pong {
            return Err(to_cnx_error(
                "received command before Connect handshake completed",
            ));
        }

        if matches!(self.state, State::Failed | State::Closing | State::Closed) {
            return Err(to_cnx_error(
                "received command on closing/closed connection",
            ));
        }

        match base_command.r#type {
            x if x == base_command::Type::Connect as i32 => {
                self.handle_connect(base_command).await?
            }
            x if x == base_command::Type::PartitionedMetadata as i32 => {
                self.handle_partition_metadata(base_command).await?
            }
            x if x == base_command::Type::Lookup as i32 => self.handle_lookup(base_command).await?,
            x if x == base_command::Type::Producer as i32 => {
                self.handle_producer(base_command).await?
            }
            x if x == base_command::Type::Send as i32 => {
                self.handle_send(base_command, frame).await?
            }
            x if x == base_command::Type::Subscribe as i32 => {
                self.handle_subscribe(base_command).await?
            }
            x if x == base_command::Type::Flow as i32 => self.handle_flow(base_command).await?,
            x if x == base_command::Type::Ack as i32 => self.handle_ack(base_command).await?,
            x if x == base_command::Type::RedeliverUnacknowledgedMessages as i32 => {
                self.handle_redeliver_unacknowledged_messages(base_command)
                    .await?
            }
            x if x == base_command::Type::Ping as i32 => self.handle_ping().await?,
            x if x == base_command::Type::Pong as i32 => self.handle_pong(base_command).await?,
            x if x == base_command::Type::CloseProducer as i32 => {
                self.handle_close_producer(base_command).await?
            }
            x if x == base_command::Type::CloseConsumer as i32 => {
                self.handle_close_consumer(base_command).await?
            }
            _ => log::warn!("Unsupported command type: {}", base_command.r#type),
        }

        Ok(())
    }

    async fn handle_keep_alive_tick(&mut self) -> CnxResult<bool> {
        match self.state {
            State::Start | State::Connecting => {
                if !self.handshake_completed
                    && self.last_activity.elapsed() >= self.handshake_timeout
                {
                    log::warn!(
                        "Connection {} handshake timed out after {:?}, closing connection",
                        self.connection_id,
                        self.handshake_timeout
                    );
                    self.set_failed(CloseReason::HandshakeTimeout);
                    return Ok(true);
                }
            }
            State::Connected => {
                if !self.supports_keep_alive() {
                    // Older protocol versions do not support application-level Ping/Pong, so keep-alive stays disabled for compatibility.
                    return Ok(false);
                }

                if self.waiting_for_pong {
                    log::warn!(
                        "Connection {} still waiting for keep-alive response after {:?}",
                        self.connection_id,
                        self.keep_alive_interval
                    );
                    if self.connection_check_in_progress.is_none() {
                        self.connection_check_in_progress = Some(ConnectionCheck {
                            deadline: Instant::now() + self.connection_liveness_check_timeout,
                            timeout_reason: CloseReasonKind::KeepAliveTimeout,
                        });
                    }
                    return Ok(false);
                }

                // Periodic keep-alive is implemented as a recurring liveness probe.
                self.start_connection_liveness_check(CloseReasonKind::KeepAliveTimeout)
                    .await?;
            }
            State::Failed | State::Closing | State::Closed => return Ok(true),
        }

        Ok(false)
    }

    async fn cleanup(&mut self) -> CnxResult<()> {
        log::debug!(
            "Cleaning up connection: {} producers, {} consumers",
            self.producers.len(),
            self.consumers.len()
        );

        for (producer_id, producer) in self.producers.drain() {
            let topic = producer.get_topic();
            let mut topic_guard = topic.write().await;
            topic_guard.remove_producer(producer_id);
            log::debug!("Closed producer {} on connection cleanup", producer_id);
        }

        for (consumer_id, consumer) in self.consumers.drain() {
            {
                // Shared subscriptions must still route disconnects through recovery instead of doing a plain remove.
                let mut sub_guard = consumer.subscription.write().await;
                sub_guard.remove_consumer_with_recovery(consumer_id).await;
            }
            log::debug!("Closed consumer {} on connection cleanup", consumer_id);
        }

        Ok(())
    }

    async fn handle_connect(&mut self, cmd: BaseCommand) -> CnxResult<()> {
        self.state = State::Connecting;
        let remote_protocol_version = handler::handle_connect(&mut self.framed, cmd)
            .await
            .map_err(to_cnx_error)?;
        // The broker keeps the negotiated client protocol version and uses it to decide whether active keep-alive is supported.
        self.remote_protocol_version = remote_protocol_version;
        self.handshake_completed = true;
        self.state = State::Connected;
        self.waiting_for_pong = false;
        self.connection_check_in_progress = None;
        self.last_activity = Instant::now();
        log::info!(
            "Connection {} handshake completed with protocol version {} (keep-alive enabled: {})",
            self.connection_id,
            self.remote_protocol_version,
            self.supports_keep_alive()
        );
        Ok(())
    }

    async fn handle_partition_metadata(&mut self, cmd: BaseCommand) -> CnxResult<()> {
        handler::handle_partition_metadata(&mut self.framed, cmd, &self.topic_manager)
            .await
            .map_err(to_cnx_error)
    }

    async fn handle_lookup(&mut self, cmd: BaseCommand) -> CnxResult<()> {
        handler::handle_lookup(&mut self.framed, cmd, &self.broker_service_url)
            .await
            .map_err(to_cnx_error)
    }

    async fn handle_producer(&mut self, cmd: BaseCommand) -> CnxResult<()> {
        handler::handle_producer(
            &mut self.framed,
            cmd,
            &mut self.producers,
            self.topic_manager.clone(),
        )
        .await
        .map_err(to_cnx_error)
    }

    async fn handle_send(&mut self, cmd: BaseCommand, frame: PulsarFrame) -> CnxResult<()> {
        let send_cmd = cmd
            .send
            .as_ref()
            .ok_or_else(|| to_cnx_error("Missing send command"))?;
        let producer_id = send_cmd.producer_id;
        let sequence_id = send_cmd.sequence_id;
        let producer = self
            .producers
            .get(&producer_id)
            .cloned()
            .ok_or_else(|| to_cnx_error(format!("Unknown producer ID: {}", producer_id)))?;

        let topic = producer.get_topic();
        let is_non_persistent = {
            let topic_guard = topic.read().await;
            topic_guard.runtime_mode()
                == crate::broker::service::topic::TopicRuntimeMode::NonPersistent
        };

        let message_size = frame
            .metadata
            .as_ref()
            .map(|value| value.len())
            .unwrap_or(0)
            + frame.payload.len();
        if message_size > self.max_message_size {
            self.framed
                .send(ServerCommand::SendError {
                    producer_id: send_cmd.producer_id,
                    sequence_id: send_cmd.sequence_id,
                    error: ServerError::NotAllowedError,
                    message: format!(
                        "Exceed maximum message size: {} > {}",
                        message_size, self.max_message_size
                    ),
                })
                .await
                .map_err(to_cnx_error)?;
            return Ok(());
        }

        if is_non_persistent {
            if self.pending_send_requests >= self.max_concurrent_non_persistent {
                self.framed
                    .send(ServerCommand::SendReceipt {
                        producer_id,
                        sequence_id,
                        ledger_id: u64::MAX,
                        entry_id: u64::MAX,
                        partition: -1,
                    })
                    .await
                    .map_err(to_cnx_error)?;
                return Ok(());
            }

            self.pending_send_requests += 1;
            if self.pending_send_requests >= self.max_pending_publish_requests {
                self.read_paused = true;
            }
        }

        let result = handler::handle_send(&mut self.framed, cmd, frame, &self.producers).await;

        if is_non_persistent {
            self.pending_send_requests = self.pending_send_requests.saturating_sub(1);
            if self.read_paused && self.pending_send_requests <= self.resume_threshold {
                self.read_paused = false;
            }
        }

        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                if let Some(rate_error) = error.downcast_ref::<TopicPublishRateExceeded>() {
                    self.framed
                        .send(ServerCommand::SendError {
                            producer_id,
                            sequence_id,
                            error: ServerError::ServiceNotReady,
                            message: rate_error.to_string(),
                        })
                        .await
                        .map_err(to_cnx_error)?;
                    Ok(())
                } else {
                    Err(to_cnx_error(error))
                }
            }
        }
    }

    async fn handle_subscribe(&mut self, cmd: BaseCommand) -> CnxResult<()> {
        handler::handle_subscribe(
            &mut self.framed,
            cmd,
            &mut self.consumers,
            self.topic_manager.clone(),
            self.connection_id.clone(),
            self.message_tx.clone(),
            self.connection_write_state.clone(),
        )
        .await
        .map_err(to_cnx_error)
    }

    async fn handle_flow(&mut self, cmd: BaseCommand) -> CnxResult<()> {
        handler::handle_flow(cmd, &mut self.consumers)
            .await
            .map_err(to_cnx_error)
    }

    async fn handle_ack(&mut self, cmd: BaseCommand) -> CnxResult<()> {
        handler::handle_ack(&mut self.framed, cmd, &self.consumers)
            .await
            .map_err(to_cnx_error)
    }

    async fn handle_redeliver_unacknowledged_messages(
        &mut self,
        cmd: BaseCommand,
    ) -> CnxResult<()> {
        handler::handle_redeliver_unacknowledged_messages(cmd, &self.consumers)
            .await
            .map_err(to_cnx_error)
    }

    async fn handle_ping(&mut self) -> CnxResult<()> {
        handler::handle_ping(&mut self.framed)
            .await
            .map_err(to_cnx_error)
    }

    async fn handle_pong(&mut self, cmd: BaseCommand) -> CnxResult<()> {
        let pong = cmd
            .pong
            .ok_or_else(|| to_cnx_error("missing pong command payload"))?;
        handler::handle_pong(pong).await.map_err(to_cnx_error)
    }

    async fn handle_close_producer(&mut self, cmd: BaseCommand) -> CnxResult<()> {
        handler::handle_close_producer(
            &mut self.framed,
            cmd,
            &mut self.producers,
            self.topic_manager.clone(),
        )
        .await
        .map_err(to_cnx_error)
    }

    async fn handle_close_consumer(&mut self, cmd: BaseCommand) -> CnxResult<()> {
        handler::handle_close_consumer(&mut self.framed, cmd, &mut self.consumers)
            .await
            .map_err(to_cnx_error)
    }
}

pub async fn handle_connection(
    socket: tokio::net::TcpStream,
    storage: SharedStorage,
    topic_manager: SharedBrokerService,
    keep_alive_interval: Duration,
    handshake_timeout: Duration,
    connection_liveness_check_timeout: Duration,
    max_non_persistent_pending_messages: usize,
    max_pending_publish_requests: usize,
    max_message_size: usize,
    broker_service_url: String,
    channel_write_buffer_high_water_mark_bytes: usize,
    channel_write_buffer_low_water_mark_bytes: usize,
) -> CnxResult<()> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static CONNECTION_COUNTER: AtomicU64 = AtomicU64::new(0);

    let connection_id = format!(
        "conn-{}",
        CONNECTION_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let mut server_cnx = ServerCnx::new(
        socket,
        storage,
        topic_manager,
        connection_id,
        keep_alive_interval,
        handshake_timeout,
        connection_liveness_check_timeout,
        max_non_persistent_pending_messages,
        max_pending_publish_requests,
        max_message_size,
        broker_service_url,
        channel_write_buffer_high_water_mark_bytes,
        channel_write_buffer_low_water_mark_bytes,
    );
    server_cnx.run().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::broker_service::{BrokerService, TopicRef};
    use crate::broker::service::{topic::TopicRuntimeMode, Producer};
    use crate::protocol::codec::proto::pulsar::{
        base_command, BaseCommand, CommandConnect, CommandPing, CommandSend,
    };
    use crate::storage::Storage;
    use bytes::Bytes;
    use std::path::Path;
    use std::sync::Arc;
    use tokio::io::{duplex, DuplexStream};
    use tokio::sync::{Mutex, RwLock};

    fn build_test_connection() -> (
        ServerCnx<DuplexStream>,
        Framed<DuplexStream, PulsarFrameCodec>,
    ) {
        let (server, client) = duplex(4096);
        let storage = Arc::new(Mutex::new(
            Storage::new_memory(Path::new("./test.db")).unwrap(),
        ));
        let broker = Arc::new(RwLock::new(BrokerService::with_config(storage.clone(), 0)));
        let server_cnx = ServerCnx::new(
            server,
            storage,
            broker,
            "test-conn".to_string(),
            Duration::from_secs(30),
            Duration::from_secs(30),
            Duration::from_secs(10),
            1000,
            1000,
            5 * 1024 * 1024,
            "pulsar://127.0.0.1:6650".to_string(),
            64 * 1024,
            32 * 1024,
        );
        let client_framed = Framed::new(client, PulsarFrameCodec::new());
        (server_cnx, client_framed)
    }

    fn connect_command(protocol_version: i32) -> BaseCommand {
        BaseCommand {
            r#type: base_command::Type::Connect as i32,
            connect: Some(CommandConnect {
                client_version: "test-client".to_string(),
                protocol_version: Some(protocol_version),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn send_command(producer_id: u64, sequence_id: u64) -> BaseCommand {
        BaseCommand {
            r#type: base_command::Type::Send as i32,
            send: Some(CommandSend {
                producer_id,
                sequence_id,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    async fn attach_non_persistent_producer(
        server_cnx: &mut ServerCnx<DuplexStream>,
        topic_name: &str,
        producer_id: u64,
    ) -> Arc<Producer> {
        let topic = {
            let mut broker = server_cnx.topic_manager.write().await;
            match broker.get_or_create_topic_auto(topic_name).await {
                TopicRef::NonPartitioned(topic) | TopicRef::Partition(topic) => topic,
                TopicRef::Partitioned(_) => panic!("expected concrete topic"),
            }
        };

        topic
            .write()
            .await
            .set_runtime_mode(TopicRuntimeMode::NonPersistent);
        let producer = Arc::new(Producer::new(
            producer_id,
            format!("producer-{producer_id}"),
            topic.clone(),
            server_cnx.connection_id.clone(),
        ));
        topic.write().await.add_producer(producer.clone()).unwrap();
        server_cnx.producers.insert(producer_id, producer.clone());
        producer
    }

    #[tokio::test]
    async fn connect_transitions_to_connected_and_records_protocol_version() {
        let (mut server_cnx, _client) = build_test_connection();

        server_cnx
            .handle_connect(connect_command(ProtocolVersion::V1 as i32))
            .await
            .unwrap();

        assert_eq!(server_cnx.state, State::Connected);
        assert!(server_cnx.handshake_completed);
        assert_eq!(
            server_cnx.remote_protocol_version,
            ProtocolVersion::V1 as i32
        );
        assert!(!server_cnx.waiting_for_pong);
    }

    #[tokio::test]
    async fn handshake_timeout_closes_connection_before_connect() {
        let (mut server_cnx, _client) = build_test_connection();
        server_cnx.last_activity = Instant::now() - Duration::from_secs(31);

        let should_close = server_cnx.handle_keep_alive_tick().await.unwrap();

        assert!(should_close);
        assert_eq!(server_cnx.state, State::Failed);
        assert!(matches!(
            server_cnx.close_reason,
            Some(CloseReason::HandshakeTimeout)
        ));
    }

    #[tokio::test]
    async fn keep_alive_ping_is_only_enabled_for_protocol_v1_or_above() {
        let (mut server_cnx_v0, mut client_v0) = build_test_connection();
        server_cnx_v0.state = State::Connected;
        server_cnx_v0.handshake_completed = true;
        server_cnx_v0.remote_protocol_version = ProtocolVersion::V0 as i32;

        assert!(!server_cnx_v0.handle_keep_alive_tick().await.unwrap());
        assert!(!server_cnx_v0.waiting_for_pong);
        assert!(server_cnx_v0.connection_check_in_progress.is_none());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), client_v0.next())
                .await
                .is_err()
        );

        let (mut server_cnx_v1, mut client_v1) = build_test_connection();
        server_cnx_v1.state = State::Connected;
        server_cnx_v1.handshake_completed = true;
        server_cnx_v1.remote_protocol_version = ProtocolVersion::V1 as i32;

        assert!(!server_cnx_v1.handle_keep_alive_tick().await.unwrap());
        assert!(server_cnx_v1.waiting_for_pong);
        assert!(server_cnx_v1.connection_check_in_progress.is_some());

        let frame = client_v1
            .next()
            .await
            .expect("ping frame")
            .expect("decoded ping frame");
        let cmd = BaseCommand::decode(&frame.command[..]).unwrap();
        assert_eq!(cmd.r#type, base_command::Type::Ping as i32);
    }

    #[tokio::test]
    async fn inbound_activity_clears_waiting_for_pong_and_liveness_check() {
        let (mut server_cnx, mut client) = build_test_connection();
        server_cnx.state = State::Connected;
        server_cnx.handshake_completed = true;
        server_cnx.remote_protocol_version = ProtocolVersion::V1 as i32;
        server_cnx.waiting_for_pong = true;
        server_cnx.connection_check_in_progress = Some(ConnectionCheck {
            deadline: Instant::now() + Duration::from_secs(5),
            timeout_reason: CloseReasonKind::KeepAliveTimeout,
        });

        let ping_cmd = BaseCommand {
            r#type: base_command::Type::Ping as i32,
            ping: Some(CommandPing::default()),
            ..Default::default()
        };

        server_cnx.mark_inbound_activity();
        server_cnx
            .handle_command(
                ping_cmd,
                PulsarFrame {
                    command: Bytes::new(),
                    metadata: None,
                    payload: Bytes::new(),
                    checksum: None,
                },
            )
            .await
            .unwrap();

        assert!(!server_cnx.waiting_for_pong);
        assert!(server_cnx.connection_check_in_progress.is_none());

        let frame = client
            .next()
            .await
            .expect("pong frame")
            .expect("decoded pong frame");
        let cmd = BaseCommand::decode(&frame.command[..]).unwrap();
        assert_eq!(cmd.r#type, base_command::Type::Pong as i32);
    }

    #[tokio::test]
    async fn non_persistent_send_too_large_returns_send_error() {
        let (mut server_cnx, mut client) = build_test_connection();
        server_cnx.max_message_size = 4;
        attach_non_persistent_producer(
            &mut server_cnx,
            "non-persistent://public/default/non-persistent-limit-topic",
            7,
        )
        .await;

        server_cnx
            .handle_send(
                send_command(7, 11),
                PulsarFrame {
                    command: Bytes::new(),
                    metadata: Some(Bytes::from(vec![1, 2])),
                    payload: Bytes::from(vec![3, 4, 5]),
                    checksum: None,
                },
            )
            .await
            .unwrap();

        let frame = client.next().await.unwrap().unwrap();
        let cmd = BaseCommand::decode(&frame.command[..]).unwrap();
        assert_eq!(cmd.r#type, base_command::Type::SendError as i32);
        let send_error = cmd.send_error.expect("send error payload");
        assert_eq!(send_error.producer_id, 7);
        assert_eq!(send_error.sequence_id, 11);
        assert_eq!(
            send_error.error,
            crate::protocol::codec::proto::pulsar::ServerError::NotAllowedError as i32
        );
        assert!(send_error.message.contains("Exceed maximum message size"));
    }

    #[tokio::test]
    async fn non_persistent_send_limit_returns_negative_send_receipt() {
        let (mut server_cnx, mut client) = build_test_connection();
        server_cnx.max_concurrent_non_persistent = 0;
        server_cnx.pending_send_requests = 0;
        attach_non_persistent_producer(
            &mut server_cnx,
            "non-persistent://public/default/non-persistent-slot-topic",
            8,
        )
        .await;

        server_cnx
            .handle_send(
                send_command(8, 12),
                PulsarFrame {
                    command: Bytes::new(),
                    metadata: None,
                    payload: Bytes::from(vec![1, 2, 3]),
                    checksum: None,
                },
            )
            .await
            .unwrap();

        let frame = client.next().await.unwrap().unwrap();
        let cmd = BaseCommand::decode(&frame.command[..]).unwrap();
        assert_eq!(cmd.r#type, base_command::Type::SendReceipt as i32);
        let send_receipt = cmd.send_receipt.expect("send receipt payload");
        let message_id = send_receipt.message_id.expect("message id");
        assert_eq!(send_receipt.producer_id, 8);
        assert_eq!(send_receipt.sequence_id, 12);
        assert_eq!(message_id.ledger_id, u64::MAX);
        assert_eq!(message_id.entry_id, u64::MAX);
        assert_eq!(message_id.partition, Some(-1));
    }

    #[tokio::test]
    async fn non_persistent_publish_rate_limit_returns_send_error_without_closing_connection() {
        let (mut server_cnx, mut client) = build_test_connection();
        attach_non_persistent_producer(
            &mut server_cnx,
            "non-persistent://public/default/non-persistent-rate-limit-topic",
            10,
        )
        .await;

        if let Some(producer) = server_cnx.producers.get(&10) {
            let topic = producer.get_topic();
            topic
                .write()
                .await
                .set_publish_rate(crate::broker::service::topic::TopicPublishRate {
                    messages_per_sec: 0,
                    bytes_per_sec: 1,
                });
        }

        server_cnx
            .handle_send(
                send_command(10, 33),
                PulsarFrame {
                    command: Bytes::new(),
                    metadata: None,
                    payload: Bytes::from(vec![1, 2, 3]),
                    checksum: None,
                },
            )
            .await
            .unwrap();

        let frame = client.next().await.unwrap().unwrap();
        let cmd = BaseCommand::decode(&frame.command[..]).unwrap();
        assert_eq!(cmd.r#type, base_command::Type::SendError as i32);
        let send_error = cmd.send_error.expect("send error payload");
        assert_eq!(send_error.producer_id, 10);
        assert_eq!(send_error.sequence_id, 33);
        assert_eq!(send_error.error, ServerError::ServiceNotReady as i32);
        assert!(send_error.message.contains("publish rate exceeded"));
        assert_ne!(server_cnx.state, State::Failed);
        assert_ne!(server_cnx.state, State::Closed);
    }

    #[tokio::test]
    async fn non_persistent_send_completes_without_leaving_pending_backlog() {
        let (mut server_cnx, _client) = build_test_connection();
        attach_non_persistent_producer(
            &mut server_cnx,
            "non-persistent://public/default/non-persistent-sync-send-topic",
            13,
        )
        .await;

        server_cnx
            .handle_send(
                send_command(13, 44),
                PulsarFrame {
                    command: Bytes::new(),
                    metadata: None,
                    payload: Bytes::from(vec![1, 2, 3]),
                    checksum: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(server_cnx.pending_send_requests, 0);
        assert!(!server_cnx.read_paused);
    }
}
