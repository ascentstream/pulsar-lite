/*
 * Server Connection Handler
 * Handles individual client connections, inspired by Apache Pulsar's ServerCnx
 */

use futures::{StreamExt, SinkExt};
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

use crate::protocol::codec::{PulsarFrameCodec, PulsarFrame, proto::pulsar::{BaseCommand, base_command}};
use super::{Producer, Consumer, SharedStorage};
use super::consumer::PendingMessage;
use crate::broker::handler;
use crate::broker::broker_service::SharedBrokerService;
use crate::protocol::ServerCommand;

/// Connection state
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum State {
    Start,      // Initial state
    Connected,  // Connection established
    Closing,    // Connection is closing
    Closed,     // Connection closed
}

/// Server Connection - handles a single client connection
/// Inspired by Apache Pulsar's ServerCnx design
pub struct ServerCnx<T>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    /// Framed socket for async read/write
    framed: Framed<T, PulsarFrameCodec>,

    /// Connection state
    state: State,

    /// Producers on this connection (producer_id -> Producer)
    producers: HashMap<u64, Arc<Producer>>,

    /// Consumers on this connection (consumer_id -> Consumer) (Apache Pulsar style)
    consumers: HashMap<u64, Arc<Consumer>>,

    /// Message channel receiver - receives messages from consumers to send to client
    /// All consumers on this connection share the same channel
    message_rx: mpsc::UnboundedReceiver<(u64, PendingMessage)>,

    /// Message channel sender - cloned and passed to each consumer
    message_tx: mpsc::UnboundedSender<(u64, PendingMessage)>,

    /// Connection ID (unique identifier for this connection)
    connection_id: String,

    /// Next producer ID for this connection
    next_producer_id: u64,

    /// Next consumer ID for this connection
    next_consumer_id: u64,

    /// Shared storage reference
    storage: SharedStorage,

    /// Topic manager reference
    topic_manager: SharedBrokerService,
}

impl<T> ServerCnx<T>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    /// Create a new ServerCnx
    pub fn new(
        socket: T,
        storage: SharedStorage,
        topic_manager: SharedBrokerService,
        connection_id: String,
    ) -> Self {
        let (message_tx, message_rx) = mpsc::unbounded_channel();

        Self {
            framed: Framed::new(socket, PulsarFrameCodec::new()),
            state: State::Start,
            producers: HashMap::new(),
            consumers: HashMap::new(),
            message_rx,
            message_tx,
            connection_id,
            next_producer_id: 0,
            next_consumer_id: 0,
            storage,
            topic_manager,
        }
    }

    /// Get a cloned sender for passing to consumers
    pub fn get_message_sender(&self) -> mpsc::UnboundedSender<(u64, PendingMessage)> {
        self.message_tx.clone()
    }

    /// Main connection handling loop with tokio::select!
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.state = State::Connected;

        loop {
            tokio::select! {
                // Handle incoming client requests
                frame_result = self.framed.next() => {
                    match frame_result {
                        Some(frame) => {
                            let frame = frame?;
                            // Parse the command
                            let base_command = BaseCommand::decode(&frame.command[..])?;

                            log::debug!("Received command: {:?}", base_command.r#type);

                            // Handle command
                            if let Err(e) = self.handle_command(base_command, frame).await {
                                log::error!("Error handling command: {}", e);
                                return Err(e);
                            }
                        }
                        None => break, // Connection closed
                    }
                }

                // Handle outgoing messages to send to client
                Some((consumer_id, pending_msg)) = self.message_rx.recv() => {
                    if let Err(e) = self.send_message_to_client(consumer_id, pending_msg).await {
                        log::error!("Error sending message to client: {}", e);
                        return Err(e);
                    }
                }
            }
        }

        // Connection closed, cleanup
        self.state = State::Closing;
        self.cleanup().await?;
        self.state = State::Closed;

        Ok(())
    }

    /// Send a message to the client
    async fn send_message_to_client(
        &mut self,
        consumer_id: u64,
        pending_msg: PendingMessage,
    ) -> Result<(), Box<dyn std::error::Error>> {
        log::debug!(
            "Sending message {}:{}:{} to consumer {} on connection {}",
            pending_msg.message_id.ledger,
            pending_msg.message_id.entry,
            pending_msg.message_id.partition,
            consumer_id,
            self.connection_id
        );

        // Create Message command
        let cmd = ServerCommand::Message {
            consumer_id,
            ledger_id: pending_msg.message_id.ledger,
            entry_id: pending_msg.message_id.entry,
            partition: pending_msg.message_id.partition,
            payload: pending_msg.payload,
        };

        // Send via framed connection
        self.framed.send(cmd).await?;

        log::debug!("Message sent successfully to consumer {}", consumer_id);
        Ok(())
    }

    /// Handle a single command
    async fn handle_command(
        &mut self,
        base_command: BaseCommand,
        frame: PulsarFrame,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match base_command.r#type {
            x if x == base_command::Type::Connect as i32 => {
                self.handle_connect(base_command).await?;
            }
            x if x == base_command::Type::PartitionedMetadata as i32 => {
                self.handle_partition_metadata(base_command).await?;
            }
            x if x == base_command::Type::Lookup as i32 => {
                self.handle_lookup(base_command).await?;
            }
            x if x == base_command::Type::Producer as i32 => {
                self.handle_producer(base_command).await?;
            }
            x if x == base_command::Type::Send as i32 => {
                self.handle_send(base_command, frame).await?;
            }
            x if x == base_command::Type::Subscribe as i32 => {
                self.handle_subscribe(base_command).await?;
            }
            x if x == base_command::Type::Flow as i32 => {
                self.handle_flow(base_command).await?;
            }
            x if x == base_command::Type::Ack as i32 => {
                self.handle_ack(base_command).await?;
            }
            x if x == base_command::Type::Ping as i32 => {
                self.handle_ping().await?;
            }
            x if x == base_command::Type::CloseProducer as i32 => {
                self.handle_close_producer(base_command).await?;
            }
            x if x == base_command::Type::CloseConsumer as i32 => {
                self.handle_close_consumer(base_command).await?;
            }
            _ => {
                log::warn!("Unsupported command type: {}", base_command.r#type);
            }
        }

        Ok(())
    }

    /// Cleanup connection resources
    async fn cleanup(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        log::debug!("Cleaning up connection: {} producers, {} consumers",
            self.producers.len(), self.consumers.len());

        // Close all producers (use Producer's topic reference)
        for (producer_id, producer) in self.producers.drain() {
            let topic = producer.get_topic();
            let mut topic_guard = topic.write().await;
            topic_guard.remove_producer(producer_id);
            log::debug!("Closed producer {} on connection cleanup", producer_id);
        }

        // Close all consumers (Apache Pulsar style - Consumer has Subscription reference)
        for (consumer_id, consumer) in self.consumers.drain() {
            // Remove consumer from Subscription (no need to lookup topic)
            {
                let mut sub_guard = consumer.subscription.write().await;
                sub_guard.remove_consumer(consumer_id);
            }
            log::debug!("Closed consumer {} on connection cleanup", consumer_id);
        }

        Ok(())
    }

    // Delegate to handler functions

    async fn handle_connect(&mut self, cmd: BaseCommand) -> Result<(), Box<dyn std::error::Error>> {
        handler::handle_connect(&mut self.framed, cmd).await
    }

    async fn handle_partition_metadata(&mut self, cmd: BaseCommand) -> Result<(), Box<dyn std::error::Error>> {
        handler::handle_partition_metadata(&mut self.framed, cmd, &self.topic_manager).await
    }

    async fn handle_lookup(&mut self, cmd: BaseCommand) -> Result<(), Box<dyn std::error::Error>> {
        handler::handle_lookup(&mut self.framed, cmd).await
    }

    async fn handle_producer(&mut self, cmd: BaseCommand) -> Result<(), Box<dyn std::error::Error>> {
        handler::handle_producer(
            &mut self.framed,
            cmd,
            &mut self.producers,
            &mut self.next_producer_id,
            self.topic_manager.clone(),
        ).await
    }

    async fn handle_send(&mut self, cmd: BaseCommand, frame: PulsarFrame) -> Result<(), Box<dyn std::error::Error>> {
        handler::handle_send(&mut self.framed, cmd, frame, &self.producers).await
    }

    async fn handle_subscribe(&mut self, cmd: BaseCommand) -> Result<(), Box<dyn std::error::Error>> {
        handler::handle_subscribe(
            &mut self.framed,
            cmd,
            &mut self.consumers,
            &mut self.next_consumer_id,
            self.topic_manager.clone(),
            self.connection_id.clone(),
            self.message_tx.clone(),  // Pass message sender
        ).await
    }

    async fn handle_flow(&mut self, cmd: BaseCommand) -> Result<(), Box<dyn std::error::Error>> {
        handler::handle_flow(
            cmd,
            &mut self.consumers,
        ).await
    }

    async fn handle_ack(&mut self, cmd: BaseCommand) -> Result<(), Box<dyn std::error::Error>> {
        handler::handle_ack(
            &mut self.framed,
            cmd,
            &self.consumers,
            self.storage.clone(),
        ).await
    }

    async fn handle_ping(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        handler::handle_ping(&mut self.framed).await
    }

    async fn handle_close_producer(&mut self, cmd: BaseCommand) -> Result<(), Box<dyn std::error::Error>> {
        handler::handle_close_producer(
            &mut self.framed,
            cmd,
            &mut self.producers,
            self.topic_manager.clone(),
        ).await
    }

    async fn handle_close_consumer(&mut self, cmd: BaseCommand) -> Result<(), Box<dyn std::error::Error>> {
        handler::handle_close_consumer(
            &mut self.framed,
            cmd,
            &mut self.consumers,
        ).await
    }
}

/// Handle a single client connection (compatibility wrapper)
pub async fn handle_connection(
    socket: tokio::net::TcpStream,
    storage: SharedStorage,
    topic_manager: SharedBrokerService,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static CONNECTION_COUNTER: AtomicU64 = AtomicU64::new(0);

    let connection_id = format!("conn-{}", CONNECTION_COUNTER.fetch_add(1, Ordering::Relaxed));
    let mut server_cnx = ServerCnx::new(socket, storage, topic_manager, connection_id);
    server_cnx.run().await
}
