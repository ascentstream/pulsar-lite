/*
 * Producer Command Handlers
 * Handles producer-related commands: Producer, Send, CloseProducer
 */

use futures::SinkExt;
use std::collections::HashMap;
use std::sync::Arc;
use crate::protocol::codec::{PulsarFrameCodec, PulsarFrame, proto::pulsar::BaseCommand};
use crate::protocol::ServerCommand;
use tokio_util::codec::Framed;
use crate::broker::service::Producer;
use crate::broker::broker_service::SharedBrokerService;

/// Handle Producer command
pub async fn handle_producer<T>(
    framed: &mut Framed<T, PulsarFrameCodec>,
    cmd: BaseCommand,
    producers: &mut HashMap<u64, Arc<Producer>>,
    next_producer_id: &mut u64,
    topic_manager: SharedBrokerService,
) -> Result<(), Box<dyn std::error::Error>>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let producer_cmd = cmd.producer.as_ref().ok_or("Missing producer command")?;
    log::info!("Handling Producer command: topic={}, producer_name={:?}",
        producer_cmd.topic, producer_cmd.producer_name);

    let producer_id = *next_producer_id;
    *next_producer_id += 1;

    let producer_name = producer_cmd.producer_name
        .clone()
        .unwrap_or_else(|| format!("producer-{}", producer_id));

    // Get or create topic (Apache Pulsar style)
    let topic = {
        let mut manager = topic_manager.write().await;
        manager.get_or_create_topic(&producer_cmd.topic).await
    };

    // Create Producer object with Topic reference (Apache Pulsar style)
    let connection_id = format!("conn-{}", producer_id);
    let producer = Arc::new(Producer::new(
        producer_id,
        producer_name.clone(),
        topic.clone(),
        connection_id,
    ));

    // Add producer to Topic
    {
        let mut topic_guard = topic.write().await;
        topic_guard.add_producer(producer.clone())?;
    }

    // Store producer in connection tracking
    producers.insert(producer_id, producer.clone());

    // Send ProducerSuccess response
    let response = ServerCommand::ProducerSuccess {
        request_id: producer_cmd.request_id,
        producer_name,
        producer_id,
    };

    framed.send(response).await?;
    log::info!("Sent ProducerSuccess for producer {}", producer_id);

    Ok(())
}

/// Handle Send command (Push mode - Apache Pulsar style)
///
/// This handler:
/// 1. Publishes the message to storage
/// 2. Immediately dispatches to all subscriptions (Push mode)
/// 3. Sends SendReceipt to producer
pub async fn handle_send<T>(
    framed: &mut Framed<T, PulsarFrameCodec>,
    cmd: BaseCommand,
    frame: PulsarFrame,
    producers: &HashMap<u64, Arc<Producer>>
) -> Result<(), Box<dyn std::error::Error>>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let send_cmd = cmd.send.as_ref().ok_or("Missing send command")?;
    log::debug!("Handling Send command: producer_id={}, sequence_id={}",
        send_cmd.producer_id, send_cmd.sequence_id);

    let producer = producers.get(&send_cmd.producer_id)
        .ok_or_else(|| format!("Unknown producer ID: {}", send_cmd.producer_id))?
        .clone();

    // Publish message directly through Producer (Apache Pulsar style)
    let message_id = producer.publish_message(&frame.payload).await?;

    log::debug!("Stored message {}:{}:{} for topic '{}'",
        message_id.ledger, message_id.entry, message_id.partition, producer.get_topic_name());

    // Push mode: Dispatch message to all subscriptions immediately
    // This is consistent with Apache Pulsar's behavior
    {
        let topic = producer.get_topic();
        let topic_guard = topic.read().await;
        topic_guard.dispatch_to_subscriptions().await;
    }

    // Send SendReceipt response
    let response = ServerCommand::SendReceipt {
        producer_id: send_cmd.producer_id,
        sequence_id: send_cmd.sequence_id,
        ledger_id: message_id.ledger,
        entry_id: message_id.entry,
        partition: message_id.partition,
    };

    framed.send(response).await?;
    log::debug!("Sent SendReceipt for sequence {}", send_cmd.sequence_id);

    Ok(())
}

/// Handle CloseProducer command (optimized - use Producer's topic reference)
pub async fn handle_close_producer<T>(
    framed: &mut Framed<T, PulsarFrameCodec>,
    cmd: BaseCommand,
    producers: &mut HashMap<u64, Arc<Producer>>,
    _topic_manager: SharedBrokerService, // Keep for compatibility, but not used
) -> Result<(), Box<dyn std::error::Error>>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let close_cmd = cmd.close_producer.as_ref().ok_or("Missing close producer command")?;
    log::info!("Handling CloseProducer command: producer_id={}, request_id={}",
        close_cmd.producer_id, close_cmd.request_id);

    // Remove producer from connection tracking
    if let Some(producer) = producers.remove(&close_cmd.producer_id) {
        // Remove producer from Topic (use Producer's topic reference)
        let topic = producer.get_topic();
        let mut topic_guard = topic.write().await;
        topic_guard.remove_producer(producer.get_producer_id());
        log::info!("Removed producer {} from topic '{}' in Topic",
            producer.get_producer_id(), topic_guard.name);
        log::info!("Closed producer {} ({})",
            producer.get_producer_id(), producer.get_producer_name());
    } else {
        log::warn!("Attempted to close unknown producer {}", close_cmd.producer_id);
    }

    // Send Success response
    let response = ServerCommand::Success {
        request_id: close_cmd.request_id,
    };

    framed.send(response).await?;
    log::info!("Sent Success response for CloseProducer request {}", close_cmd.request_id);

    Ok(())
}
