/*
 * Consumer Command Handlers
 * Handles consumer-related commands: Subscribe, Flow, Ack, CloseConsumer
 */

use futures::SinkExt;
use std::sync::Arc;
use std::collections::HashMap;
use crate::protocol::codec::{PulsarFrameCodec, proto::pulsar::BaseCommand};
use crate::protocol::ServerCommand;
use tokio_util::codec::Framed;
use tokio::sync::mpsc;
use crate::broker::service::{Consumer, SharedStorage};
use crate::broker::service::consumer::PendingMessage;
use crate::broker::service::topic::SubscriptionType;
use crate::broker::broker_service::SharedBrokerService;

/// Handle Subscribe command (Apache Pulsar style)
pub async fn handle_subscribe<T>(
    framed: &mut Framed<T, PulsarFrameCodec>,
    cmd: BaseCommand,
    consumers: &mut HashMap<u64, Arc<Consumer>>,
    next_consumer_id: &mut u64,
    broker_service: SharedBrokerService,
    connection_id: String,
    message_tx: mpsc::UnboundedSender<(u64, PendingMessage)>,
) -> Result<(), Box<dyn std::error::Error>>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let subscribe_cmd = cmd.subscribe.as_ref().ok_or("Missing subscribe command")?;
    log::info!("Handling Subscribe command: topic={}, subscription={}, subType={:?}",
        subscribe_cmd.topic, subscribe_cmd.subscription, subscribe_cmd.sub_type);

    // Convert subscription type from proto to our enum
    let sub_type = match subscribe_cmd.sub_type {
        0 => SubscriptionType::Exclusive,
        1 => SubscriptionType::Shared,
        2 => SubscriptionType::Failover,
        3 => SubscriptionType::KeyShared,
        _ => SubscriptionType::Exclusive,
    };

    let consumer_id = *next_consumer_id;
    *next_consumer_id += 1;

    let consumer_name = subscribe_cmd.consumer_name
        .clone()
        .unwrap_or_else(|| format!("consumer-{}", consumer_id));

    // Get or create subscription, then create Consumer (Apache Pulsar style)
    let consumer = {
        let mut broker = broker_service.write().await;
        let topic = broker.get_or_create_topic(&subscribe_cmd.topic).await;
        let mut topic_guard = topic.write().await;

        // Get or create subscription - returns Arc<RwLock<Subscription>> (Apache Pulsar style)
        let subscription_arc = topic_guard
            .get_or_create_subscription(&subscribe_cmd.subscription, sub_type)
            .await?;

        // Create Consumer entity with message sender
        // Consumer will automatically prepend its consumer_id when sending messages
        let consumer = Arc::new(Consumer::new(
            consumer_id,
            consumer_name.clone(),
            subscription_arc.clone(),
            connection_id,
            message_tx,  // Pass the sender - Consumer will prepend its ID
        ));

        // Add consumer to Subscription
        {
            let mut sub_guard = subscription_arc.write().await;
            sub_guard.add_consumer(consumer.clone())?;
        }

        consumer
    };

    // Store consumer in connection tracking
    consumers.insert(consumer_id, consumer.clone());

    log::info!("Consumer {} created: topic={}, subscription={}, sub_type={:?}",
        consumer_id, subscribe_cmd.topic, subscribe_cmd.subscription, sub_type);

    // Send Success response
    let response = ServerCommand::Success {
        request_id: subscribe_cmd.request_id,
    };

    framed.send(response).await?;
    log::info!("Sent SubscribeSuccess for consumer {}", consumer_id);

    Ok(())
}

/// Handle Flow command - Client requests more messages (Push mode)
///
/// When a consumer sends Flow command, the broker:
/// 1. Updates the consumer's permits
/// 2. Triggers message dispatch to consumers with available permits
pub async fn handle_flow(
    cmd: BaseCommand,
    consumers: &mut HashMap<u64, Arc<Consumer>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let flow_cmd = cmd.flow.as_ref().ok_or("Missing flow command")?;
    log::info!("Handling Flow command: consumer_id={}, permits={}",
        flow_cmd.consumer_id, flow_cmd.message_permits);

    // Get consumer (Apache Pulsar style - directly from consumers map)
    let consumer = consumers.get(&flow_cmd.consumer_id)
        .ok_or_else(|| format!("Unknown consumer ID: {}", flow_cmd.consumer_id))?;

    // Flow permits to consumer and trigger dispatch via Subscription
    let consumer_id = consumer.consumer_id;
    let subscription = consumer.get_subscription();
    let sub_guard = subscription.read().await;
    sub_guard.consumer_flow(consumer_id, flow_cmd.message_permits).await;
    Ok(())
}

/// Handle Ack command - Message acknowledgment (Apache Pulsar style)
///
/// For Shared subscription:
/// 1. Validate that the message belongs to this consumer
/// 2. Remove from pending_acks
/// 3. Update storage cursor using ack_message_shared()
pub async fn handle_ack<T>(
    framed: &mut Framed<T, PulsarFrameCodec>,
    cmd: BaseCommand,
    consumers: &HashMap<u64, Arc<Consumer>>,
    storage: SharedStorage,
) -> Result<(), Box<dyn std::error::Error>>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let ack_cmd = cmd.ack.as_ref().ok_or("Missing ack command")?;
    log::debug!("Handling Ack command: consumer_id={}, ack_type={:?}",
        ack_cmd.consumer_id, ack_cmd.ack_type);

    // Get consumer (Apache Pulsar style - directly from consumers map)
    let consumer = consumers.get(&ack_cmd.consumer_id)
        .ok_or_else(|| format!("Unknown consumer ID: {}", ack_cmd.consumer_id))?;

    // Acknowledge message (Apache Pulsar style)
    if let Some(message_id) = ack_cmd.message_id.first() {
        let msg_id = crate::storage::MessageId {
            ledger: message_id.ledger_id,
            entry: message_id.entry_id,
            partition: message_id.partition.unwrap_or(0),
        };

        // Get subscription type
        let sub_type = consumer.get_sub_type();

        if sub_type == SubscriptionType::Shared {
            let ack_owner = if consumer.has_pending_ack(&msg_id).await {
                Some(consumer.clone())
            } else {
                let subscription = consumer.get_subscription();
                let subscription_consumers = {
                    let sub_guard = subscription.read().await;
                    sub_guard.get_consumers()
                };

                let mut owner = None;
                for candidate in subscription_consumers {
                    if candidate.consumer_id != consumer.consumer_id && candidate.has_pending_ack(&msg_id).await {
                        owner = Some(candidate);
                        break;
                    }
                }
                owner
            };

            if let Some(owner_consumer) = ack_owner {
                owner_consumer.remove_pending_ack(&msg_id).await;
                owner_consumer.record_message_acked().await;

                let mut guard = storage.lock().await;
                let topic_name = consumer.get_topic_name();
                let sub_name = consumer.get_subscription_name();
                guard.ack_message_shared(&topic_name, &sub_name, msg_id)?;
            } else {
                log::warn!(
                    "Consumer {} attempted to ack message {}:{} without ownership; ignoring storage ack",
                    ack_cmd.consumer_id, message_id.ledger_id, message_id.entry_id
                );
            }
        } else {
            // Non-Shared mode: original behavior
            consumer.ack_message(msg_id.clone()).await;

            let mut guard = storage.lock().await;
            let topic_name = consumer.get_topic_name();
            let sub_name = consumer.get_subscription_name();
            guard.ack_message(&topic_name, &sub_name, msg_id)?;
        }

        log::info!("Message {}:{} acknowledged for consumer {}",
            message_id.ledger_id, message_id.entry_id, ack_cmd.consumer_id);

        // Only send AckResponse when Ack command includes request_id
        if let Some(request_id) = ack_cmd.request_id {
            let response = ServerCommand::AckResponse {
                consumer_id: ack_cmd.consumer_id,
                request_id,
            };

            framed.send(response).await?;
            log::debug!("Sent AckResponse for consumer {} with request_id {}", ack_cmd.consumer_id, request_id);
        }
    }

    Ok(())
}

/// Handle CloseConsumer command (Apache Pulsar style)
pub async fn handle_close_consumer<T>(
    framed: &mut Framed<T, PulsarFrameCodec>,
    cmd: BaseCommand,
    consumers: &mut HashMap<u64, Arc<Consumer>>,
) -> Result<(), Box<dyn std::error::Error>>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let close_cmd = cmd.close_consumer.as_ref().ok_or("Missing close consumer command")?;
    log::info!("Handling CloseConsumer command: consumer_id={}, request_id={}",
        close_cmd.consumer_id, close_cmd.request_id);

    // Remove consumer from connection tracking (Apache Pulsar style)
    if let Some(consumer) = consumers.remove(&close_cmd.consumer_id) {
        // Remove consumer from Subscription (no need to lookup topic - Consumer has reference)
        {
            let mut sub_guard = consumer.subscription.write().await;
            sub_guard.remove_consumer_with_recovery(consumer.consumer_id).await;
            log::info!("Removed consumer {} from subscription {}",
                consumer.consumer_id, sub_guard.name);
        }
        log::info!("Closed consumer {} (subscription={})",
            consumer.consumer_id, consumer.get_subscription_name());
    } else {
        log::warn!("Attempted to close unknown consumer {}", close_cmd.consumer_id);
    }

    // Send Success response
    let response = ServerCommand::Success {
        request_id: close_cmd.request_id,
    };

    framed.send(response).await?;
    log::info!("Sent Success response for CloseConsumer request {}", close_cmd.request_id);

    Ok(())
}
