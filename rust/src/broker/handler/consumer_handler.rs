/*
 * Consumer Command Handlers
 * Handles consumer-related commands: Subscribe, Flow, Ack, CloseConsumer
 */

use crate::broker::broker_service::{SharedBrokerService, TopicRef};
use crate::broker::service::consumer::PendingMessage;
use crate::broker::service::topic::{
    KeySharedHashRange, KeySharedMode, KeySharedPolicy, SubscriptionType,
};
use crate::broker::service::{Consumer, SharedStorage};
use crate::protocol::codec::{proto::pulsar::BaseCommand, PulsarFrameCodec};
use crate::protocol::ServerCommand;
use futures::SinkExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

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
    log::info!(
        "Handling Subscribe command: topic={}, subscription={}, subType={:?}",
        subscribe_cmd.topic,
        subscribe_cmd.subscription,
        subscribe_cmd.sub_type
    );

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

    let consumer_name = subscribe_cmd
        .consumer_name
        .clone()
        .unwrap_or_else(|| format!("consumer-{}", consumer_id));
    let priority_level = subscribe_cmd.priority_level.unwrap_or(0);
    let subscription_properties = subscribe_cmd
        .subscription_properties
        .iter()
        .map(|kv| (kv.key.clone(), kv.value.clone()))
        .collect::<HashMap<_, _>>();
    let key_shared_policy = subscribe_cmd
        .key_shared_meta
        .as_ref()
        .map(|meta| KeySharedPolicy {
            mode: match meta.key_shared_mode {
                1 => KeySharedMode::Sticky,
                _ => KeySharedMode::AutoSplit,
            },
            ranges: meta
                .hash_ranges
                .iter()
                .map(|range| KeySharedHashRange {
                    start: range.start,
                    end: range.end,
                })
                .collect(),
            allow_out_of_order_delivery: meta.allow_out_of_order_delivery.unwrap_or(false),
        });

    // Get or create subscription, then create Consumer (Apache Pulsar style)
    let consumer = {
        let mut broker = broker_service.write().await;
        let topic = match broker.get_or_create_topic_auto(&subscribe_cmd.topic).await {
            TopicRef::NonPartitioned(topic) | TopicRef::Partition(topic) => topic,
            TopicRef::Partitioned(_) => {
                return Err(format!(
                    "Subscribe command must target a concrete topic or partition: {}",
                    subscribe_cmd.topic
                )
                .into())
            }
        };
        let mut topic_guard = topic.write().await;

        // Get or create subscription - returns Arc<RwLock<Subscription>> (Apache Pulsar style)
        let subscription_arc = topic_guard
            .get_or_create_subscription_with_options(
                &subscribe_cmd.subscription,
                sub_type,
                subscription_properties.clone(),
                key_shared_policy.clone(),
            )
            .await?;

        // Create Consumer entity with message sender
        // Consumer will automatically prepend its consumer_id when sending messages
        let consumer = Arc::new(Consumer::new_with_options(
            consumer_id,
            consumer_name.clone(),
            subscription_arc.clone(),
            connection_id,
            message_tx, // Pass the sender - Consumer will prepend its ID
            priority_level,
            key_shared_policy,
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

    log::info!(
        "Consumer {} created: topic={}, subscription={}, sub_type={:?}",
        consumer_id,
        subscribe_cmd.topic,
        subscribe_cmd.subscription,
        sub_type
    );
    log::debug!(
        "Consumer {} subscribed with priority level {}",
        consumer_id,
        priority_level
    );

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
    log::info!(
        "Handling Flow command: consumer_id={}, permits={}",
        flow_cmd.consumer_id,
        flow_cmd.message_permits
    );

    // Get consumer (Apache Pulsar style - directly from consumers map)
    let consumer = consumers
        .get(&flow_cmd.consumer_id)
        .ok_or_else(|| format!("Unknown consumer ID: {}", flow_cmd.consumer_id))?;

    // Native Pulsar updates permits on the consumer and then notifies the
    // dispatcher/subscription path. Non-persistent single-active dispatchers
    // read consumer-local permits directly, while shared variants also keep
    // dispatcher-level aggregates.
    consumer.add_permits(flow_cmd.message_permits).await;

    // Flow permits to dispatcher and trigger dispatch via Subscription
    let consumer_id = consumer.consumer_id;
    let subscription = consumer.get_subscription();
    let sub_guard = subscription.read().await;
    sub_guard
        .consumer_flow(consumer_id, flow_cmd.message_permits)
        .await;
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
    log::debug!(
        "Handling Ack command: consumer_id={}, ack_type={:?}",
        ack_cmd.consumer_id,
        ack_cmd.ack_type
    );

    // Get consumer (Apache Pulsar style - directly from consumers map)
    let consumer = consumers
        .get(&ack_cmd.consumer_id)
        .ok_or_else(|| format!("Unknown consumer ID: {}", ack_cmd.consumer_id))?;

    // Acknowledge message (Apache Pulsar style)
    if let Some(message_id) = ack_cmd.message_id.first() {
        // Pulsar protocol defaults non-partitioned message ids to partition = -1.
        // Keep the protocol/broker boundary aligned so Shared ownership can be
        // checked using the full MessageId instead of falling back to partial matching.
        let protocol_msg_id = crate::storage::MessageId {
            ledger: message_id.ledger_id,
            entry: message_id.entry_id,
            partition: message_id.partition.unwrap_or(-1),
        };

        let subscription = consumer.get_subscription();
        let (sub_type, is_non_persistent, subscription_consumers) = {
            let sub_guard = subscription.read().await;
            (
                sub_guard.get_sub_type(),
                sub_guard.is_non_persistent(),
                sub_guard.get_consumers(),
            )
        };

        if matches!(
            sub_type,
            SubscriptionType::Shared | SubscriptionType::KeyShared
        ) {
            let ack_owner = if consumer.has_pending_ack(&protocol_msg_id).await {
                Some(consumer.clone())
            } else {
                let mut owner = None;
                for candidate in subscription_consumers {
                    if candidate.consumer_id != consumer.consumer_id
                        && candidate.has_pending_ack(&protocol_msg_id).await
                    {
                        owner = Some(candidate);
                        break;
                    }
                }
                owner
            };

            if let Some(owner_consumer) = ack_owner {
                let removed = owner_consumer.remove_pending_ack(&protocol_msg_id).await;

                if removed {
                    owner_consumer.record_message_acked().await;

                    if !is_non_persistent {
                        let mut guard = storage.lock().await;
                        let topic_name = consumer.get_topic_name();
                        let sub_name = consumer.get_subscription_name();
                        guard.ack_message_shared(
                            &topic_name,
                            &sub_name,
                            protocol_msg_id.clone(),
                        )?;
                    }
                } else {
                    log::warn!(
                        "Consumer {} found owner {} for message {}:{}:{} but pending ack removal failed; ignoring ack",
                        ack_cmd.consumer_id,
                        owner_consumer.consumer_id,
                        protocol_msg_id.ledger,
                        protocol_msg_id.entry,
                        protocol_msg_id.partition
                    );
                }
            } else {
                log::warn!(
                    "Consumer {} attempted to ack message {}:{}:{} without ownership; ignoring ack",
                    ack_cmd.consumer_id,
                    protocol_msg_id.ledger,
                    protocol_msg_id.entry,
                    protocol_msg_id.partition
                );
            }
        } else {
            consumer.ack_message(protocol_msg_id.clone()).await;

            if !is_non_persistent {
                let mut guard = storage.lock().await;
                let topic_name = consumer.get_topic_name();
                let sub_name = consumer.get_subscription_name();
                guard.ack_message(&topic_name, &sub_name, protocol_msg_id)?;
            }
        }

        log::info!(
            "Message {}:{} acknowledged for consumer {}",
            message_id.ledger_id,
            message_id.entry_id,
            ack_cmd.consumer_id
        );

        // Only send AckResponse when Ack command includes request_id
        if let Some(request_id) = ack_cmd.request_id {
            let response = ServerCommand::AckResponse {
                consumer_id: ack_cmd.consumer_id,
                request_id,
            };

            framed.send(response).await?;
            log::debug!(
                "Sent AckResponse for consumer {} with request_id {}",
                ack_cmd.consumer_id,
                request_id
            );
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
    let close_cmd = cmd
        .close_consumer
        .as_ref()
        .ok_or("Missing close consumer command")?;
    log::info!(
        "Handling CloseConsumer command: consumer_id={}, request_id={}",
        close_cmd.consumer_id,
        close_cmd.request_id
    );

    // Remove consumer from connection tracking (Apache Pulsar style)
    if let Some(consumer) = consumers.remove(&close_cmd.consumer_id) {
        // Remove consumer from Subscription (no need to lookup topic - Consumer has reference)
        {
            let mut sub_guard = consumer.subscription.write().await;
            sub_guard
                .remove_consumer_with_recovery(consumer.consumer_id)
                .await;
            log::info!(
                "Removed consumer {} from subscription {}",
                consumer.consumer_id,
                sub_guard.name
            );
        }
        log::info!(
            "Closed consumer {} (subscription={})",
            consumer.consumer_id,
            consumer.get_subscription_name()
        );
    } else {
        log::warn!(
            "Attempted to close unknown consumer {}",
            close_cmd.consumer_id
        );
    }

    // Send Success response
    let response = ServerCommand::Success {
        request_id: close_cmd.request_id,
    };

    framed.send(response).await?;
    log::info!(
        "Sent Success response for CloseConsumer request {}",
        close_cmd.request_id
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::broker_service::{BrokerService, TopicRef};
    use crate::broker::service::topic::{KeySharedMode, TopicRuntimeMode};
    use crate::protocol::codec::proto::pulsar::{
        base_command, CommandAck, CommandSubscribe, IntRange, KeySharedMeta, KeyValue,
        MessageIdData,
    };
    use crate::storage::Storage;
    use futures::StreamExt;
    use prost::Message;
    use std::path::Path;
    use tokio::io::duplex;
    use tokio::sync::{Mutex, RwLock};

    async fn create_subscribe_test_context() -> (
        Framed<tokio::io::DuplexStream, PulsarFrameCodec>,
        Framed<tokio::io::DuplexStream, PulsarFrameCodec>,
        HashMap<u64, Arc<Consumer>>,
        SharedBrokerService,
        SharedStorage,
        mpsc::UnboundedSender<(u64, PendingMessage)>,
        mpsc::UnboundedReceiver<(u64, PendingMessage)>,
    ) {
        let (server_io, client_io) = duplex(4096);
        let server_framed = Framed::new(server_io, PulsarFrameCodec::new());
        let client_framed = Framed::new(client_io, PulsarFrameCodec::new());
        let storage = Arc::new(Mutex::new(
            Storage::new(Path::new("/tmp/test-consumer-handler-storage")).unwrap(),
        ));
        let broker_service = Arc::new(RwLock::new(BrokerService::with_config(storage.clone(), 0)));
        let (message_tx, message_rx) = mpsc::unbounded_channel();

        (
            server_framed,
            client_framed,
            HashMap::new(),
            broker_service,
            storage,
            message_tx,
            message_rx,
        )
    }

    fn build_subscribe_command(
        topic: &str,
        sub_type: i32,
        priority_level: Option<i32>,
    ) -> BaseCommand {
        build_subscribe_command_with_options(topic, sub_type, priority_level, Vec::new(), None)
    }

    fn build_subscribe_command_with_options(
        topic: &str,
        sub_type: i32,
        priority_level: Option<i32>,
        subscription_properties: Vec<KeyValue>,
        key_shared_meta: Option<KeySharedMeta>,
    ) -> BaseCommand {
        BaseCommand {
            r#type: base_command::Type::Subscribe as i32,
            subscribe: Some(CommandSubscribe {
                topic: topic.to_string(),
                subscription: "test-sub".to_string(),
                sub_type,
                consumer_id: 11,
                request_id: 22,
                consumer_name: Some("test-consumer".to_string()),
                priority_level,
                subscription_properties,
                key_shared_meta,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn build_ack_command(
        consumer_id: u64,
        ledger_id: u64,
        entry_id: u64,
        partition: i32,
    ) -> BaseCommand {
        BaseCommand {
            r#type: base_command::Type::Ack as i32,
            ack: Some(CommandAck {
                consumer_id,
                ack_type: 0,
                message_id: vec![MessageIdData {
                    ledger_id,
                    entry_id,
                    partition: Some(partition),
                    ..Default::default()
                }],
                request_id: Some(99),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn subscribe_priority_level_is_propagated_to_consumer() {
        let (
            mut server_framed,
            mut client_framed,
            mut consumers,
            broker_service,
            _storage,
            message_tx,
            _message_rx,
        ) = create_subscribe_test_context().await;
        let mut next_consumer_id = 0;

        handle_subscribe(
            &mut server_framed,
            build_subscribe_command("persistent://public/default/test-topic", 1, Some(3)),
            &mut consumers,
            &mut next_consumer_id,
            broker_service,
            "conn-1".to_string(),
            message_tx,
        )
        .await
        .unwrap();

        let consumer = consumers.values().next().unwrap();
        assert_eq!(consumer.get_priority_level(), 3);

        let response = client_framed.next().await.unwrap().unwrap();
        let cmd = BaseCommand::decode(&response.command[..]).unwrap();
        assert_eq!(cmd.r#type, base_command::Type::Success as i32);
    }

    #[tokio::test]
    async fn subscribe_priority_level_defaults_to_zero() {
        let (
            mut server_framed,
            _client_framed,
            mut consumers,
            broker_service,
            _storage,
            message_tx,
            _message_rx,
        ) = create_subscribe_test_context().await;
        let mut next_consumer_id = 0;

        handle_subscribe(
            &mut server_framed,
            build_subscribe_command("persistent://public/default/test-topic", 1, None),
            &mut consumers,
            &mut next_consumer_id,
            broker_service,
            "conn-2".to_string(),
            message_tx,
        )
        .await
        .unwrap();

        let consumer = consumers.values().next().unwrap();
        assert_eq!(consumer.get_priority_level(), 0);
    }

    #[tokio::test]
    async fn non_persistent_shared_ack_clears_ownership_without_advancing_storage_frontier() {
        let topic_name = "non-persistent://public/default/test-np-shared-ack";
        let (
            mut server_framed,
            mut client_framed,
            mut consumers,
            broker_service,
            storage,
            message_tx,
            _message_rx,
        ) = create_subscribe_test_context().await;
        let mut next_consumer_id = 0;

        let topic = {
            let mut broker = broker_service.write().await;
            match broker.get_or_create_topic_auto(topic_name).await {
                TopicRef::NonPartitioned(topic) | TopicRef::Partition(topic) => topic,
                TopicRef::Partitioned(_) => panic!("expected concrete topic"),
            }
        };
        topic
            .write()
            .await
            .set_runtime_mode(TopicRuntimeMode::NonPersistent);

        handle_subscribe(
            &mut server_framed,
            build_subscribe_command(topic_name, 1, None),
            &mut consumers,
            &mut next_consumer_id,
            broker_service,
            "conn-3".to_string(),
            message_tx,
        )
        .await
        .unwrap();

        let _ = client_framed.next().await.unwrap().unwrap();

        let consumer = consumers.values().next().unwrap().clone();
        consumer.add_permits(1).await;
        let subscription = consumer.get_subscription();
        subscription
            .read()
            .await
            .consumer_flow(consumer.consumer_id, 1)
            .await;

        {
            let mut topic_guard = topic.write().await;
            topic_guard.publish_message(None, b"hello").await.unwrap();
            topic_guard.dispatch_to_subscriptions().await;
        }

        assert_eq!(consumer.pending_ack_count().await, 1);

        handle_ack(
            &mut server_framed,
            build_ack_command(consumer.consumer_id, 0, 0, -1),
            &consumers,
            storage.clone(),
        )
        .await
        .unwrap();

        assert_eq!(consumer.pending_ack_count().await, 0);
        assert_eq!(consumer.get_stats().await.messages_acked, 1);
        assert_eq!(
            storage
                .lock()
                .await
                .get_mark_delete_position(topic_name, "test-sub"),
            None
        );

        let ack_response = client_framed.next().await.unwrap().unwrap();
        let ack_cmd = BaseCommand::decode(&ack_response.command[..]).unwrap();
        assert_eq!(ack_cmd.r#type, base_command::Type::AckResponse as i32);
    }

    #[tokio::test]
    async fn non_persistent_exclusive_ack_updates_consumer_stats_without_storage_ack() {
        let topic_name = "non-persistent://public/default/test-np-exclusive-ack";
        let (
            mut server_framed,
            mut client_framed,
            mut consumers,
            broker_service,
            storage,
            message_tx,
            _message_rx,
        ) = create_subscribe_test_context().await;
        let mut next_consumer_id = 0;

        let topic = {
            let mut broker = broker_service.write().await;
            match broker.get_or_create_topic_auto(topic_name).await {
                TopicRef::NonPartitioned(topic) | TopicRef::Partition(topic) => topic,
                TopicRef::Partitioned(_) => panic!("expected concrete topic"),
            }
        };
        topic
            .write()
            .await
            .set_runtime_mode(TopicRuntimeMode::NonPersistent);

        handle_subscribe(
            &mut server_framed,
            build_subscribe_command(topic_name, 0, None),
            &mut consumers,
            &mut next_consumer_id,
            broker_service,
            "conn-4".to_string(),
            message_tx,
        )
        .await
        .unwrap();

        let _ = client_framed.next().await.unwrap().unwrap();

        let consumer = consumers.values().next().unwrap().clone();
        consumer.add_permits(1).await;
        let subscription = consumer.get_subscription();
        subscription
            .read()
            .await
            .consumer_flow(consumer.consumer_id, 1)
            .await;

        {
            let mut topic_guard = topic.write().await;
            topic_guard.publish_message(None, b"hello").await.unwrap();
            topic_guard.dispatch_to_subscriptions().await;
        }

        assert_eq!(consumer.get_stats().await.messages_acked, 0);

        handle_ack(
            &mut server_framed,
            build_ack_command(consumer.consumer_id, 0, 0, -1),
            &consumers,
            storage.clone(),
        )
        .await
        .unwrap();

        assert_eq!(consumer.get_stats().await.messages_acked, 1);
        assert_eq!(
            storage
                .lock()
                .await
                .get_mark_delete_position(topic_name, "test-sub"),
            None
        );

        let ack_response = client_framed.next().await.unwrap().unwrap();
        let ack_cmd = BaseCommand::decode(&ack_response.command[..]).unwrap();
        assert_eq!(ack_cmd.r#type, base_command::Type::AckResponse as i32);
    }

    #[tokio::test]
    async fn subscribe_propagates_non_persistent_properties_and_key_shared_policy() {
        let (
            mut server_framed,
            _client_framed,
            mut consumers,
            broker_service,
            _storage,
            message_tx,
            _message_rx,
        ) = create_subscribe_test_context().await;
        let mut next_consumer_id = 0;
        let topic_name = "non-persistent://public/default/test-key-shared";

        {
            let mut broker = broker_service.write().await;
            let topic = match broker.get_or_create_topic_auto(topic_name).await {
                TopicRef::NonPartitioned(topic) | TopicRef::Partition(topic) => topic,
                TopicRef::Partitioned(_) => panic!("expected concrete topic"),
            };
            topic
                .write()
                .await
                .set_runtime_mode(TopicRuntimeMode::NonPersistent);
        }

        handle_subscribe(
            &mut server_framed,
            build_subscribe_command_with_options(
                topic_name,
                3,
                None,
                vec![KeyValue {
                    key: "env".to_string(),
                    value: "test".to_string(),
                }],
                Some(KeySharedMeta {
                    key_shared_mode: 1,
                    hash_ranges: vec![IntRange { start: 0, end: 10 }],
                    allow_out_of_order_delivery: Some(false),
                }),
            ),
            &mut consumers,
            &mut next_consumer_id,
            broker_service.clone(),
            "conn-5".to_string(),
            message_tx,
        )
        .await
        .unwrap();

        let consumer = consumers.values().next().unwrap();
        let policy = consumer.key_shared_policy().expect("key shared policy");
        assert_eq!(policy.mode, KeySharedMode::Sticky);
        assert_eq!(policy.ranges.len(), 1);

        let broker = broker_service.read().await;
        let topic = broker.get_topic(topic_name).expect("topic");
        let subscription = topic.read().await.get_subscription("test-sub").unwrap();
        let guard = subscription.read().await;
        assert_eq!(guard.properties().get("env"), Some(&String::from("test")));
        assert_eq!(
            guard.key_shared_policy().map(|value| value.mode),
            Some(KeySharedMode::Sticky)
        );
    }
}
