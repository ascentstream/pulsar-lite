/*
 * Topic Command Handlers
 * Handles topic metadata and lookup commands
 */

use futures::SinkExt;
use crate::protocol::codec::{PulsarFrameCodec, proto::pulsar::BaseCommand};
use crate::protocol::ServerCommand;
use crate::broker::SharedBrokerService;
use tokio_util::codec::Framed;

/// Handle PartitionMetadata command
pub async fn handle_partition_metadata<T>(
    framed: &mut Framed<T, PulsarFrameCodec>,
    cmd: BaseCommand,
    broker_service: &SharedBrokerService,
) -> Result<(), Box<dyn std::error::Error>>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let partition_cmd = cmd.partition_metadata.as_ref().ok_or("Missing partition metadata command")?;
    log::debug!("Handling PartitionMetadata command: topic={}, request_id={}",
        partition_cmd.topic, partition_cmd.request_id);

    // Get partition count from BrokerService
    let guard = broker_service.read().await;
    let partitions = guard.get_partition_metadata_response_count(&partition_cmd.topic);
    drop(guard);

    log::debug!("Returning partition metadata: topic={}, partitions={}", partition_cmd.topic, partitions);

    let response = ServerCommand::PartitionMetadataResponse {
        request_id: partition_cmd.request_id,
        partitions,
    };

    framed.send(response).await?;
    log::debug!("Sent PartitionMetadataResponse for request {}", partition_cmd.request_id);

    Ok(())
}

/// Handle Lookup command
pub async fn handle_lookup<T>(
    framed: &mut Framed<T, PulsarFrameCodec>,
    cmd: BaseCommand,
    broker_service_url: &str,
) -> Result<(), Box<dyn std::error::Error>>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let lookup_cmd = cmd.lookup_topic.as_ref().ok_or("Missing lookup command")?;
    log::debug!("Handling Lookup command: topic={}, request_id={}",
        lookup_cmd.topic, lookup_cmd.request_id);

    // Return local broker URL with pulsar:// protocol
    let response = ServerCommand::LookupResponse {
        request_id: lookup_cmd.request_id,
        broker_service_url: broker_service_url.to_string(),
    };

    framed.send(response).await?;
    log::debug!("Sent LookupResponse for request {}", lookup_cmd.request_id);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::broker_service::BrokerService;
    use crate::protocol::codec::proto::pulsar::{
        base_command,
        CommandLookupTopic,
        CommandPartitionedTopicMetadata,
    };
    use crate::storage::Storage;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::io::{duplex, AsyncReadExt};
    use tokio::sync::{Mutex, RwLock};

    fn create_test_broker_service() -> SharedBrokerService {
        Arc::new(RwLock::new(BrokerService::new(Arc::new(Mutex::new(
            Storage::new(Path::new("/tmp/test-lookup-handler")).unwrap(),
        )))))
    }

    fn create_lookup_command(request_id: u64) -> BaseCommand {
        BaseCommand {
            r#type: base_command::Type::Lookup as i32,
            lookup_topic: Some(CommandLookupTopic {
                topic: "persistent://public/default/perf-topic".to_string(),
                request_id,
                authoritative: Some(false),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn create_partition_metadata_command(request_id: u64) -> BaseCommand {
        BaseCommand {
            r#type: base_command::Type::PartitionedMetadata as i32,
            partition_metadata: Some(CommandPartitionedTopicMetadata {
                topic: "persistent://public/default/perf-topic".to_string(),
                request_id,
                metadata_auto_creation_enabled: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    async fn spawn_drain_task<T>(mut io: T) -> tokio::task::JoinHandle<()>
    where
        T: tokio::io::AsyncRead + Unpin + Send + 'static,
    {
        tokio::spawn(async move {
            let mut buf = [0u8; 8192];
            loop {
                match io.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        })
    }

    #[tokio::test]
    #[ignore]
    async fn perf_query_lookup_handler_10k_requests() {
        const REQUEST_COUNT: usize = 10_000;

        let (client, server) = duplex(1 << 20);
        let mut framed = Framed::new(server, PulsarFrameCodec::new());
        let drain_task = spawn_drain_task(client).await;
        let command = create_lookup_command(1);

        let start = Instant::now();
        for request_id in 0..REQUEST_COUNT as u64 {
            let mut command = command.clone();
            command.lookup_topic.as_mut().unwrap().request_id = request_id;
            handle_lookup(&mut framed, command, "pulsar://127.0.0.1:6650")
                .await
                .unwrap();
        }
        let elapsed = start.elapsed();

        println!(
            "PERF query lookup handler: requests={REQUEST_COUNT}, elapsed_ms={}",
            elapsed.as_millis()
        );

        drop(framed);
        drain_task.await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn perf_query_partition_metadata_handler_10k_requests() {
        const REQUEST_COUNT: usize = 10_000;

        let broker_service = create_test_broker_service();
        {
            let mut guard = broker_service.write().await;
            guard
                .get_or_create_partitioned_topic("persistent://public/default/perf-topic", 8)
                .await;
        }

        let (client, server) = duplex(1 << 20);
        let mut framed = Framed::new(server, PulsarFrameCodec::new());
        let drain_task = spawn_drain_task(client).await;
        let command = create_partition_metadata_command(1);

        let start = Instant::now();
        for request_id in 0..REQUEST_COUNT as u64 {
            let mut command = command.clone();
            command.partition_metadata.as_mut().unwrap().request_id = request_id;
            handle_partition_metadata(&mut framed, command, &broker_service)
                .await
                .unwrap();
        }
        let elapsed = start.elapsed();

        println!(
            "PERF query partition metadata handler: requests={REQUEST_COUNT}, elapsed_ms={}",
            elapsed.as_millis()
        );

        drop(framed);
        drain_task.await.unwrap();
    }
}
