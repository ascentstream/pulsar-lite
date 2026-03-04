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
    log::info!("Handling PartitionMetadata command: topic={}, request_id={}",
        partition_cmd.topic, partition_cmd.request_id);

    // Get partition count from BrokerService
    let guard = broker_service.read().await;
    let partitions = if guard.should_be_partitioned(&partition_cmd.topic) {
        guard.get_partition_count(&partition_cmd.topic)
            .unwrap_or(guard.get_default_partitions()) as i32
    } else {
        0  // Non-partitioned topic
    };
    drop(guard);

    log::info!("Returning partition metadata: topic={}, partitions={}", partition_cmd.topic, partitions);

    let response = ServerCommand::PartitionMetadataResponse {
        request_id: partition_cmd.request_id,
        partitions,
    };

    framed.send(response).await?;
    log::info!("Sent PartitionMetadataResponse for request {}", partition_cmd.request_id);

    Ok(())
}

/// Handle Lookup command
pub async fn handle_lookup<T>(
    framed: &mut Framed<T, PulsarFrameCodec>,
    cmd: BaseCommand,
) -> Result<(), Box<dyn std::error::Error>>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let lookup_cmd = cmd.lookup_topic.as_ref().ok_or("Missing lookup command")?;
    log::info!("Handling Lookup command: topic={}, request_id={}",
        lookup_cmd.topic, lookup_cmd.request_id);

    // Return local broker URL with pulsar:// protocol
    let response = ServerCommand::LookupResponse {
        request_id: lookup_cmd.request_id,
        broker_service_url: "pulsar://localhost:6650".to_string(),
    };

    framed.send(response).await?;
    log::info!("Sent LookupResponse for request {}", lookup_cmd.request_id);

    Ok(())
}
