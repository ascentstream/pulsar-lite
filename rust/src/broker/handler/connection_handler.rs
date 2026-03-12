/*
 * Connection Command Handlers
 * Handles connection-level commands: Connect, Ping/Pong
 */

use futures::SinkExt;
use crate::protocol::codec::{PulsarFrameCodec, proto::pulsar::{BaseCommand, CommandPong}};
use crate::protocol::ServerCommand;
use tokio_util::codec::Framed;

/// Handle Connect command
pub async fn handle_connect<T>(
    framed: &mut Framed<T, PulsarFrameCodec>,
    cmd: BaseCommand,
) -> Result<i32, Box<dyn std::error::Error>>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    log::info!("Handling Connect command");
    let protocol_version = cmd
        .connect
        .as_ref()
        .and_then(|connect| connect.protocol_version)
        .unwrap_or_default();

    // Send Connected response
    let response = ServerCommand::Connected {
        server_version: "Pulsar-Lite-0.1.0".to_string(),
        protocol_version,
    };

    // Debug: print the command bytes
    let cmd_bytes = response.to_bytes();
    log::debug!("Command protobuf ({} bytes): {:02x?}", cmd_bytes.len(), cmd_bytes);

    // Calculate frame size
    let total_size = 4 + cmd_bytes.len() + 4; // cmd_size + cmd + metadata_size
    log::debug!("Frame total_size: {}", total_size);
    log::debug!("Frame layout: [4B total_size] [4B cmd_size] [{}B cmd] [4B metadata_size]", cmd_bytes.len());

    framed.send(response).await?;

    log::info!("Sent Connected response");
    Ok(protocol_version)
}

/// Handle Ping command
pub async fn handle_ping<T>(framed: &mut Framed<T, PulsarFrameCodec>) -> Result<(), Box<dyn std::error::Error>>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    log::debug!("Handling Ping command");

    // Send Pong response
    let response = ServerCommand::Pong;
    framed.send(response).await?;

    log::debug!("Sent Pong response");
    Ok(())
}

/// Handle Pong command
pub async fn handle_pong(_pong: CommandPong) -> Result<(), Box<dyn std::error::Error>> {
    log::debug!("Handling Pong command");
    Ok(())
}
