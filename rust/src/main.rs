/*
 * Pulsar Lite Binary Protocol Server
 * Entry point for the broker service
 */

use pulsar_lite::broker::handle_connection;
use pulsar_lite::broker::{BrokerService, ConnectionLimiter};
use pulsar_lite::config::Config;
use pulsar_lite::protocol::codec::PulsarFrameCodec;
use pulsar_lite::protocol::ServerCommand;
use pulsar_lite::storage::Storage;
use futures::SinkExt;
use std::sync::Arc;
use tokio::net::TcpListener;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::sync::{Mutex, RwLock};
use tokio::time::Duration;
use tokio_util::codec::Framed;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration
    let config = Config::from_file_or_default("pulsar-lite.toml");

    // Initialize logger
    env_logger::Builder::from_default_env()
        .filter_level(config.log_level.parse().unwrap_or(log::LevelFilter::Info))
        .init();

    log::info!("Starting Pulsar Lite binary protocol server on {}", config.addr);
    log::info!("Database path: {:?}", config.db_path);
    log::info!("Default partitions: {}", config.default_partitions);
    log::info!(
        "Keep-alive interval: {}s, handshake timeout: {}s, liveness timeout: {}s",
        config.keep_alive_interval_secs,
        config.handshake_timeout_secs,
        config.connection_liveness_check_timeout_secs
    );
    log::info!(
        "Connection limits: max_connections={}, max_connections_per_ip={}",
        config.max_connections,
        config.max_connections_per_ip
    );

    // Initialize storage
    let storage = Arc::new(Mutex::new(Storage::new(&config.db_path)?));
    let restored_partition_metadata = {
        let guard = storage.lock().await;
        guard.get_partitioned_topic_metadata()
    };

    // Initialize broker service with configuration
    let mut broker = BrokerService::with_config(storage.clone(), config.default_partitions);
    broker.restore_partition_metadata(restored_partition_metadata);
    let broker_service = Arc::new(RwLock::new(broker));
    log::info!("BrokerService initialized");

    // Bind TCP listener
    let listener = TcpListener::bind(&config.addr).await?;
    log::info!("Server listening on {}", config.addr);
    let advertised_broker_url = advertised_broker_url(listener.local_addr()?);
    let keep_alive_interval = Duration::from_secs(config.keep_alive_interval_secs);
    let handshake_timeout = Duration::from_secs(config.handshake_timeout_secs);
    let connection_liveness_check_timeout =
        Duration::from_secs(config.connection_liveness_check_timeout_secs);
    let connection_limiter = ConnectionLimiter::new(config.max_connections, config.max_connections_per_ip);

    loop {
        let (socket, peer_addr) = listener.accept().await?;
        log::info!("New connection from {}", peer_addr);

        let permit = match connection_limiter.try_acquire(peer_addr.ip()) {
            Ok(permit) => permit,
            Err(error) => {
                log::warn!("Rejecting connection from {}: {}", peer_addr, error);
                let mut framed = Framed::new(socket, PulsarFrameCodec::new());
                let _ = framed
                    .send(ServerCommand::Error {
                        request_id: 0,
                        error,
                    })
                    .await;
                continue;
            }
        };

        let storage = Arc::clone(&storage);
        let broker_service = Arc::clone(&broker_service);
        let advertised_broker_url = advertised_broker_url.clone();
        tokio::spawn(async move {
            let _connection_permit = permit;
            if let Err(e) = handle_connection(
                socket,
                storage,
                broker_service,
                keep_alive_interval,
                handshake_timeout,
                connection_liveness_check_timeout,
                advertised_broker_url,
            ).await {
                log::error!("Connection error from {}: {}", peer_addr, e);
            }
            log::info!("Connection closed from {}", peer_addr);
        });
    }
}

fn advertised_broker_url(addr: SocketAddr) -> String {
    let host = match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => Ipv4Addr::LOCALHOST.to_string(),
        IpAddr::V6(ip) if ip.is_unspecified() => "::1".to_string(),
        ip => ip.to_string(),
    };
    format!("pulsar://{}:{}", host, addr.port())
}
