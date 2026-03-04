/*
 * Pulsar Lite Binary Protocol Server
 * Entry point for the broker service
 */

use pulsar_lite::broker::handle_connection;
use pulsar_lite::broker::BrokerService;
use pulsar_lite::config::Config;
use pulsar_lite::storage::Storage;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, RwLock};

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

    // Initialize storage
    let storage = Arc::new(Mutex::new(Storage::new(&config.db_path)?));

    // Initialize broker service with configuration
    let broker_service = Arc::new(RwLock::new(
        BrokerService::with_config(storage.clone(), config.default_partitions)
    ));
    log::info!("BrokerService initialized");

    // Bind TCP listener
    let listener = TcpListener::bind(&config.addr).await?;
    log::info!("Server listening on {}", config.addr);

    loop {
        let (socket, peer_addr) = listener.accept().await?;
        log::info!("New connection from {}", peer_addr);

        let storage = Arc::clone(&storage);
        let broker_service = Arc::clone(&broker_service);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, storage, broker_service).await {
                log::error!("Connection error from {}: {}", peer_addr, e);
            }
            log::info!("Connection closed from {}", peer_addr);
        });
    }
}
