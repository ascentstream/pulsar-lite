/*
 * Broker Service Module
 * Core service that handles client connections and manages producers/consumers
 * Inspired by Apache Pulsar's service structure
 */

use std::sync::Arc;
use tokio::sync::Mutex;
use crate::storage::Storage;

mod server_cnx;
mod producer;
pub mod consumer;  // Make public so PendingMessage can be used
pub mod topic;

// Shared storage type alias
pub type SharedStorage = Arc<Mutex<Storage>>;

// Connection handler
pub use server_cnx::{ServerCnx, handle_connection, State as ConnectionState};

// Consumer types
pub use consumer::{Consumer, ConsumerStats, PendingMessage};
// Producer entity (new design, inspired by Apache Pulsar)
pub use producer::Producer;
