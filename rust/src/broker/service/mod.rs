/*
 * Broker Service Module
 * Core service that handles client connections and manages producers/consumers
 * Inspired by Apache Pulsar's service structure
 */

use crate::storage::Storage;
use std::sync::Arc;
use tokio::sync::Mutex;

pub mod consumer; // Make public so PendingMessage can be used
mod connection_write_state;
mod pending_acks;
mod producer;
mod server_cnx;
pub mod topic;

// Shared storage type alias
pub type SharedStorage = Arc<Mutex<Storage>>;

// Connection handler
pub use server_cnx::{handle_connection, ServerCnx, State as ConnectionState};

// Consumer types
pub use consumer::{Consumer, ConsumerStats, DispatchReservation, PendingMessage};
pub use connection_write_state::ConnectionWriteState;
// Producer entity (new design, inspired by Apache Pulsar)
pub use producer::Producer;

// Pending Acknowledgments Map
pub use pending_acks::{PendingAck, PendingAcksMap};
