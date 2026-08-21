/*
 * Message Dispatcher Module
 * Handles message distribution strategies for different subscription modes
 */

mod enums;
mod exclusive;
mod failover;
mod key_shared;
mod read_position;
pub mod redelivery_controller;
mod shared;
pub(crate) use shared::DEFAULT_MAX_UNACKED_MESSAGES_PER_CONSUMER;
mod single_active;
pub(crate) mod sticky_key;
mod traits;

pub use enums::DispatcherEnum;
pub use exclusive::ExclusiveDispatcher;
pub use failover::FailoverDispatcher;
pub use key_shared::KeySharedDispatcher;
pub use shared::SharedDispatcher;
pub use single_active::rewind_read_position;
pub use traits::Dispatcher;

/// Client-visible message count per entry (batch-aware); shared with the
/// write-queue commit accounting. See `pulsar_lite_proto::codec`.
pub use pulsar_lite_proto::codec::messages_in_batch;
