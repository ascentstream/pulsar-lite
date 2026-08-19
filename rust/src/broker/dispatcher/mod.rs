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

/// Number of client-visible messages carried by one entry's metadata batch.
/// Permit accounting is per client-visible message (Apache Pulsar semantics):
/// a batch entry of N messages consumes N permits. Entries without batch
/// metadata count as a single message.
pub fn messages_in_batch(metadata: &[u8]) -> u32 {
    use prost::Message;
    use pulsar_lite_proto::codec::proto::pulsar::MessageMetadata;

    MessageMetadata::decode(metadata)
        .ok()
        .and_then(|m| m.num_messages_in_batch)
        .filter(|n| *n > 0)
        .map(|n| n as u32)
        .unwrap_or(1)
}
