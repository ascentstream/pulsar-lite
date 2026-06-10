/*
 * Message Dispatcher Module
 * Handles message distribution strategies for different subscription modes
 */

mod enums;
mod exclusive;
mod failover;
mod read_position;
mod shared;
mod traits;

pub use enums::DispatcherEnum;
pub use exclusive::ExclusiveDispatcher;
pub use failover::FailoverDispatcher;
pub use shared::SharedDispatcher;
pub use traits::Dispatcher;
