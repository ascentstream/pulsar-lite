/*
 * Message Dispatcher Module
 * Handles message distribution strategies for different subscription modes
 */

mod enums;
mod exclusive;
mod failover;
mod key_shared;
mod read_position;
mod shared;
mod single_active;
mod traits;

pub use enums::DispatcherEnum;
pub use exclusive::ExclusiveDispatcher;
pub use failover::FailoverDispatcher;
pub use key_shared::KeySharedDispatcher;
pub use shared::SharedDispatcher;
pub use single_active::rewind_read_position;
pub use traits::Dispatcher;
