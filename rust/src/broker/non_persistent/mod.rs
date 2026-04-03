/*
 * Non-persistent runtime module
 *
 * This module is intentionally skeletal for now. It marks the start of the
 * runtime-only split after the protocol layer, while external topic/protocol
 * entry points still remain unchanged.
 */

pub mod dispatcher;
pub mod runtime;

pub use self::dispatcher::NonPersistentDispatcherEnum;
pub use self::runtime::{NonPersistentSubscriptionRuntime, NonPersistentTopicRuntime};
