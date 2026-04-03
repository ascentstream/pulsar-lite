/*
 * Non-persistent runtime module
 *
 * This first-step module intentionally exposes only the runtime foundation.
 * Dispatcher implementations and protocol wiring will land in follow-up PRs.
 */

pub mod runtime;

pub use self::runtime::{NonPersistentSubscriptionRuntime, NonPersistentTopicRuntime};
