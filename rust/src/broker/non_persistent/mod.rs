/*
 * Non-persistent dispatcher/runtime module
 *
 * The topic path now dispatches immediately on publish. The remaining runtime
 * surface is subscription-scoped and centered on the dispatcher family.
 */

pub mod dispatcher;
pub mod runtime;

pub use self::dispatcher::NonPersistentDispatcherEnum;
pub use self::runtime::NonPersistentSubscriptionRuntime;
