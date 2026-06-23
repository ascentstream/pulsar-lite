use crate::broker::service::SharedStorage;
use crate::storage::{CursorInitOptions, CursorOpenResult, MessageId, Storage};
use bytes::Bytes;

#[derive(Debug, Clone)]
pub(crate) struct PersistentTopicRuntime {
    storage: SharedStorage,
}

impl PersistentTopicRuntime {
    pub(crate) fn new(storage: SharedStorage) -> Self {
        Self { storage }
    }

    pub(crate) async fn open_subscription_cursor(
        &self,
        topic_name: &str,
        subscription_name: &str,
        cursor_options: CursorInitOptions,
    ) -> Result<CursorOpenResult, String> {
        let mut guard = self.storage.lock().await;

        if let Err(e) = guard.resources_mut().ensure_subscription(
            topic_name,
            subscription_name,
            Storage::METADATA_VERSION,
        ) {
            log::warn!(
                "Skipping metadata persistence for subscription '{}' on topic '{}': {}",
                subscription_name,
                topic_name,
                e
            );
        }

        guard
            .initialize_or_open_cursor(topic_name, subscription_name, cursor_options)
            .map_err(|e| format!("Failed to initialize cursor in storage: {}", e))
    }

    pub(crate) async fn publish_message(
        &self,
        topic_name: &str,
        partition: i32,
        metadata: Option<Bytes>,
        payload: Bytes,
    ) -> Result<MessageId, Box<dyn std::error::Error + Send + Sync>> {
        let mut guard = self.storage.lock().await;
        let metadata = metadata.unwrap_or_default();
        let message_id = guard.append_message_with_metadata(
            topic_name,
            partition,
            metadata.as_ref(),
            payload.as_ref(),
        )?;
        Ok(message_id)
    }

    pub(crate) async fn get_last_message_id(
        &self,
        topic_name: &str,
    ) -> Result<Option<MessageId>, String> {
        let guard = self.storage.lock().await;
        guard
            .get_last_position(topic_name)
            .map(|position| position.map(MessageId::from))
            .map_err(|e| e.to_string())
    }
}
