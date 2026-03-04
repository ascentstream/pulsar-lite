use anyhow::Result;
use log::{debug, info};
use std::collections::HashMap;
use std::path::Path;

/// 消息 ID (ledger:entry:partition)
/// partition 默认为 -1（非分区 topic）
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MessageId {
    pub ledger: u64,
    pub entry: u64,
    pub partition: i32,
}

/// 存储引擎（基于内存，MVP 版本）
#[derive(Debug)]
pub struct Storage {
    // 内存中的 Topic 消息队列
    topics: HashMap<String, Vec<(MessageId, Vec<u8>)>>,
    // 内存中的游标缓存（订阅位置）
    cursors: HashMap<String, u64>,
    // 消息分配状态（topic:subscription:entry_id -> consumer_id）
    // 用于 Shared 模式，跟踪哪些消息已经分配给哪个消费者
    message_assignments: HashMap<String, u64>,
    // 每个 Topic 的 ledger ID（模拟 BookKeeper 的 ledger ID）
    // 每个 Topic 有独立的 ledger ID，创建 Topic 时分配
    topic_ledger_ids: HashMap<String, u64>,
    // 全局 ledger ID 计数器，用于分配新的 ledger ID
    next_ledger_id: u64,
}

impl Storage {
    /// 创建存储
    pub fn new(_path: &Path) -> Result<Self> {
        info!("In-memory storage initialized (MVP version)");

        Ok(Self {
            topics: HashMap::new(),
            cursors: HashMap::new(),
            message_assignments: HashMap::new(),
            topic_ledger_ids: HashMap::new(),
            next_ledger_id: 0,
        })
    }

    /// 创建 Topic
    pub fn create_topic(&mut self, name: &str) -> Result<()> {
        if !self.topics.contains_key(name) {
            self.topics.insert(name.to_string(), Vec::new());
            // 为新 Topic 分配唯一的 ledger ID
            let ledger_id = self.next_ledger_id;
            self.next_ledger_id += 1;
            self.topic_ledger_ids.insert(name.to_string(), ledger_id);
            info!("Topic created: {} (ledger_id={})", name, ledger_id);
        }
        Ok(())
    }

    /// 追加消息（严格按照 Pulsar 协议）
    pub fn append_message(&mut self, topic: &str, partition: i32, data: &[u8]) -> Result<MessageId> {
        // 确保 Topic 存在（如果不存在则创建）
        if !self.topics.contains_key(topic) {
            self.topics.insert(topic.to_string(), Vec::new());
            // 为新 Topic 分配唯一的 ledger ID
            let ledger_id = self.next_ledger_id;
            self.next_ledger_id += 1;
            self.topic_ledger_ids.insert(topic.to_string(), ledger_id);
        }

        let messages = self.topics.get_mut(topic).unwrap();
        // 获取该 Topic 的 ledger ID
        let ledger = *self.topic_ledger_ids.get(topic).unwrap_or(&0);

        // 按照 Pulsar 协议生成消息 ID：
        // - ledger_id: 每个 Topic 独立的 ledger ID
        // - entry_id: 在同一 ledger 内严格单调递增（从 0 开始）
        // - partition: 由调用方传入（-1 表示非分区 topic，0+ 表示分区 ID）
        let entry = messages.len() as u64;  // 严格递增：0, 1, 2, 3...

        let message_id = MessageId { ledger, entry, partition };
        messages.push((message_id.clone(), data.to_vec()));

        debug!("Message appended to {}: ledger={}, entry={}, partition={}", topic, ledger, entry, partition);

        Ok(message_id)
    }

    /// 订阅 Topic
    pub fn subscribe(&mut self, topic: &str, subscription: &str) -> Result<()> {
        let key = format!("{}:{}", topic, subscription);
        // 游标初始化为 -1（或使用 None），表示从头开始消费
        // 这里使用 u64::MAX 作为特殊值表示"尚未开始"
        if !self.cursors.contains_key(&key) {
            self.cursors.insert(key, u64::MAX);  // 使用 MAX 表示游标尚未定位

            info!("Subscribed to topic {} with subscription {}", topic, subscription);
        } else {
            info!("Subscription {} already exists for topic {}", subscription, topic);
        }

        Ok(())
    }

    /// 获取下一条未分配的消息（用于 Shared 模式）
    /// 返回消息和消息分配键
    pub fn get_next_unassigned_message(
        &mut self,
        topic: &str,
        subscription: &str,
        consumer_id: u64,
    ) -> Result<Option<(MessageId, Vec<u8>)>> {
        let cursor_key = format!("{}:{}", topic, subscription);

        // 获取当前游标（u64::MAX 表示尚未开始）
        let current_entry = self.cursors.get(&cursor_key).copied().unwrap_or(u64::MAX);

        // 获取 Topic 的消息
        if let Some(messages) = self.topics.get(topic) {
            if messages.is_empty() {
                return Ok(None);
            }

            // 查找下一条未消费且未分配的消息
            for (message_id, data) in messages.iter() {
                // 跳过已经确认的消息
                if current_entry != u64::MAX && message_id.entry <= current_entry {
                    continue;
                }

                // 检查消息是否已经分配
                let assignment_key = format!("{}:{}:{}", topic, subscription, message_id.entry);

                if !self.message_assignments.contains_key(&assignment_key) {
                    // 找到未分配的消息，标记为已分配
                    self.message_assignments.insert(assignment_key, consumer_id);

                    return Ok(Some((message_id.clone(), data.clone())));
                }
            }
        }

        Ok(None)
    }

    /// 确认消息
    pub fn ack_message(&mut self, topic: &str, subscription: &str, message_id: MessageId) -> Result<()> {
        // 更新游标到当前消息（Shared 模式下）
        let cursor_key = format!("{}:{}", topic, subscription);
        self.cursors.insert(cursor_key, message_id.entry);

        // 清除消息分配状态
        let assignment_key = format!("{}:{}:{}", topic, subscription, message_id.entry);
        self.message_assignments.remove(&assignment_key);

        info!("Message acknowledged for topic {} subscription {}: ledger={}, entry={}",
            topic, subscription, message_id.ledger, message_id.entry);
        Ok(())
    }
}
