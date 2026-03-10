use anyhow::Result;
use log::{debug, info, warn};
use std::collections::{BTreeSet, HashMap};
use std::path::Path;

/// 消息 ID (ledger:entry:partition)
/// partition 默认为 -1（非分区 topic）
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct MessageId {
    pub ledger: u64,
    pub entry: u64,
    pub partition: i32,
}

/// 订阅游标状态 (Shared 模式)
/// 实现类似 Pulsar ManagedCursor 的 mark_delete + individual_deleted_messages 模型
#[derive(Debug, Clone, Default)]
pub struct SubscriptionCursor {
    /// 连续确认前沿 (mark_delete_position)
    /// None 表示尚未初始化，Some(entry_id) 表示已确认到该位置
    pub mark_delete: Option<u64>,
    /// 前沿之后已确认但非连续的洞位
    /// 这些消息已被 ack，但因为有更早的消息未 ack，所以 cursor 无法推进
    pub acked_holes: BTreeSet<u64>,
}

/// 存储引擎（基于内存，MVP 版本）
#[derive(Debug)]
pub struct Storage {
    // 内存中的 Topic 消息队列
    topics: HashMap<String, Vec<(MessageId, Vec<u8>)>>,
    // 内存中的游标缓存（订阅位置）- 用于 Exclusive 模式
    cursors: HashMap<String, u64>,
    // 订阅游标状态 (Shared 模式) - topic:subscription -> cursor
    subscription_cursors: HashMap<String, SubscriptionCursor>,
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
    fn is_shared_message_acknowledged(cursor: Option<&SubscriptionCursor>, entry: u64) -> bool {
        cursor
            .map(|cursor| {
                cursor.mark_delete.is_some_and(|mark_delete| entry <= mark_delete)
                    || cursor.acked_holes.contains(&entry)
            })
            .unwrap_or(false)
    }

    /// 创建存储
    pub fn new(_path: &Path) -> Result<Self> {
        info!("In-memory storage initialized (MVP version)");

        Ok(Self {
            topics: HashMap::new(),
            cursors: HashMap::new(),
            message_assignments: HashMap::new(),
            subscription_cursors: HashMap::new(),
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
        let shared_cursor = self.subscription_cursors.get(&cursor_key);

        // 获取 Topic 的消息
        let mut next_message = None;
        if let Some(messages) = self.topics.get(topic) {
            if messages.is_empty() {
                return Ok(None);
            }

            // 查找下一条未消费且未分配的消息
            for (message_id, data) in messages.iter() {
                let is_acknowledged = if shared_cursor.is_some() {
                    Self::is_shared_message_acknowledged(shared_cursor, message_id.entry)
                } else {
                    current_entry != u64::MAX && message_id.entry <= current_entry
                };

                if is_acknowledged {
                    continue;
                }

                if self.get_assignment_owner(topic, subscription, message_id).is_none() {
                    next_message = Some((message_id.clone(), data.clone()));
                    break;
                }
            }
        }

        if let Some((message_id, data)) = next_message {
            self.assign_message(topic, subscription, &message_id, consumer_id);
            return Ok(Some((message_id, data)));
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

    fn clear_assignment(&mut self, topic: &str, subscription: &str, message_id: &MessageId) {
        let assignment_key = format!("{}:{}:{}", topic, subscription, message_id.entry);
        self.message_assignments.remove(&assignment_key);
    }

    fn advance_shared_mark_delete(cursor: &mut SubscriptionCursor) {
        let mut next_expected = cursor.mark_delete.map_or(0, |mark_delete| mark_delete + 1);
        while cursor.acked_holes.remove(&next_expected) {
            cursor.mark_delete = Some(next_expected);
            next_expected += 1;
        }
    }

    // ==================== Shared 模式 Ack 前沿模型 ====================

    /// Shared 模式确认消息
    ///
    /// 使用 mark_delete + acked_holes 模型：
    /// - mark_delete: 连续确认前沿
    /// - acked_holes: 前沿之后已确认但非连续的洞位
    ///
    /// 当消息被 ack 时：
    /// 1. 如果消息在前沿之后，加入 acked_holes
    /// 2. 检查是否可以推进前沿（从 mark_delete + 1 开始的连续区间）
    /// 3. 清除分配状态
    pub fn ack_message_shared(&mut self, topic: &str, subscription: &str, message_id: MessageId) -> Result<()> {
        let cursor_key = format!("{}:{}", topic, subscription);
        let (mark_delete, holes_count) = {
            let cursor = self.subscription_cursors.entry(cursor_key).or_insert(SubscriptionCursor {
                mark_delete: None,
                acked_holes: BTreeSet::new(),
            });

            if Self::is_shared_message_acknowledged(Some(cursor), message_id.entry) {
                (cursor.mark_delete, cursor.acked_holes.len())
            } else {
                match cursor.mark_delete {
                    None => {
                        if message_id.entry == 0 {
                            cursor.mark_delete = Some(0);
                            Self::advance_shared_mark_delete(cursor);
                        } else {
                            cursor.acked_holes.insert(message_id.entry);
                        }
                    }
                    Some(mark_delete) => {
                        if message_id.entry == mark_delete + 1 {
                            cursor.mark_delete = Some(message_id.entry);
                            Self::advance_shared_mark_delete(cursor);
                        } else if message_id.entry > mark_delete + 1 {
                            // 形成 ack hole，但不推进前沿
                            cursor.acked_holes.insert(message_id.entry);
                        }
                    }
                }

                (cursor.mark_delete, cursor.acked_holes.len())
            }
        };

        self.clear_assignment(topic, subscription, &message_id);

        debug!(
            "Shared ack: topic={}, sub={}, entry={}, mark_delete={:?}, holes_count={}",
            topic, subscription, message_id.entry,
            mark_delete, holes_count
        );

        Ok(())
    }

    /// 显式建立消息 assignment
    pub fn assign_message(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
        consumer_id: u64,
    ) {
        let assignment_key = format!("{}:{}:{}", topic, subscription, message_id.entry);
        self.message_assignments.insert(assignment_key, consumer_id);
    }

    /// 释放消息分配状态（带 owner 校验）
    ///
    /// 当 Consumer 断开时，释放其持有的消息分配状态
    /// 如果 consumer_id 不匹配，不会释放（防止误删）
    ///
    /// 返回是否成功释放
    pub fn release_assignment(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
        owner_consumer_id: u64,
    ) -> bool {
        let assignment_key = format!("{}:{}:{}", topic, subscription, message_id.entry);

        if let Some(assigned_consumer) = self.message_assignments.get(&assignment_key) {
            if *assigned_consumer == owner_consumer_id {
                self.message_assignments.remove(&assignment_key);
                debug!(
                    "Released assignment: topic={}, sub={}, entry={}, consumer={}",
                    topic, subscription, message_id.entry, owner_consumer_id
                );
                true
            } else {
                warn!(
                    "Assignment owner mismatch: topic={}, sub={}, entry={}, expected={}, actual={}",
                    topic, subscription, message_id.entry, owner_consumer_id, assigned_consumer
                );
                false
            }
        } else {
            false
        }
    }

    /// 按完整 MessageId 获取消息（用于重投递）
    pub fn get_message_by_id(&self, topic: &str, message_id: &MessageId) -> Option<(MessageId, Vec<u8>)> {
        let messages = self.topics.get(topic)?;
        if let Some((stored_id, data)) = messages.get(message_id.entry as usize) {
            if stored_id == message_id {
                return Some((stored_id.clone(), data.clone()));
            }
        }

        messages
            .iter()
            .find(|(stored_id, _)| stored_id == message_id)
            .map(|(stored_id, data)| (stored_id.clone(), data.clone()))
    }

    /// 判断 Shared 模式下消息是否已经确认
    pub fn is_acknowledged_shared(&self, topic: &str, subscription: &str, message_id: &MessageId) -> bool {
        let cursor_key = format!("{}:{}", topic, subscription);
        Self::is_shared_message_acknowledged(self.subscription_cursors.get(&cursor_key), message_id.entry)
    }

    /// 获取订阅的 mark_delete 位置
    pub fn get_mark_delete_position(&self, topic: &str, subscription: &str) -> Option<u64> {
        let cursor_key = format!("{}:{}", topic, subscription);
        self.subscription_cursors.get(&cursor_key)?.mark_delete
    }

    /// 获取消息分配的 consumer_id
    pub fn get_assignment_owner(&self, topic: &str, subscription: &str, message_id: &MessageId) -> Option<u64> {
        let assignment_key = format!("{}:{}:{}", topic, subscription, message_id.entry);
        self.message_assignments.get(&assignment_key).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_storage() -> Storage {
        Storage::new(Path::new("/tmp/test-storage")).unwrap()
    }

    #[test]
    fn shared_ack_out_of_order_advances_only_when_contiguous() {
        let mut storage = create_storage();
        let topic = "persistent://public/default/test";
        let sub = "sub";

        storage.create_topic(topic).unwrap();
        storage.subscribe(topic, sub).unwrap();

        let msg0 = storage.append_message(topic, -1, b"0").unwrap();
        let msg1 = storage.append_message(topic, -1, b"1").unwrap();
        let msg2 = storage.append_message(topic, -1, b"2").unwrap();

        storage.ack_message_shared(topic, sub, msg2.clone()).unwrap();
        assert_eq!(storage.get_mark_delete_position(topic, sub), None);
        assert!(storage.is_acknowledged_shared(topic, sub, &msg2));

        storage.ack_message_shared(topic, sub, msg1.clone()).unwrap();
        assert_eq!(storage.get_mark_delete_position(topic, sub), None);
        assert!(storage.is_acknowledged_shared(topic, sub, &msg1));

        storage.ack_message_shared(topic, sub, msg0).unwrap();
        assert_eq!(storage.get_mark_delete_position(topic, sub), Some(2));
    }

    #[test]
    fn shared_first_ack_non_zero_does_not_jump_frontier() {
        let mut storage = create_storage();
        let topic = "persistent://public/default/test";
        let sub = "sub";

        storage.create_topic(topic).unwrap();
        storage.subscribe(topic, sub).unwrap();
        for i in 0..6u8 {
            storage.append_message(topic, -1, &[i]).unwrap();
        }

        let msg5 = MessageId { ledger: 0, entry: 5, partition: -1 };
        storage.ack_message_shared(topic, sub, msg5.clone()).unwrap();

        assert_eq!(storage.get_mark_delete_position(topic, sub), None);
        let next = storage.get_next_unassigned_message(topic, sub, 1).unwrap().unwrap();
        assert_eq!(next.0.entry, 0);
        assert!(storage.is_acknowledged_shared(topic, sub, &msg5));
    }
}
