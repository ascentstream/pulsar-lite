use crate::storage::MessageId;
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedeliveryEntry {
    pub message_id: MessageId,
    pub redelivery_count: u32,
    pub sticky_key_hash: Option<i32>,
}

#[derive(Debug, Default)]
pub struct RedeliveryController {
    entries: BTreeMap<MessageId, RedeliveryEntry>,
    blocked_hashes: HashMap<i32, usize>,
    block_hashes: bool,
}

impl RedeliveryController {
    pub fn new(block_hashes: bool) -> Self {
        Self {
            entries: BTreeMap::new(),
            blocked_hashes: HashMap::new(),
            block_hashes,
        }
    }

    pub fn add(&mut self, entry: RedeliveryEntry) {
        let message_id = entry.message_id.clone();
        if let Some(existing) = self.entries.get_mut(&message_id) {
            existing.redelivery_count = existing.redelivery_count.max(entry.redelivery_count);
            if existing.sticky_key_hash.is_none() {
                existing.sticky_key_hash = entry.sticky_key_hash;
                self.increment_hash(entry.sticky_key_hash);
            }
            return;
        }

        self.increment_hash(entry.sticky_key_hash);
        self.entries.insert(message_id, entry);
    }

    pub fn pop_next(&mut self) -> Option<RedeliveryEntry> {
        let (_message_id, entry) = self.entries.pop_first()?;
        self.decrement_hash(entry.sticky_key_hash);
        Some(entry)
    }

    pub fn restore(&mut self, entry: RedeliveryEntry) {
        self.add(entry);
    }

    pub fn remove(&mut self, message_id: &MessageId) -> Option<RedeliveryEntry> {
        let removed = self.entries.remove(message_id)?;
        self.decrement_hash(removed.sticky_key_hash);
        Some(removed)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn queued_message_ids(&self) -> Vec<MessageId> {
        self.entries.keys().cloned().collect()
    }

    pub fn is_hash_blocked(&self, sticky_key_hash: i32) -> bool {
        self.blocked_hashes.contains_key(&sticky_key_hash)
    }

    fn increment_hash(&mut self, sticky_key_hash: Option<i32>) {
        if !self.block_hashes {
            return;
        }
        if let Some(hash) = sticky_key_hash {
            *self.blocked_hashes.entry(hash).or_insert(0) += 1;
        }
    }

    fn decrement_hash(&mut self, sticky_key_hash: Option<i32>) {
        if !self.block_hashes {
            return;
        }
        let Some(hash) = sticky_key_hash else {
            return;
        };
        let Some(count) = self.blocked_hashes.get_mut(&hash) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            self.blocked_hashes.remove(&hash);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(entry: u64) -> MessageId {
        MessageId {
            ledger: 0,
            entry,
            partition: -1,
        }
    }

    #[test]
    fn redelivery_controller_pops_entries_in_message_order() {
        let mut controller = RedeliveryController::new(false);

        controller.add(RedeliveryEntry {
            message_id: msg(2),
            redelivery_count: 3,
            sticky_key_hash: None,
        });
        controller.add(RedeliveryEntry {
            message_id: msg(1),
            redelivery_count: 1,
            sticky_key_hash: None,
        });

        assert_eq!(controller.len(), 2);
        assert_eq!(controller.pop_next().unwrap().message_id, msg(1));
        assert_eq!(controller.pop_next().unwrap().message_id, msg(2));
        assert!(controller.pop_next().is_none());
    }

    #[test]
    fn duplicate_add_updates_redelivery_count_without_double_blocking_hash() {
        let mut controller = RedeliveryController::new(true);

        controller.add(RedeliveryEntry {
            message_id: msg(7),
            redelivery_count: 1,
            sticky_key_hash: Some(42),
        });
        controller.add(RedeliveryEntry {
            message_id: msg(7),
            redelivery_count: 5,
            sticky_key_hash: Some(42),
        });

        assert_eq!(controller.len(), 1);
        assert!(controller.is_hash_blocked(42));

        let popped = controller.pop_next().unwrap();
        assert_eq!(popped.redelivery_count, 5);
        assert_eq!(popped.sticky_key_hash, Some(42));
        assert!(!controller.is_hash_blocked(42));
    }

    #[test]
    fn restore_reblocks_hash_until_last_entry_is_removed() {
        let mut controller = RedeliveryController::new(true);

        controller.add(RedeliveryEntry {
            message_id: msg(1),
            redelivery_count: 1,
            sticky_key_hash: Some(9),
        });
        controller.add(RedeliveryEntry {
            message_id: msg(2),
            redelivery_count: 1,
            sticky_key_hash: Some(9),
        });

        assert!(controller.is_hash_blocked(9));
        let first = controller.pop_next().unwrap();
        assert!(controller.is_hash_blocked(9));

        controller.restore(first);
        assert!(controller.is_hash_blocked(9));

        controller.remove(&msg(1));
        assert!(controller.is_hash_blocked(9));
        controller.remove(&msg(2));
        assert!(!controller.is_hash_blocked(9));
    }

    #[test]
    fn allow_out_of_order_delivery_disables_hash_blocking() {
        let mut controller = RedeliveryController::new(false);

        controller.add(RedeliveryEntry {
            message_id: msg(3),
            redelivery_count: 1,
            sticky_key_hash: Some(77),
        });

        assert!(!controller.is_hash_blocked(77));
        assert_eq!(controller.queued_message_ids(), vec![msg(3)]);
    }
}
