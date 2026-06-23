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
    in_flight_hashes: HashMap<MessageId, i32>,
    blocked_hashes: HashMap<i32, usize>,
    block_hashes: bool,
}

impl RedeliveryController {
    pub fn new(block_hashes: bool) -> Self {
        Self {
            entries: BTreeMap::new(),
            in_flight_hashes: HashMap::new(),
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

    pub fn take_for_delivery(&mut self, message_id: &MessageId) -> Option<RedeliveryEntry> {
        self.take_for_delivery_with_hash(message_id, None)
    }

    pub fn take_for_delivery_with_hash(
        &mut self,
        message_id: &MessageId,
        sticky_key_hash: Option<i32>,
    ) -> Option<RedeliveryEntry> {
        let entry = self.entries.remove(message_id)?;
        let should_increment_supplied_hash =
            entry.sticky_key_hash.is_none() && sticky_key_hash.is_some();
        let entry = if should_increment_supplied_hash {
            RedeliveryEntry {
                sticky_key_hash,
                ..entry
            }
        } else {
            entry
        };
        if self.block_hashes {
            if let Some(hash) = entry.sticky_key_hash {
                if should_increment_supplied_hash {
                    self.increment_hash(Some(hash));
                }
                self.in_flight_hashes.insert(entry.message_id.clone(), hash);
            }
        } else {
            self.decrement_hash(entry.sticky_key_hash);
        }
        Some(entry)
    }

    pub fn restore(&mut self, entry: RedeliveryEntry) {
        self.release_in_flight_hash(&entry.message_id);
        self.add(entry);
    }

    pub fn remove(&mut self, message_id: &MessageId) -> Option<RedeliveryEntry> {
        self.release_in_flight_hash(message_id);
        let removed = self.entries.remove(message_id)?;
        self.decrement_hash(removed.sticky_key_hash);
        Some(removed)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn queued_message_ids(&self) -> Vec<MessageId> {
        self.entries.keys().cloned().collect()
    }

    pub fn queued_entries(&self) -> Vec<RedeliveryEntry> {
        self.entries.values().cloned().collect()
    }

    pub fn is_hash_blocked(&self, sticky_key_hash: i32) -> bool {
        self.blocked_hashes.contains_key(&sticky_key_hash)
    }

    pub fn has_in_flight_hash(&self, sticky_key_hash: i32) -> bool {
        self.in_flight_hashes
            .values()
            .any(|hash| *hash == sticky_key_hash)
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

    fn release_in_flight_hash(&mut self, message_id: &MessageId) {
        let Some(hash) = self.in_flight_hashes.remove(message_id) else {
            return;
        };
        self.decrement_hash(Some(hash));
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

    #[test]
    fn take_for_delivery_keeps_hash_blocked_until_ack_remove() {
        let mut controller = RedeliveryController::new(true);

        controller.add(RedeliveryEntry {
            message_id: msg(5),
            redelivery_count: 1,
            sticky_key_hash: Some(33),
        });

        let taken = controller.take_for_delivery(&msg(5)).unwrap();
        assert_eq!(taken.message_id, msg(5));
        assert_eq!(controller.len(), 0);
        assert!(controller.is_hash_blocked(33));
        assert!(controller.has_in_flight_hash(33));

        assert!(controller.remove(&msg(5)).is_none());
        assert!(!controller.is_hash_blocked(33));
        assert!(!controller.has_in_flight_hash(33));
    }

    #[test]
    fn restore_requeues_taken_entry_without_double_blocking_hash() {
        let mut controller = RedeliveryController::new(true);

        controller.add(RedeliveryEntry {
            message_id: msg(8),
            redelivery_count: 1,
            sticky_key_hash: Some(44),
        });

        let taken = controller.take_for_delivery(&msg(8)).unwrap();
        controller.restore(RedeliveryEntry {
            redelivery_count: 2,
            ..taken
        });

        assert!(controller.is_hash_blocked(44));
        assert_eq!(controller.pop_next().unwrap().redelivery_count, 2);
        assert!(!controller.is_hash_blocked(44));
    }

    #[test]
    fn take_for_delivery_with_supplied_hash_blocks_unknown_entry_hash() {
        let mut controller = RedeliveryController::new(true);

        controller.add(RedeliveryEntry {
            message_id: msg(9),
            redelivery_count: 1,
            sticky_key_hash: None,
        });
        assert!(!controller.is_hash_blocked(55));

        let taken = controller
            .take_for_delivery_with_hash(&msg(9), Some(55))
            .unwrap();

        assert_eq!(taken.sticky_key_hash, Some(55));
        assert!(controller.is_hash_blocked(55));
        controller.remove(&msg(9));
        assert!(!controller.is_hash_blocked(55));
    }
}
