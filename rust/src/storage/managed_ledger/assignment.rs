use super::MessageId;
use std::collections::HashMap;

/// Shared-subscription assignment state kept alongside the managed-ledger
/// runtime state in the current in-memory implementation.
#[derive(Debug, Clone, Default)]
pub struct AssignmentStore {
    assignments: HashMap<String, u64>,
}

impl AssignmentStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn assign_message(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
        consumer_id: u64,
    ) {
        self.assignments
            .insert(Self::assignment_key(topic, subscription, message_id), consumer_id);
    }

    pub fn clear_assignment(&mut self, topic: &str, subscription: &str, message_id: &MessageId) {
        self.assignments
            .remove(&Self::assignment_key(topic, subscription, message_id));
    }

    pub fn release_assignment(
        &mut self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
        owner_consumer_id: u64,
    ) -> bool {
        let key = Self::assignment_key(topic, subscription, message_id);
        match self.assignments.get(&key) {
            Some(assigned_consumer) if *assigned_consumer == owner_consumer_id => {
                self.assignments.remove(&key);
                true
            }
            _ => false,
        }
    }

    pub fn get_assignment_owner(
        &self,
        topic: &str,
        subscription: &str,
        message_id: &MessageId,
    ) -> Option<u64> {
        self.assignments
            .get(&Self::assignment_key(topic, subscription, message_id))
            .copied()
    }

    fn assignment_key(topic: &str, subscription: &str, message_id: &MessageId) -> String {
        format!("{}:{}:{}", topic, subscription, message_id.entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assignment_owner_and_release_follow_owner_guard() {
        let mut assignments = AssignmentStore::new();
        let msg = MessageId {
            ledger: 1,
            entry: 7,
            partition: -1,
        };

        assignments.assign_message("t", "s", &msg, 42);
        assert_eq!(assignments.get_assignment_owner("t", "s", &msg), Some(42));
        assert!(!assignments.release_assignment("t", "s", &msg, 7));
        assert_eq!(assignments.get_assignment_owner("t", "s", &msg), Some(42));
        assert!(assignments.release_assignment("t", "s", &msg, 42));
        assert_eq!(assignments.get_assignment_owner("t", "s", &msg), None);
    }
}
