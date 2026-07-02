use crate::position::{ManagedLedgerPosition, MessageId};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum InitialPosition {
    #[default]
    Latest,
    Earliest,
}

#[derive(Debug, Clone, Default)]
pub struct CursorInitOptions {
    pub initial_position: InitialPosition,
    pub start_message_id: Option<MessageId>,
}

#[derive(Debug, Clone)]
pub struct CursorOpenResult {
    pub created: bool,
    pub first_unacked: Option<ManagedLedgerPosition>,
}
