/*
 * Server Command Types
 * Defines server-side commands for Pulsar protocol
 */

use prost::Message;

// Import generated protobuf types
use super::codec::proto::pulsar::{
    base_command, BaseCommand, CommandAckResponse, CommandConnected, CommandConsumerStatsResponse,
    CommandError, CommandLookupTopicResponse, CommandMessage, CommandPartitionedTopicMetadataResponse,
    CommandPong, CommandProducerSuccess, CommandSendReceipt, CommandSuccess, MessageIdData,
    MessageMetadata, command_lookup_topic_response, command_partitioned_topic_metadata_response,
};

/// Command to send to client
#[derive(Debug)]
pub enum ServerCommand {
    Connected {
        server_version: String,
    },
    LookupResponse {
        request_id: u64,
        broker_service_url: String,
    },
    PartitionMetadataResponse {
        request_id: u64,
        partitions: i32,
    },
    SendReceipt {
        producer_id: u64,
        sequence_id: u64,
        ledger_id: u64,
        entry_id: u64,
        partition: i32,
    },
    Success {
        request_id: u64,
    },
    Error {
        request_id: u64,
        error: String,
    },
    ProducerSuccess {
        request_id: u64,
        producer_name: String,
        producer_id: u64,
    },
    ConsumerStatsResponse {
        request_id: u64,
        consumer_name: String,
    },
    Message {
        consumer_id: u64,
        ledger_id: u64,
        entry_id: u64,
        partition: i32,
        payload: Vec<u8>,
    },
    AckResponse {
        consumer_id: u64,
        request_id: u64,
    },
    Pong,
}

impl ServerCommand {
    /// Serialize command to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let command = self.to_base_command();
        command.encode_to_vec()
    }

    /// Convert to protobuf BaseCommand
    pub fn to_base_command(&self) -> BaseCommand {
        use base_command::Type;

        match self {
            ServerCommand::Connected { server_version } => BaseCommand {
                r#type: Type::Connected as i32,
                connected: Some(CommandConnected {
                    server_version: server_version.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ServerCommand::LookupResponse {
                request_id,
                broker_service_url,
            } => BaseCommand {
                r#type: Type::LookupResponse as i32,
                lookup_topic_response: Some(CommandLookupTopicResponse {
                    request_id: *request_id,
                    broker_service_url: Some(broker_service_url.clone()),
                    response: Some(command_lookup_topic_response::LookupType::Connect as i32),
                    authoritative: Some(false),
                    proxy_through_service_url: Some(false),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ServerCommand::PartitionMetadataResponse {
                request_id,
                partitions,
            } => BaseCommand {
                r#type: Type::PartitionedMetadataResponse as i32,
                partition_metadata_response: Some(CommandPartitionedTopicMetadataResponse {
                    request_id: *request_id,
                    partitions: Some(*partitions as u32),
                    response: Some(
                        command_partitioned_topic_metadata_response::LookupType::Success as i32,
                    ),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ServerCommand::SendReceipt {
                producer_id,
                sequence_id,
                ledger_id,
                entry_id,
                partition,
            } => BaseCommand {
                r#type: Type::SendReceipt as i32,
                send_receipt: Some(CommandSendReceipt {
                    producer_id: *producer_id,
                    sequence_id: *sequence_id,
                    message_id: Some(MessageIdData {
                        ledger_id: *ledger_id,
                        entry_id: *entry_id,
                        partition: Some(*partition),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ServerCommand::Success { request_id } => BaseCommand {
                r#type: Type::Success as i32,
                success: Some(CommandSuccess {
                    request_id: *request_id,
                    ..Default::default()
                }),
                ..Default::default()
            },
            ServerCommand::Error { request_id, error } => BaseCommand {
                r#type: Type::Error as i32,
                error: Some(CommandError {
                    request_id: *request_id,
                    message: error.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ServerCommand::ProducerSuccess {
                request_id,
                producer_name,
                producer_id: _,
            } => BaseCommand {
                r#type: Type::ProducerSuccess as i32,
                producer_success: Some(CommandProducerSuccess {
                    request_id: *request_id,
                    producer_name: producer_name.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ServerCommand::ConsumerStatsResponse {
                request_id,
                consumer_name,
            } => BaseCommand {
                r#type: Type::ConsumerStatsResponse as i32,
                consumer_stats_response: Some(CommandConsumerStatsResponse {
                    request_id: *request_id,
                    consumer_name: Some(consumer_name.clone()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ServerCommand::Message {
                consumer_id,
                ledger_id,
                entry_id,
                partition,
                payload: _,
            } => BaseCommand {
                r#type: Type::Message as i32,
                message: Some(CommandMessage {
                    consumer_id: *consumer_id,
                    message_id: MessageIdData {
                        ledger_id: *ledger_id,
                        entry_id: *entry_id,
                        partition: Some(*partition),
                        ..Default::default()
                    },
                    ..Default::default()
                }),
                ..Default::default()
            },
            ServerCommand::AckResponse {
                consumer_id,
                request_id,
            } => BaseCommand {
                r#type: Type::AckResponse as i32,
                ack_response: Some(CommandAckResponse {
                    consumer_id: *consumer_id,
                    request_id: Some(*request_id),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ServerCommand::Pong => BaseCommand {
                r#type: Type::Pong as i32,
                pong: Some(CommandPong {
                    ..Default::default()
                }),
                ..Default::default()
            },
        }
    }

    /// Check if this command has payload (Message command)
    pub fn has_payload(&self) -> bool {
        matches!(self, ServerCommand::Message { .. })
    }

    /// Get payload reference if available
    pub fn get_payload(&self) -> Option<&[u8]> {
        match self {
            ServerCommand::Message { payload, .. } => Some(payload),
            _ => None,
        }
    }
}

/// Helper to create message metadata
pub fn create_message_metadata(sequence_id: u64) -> MessageMetadata {
    MessageMetadata {
        sequence_id,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_command_serialization() {
        let cmd = ServerCommand::ProducerSuccess {
            request_id: 1,
            producer_name: "test-producer".to_string(),
            producer_id: 100,
        };

        let bytes = cmd.to_bytes();
        assert!(!bytes.is_empty());

        // Verify we can deserialize it
        let decoded = super::super::codec::proto::pulsar::BaseCommand::decode(&bytes[..]).unwrap();
        assert_eq!(
            decoded.r#type,
            super::super::codec::proto::pulsar::base_command::Type::ProducerSuccess as i32
        );
    }

    #[test]
    fn test_has_payload() {
        let msg_cmd = ServerCommand::Message {
            consumer_id: 1,
            ledger_id: 2,
            entry_id: 3,
            partition: 0,
            payload: vec![1, 2, 3],
        };
        assert!(msg_cmd.has_payload());

        let success_cmd = ServerCommand::Success { request_id: 1 };
        assert!(!success_cmd.has_payload());
    }
}
