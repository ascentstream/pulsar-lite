/*
 * Pulsar Binary Protocol Implementation
 *
 * Wire format:
 * [TOTAL_SIZE (4B)] [CMD_SIZE (4B)] [CMD (Protobuf)] [MAGIC_NUMBER (2B, optional)] [CHECKSUM (4B, optional)] [METADATA_SIZE (4B)] [METADATA (Protobuf)] [PAYLOAD]
 *
 * Frame decoder: LengthFieldBasedFrameDecoder(maxMessageSize + 10KB, 0, 4, 0, 4)
 */

use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;
use std::io::{self, Cursor};
use tokio_util::codec::{Decoder, Encoder};

// Include the generated Pulsar API protobuf code
pub mod proto {
    pub mod pulsar {
        include!(concat!(env!("OUT_DIR"), "/pulsar.proto.rs"));
    }
}

// Import ServerCommand from command module
use super::command::ServerCommand;
use crate::broker::service::PendingMessage;

/// Magic number for checksum verification (0x0e01)
const MAGIC_NUMBER: u16 = 0x0e01;

/// Maximum message size (from Pulsar defaults)
const MAX_MESSAGE_SIZE: usize = 5 * 1024 * 1024; // 5MB

/// Frame decoder/encoder for Pulsar binary protocol
pub struct PulsarFrameCodec {
    max_frame_size: usize,
}

impl PulsarFrameCodec {
    pub fn new() -> Self {
        Self {
            max_frame_size: MAX_MESSAGE_SIZE + 10 * 1024, // maxMessageSize + 10KB
        }
    }
}

impl Default for PulsarFrameCodec {
    fn default() -> Self {
        Self::new()
    }
}

/// Decoded Pulsar frame
#[derive(Debug)]
pub struct PulsarFrame {
    pub command: Bytes,          // Serialized command protobuf
    pub metadata: Option<Bytes>, // Serialized metadata protobuf (optional)
    pub payload: Bytes,          // Message payload
    pub checksum: Option<u32>,   // Checksum (optional)
}

#[derive(Debug, Clone)]
pub struct EncodedMessageParts {
    pub header: Bytes,
    pub metadata: Bytes,
    pub payload: Bytes,
}

pub fn estimate_message_parts_size(
    consumer_id: u64,
    ledger_id: u64,
    entry_id: u64,
    partition: i32,
    metadata: &Bytes,
    payload: &Bytes,
    redelivery_count: u32,
) -> usize {
    use proto::pulsar::*;

    let command = BaseCommand {
        r#type: base_command::Type::Message as i32,
        message: Some(CommandMessage {
            consumer_id,
            message_id: MessageIdData {
                ledger_id,
                entry_id,
                partition: Some(partition),
                ..Default::default()
            },
            redelivery_count: Some(redelivery_count),
            ..Default::default()
        }),
        ..Default::default()
    };

    let cmd_len = command.encoded_len();
    let metadata_len = if metadata.is_empty() {
        MessageMetadata {
            sequence_id: entry_id,
            ..Default::default()
        }
        .encoded_len()
    } else {
        metadata.len()
    };

    // Total encoded bytes:
    // [TOTAL_SIZE(4)] [CMD_SIZE(4)] [CMD] [METADATA_SIZE(4)] [METADATA] [PAYLOAD]
    (4 + 4 + cmd_len + 4) + metadata_len + payload.len()
}

pub fn encode_message_parts(
    consumer_id: u64,
    ledger_id: u64,
    entry_id: u64,
    partition: i32,
    metadata: &Bytes,
    payload: &Bytes,
    redelivery_count: u32,
) -> Result<EncodedMessageParts, io::Error> {
    use proto::pulsar::*;

    let command = BaseCommand {
        r#type: base_command::Type::Message as i32,
        message: Some(CommandMessage {
            consumer_id,
            message_id: MessageIdData {
                ledger_id,
                entry_id,
                partition: Some(partition),
                ..Default::default()
            },
            redelivery_count: Some(redelivery_count),
            ..Default::default()
        }),
        ..Default::default()
    };

    let cmd_len = command.encoded_len();
    let metadata_bytes = if metadata.is_empty() {
        let synthesized = MessageMetadata {
            sequence_id: entry_id,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(synthesized.encoded_len());
        synthesized
            .encode(&mut buf)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        buf.freeze()
    } else {
        metadata.clone()
    };

    // Pulsar wire format for messages with payload:
    // [TOTAL_SIZE (4B)] [CMD_SIZE (4B)] [CMD] [METADATA_SIZE (4B)] [METADATA] [PAYLOAD]
    let total_size = 4 + cmd_len + 4 + metadata_bytes.len() + payload.len();

    let mut header = BytesMut::with_capacity(4 + 4 + cmd_len + 4);
    header.put_u32(total_size as u32);
    header.put_u32(cmd_len as u32);
    command
        .encode(&mut header)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    header.put_u32(metadata_bytes.len() as u32);

    Ok(EncodedMessageParts {
        header: header.freeze(),
        metadata: metadata_bytes,
        payload: payload.clone(),
    })
}

impl Decoder for PulsarFrameCodec {
    type Item = PulsarFrame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Need at least 4 bytes for total size
        if src.len() < 4 {
            return Ok(None);
        }

        // Read total size (big-endian)
        let total_size = Cursor::new(&src[..4]).get_u32() as usize;

        // Check frame size limit
        if total_size > self.max_frame_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Frame too large: {} bytes", total_size),
            ));
        }

        // Wait for complete frame
        if src.len() < 4 + total_size {
            src.reserve(4 + total_size - src.len());
            return Ok(None);
        }

        // Split off the frame
        let mut frame_data = src.split_to(4 + total_size);
        frame_data.advance(4); // Skip total size

        // Read command size
        if frame_data.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Missing command size",
            ));
        }
        let cmd_size = Cursor::new(&frame_data[..4]).get_u32() as usize;
        frame_data.advance(4);

        // Read command
        if frame_data.len() < cmd_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Incomplete command data",
            ));
        }
        let command = frame_data.split_to(cmd_size).freeze();

        // Check for checksum (optional)
        let (checksum, mut remaining) = if frame_data.len() >= 6 {
            let magic = Cursor::new(&frame_data[..2]).get_u16();
            if magic == MAGIC_NUMBER {
                // Has checksum
                let checksum = Cursor::new(&frame_data[2..6]).get_u32();
                frame_data.advance(6);
                (Some(checksum), frame_data)
            } else {
                (None, frame_data)
            }
        } else {
            (None, frame_data)
        };

        // Read metadata and payload (zero-copy via freeze())
        let (metadata, payload) = if remaining.len() >= 4 {
            let metadata_size = Cursor::new(&remaining[..4]).get_u32() as usize;
            remaining.advance(4);

            if remaining.len() >= metadata_size {
                let metadata = remaining.split_to(metadata_size).freeze();
                let payload = remaining.freeze();
                (Some(metadata), payload)
            } else {
                (None, remaining.freeze())
            }
        } else {
            (None, remaining.freeze())
        };

        Ok(Some(PulsarFrame {
            command,
            metadata,
            payload,
            checksum,
        }))
    }
}

impl Encoder<ServerCommand> for PulsarFrameCodec {
    type Error = io::Error;

    fn encode(&mut self, item: ServerCommand, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // 特殊处理 Message 命令（包含 payload）
        if let ServerCommand::Message {
            consumer_id,
            ledger_id,
            entry_id,
            partition,
            metadata,
            payload,
        } = &item
        {
            return self.encode_message(
                *consumer_id,
                *ledger_id,
                *entry_id,
                *partition,
                metadata,
                payload,
                0,
                dst,
            );
        }

        let command = item.to_base_command();
        let cmd_len = command.encoded_len();

        // Pulsar wire format for commands without payload:
        // [TOTAL_SIZE (4B)] [CMD_SIZE (4B)] [CMD]
        // where TOTAL_SIZE = CMD_SIZE (4) + CMD

        let total_size = 4 + cmd_len; // cmd_size field (4) + cmd

        // Write frame
        dst.reserve(total_size + 4); // +4 for total_size field itself
        dst.put_u32(total_size as u32); // TOTAL_SIZE
        dst.put_u32(cmd_len as u32); // CMD_SIZE
        command
            .encode(dst)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;

        Ok(())
    }
}

impl Encoder<(u64, PendingMessage)> for PulsarFrameCodec {
    type Error = io::Error;

    fn encode(
        &mut self,
        item: (u64, PendingMessage),
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let (consumer_id, msg) = item;
        self.encode_message(
            consumer_id,
            msg.message_id.ledger,
            msg.message_id.entry,
            msg.message_id.partition,
            &msg.metadata,
            &msg.payload,
            msg.redelivery_count,
            dst,
        )
    }
}

impl PulsarFrameCodec {
    /// Encode a Message command with payload
    fn encode_message(
        &self,
        consumer_id: u64,
        ledger_id: u64,
        entry_id: u64,
        partition: i32,
        metadata: &Bytes,
        payload: &Bytes,
        redelivery_count: u32,
        dst: &mut BytesMut,
    ) -> Result<(), io::Error> {
        let parts = encode_message_parts(
            consumer_id,
            ledger_id,
            entry_id,
            partition,
            metadata,
            payload,
            redelivery_count,
        )?;
        dst.reserve(parts.header.len() + parts.metadata.len() + parts.payload.len());
        dst.extend_from_slice(parts.header.as_ref());
        dst.extend_from_slice(parts.metadata.as_ref());
        dst.extend_from_slice(parts.payload.as_ref());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::proto::pulsar::{CompressionType, KeyValue, MessageMetadata};
    use super::*;
    use crate::protocol::command::ServerCommand;
    use bytes::Bytes;
    use prost::Message;

    #[test]
    fn test_frame_codec_simple() {
        let mut codec = PulsarFrameCodec::new();

        // Create a simple frame: total_size=12, cmd_size=4, cmd=[1,2,3,4], metadata_size=0
        let mut data = BytesMut::new();
        data.put_u32(12); // total size
        data.put_u32(4); // cmd size
        data.extend_from_slice(&[1, 2, 3, 4]); // cmd
        data.put_u32(0); // metadata size (0)

        let frame = codec.decode(&mut data).unwrap().unwrap();
        assert_eq!(frame.command, Bytes::from(vec![1u8, 2, 3, 4]));
        assert_eq!(frame.payload, Bytes::new());
    }

    #[test]
    fn test_frame_codec_with_payload() {
        let mut codec = PulsarFrameCodec::new();

        // Create frame with payload
        let mut data = BytesMut::new();
        data.put_u32(16); // total size
        data.put_u32(4); // cmd size
        data.extend_from_slice(&[1, 2, 3, 4]); // cmd
        data.put_u32(0); // metadata size (0)
        data.extend_from_slice(&[5, 6, 7, 8]); // payload

        let frame = codec.decode(&mut data).unwrap().unwrap();
        assert_eq!(frame.command, Bytes::from(vec![1, 2, 3, 4]));
        assert_eq!(frame.payload, Bytes::from(vec![5, 6, 7, 8]));
    }

    #[test]
    fn test_message_encode_preserves_metadata_and_compression() {
        let mut codec = PulsarFrameCodec::new();
        let metadata = MessageMetadata {
            producer_name: "producer-1".to_string(),
            sequence_id: 42,
            publish_time: 100,
            properties: vec![KeyValue {
                key: "env".to_string(),
                value: "test".to_string(),
            }],
            compression: Some(CompressionType::Lz4 as i32),
            uncompressed_size: Some(128),
            event_time: Some(200),
            ordering_key: Some(b"order-key".to_vec()),
            ..Default::default()
        }
        .encode_to_vec();

        let payload = b"compressed-payload".to_vec();
        let command = ServerCommand::Message {
            consumer_id: 7,
            ledger_id: 9,
            entry_id: 11,
            partition: -1,
            metadata: Bytes::from(metadata.clone()),
            payload: Bytes::from(payload.clone()),
        };

        let mut encoded = BytesMut::new();
        codec.encode(command, &mut encoded).unwrap();
        let frame = codec.decode(&mut encoded).unwrap().unwrap();
        let decoded_metadata = MessageMetadata::decode(&frame.metadata.unwrap()[..]).unwrap();

        assert_eq!(decoded_metadata.sequence_id, 42);
        assert_eq!(decoded_metadata.event_time, Some(200));
        assert_eq!(decoded_metadata.ordering_key, Some(b"order-key".to_vec()));
        assert_eq!(
            decoded_metadata.compression,
            Some(CompressionType::Lz4 as i32)
        );
        assert_eq!(decoded_metadata.uncompressed_size, Some(128));
        assert_eq!(decoded_metadata.properties.len(), 1);
        assert_eq!(decoded_metadata.properties[0].key, "env");
        assert_eq!(decoded_metadata.properties[0].value, "test");
        assert_eq!(frame.payload, Bytes::from(payload));
    }

    #[test]
    fn test_encode_message_parts_matches_codec_output() {
        let mut codec = PulsarFrameCodec::new();
        let command = ServerCommand::Message {
            consumer_id: 7,
            ledger_id: 11,
            entry_id: 22,
            partition: -1,
            metadata: Bytes::new(),
            payload: Bytes::from_static(b"payload"),
        };

        let mut encoded = BytesMut::new();
        codec.encode(command, &mut encoded).unwrap();

        let parts = encode_message_parts(
            7,
            11,
            22,
            -1,
            &Bytes::new(),
            &Bytes::from_static(b"payload"),
            0,
        )
        .unwrap();
        let mut rebuilt = BytesMut::new();
        rebuilt.extend_from_slice(parts.header.as_ref());
        rebuilt.extend_from_slice(parts.metadata.as_ref());
        rebuilt.extend_from_slice(parts.payload.as_ref());

        assert_eq!(encoded.freeze(), rebuilt.freeze());
    }
}
