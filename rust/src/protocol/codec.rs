/*
 * Pulsar Binary Protocol Implementation
 *
 * Wire format:
 * [TOTAL_SIZE (4B)] [CMD_SIZE (4B)] [CMD (Protobuf)] [MAGIC_NUMBER (2B, optional)] [CHECKSUM (4B, optional)] [METADATA_SIZE (4B)] [METADATA (Protobuf)] [PAYLOAD]
 *
 * Frame decoder: LengthFieldBasedFrameDecoder(maxMessageSize + 10KB, 0, 4, 0, 4)
 */

use bytes::{Buf, BufMut, BytesMut};
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
use super::command::{ServerCommand, create_message_metadata};

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
    pub command: Vec<u8>,           // Serialized command protobuf
    pub metadata: Option<Vec<u8>>,  // Serialized metadata protobuf (optional)
    pub payload: Vec<u8>,           // Message payload
    pub checksum: Option<u32>,      // Checksum (optional)
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
        let command = frame_data.split_to(cmd_size).to_vec();

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

        // Read metadata and payload
        let (metadata, payload) = if remaining.len() >= 4 {
            let metadata_size = Cursor::new(&remaining[..4]).get_u32() as usize;
            remaining.advance(4);

            if remaining.len() >= metadata_size {
                let metadata = remaining.split_to(metadata_size).to_vec();
                let payload = remaining.to_vec();
                (Some(metadata), payload)
            } else {
                (None, remaining.to_vec())
            }
        } else {
            (None, remaining.to_vec())
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
        if let ServerCommand::Message { consumer_id, ledger_id, entry_id, partition, payload } = &item {
            return self.encode_message(*consumer_id, *ledger_id, *entry_id, *partition, payload, dst);
        }

        let cmd_bytes = item.to_bytes();

        // Pulsar wire format for commands without payload:
        // [TOTAL_SIZE (4B)] [CMD_SIZE (4B)] [CMD]
        // where TOTAL_SIZE = CMD_SIZE (4) + CMD

        let total_size = 4 + cmd_bytes.len(); // cmd_size field (4) + cmd

        // Write frame
        dst.reserve(total_size + 4); // +4 for total_size field itself
        dst.put_u32(total_size as u32);  // TOTAL_SIZE
        dst.put_u32(cmd_bytes.len() as u32);  // CMD_SIZE
        dst.extend_from_slice(&cmd_bytes);  // CMD

        Ok(())
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
        payload: &[u8],
        dst: &mut BytesMut,
    ) -> Result<(), io::Error> {
        use proto::pulsar::*;

        // 创建 Message 命令
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
                ..Default::default()
            }),
            ..Default::default()
        };

        let cmd_bytes = command.encode_to_vec();

        // 使用 helper 创建 MessageMetadata
        let metadata = create_message_metadata(entry_id);
        let metadata_bytes = metadata.encode_to_vec();

        // Pulsar wire format for messages with payload:
        // [TOTAL_SIZE (4B)] [CMD_SIZE (4B)] [CMD] [METADATA_SIZE (4B)] [METADATA] [PAYLOAD]
        let total_size = 4 + cmd_bytes.len() + 4 + metadata_bytes.len() + payload.len();

        dst.reserve(total_size + 4);
        dst.put_u32(total_size as u32);              // TOTAL_SIZE
        dst.put_u32(cmd_bytes.len() as u32);         // CMD_SIZE
        dst.extend_from_slice(&cmd_bytes);           // CMD
        dst.put_u32(metadata_bytes.len() as u32);    // METADATA_SIZE
        dst.extend_from_slice(&metadata_bytes);      // METADATA
        dst.extend_from_slice(payload);              // PAYLOAD

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_codec_simple() {
        let mut codec = PulsarFrameCodec::new();

        // Create a simple frame: total_size=12, cmd_size=4, cmd=[1,2,3,4], metadata_size=0
        let mut data = BytesMut::new();
        data.put_u32(12);  // total size
        data.put_u32(4);   // cmd size
        data.extend_from_slice(&[1, 2, 3, 4]);  // cmd
        data.put_u32(0);   // metadata size (0)

        let frame = codec.decode(&mut data).unwrap().unwrap();
        assert_eq!(frame.command, vec![1u8, 2, 3, 4]);
        assert_eq!(frame.payload, Vec::<u8>::new());
    }

    #[test]
    fn test_frame_codec_with_payload() {
        let mut codec = PulsarFrameCodec::new();

        // Create frame with payload
        let mut data = BytesMut::new();
        data.put_u32(16);  // total size
        data.put_u32(4);   // cmd size
        data.extend_from_slice(&[1, 2, 3, 4]);  // cmd
        data.put_u32(0);   // metadata size (0)
        data.extend_from_slice(&[5, 6, 7, 8]);  // payload

        let frame = codec.decode(&mut data).unwrap().unwrap();
        assert_eq!(frame.command, vec![1, 2, 3, 4]);
        assert_eq!(frame.payload, vec![5, 6, 7, 8]);
    }
}
