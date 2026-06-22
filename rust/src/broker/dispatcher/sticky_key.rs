use crate::protocol::codec::proto::pulsar::MessageMetadata;
use prost::Message;

const RANGE_SIZE: u32 = 2 << 15;

pub(crate) fn resolve_sticky_key(metadata: &[u8]) -> Vec<u8> {
    let Ok(metadata) = MessageMetadata::decode(metadata) else {
        return Vec::new();
    };

    if let Some(ordering_key) = metadata.ordering_key {
        return ordering_key;
    }
    if let Some(partition_key) = metadata.partition_key {
        return partition_key.into_bytes();
    }
    if !metadata.producer_name.is_empty() {
        return format!("{}-{}", metadata.producer_name, metadata.sequence_id).into_bytes();
    }

    Vec::new()
}

pub(crate) fn sticky_key_hash_from_metadata(metadata: &[u8]) -> i32 {
    sticky_key_hash(&resolve_sticky_key(metadata))
}

pub(crate) fn sticky_key_hash(sticky_key: &[u8]) -> i32 {
    (murmur3_32(sticky_key, 0) % RANGE_SIZE) as i32
}

fn murmur3_32(bytes: &[u8], seed: u32) -> u32 {
    const C1: u32 = 0xcc9e2d51;
    const C2: u32 = 0x1b873593;

    let mut hash = seed;
    let mut chunks = bytes.chunks_exact(4);

    for chunk in &mut chunks {
        let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(C1);
        k = k.rotate_left(15);
        k = k.wrapping_mul(C2);

        hash ^= k;
        hash = hash.rotate_left(13);
        hash = hash.wrapping_mul(5).wrapping_add(0xe6546b64);
    }

    let tail = chunks.remainder();
    let mut k1 = 0u32;
    match tail.len() {
        3 => {
            k1 ^= (tail[2] as u32) << 16;
            k1 ^= (tail[1] as u32) << 8;
            k1 ^= tail[0] as u32;
        }
        2 => {
            k1 ^= (tail[1] as u32) << 8;
            k1 ^= tail[0] as u32;
        }
        1 => {
            k1 ^= tail[0] as u32;
        }
        _ => {}
    }
    if !tail.is_empty() {
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(C2);
        hash ^= k1;
    }

    hash ^= bytes.len() as u32;
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85ebca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2ae35);
    hash ^= hash >> 16;
    hash
}
