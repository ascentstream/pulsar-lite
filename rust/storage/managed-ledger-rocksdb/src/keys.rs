pub fn managed_cursor_key(ledger_name: &str, cursor_name: &str) -> Vec<u8> {
    format!("/managed-ledgers/{ledger_name}/{cursor_name}").into_bytes()
}

pub fn managed_ledger_key(ledger_name: &str) -> Vec<u8> {
    format!("/managed-ledgers/{ledger_name}").into_bytes()
}

pub fn ledger_id_allocator_key() -> Vec<u8> {
    b"managed_ledger|next_ledger_id".to_vec()
}

pub fn managed_entry_key(ledger_id: u64, entry_id: u64) -> Vec<u8> {
    format!("entry|{ledger_id}|{entry_id}").into_bytes()
}

pub fn managed_ledger_name(topic: &str) -> String {
    if let Some((domain, rest)) = topic.split_once("://") {
        let mut parts = rest.splitn(3, '/');
        if let (Some(tenant), Some(namespace), Some(local_name)) =
            (parts.next(), parts.next(), parts.next())
        {
            return format!("{tenant}/{namespace}/{domain}/{local_name}");
        }
    }

    topic.to_string()
}

pub fn encode_cursor_name(name: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(name.len());

    for byte in name.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0x0f) as usize] as char);
        }
    }

    encoded
}
