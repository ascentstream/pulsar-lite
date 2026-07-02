use anyhow::{anyhow, bail, Context, Result};
use std::fs;
use std::fs::OpenOptions;
use std::io::SeekFrom;
use std::io::Write;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const ENTRY_MAGIC: u32 = 0x504C4547; // "PLEG"
const ENTRY_VERSION_LEGACY: u16 = 1;
const ENTRY_VERSION: u16 = 2;
const ENTRY_HEADER_LEN_LEGACY: u16 = 40;
const ENTRY_HEADER_LEN: u16 = 44;
const DEFAULT_LOG_SIZE_LIMIT: u64 = 2 * 1024 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct EntryIndex {
    pub ledger_id: u64,
    pub entry_id: u64,
    pub file_id: u64,
    pub offset: u64,
    pub len: u64,
    pub checksum: u64,
    pub partition: i32,
}

#[derive(Debug, Clone)]
pub struct EntryRecord {
    pub partition: i32,
    pub metadata: Vec<u8>,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
struct EntryLogState {
    active_file_id: u64,
    active_offset: u64,
}

#[derive(Debug)]
pub struct EntryLogStore {
    dir: PathBuf,
    log_size_limit: u64,
    state: Mutex<EntryLogState>,
}

impl EntryLogStore {
    pub fn open(root: &Path) -> Result<Self> {
        Self::open_with_limit(root, DEFAULT_LOG_SIZE_LIMIT)
    }

    #[doc(hidden)]
    pub fn open_with_log_size_limit(root: &Path, log_size_limit: u64) -> Result<Self> {
        Self::open_with_limit(root, log_size_limit)
    }

    fn open_with_limit(root: &Path, log_size_limit: u64) -> Result<Self> {
        let dir = root.join("entrylog");
        fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create entrylog dir {}", dir.display()))?;
        let active_file_id = Self::next_entry_log_file_id(&dir)?;
        Ok(Self {
            dir,
            log_size_limit,
            state: Mutex::new(EntryLogState {
                active_file_id,
                active_offset: 0,
            }),
        })
    }

    #[doc(hidden)]
    pub fn default_log_size_limit() -> u64 {
        DEFAULT_LOG_SIZE_LIMIT
    }

    fn next_entry_log_file_id(dir: &Path) -> Result<u64> {
        let mut max_file_id: Option<u64> = None;

        for entry in fs::read_dir(dir)
            .with_context(|| format!("failed to read entrylog dir {}", dir.display()))?
        {
            let entry = entry?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Some(file_id) = Self::parse_log_file_id(&name) else {
                continue;
            };

            max_file_id = Some(max_file_id.map_or(file_id, |max| max.max(file_id)));
        }

        max_file_id.map_or(Ok(0), |file_id| {
            file_id
                .checked_add(1)
                .ok_or_else(|| anyhow!("entrylog file id overflow"))
        })
    }

    fn parse_log_file_id(name: &str) -> Option<u64> {
        name.strip_suffix(".log")?.parse::<u64>().ok()
    }

    fn entry_log_path(&self, file_id: u64) -> PathBuf {
        self.dir.join(format!("{file_id}.log"))
    }

    fn checksum(parts: &[&[u8]]) -> u64 {
        parts
            .iter()
            .flat_map(|part| part.iter())
            .fold(0u64, |acc, byte| acc.wrapping_add(*byte as u64))
    }

    #[doc(hidden)]
    pub fn append(
        &self,
        ledger_id: u64,
        entry_id: u64,
        partition: i32,
        payload: &[u8],
    ) -> Result<EntryIndex> {
        self.append_with_metadata(ledger_id, entry_id, partition, &[], payload)
    }

    pub fn append_with_metadata(
        &self,
        ledger_id: u64,
        entry_id: u64,
        partition: i32,
        metadata: &[u8],
        payload: &[u8],
    ) -> Result<EntryIndex> {
        let mut state = self.state.lock().unwrap();
        let checksum = EntryLogStore::checksum(&[metadata, payload]);
        let metadata_len = metadata.len() as u32;
        let payload_len = payload.len() as u32;
        let len = ENTRY_HEADER_LEN as u64 + metadata.len() as u64 + payload.len() as u64;
        if state.active_offset > 0 && state.active_offset + len > self.log_size_limit {
            state.active_file_id += 1;
            state.active_offset = 0;
        }
        let file_id = state.active_file_id;
        let offset = state.active_offset;
        let path = self.entry_log_path(file_id);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        file.write_all(&ENTRY_MAGIC.to_le_bytes())?;
        file.write_all(&ENTRY_VERSION.to_le_bytes())?;
        file.write_all(&ENTRY_HEADER_LEN.to_le_bytes())?;
        file.write_all(&ledger_id.to_le_bytes())?;
        file.write_all(&entry_id.to_le_bytes())?;
        file.write_all(&partition.to_le_bytes())?;
        file.write_all(&metadata_len.to_le_bytes())?;
        file.write_all(&payload_len.to_le_bytes())?;
        file.write_all(&checksum.to_le_bytes())?;
        file.write_all(metadata)?;
        file.write_all(payload)?;
        file.flush()?;
        state.active_offset += len;

        Ok(EntryIndex {
            ledger_id,
            entry_id,
            file_id,
            offset,
            len,
            checksum,
            partition,
        })
    }

    pub fn read(&self, index: &EntryIndex) -> Result<EntryRecord> {
        let path = self.entry_log_path(index.file_id);
        let mut file = OpenOptions::new().read(true).open(&path)?;

        file.seek(SeekFrom::Start(index.offset))?;

        let mut header_prefix = [0u8; 8];
        file.read_exact(&mut header_prefix)?;

        let magic = u32::from_le_bytes(header_prefix[0..4].try_into()?);
        let version = u16::from_le_bytes(header_prefix[4..6].try_into()?);
        let header_len = u16::from_le_bytes(header_prefix[6..8].try_into()?);
        let mut header = header_prefix.to_vec();
        header.resize(header_len as usize, 0);
        file.read_exact(&mut header[8..])?;

        let ledger_id = u64::from_le_bytes(header[8..16].try_into()?);
        let entry_id = u64::from_le_bytes(header[16..24].try_into()?);
        let partition = i32::from_le_bytes(header[24..28].try_into()?);

        if magic != ENTRY_MAGIC {
            bail!("invalid entrylog magic");
        }
        if version != ENTRY_VERSION && version != ENTRY_VERSION_LEGACY {
            bail!("unsupported entrylog version {}", version);
        }
        if version == ENTRY_VERSION_LEGACY && header_len != ENTRY_HEADER_LEN_LEGACY {
            bail!("invalid entrylog header length {}", header_len);
        }
        if version == ENTRY_VERSION && header_len != ENTRY_HEADER_LEN {
            bail!("invalid entrylog header length {}", header_len);
        }
        if ledger_id != index.ledger_id || entry_id != index.entry_id {
            bail!("entrylog position does not match index");
        }

        let (metadata_len, payload_len, expected_checksum) = if version == ENTRY_VERSION_LEGACY {
            let payload_len = u32::from_le_bytes(header[28..32].try_into()?);
            let expected_checksum = u64::from_le_bytes(header[32..40].try_into()?);
            (0u32, payload_len, expected_checksum)
        } else {
            let metadata_len = u32::from_le_bytes(header[28..32].try_into()?);
            let payload_len = u32::from_le_bytes(header[32..36].try_into()?);
            let expected_checksum = u64::from_le_bytes(header[36..44].try_into()?);
            (metadata_len, payload_len, expected_checksum)
        };

        let actual_len = header_len as u64 + metadata_len as u64 + payload_len as u64;
        if actual_len != index.len {
            bail!("entrylog length does not match index");
        }

        let mut metadata = vec![0u8; metadata_len as usize];
        file.read_exact(&mut metadata)?;
        let mut payload = vec![0u8; payload_len as usize];
        file.read_exact(&mut payload)?;

        if Self::checksum(&[&metadata, &payload]) != expected_checksum
            || expected_checksum != index.checksum
        {
            bail!("entrylog checksum mismatch");
        }

        Ok(EntryRecord {
            partition,
            metadata,
            payload,
        })
    }
}
