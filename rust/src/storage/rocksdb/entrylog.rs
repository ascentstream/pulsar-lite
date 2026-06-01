use super::metadata::StoredEntry;
use anyhow::{anyhow, bail, Context, Result};
use std::fs;
use std::fs::OpenOptions;
use std::io::SeekFrom;
use std::io::Write;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const ENTRY_MAGIC: u32 = 0x504C4547; // "PLEG"
const ENTRY_VERSION: u16 = 1;
const ENTRY_HEADER_LEN: u16 = 40;
const DEFAULT_LOG_SIZE_LIMIT: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(super) struct EntryIndex {
    pub ledger_id: u64,
    pub entry_id: u64,
    pub file_id: u64,
    pub offset: u64,
    pub len: u64,
    pub checksum: u64,
    pub partition: i32,
}

struct EntryLogState {
    active_file_id: u64,
    active_offset: u64,
}

pub(super) struct EntryLogStore {
    dir: PathBuf,
    log_size_limit: u64,
    state: Mutex<EntryLogState>,
}

impl EntryLogStore {
    pub(super) fn open(root: &Path) -> Result<Self> {
        let dir = root.join("entrylog");
        fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create entrylog dir {}", dir.display()))?;
        let active_file_id = Self::next_entry_log_file_id(&dir)?;
        Ok(Self {
            dir,
            log_size_limit: DEFAULT_LOG_SIZE_LIMIT,
            state: Mutex::new(EntryLogState {
                active_file_id,
                active_offset: 0,
            }),
        })
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
            let Some(file_id) = name
                .strip_prefix("entrylog-")
                .and_then(|name| name.strip_suffix(".log"))
                .and_then(|name| name.parse::<u64>().ok())
            else {
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

    fn entry_log_path(&self, file_id: u64) -> PathBuf {
        self.dir.join(format!("entrylog-{file_id:020}.log"))
    }

    fn checksum(payload: &[u8]) -> u64 {
        payload
            .iter()
            .fold(0u64, |acc, byte| acc.wrapping_add(*byte as u64))
    }

    pub(super) fn append(
        &self,
        ledger_id: u64,
        entry_id: u64,
        partition: i32,
        payload: &[u8],
    ) -> Result<EntryIndex> {
        let mut state = self.state.lock().unwrap();
        let checksum = EntryLogStore::checksum(payload);
        let payload_len = payload.len() as u32;
        let len = ENTRY_HEADER_LEN as u64 + payload.len() as u64;
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
        file.write_all(&payload_len.to_le_bytes())?;
        file.write_all(&checksum.to_le_bytes())?;
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

    pub(super) fn read(&self, index: &EntryIndex) -> Result<StoredEntry> {
        let path = self.entry_log_path(index.file_id);
        let mut file = OpenOptions::new().read(true).open(&path)?;

        file.seek(SeekFrom::Start(index.offset))?;

        let mut header = vec![0u8; ENTRY_HEADER_LEN as usize];
        file.read_exact(&mut header)?;

        let magic = u32::from_le_bytes(header[0..4].try_into()?);
        let version = u16::from_le_bytes(header[4..6].try_into()?);
        let header_len = u16::from_le_bytes(header[6..8].try_into()?);
        let ledger_id = u64::from_le_bytes(header[8..16].try_into()?);
        let entry_id = u64::from_le_bytes(header[16..24].try_into()?);
        let partition = i32::from_le_bytes(header[24..28].try_into()?);
        let payload_len = u32::from_le_bytes(header[28..32].try_into()?);
        let expected_checksum = u64::from_le_bytes(header[32..40].try_into()?);

        if magic != ENTRY_MAGIC {
            bail!("invalid entrylog magic");
        }
        if version != ENTRY_VERSION {
            bail!("unsupported entrylog version {}", version);
        }
        if header_len != ENTRY_HEADER_LEN {
            bail!("invalid entrylog header length {}", header_len);
        }
        if ledger_id != index.ledger_id || entry_id != index.entry_id {
            bail!("entrylog position does not match index");
        }

        let actual_len = ENTRY_HEADER_LEN as u64 + payload_len as u64;
        if actual_len != index.len {
            bail!("entrylog length does not match index");
        }

        let mut payload = vec![0u8; payload_len as usize];
        file.read_exact(&mut payload)?;

        if Self::checksum(&payload) != expected_checksum || expected_checksum != index.checksum {
            bail!("entrylog checksum mismatch");
        }

        Ok(StoredEntry { partition, payload })
    }
}
