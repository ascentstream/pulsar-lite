use anyhow::{anyhow, bail, Context, Result};
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::SeekFrom;
use std::io::Write;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const ENTRY_MAGIC: u32 = 0x504C4547; // "PLEG"
const ENTRY_VERSION_LEGACY: u16 = 1;
const ENTRY_VERSION: u16 = 2;
const ENTRY_HEADER_LEN_LEGACY: u16 = 40;
const ENTRY_HEADER_LEN: u16 = 44;
const DEFAULT_LOG_SIZE_LIMIT: u64 = 2 * 1024 * 1024 * 1024;
const MAX_BATCH: usize = 64;
const BATCH_WINDOW: Duration = Duration::from_micros(200);

struct WriteRequest {
    ledger_id: u64,
    entry_id: u64,
    partition: i32,
    metadata: Vec<u8>,
    payload: Vec<u8>,
    reply: mpsc::Sender<Result<EntryIndex>>,
}

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

// Holds request-queue sender, entrylog directory, and writer-thread handle.
#[derive(Debug)]
pub struct EntryLogStore {
    dir: Arc<PathBuf>,
    sender: Option<mpsc::Sender<WriteRequest>>,
    _thread: Option<JoinHandle<()>>,
}

struct WriteState {
    dir: Arc<PathBuf>,
    log_size_limit: u64,
    active_file_id: u64,
    active_offset: u64,
    file: File,
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
        let Some(file_id) = parse_log_file_id(&name) else {
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

impl WriteState {
    fn open(dir: Arc<PathBuf>, log_size_limit: u64) -> Result<Self> {
        let active_file_id = next_entry_log_file_id(dir.as_ref())?;
        let path = dir.join(format!("{active_file_id}.log"));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        Ok(Self {
            dir,
            log_size_limit,
            active_file_id,
            active_offset: 0,
            file,
        })
    }

    // Writes a batch of entries to the log, returning a vector of results indicating success or failure.
    fn write_batch(&mut self, batch: &[WriteRequest]) -> Vec<Result<EntryIndex>> {
        let mut results = Vec::with_capacity(batch.len());
        if batch.is_empty() {
            return results;
        }
        let mut batch_len = 0;
        for req in batch {
            batch_len +=
                ENTRY_HEADER_LEN as u64 + req.metadata.len() as u64 + req.payload.len() as u64;
        }
        if self.active_offset > 0 && self.active_offset + batch_len > self.log_size_limit {
            self.active_file_id += 1;
            self.active_offset = 0;
            let path = self.dir.join(format!("{}.log", self.active_file_id));
            match OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(&path)
            {
                Ok(f) => self.file = f,
                Err(e) => {
                    let err = format!("open entrylog failed: {e}");
                    for _ in batch {
                        results.push(Err(anyhow!(err.clone())));
                    }
                    return results;
                }
            }
        }

        let mut buf = Vec::new();
        let mut pending = Vec::with_capacity(batch.len());
        let mut cursor = self.active_offset;

        for req in batch {
            let checksum = EntryLogStore::checksum(&[&req.metadata, &req.payload]);
            let metadata_len = req.metadata.len() as u32;
            let payload_len = req.payload.len() as u32;
            let len =
                ENTRY_HEADER_LEN as u64 + req.metadata.len() as u64 + req.payload.len() as u64;

            pending.push(EntryIndex {
                ledger_id: req.ledger_id,
                entry_id: req.entry_id,
                file_id: self.active_file_id,
                offset: cursor,
                len,
                checksum,
                partition: req.partition,
            });
            buf.extend_from_slice(&ENTRY_MAGIC.to_le_bytes());
            buf.extend_from_slice(&ENTRY_VERSION.to_le_bytes());
            buf.extend_from_slice(&ENTRY_HEADER_LEN.to_le_bytes());
            buf.extend_from_slice(&req.ledger_id.to_le_bytes());
            buf.extend_from_slice(&req.entry_id.to_le_bytes());
            buf.extend_from_slice(&req.partition.to_le_bytes());
            buf.extend_from_slice(&metadata_len.to_le_bytes());
            buf.extend_from_slice(&payload_len.to_le_bytes());
            buf.extend_from_slice(&checksum.to_le_bytes());
            buf.extend_from_slice(&req.metadata);
            buf.extend_from_slice(&req.payload);
            cursor += len;
        }

        if buf.is_empty() {
            return results;
        }

        match self.file.write_all(&buf).and_then(|_| self.file.flush()) {
            Ok(()) => {
                self.active_offset = cursor;
                for index in pending {
                    results.push(Ok(index));
                }
            }
            Err(e) => {
                let err = format!("write entrylog failed: {e}");
                for _ in pending {
                    results.push(Err(anyhow!(err.clone())));
                }
            }
        }
        results
    }
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
        let dir = Arc::new(root.join("entrylog"));
        fs::create_dir_all(dir.as_ref())?;

        let (sender, receiver) = mpsc::channel::<WriteRequest>();

        let dir_for_writer = Arc::clone(&dir);
        let handle = thread::Builder::new()
            .name("entrylog-writer".to_string())
            .spawn(move || {
                Self::write_loop(dir_for_writer, log_size_limit, receiver);
            })?;
        Ok(Self {
            dir,
            sender: Some(sender),
            _thread: Some(handle),
        })
    }

    fn write_loop(dir: Arc<PathBuf>, log_size_limit: u64, receiver: mpsc::Receiver<WriteRequest>) {
        let mut state = match WriteState::open(dir, log_size_limit) {
            Ok(s) => s,
            Err(e) => {
                let err = format!("{e:#}");
                while let Ok(req) = receiver.recv() {
                    let _ = req.reply.send(Err(anyhow!(err.clone())));
                }
                return;
            }
        };

        loop {
            // first, wait for the first request
            let first = match receiver.recv() {
                Ok(req) => req,
                Err(_) => break,
            };
            let mut batch = vec![first];

            // second, try to accumulate as much as possible within the batch window.
            let deadline = Instant::now() + BATCH_WINDOW;
            while batch.len() < MAX_BATCH {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match receiver.recv_timeout(remaining) {
                    Ok(req) => batch.push(req),
                    Err(mpsc::RecvTimeoutError::Timeout) => break,
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }

            // third, write the batch to disk
            let results = state.write_batch(&batch);

            for (req, result) in batch.into_iter().zip(results) {
                let _ = req.reply.send(result);
            }
        }
    }

    #[doc(hidden)]
    pub fn default_log_size_limit() -> u64 {
        DEFAULT_LOG_SIZE_LIMIT
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
        let (tx, rx) = mpsc::channel();
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| anyhow!("entrylog writer is closed"))?;

        sender
            .send(WriteRequest {
                ledger_id,
                entry_id,
                partition,
                metadata: metadata.to_vec(),
                payload: payload.to_vec(),
                reply: tx,
            })
            .map_err(|_| anyhow!("entrylog writer disconnected"))?;
        rx.recv()
            .map_err(|_| anyhow!("entrylog writer disconnected"))?
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

impl Drop for EntryLogStore {
    // Drop the sender first so write_loop's recv returns Err and the thread exits.
    // Then join the writer thread. Joining before closing the sender would deadlock.
    fn drop(&mut self) {
        drop(self.sender.take());
        if let Some(handle) = self._thread.take() {
            let _ = handle.join();
        }
    }
}
