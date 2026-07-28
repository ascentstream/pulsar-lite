use crate::factory::RocksDBManagedLedgerFactory;
use crate::keys;
use crate::ledger::RocksDBManagedLedger;
use pulsar_lite_storage_managed_ledger::MessageId;
use std::collections::HashMap;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// The maximum number of items that can be taken from the queue at one time
const MAX_BATCH: usize = 64;
/// The maximum waiting time for this batch after try_recv drain.
/// Keep short: a fixed 1ms wait regresses max_rate (worker idles while
/// in-flight slots wait on completion). Prefer try_recv drain for depth.
const BATCH_WINDOW: Duration = Duration::from_micros(200);

pub(crate) struct WriteReq {
    topic: String,
    partition: i32,
    metadata: Vec<u8>,
    payload: Vec<u8>,
    reply: oneshot::Sender<Result<MessageId, String>>,
}

/// Single-writer queue for managed-ledger appends.
///
/// Worker owns ledgers in a thread-local HashMap and writes via `&mut`
/// (no SharedLedger lock on the append path). Reply uses tokio oneshot so
/// async callers can await without blocking the Tokio worker thread.
pub(crate) struct WriteQueue {
    tx: Option<mpsc::Sender<WriteReq>>,
    worker: Option<JoinHandle<()>>,
}

impl WriteQueue {
    pub(crate) fn new(factory: RocksDBManagedLedgerFactory) -> Self {
        let (tx, rx) = mpsc::channel::<WriteReq>();

        let worker = thread::Builder::new()
            .name("managed-ledger-write-queue".to_string())
            .spawn(move || {
                Self::worker_loop(factory, rx);
            })
            .expect("spawn managed-ledger write queue worker");

        Self {
            tx: Some(tx),
            worker: Some(worker),
        }
    }

    fn worker_loop(factory: RocksDBManagedLedgerFactory, rx: mpsc::Receiver<WriteReq>) {
        // ledger cache that belongs solely to this worker thread; no redundant Arc/Mutex
        let mut ledgers: HashMap<String, RocksDBManagedLedger> = HashMap::new();

        // Batch-size metrics (aggregated to avoid log spam under stress).
        let mut metric_batches: u64 = 0;
        let mut metric_msgs: u64 = 0;
        let mut metric_max_batch: usize = 0;
        let mut metric_batch_eq1: u64 = 0;
        let mut metric_group_ops: u64 = 0;
        let mut metric_group_msgs: u64 = 0;
        let mut metric_max_group: usize = 0;
        let mut metric_group_eq1: u64 = 0;
        let mut metric_window_started = Instant::now();

        loop {
            let first = match rx.recv() {
                Ok(req) => req,
                Err(_) => break,
            };

            let mut batch = Vec::with_capacity(MAX_BATCH);
            batch.push(first);

            // Drain anything already queued so a full in-flight pipeline becomes
            // one add_entries / one entrylog flush instead of N serial flushes.
            while batch.len() < MAX_BATCH {
                match rx.try_recv() {
                    Ok(req) => batch.push(req),
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => break,
                }
            }

            // Short window to catch stragglers; after each arrival, try_recv again.
            if batch.len() < MAX_BATCH {
                let deadline = Instant::now() + BATCH_WINDOW;
                while batch.len() < MAX_BATCH {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    match rx.recv_timeout(remaining) {
                        Ok(req) => {
                            batch.push(req);
                            while batch.len() < MAX_BATCH {
                                match rx.try_recv() {
                                    Ok(req) => batch.push(req),
                                    Err(mpsc::TryRecvError::Empty) => break,
                                    Err(mpsc::TryRecvError::Disconnected) => break,
                                }
                            }
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => break,
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                }
            }

            let queue_batch_len = batch.len();
            metric_batches += 1;
            metric_msgs += queue_batch_len as u64;
            metric_max_batch = metric_max_batch.max(queue_batch_len);
            if queue_batch_len == 1 {
                metric_batch_eq1 += 1;
            }

            // Group by ledger so one rocksdb write covers many entries of the same topic.
            let mut order: Vec<String> = Vec::new();
            let mut groups: HashMap<String, Vec<WriteReq>> = HashMap::new();
            for req in batch {
                let ledger_name = keys::managed_ledger_name(&req.topic);
                if !groups.contains_key(&ledger_name) {
                    order.push(ledger_name.clone());
                }
                groups.entry(ledger_name).or_default().push(req);
            }

            for ledger_name in order {
                let reqs = match groups.remove(&ledger_name) {
                    Some(reqs) if !reqs.is_empty() => reqs,
                    _ => continue,
                };

                let group_len = reqs.len();
                metric_group_ops += 1;
                metric_group_msgs += group_len as u64;
                metric_max_group = metric_max_group.max(group_len);
                if group_len == 1 {
                    metric_group_eq1 += 1;
                }

                if !ledgers.contains_key(&ledger_name) {
                    match factory.open_owned_ledger(&ledger_name) {
                        Ok(ledger) => {
                            ledgers.insert(ledger_name.clone(), ledger);
                        }
                        Err(e) => {
                            let msg = e.to_string();
                            for req in reqs {
                                let _ = req.reply.send(Err(msg.clone()));
                            }
                            continue;
                        }
                    }
                }

                let Some(ledger) = ledgers.get_mut(&ledger_name) else {
                    for req in reqs {
                        let _ = req
                            .reply
                            .send(Err("ledger missing from worker cache".to_string()));
                    }
                    continue;
                };

                // Borrow metadata/payload only while reqs is still alive.
                let inputs: Vec<(i32, &[u8], &[u8])> = reqs
                    .iter()
                    .map(|r| (r.partition, r.metadata.as_slice(), r.payload.as_slice()))
                    .collect();

                match ledger.add_entries_with_partition_and_metadata(&inputs) {
                    Ok(positions) => {
                        for (req, position) in reqs.into_iter().zip(positions.into_iter()) {
                            let _ = req.reply.send(Ok(MessageId::from(position)));
                        }
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        for req in reqs {
                            let _ = req.reply.send(Err(msg.clone()));
                        }
                    }
                }
            }

            // Emit ~1Hz summary so stress runs stay readable.
            if metric_window_started.elapsed() >= Duration::from_secs(1) {
                let avg_queue = if metric_batches == 0 {
                    0.0
                } else {
                    metric_msgs as f64 / metric_batches as f64
                };
                let avg_group = if metric_group_ops == 0 {
                    0.0
                } else {
                    metric_group_msgs as f64 / metric_group_ops as f64
                };
                let pct_queue_eq1 = if metric_batches == 0 {
                    0.0
                } else {
                    100.0 * metric_batch_eq1 as f64 / metric_batches as f64
                };
                let pct_group_eq1 = if metric_group_ops == 0 {
                    0.0
                } else {
                    100.0 * metric_group_eq1 as f64 / metric_group_ops as f64
                };

                log::info!(
                    "write_queue metrics: queue_batches={} queue_msgs={} queue_batch_avg={:.2} queue_batch_max={} queue_batch_eq1={:.1}% group_ops={} group_msgs={} group_avg={:.2} group_max={} group_eq1={:.1}%",
                    metric_batches,
                    metric_msgs,
                    avg_queue,
                    metric_max_batch,
                    pct_queue_eq1,
                    metric_group_ops,
                    metric_group_msgs,
                    avg_group,
                    metric_max_group,
                    pct_group_eq1,
                );

                metric_batches = 0;
                metric_msgs = 0;
                metric_max_batch = 0;
                metric_batch_eq1 = 0;
                metric_group_ops = 0;
                metric_group_msgs = 0;
                metric_max_group = 0;
                metric_group_eq1 = 0;
                metric_window_started = Instant::now();
            }
        }
    }

    pub(crate) fn sender(&self) -> Result<mpsc::Sender<WriteReq>, String> {
        self.tx
            .as_ref()
            .cloned()
            .ok_or_else(|| "write queue closed".to_string())
    }

    /// Enqueue without waiting. Caller awaits/blocks on the returned receiver.
    pub(crate) fn enqueue_with_tx(
        tx: &mpsc::Sender<WriteReq>,
        topic: &str,
        partition: i32,
        metadata: &[u8],
        payload: &[u8],
    ) -> Result<oneshot::Receiver<Result<MessageId, String>>, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(WriteReq {
            topic: topic.to_string(),
            partition,
            metadata: metadata.to_vec(),
            payload: payload.to_vec(),
            reply: reply_tx,
        })
        .map_err(|_| "write queue worker disconnected".to_string())?;
        Ok(reply_rx)
    }

    /// Async submit: does not block the Tokio worker thread while waiting for disk IO.
    pub(crate) async fn submit_with_tx(
        tx: &mpsc::Sender<WriteReq>,
        topic: &str,
        partition: i32,
        metadata: &[u8],
        payload: &[u8],
    ) -> Result<MessageId, String> {
        let reply_rx = Self::enqueue_with_tx(tx, topic, partition, metadata, payload)?;
        match reply_rx.await {
            Ok(Ok(id)) => Ok(id),
            Ok(Err(e)) => Err(e),
            Err(_) => Err("write queue worker disconnected".to_string()),
        }
    }
    /// Blocking submit for sync ManagedLedgerStorage / unit tests.
    ///
    /// `oneshot::Receiver::blocking_recv` panics if called on a thread already
    /// driving a Tokio runtime (e.g. `#[tokio::test]`). In that case wait on a
    /// helper thread instead.
    pub(crate) fn submit_with_tx_blocking(
        tx: &mpsc::Sender<WriteReq>,
        topic: &str,
        partition: i32,
        metadata: &[u8],
        payload: &[u8],
    ) -> Result<MessageId, String> {
        let reply_rx = Self::enqueue_with_tx(tx, topic, partition, metadata, payload)?;
        let joined = if tokio::runtime::Handle::try_current().is_ok() {
            std::thread::spawn(move || reply_rx.blocking_recv())
                .join()
                .map_err(|_| "write queue waiter thread panicked".to_string())?
        } else {
            reply_rx.blocking_recv()
        };
        match joined {
            Ok(Ok(id)) => Ok(id),
            Ok(Err(e)) => Err(e),
            Err(_) => Err("write queue worker disconnected".to_string()),
        }
    }

    pub(crate) fn submit(
        &self,
        topic: &str,
        partition: i32,
        metadata: &[u8],
        payload: &[u8],
    ) -> Result<MessageId, String> {
        let tx = self.sender()?;
        Self::submit_with_tx_blocking(&tx, topic, partition, metadata, payload)
    }

    #[allow(dead_code)] // used by unit tests; Phase B pipeline will call async path more broadly
    pub(crate) async fn submit_async(
        &self,
        topic: &str,
        partition: i32,
        metadata: &[u8],
        payload: &[u8],
    ) -> Result<MessageId, String> {
        let tx = self.sender()?;
        Self::submit_with_tx(&tx, topic, partition, metadata, payload).await
    }
}

impl Drop for WriteQueue {
    fn drop(&mut self) {
        // Close the sender first so worker_loop's recv returns Err and exits.
        drop(self.tx.take());
        if let Some(handle) = self.worker.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entrylog::EntryLogStore;
    use rocksdb::{Options, DB};
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    fn open_test_factory(path: &std::path::Path) -> RocksDBManagedLedgerFactory {
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = Arc::new(DB::open(&options, path).unwrap());
        let entry_log = Arc::new(EntryLogStore::open(path).unwrap());
        RocksDBManagedLedgerFactory::new(db, entry_log)
    }

    #[test]
    fn single_submit_returns_entry0() {
        let dir = tempdir().unwrap();
        let q = WriteQueue::new(open_test_factory(dir.path()));
        let id = q
            .submit("persistent://public/default/t", -1, &[], b"hello")
            .unwrap();
        assert_eq!(id.ledger, 0);
        assert_eq!(id.entry, 0);
        assert_eq!(id.partition, -1);
    }

    #[tokio::test]
    async fn single_submit_async_returns_entry0() {
        let dir = tempdir().unwrap();
        let q = WriteQueue::new(open_test_factory(dir.path()));
        let id = q
            .submit_async("persistent://public/default/t", -1, &[], b"hello")
            .await
            .unwrap();
        assert_eq!(id.ledger, 0);
        assert_eq!(id.entry, 0);
        assert_eq!(id.partition, -1);
    }

    #[test]
    fn multi_thread_submit_gets_unique_entry_ids() {
        let dir = tempdir().unwrap();
        let q = Arc::new(WriteQueue::new(open_test_factory(dir.path())));
        let topic = "persistent://public/default/concurrent";
        let mut handles = vec![];

        for t in 0..8 {
            let q = Arc::clone(&q);
            handles.push(std::thread::spawn(move || {
                let mut entries = vec![];
                for i in 0..100 {
                    let payload = format!("t{t}-{i}");
                    let id = q.submit(topic, -1, &[], payload.as_bytes()).unwrap();
                    entries.push(id.entry);
                }
                entries
            }));
        }

        let mut all = vec![];
        for h in handles {
            all.extend(h.join().unwrap());
        }

        all.sort_unstable();
        assert_eq!(all.len(), 800);
        assert_eq!(all, (0..800).collect::<Vec<_>>());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multi_async_submit_gets_unique_entry_ids() {
        let dir = tempdir().unwrap();
        let q = Arc::new(WriteQueue::new(open_test_factory(dir.path())));
        let topic = "persistent://public/default/concurrent-async";

        let mut tasks = vec![];
        for t in 0..8 {
            let q = Arc::clone(&q);
            tasks.push(tokio::spawn(async move {
                let mut entries = vec![];
                for i in 0..100 {
                    let payload = format!("t{t}-{i}");
                    let id = q
                        .submit_async(topic, -1, &[], payload.as_bytes())
                        .await
                        .unwrap();
                    entries.push(id.entry);
                }
                entries
            }));
        }

        let mut all = vec![];
        for task in tasks {
            all.extend(task.await.unwrap());
        }
        all.sort_unstable();
        assert_eq!(all.len(), 800);
        assert_eq!(all, (0..800).collect::<Vec<_>>());
    }

    #[test]
    fn append_through_mutex_wrapper_still_unique() {
        let dir = tempdir().unwrap();
        let q = Arc::new(Mutex::new(WriteQueue::new(open_test_factory(dir.path()))));
        let topic = "persistent://public/default/wrapped";
        let mut handles = vec![];

        for t in 0..4 {
            let q = Arc::clone(&q);
            handles.push(std::thread::spawn(move || {
                let mut entries = vec![];
                for i in 0..50 {
                    let payload = format!("t{t}-{i}");
                    let id = q
                        .lock()
                        .unwrap()
                        .submit(topic, -1, &[], payload.as_bytes())
                        .unwrap();
                    entries.push(id.entry);
                }
                entries
            }));
        }

        let mut all = vec![];
        for h in handles {
            all.extend(h.join().unwrap());
        }
        all.sort_unstable();
        assert_eq!(all, (0..200).collect::<Vec<_>>());
    }
}
