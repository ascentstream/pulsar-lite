use crate::factory::{RocksDBManagedLedgerFactory, SharedLedger};
use crate::keys;
use pulsar_lite_metrics::PublishCommitObserver;
use pulsar_lite_storage_managed_ledger::MessageId;
use std::collections::HashMap;
use std::sync::Arc;
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

/// Result delivered to a broker connection after durable append (no await on enqueue path).
#[derive(Debug)]
pub struct ConnAppendResult {
    pub producer_id: u64,
    pub sequence_id: u64,
    pub result: Result<MessageId, String>,
}

enum WriteReply {
    /// Legacy / unit-test path: caller awaits oneshot.
    Oneshot(oneshot::Sender<Result<MessageId, String>>),
    /// Connection path: worker pushes completion; connection select writes Receipt.
    Conn {
        producer_id: u64,
        sequence_id: u64,
        tx: tokio::sync::mpsc::Sender<ConnAppendResult>,
    },
}

impl WriteReply {
    fn complete(self, result: Result<MessageId, String>) {
        match self {
            WriteReply::Oneshot(tx) => {
                let _ = tx.send(result);
            }
            WriteReply::Conn {
                producer_id,
                sequence_id,
                tx,
            } => {
                let _ = tx.blocking_send(ConnAppendResult {
                    producer_id,
                    sequence_id,
                    result,
                });
            }
        }
    }
}

pub(crate) struct WriteReq {
    topic: String,
    partition: i32,
    metadata: Vec<u8>,
    payload: Vec<u8>,
    observer: Option<Arc<dyn PublishCommitObserver>>,
    /// Set on the connection path; anchors the end-to-end publish latency
    /// histogram (enqueue → committed batch).
    enqueued_at: Option<Instant>,
    reply: WriteReply,
}

/// Single-writer queue for managed-ledger appends.
///
/// Worker caches the same `Arc<RocksDBManagedLedger>` handles as store reads so
/// durable success updates one published LAC/meta view. Entry-id assignment stays
/// serial on this thread; no outer content mutex around append IO.
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
        // Same Arc instances as store reads (singleton per ledger name).
        let mut ledgers: HashMap<String, SharedLedger> = HashMap::new();

        // Prometheus families (no-op before metrics::init).
        let metrics = pulsar_lite_metrics::storage_metrics();

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
            metrics.observe_batch(queue_batch_len as u64);

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


                if !ledgers.contains_key(&ledger_name) {
                    match factory.open_ledger(&ledger_name) {
                        Ok(ledger) => {
                            ledgers.insert(ledger_name.clone(), ledger);
                        }
                        Err(e) => {
                            let msg = e.to_string();
                            for req in reqs {
                                req.reply.complete(Err(msg.clone()));
                            }
                            continue;
                        }
                    }
                }

                let Some(ledger) = ledgers.get(&ledger_name) else {
                    for req in reqs {
                        req.reply
                            .complete(Err("ledger missing from worker cache".to_string()));
                    }
                    continue;
                };

                // Borrow metadata/payload only while reqs is still alive.
                let inputs: Vec<(i32, &[u8], &[u8])> = reqs
                    .iter()
                    .map(|r| (r.partition, r.metadata.as_slice(), r.payload.as_slice()))
                    .collect();

                // Append publishes meta then LAC only after durable OK;
                // complete only after that returns Ok.
                let append_started = Instant::now();
                let append_result =
                    ledger.add_entries_with_partition_and_metadata(&inputs);
                metrics.observe_ledger_write_latency(append_started.elapsed().as_secs_f64());
                match append_result {
                    Ok(positions) => {
                        // One observer call per committed group: all reqs in
                        // a group share the topic, so the first req's observer
                        // already targets the right counters.
                        let mut committed_messages: u64 = 0;
                        let mut committed_bytes: u64 = 0;
                        let mut observer: Option<Arc<dyn PublishCommitObserver>> = None;
                        let committed_at = Instant::now();
                        for req in &reqs {
                            // Batched producers pack N client-visible messages
                            // into one request; counters must fold N, not 1
                            // (bytes already fold the full payload).
                            committed_messages +=
                                pulsar_lite_proto::codec::messages_in_batch(&req.metadata)
                                    as u64;
                            let req_bytes = (req.metadata.len() + req.payload.len()) as u64;
                            committed_bytes += req_bytes;
                            metrics.observe_entry_size(req_bytes as f64);
                            if let Some(enqueued_at) = req.enqueued_at {
                                metrics.observe_write_latency(
                                    committed_at.duration_since(enqueued_at).as_secs_f64(),
                                );
                            }
                            if observer.is_none() {
                                observer = req.observer.clone();
                            }
                        }
                        if let Some(observer) = observer {
                            observer.on_commit(committed_messages, committed_bytes);
                        }
                        for (req, position) in reqs.into_iter().zip(positions) {
                            req.reply.complete(Ok(MessageId::from(position)));
                        }
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        for req in reqs {
                            req.reply.complete(Err(msg.clone()));
                        }
                    }
                }
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
            observer: None,
            enqueued_at: None,
            reply: WriteReply::Oneshot(reply_tx),
        })
        .map_err(|_| "write queue worker disconnected".to_string())?;
        Ok(reply_rx)
    }

    pub(crate) fn enqueue_for_connection(
        tx: &mpsc::Sender<WriteReq>,
        topic: &str,
        partition: i32,
        metadata: &[u8],
        payload: &[u8],
        producer_id: u64,
        sequence_id: u64,
        observer: Option<Arc<dyn PublishCommitObserver>>,
        completion_tx: tokio::sync::mpsc::Sender<ConnAppendResult>,
    ) -> Result<(), String> {
        tx.send(WriteReq {
            topic: topic.to_string(),
            partition,
            metadata: metadata.to_vec(),
            payload: payload.to_vec(),
            observer,
            enqueued_at: Some(Instant::now()),
            reply: WriteReply::Conn {
                producer_id,
                sequence_id,
                tx: completion_tx,
            },
        })
        .map_err(|_| "write queue worker disconnected".to_string())
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

    #[tokio::test]
    async fn enqueue_for_connection_delivers_conn_append_result() {
        let dir = tempdir().unwrap();
        let q = WriteQueue::new(open_test_factory(dir.path()));
        let tx = q.sender().unwrap();
        let (completion_tx, mut completion_rx) = tokio::sync::mpsc::channel(4);

        WriteQueue::enqueue_for_connection(
            &tx,
            "persistent://public/default/conn-path",
            3,
            b"meta",
            b"payload",
            42,
            7,
            completion_tx,
        )
        .unwrap();

        let append = tokio::time::timeout(Duration::from_secs(2), completion_rx.recv())
            .await
            .expect("conn append timed out")
            .expect("conn append channel closed");

        assert_eq!(append.producer_id, 42);
        assert_eq!(append.sequence_id, 7);
        let id = append.result.expect("append ok");
        assert_eq!(id.ledger, 0);
        assert_eq!(id.entry, 0);
        assert_eq!(id.partition, 3);
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
