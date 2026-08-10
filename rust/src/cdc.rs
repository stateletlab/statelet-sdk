//! Durable change-feed (CDC) — issue #824 (CDC epic #692, Phase 5c).
//!
//! This mirrors the canonical Python consumer (#823, Phase 5b) with identical
//! semantics: Kafka-style client-managed committed offsets, a pluggable
//! [`CheckpointStore`] with a file-backed atomic default ([`FileCheckpointStore`]),
//! bootstrap-on-`compacted` (paged `Scan` baseline then resume from
//! `snapshot_offset + 1`), heartbeat-advances-checkpoint, reconnect-on-disconnect
//! from `last_offset + 1`, and at-least-once delivery with idempotency required on
//! the boundary.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use crate::proto;

/// Pluggable store for a consumer's last fully-processed CDC offset.
///
/// Implements Kafka-style client-managed offsets: the consumer commits the
/// offset *after* a change has been processed (at-least-once). Apps may supply a
/// custom store that persists the offset transactionally with their own sink to
/// achieve exactly-once-sink semantics.
pub trait CheckpointStore: Send + Sync {
    /// Return the last fully-processed offset for `subscription_id`, or `None`
    /// when no checkpoint has been committed yet.
    fn load(&self, subscription_id: &str) -> io::Result<Option<u64>>;

    /// Durably record `offset` as fully processed for `subscription_id`.
    fn commit(&self, subscription_id: &str, offset: u64) -> io::Result<()>;
}

/// Default [`CheckpointStore`] backed by an atomically-written JSON file.
///
/// The file maps `subscription_id -> offset`. Each commit rewrites the file via
/// a temp file + [`std::fs::rename`], which is atomic on POSIX, so a crash
/// between the write and the rename leaves the previous valid offset intact
/// (never a torn/partial file).
pub struct FileCheckpointStore {
    path: PathBuf,
    state: Mutex<HashMap<String, u64>>,
}

impl FileCheckpointStore {
    /// Open (or initialize) a file-backed checkpoint store at `path`. A missing
    /// or corrupt file is treated as "no checkpoint"; a prior atomic commit
    /// could never produce a corrupt file.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let state = match std::fs::read(&path) {
            Ok(bytes) => parse_state(&bytes),
            Err(e) if e.kind() == io::ErrorKind::NotFound => HashMap::new(),
            // Unreadable file: start empty rather than fail to open.
            Err(_) => HashMap::new(),
        };
        Ok(Self {
            path,
            state: Mutex::new(state),
        })
    }
}

/// Parse the minimal `{"sub":offset,...}` JSON map. Returns an empty map on any
/// parse error (corrupt files are treated as "no checkpoint").
///
/// The parser is quote-aware and unescapes the same escapes [`json_string`]
/// emits, so it round-trips any `subscription_id` — including ones containing
/// `,`, `:`, `"`, or whitespace. (A naive comma/colon split would corrupt the
/// whole map when a single key contained those characters.)
fn parse_state(bytes: &[u8]) -> HashMap<String, u64> {
    let s = match std::str::from_utf8(bytes) {
        Ok(s) => s.trim(),
        Err(_) => return HashMap::new(),
    };
    let inner = match s.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
        Some(inner) => inner.trim(),
        None => return HashMap::new(),
    };
    parse_pairs(inner).unwrap_or_default()
}

/// Parse `"key":num,"key":num,...` (no surrounding braces). Returns `None` on
/// any malformed input so the caller can fall back to "no checkpoint".
fn parse_pairs(inner: &str) -> Option<HashMap<String, u64>> {
    let mut out = HashMap::new();
    let mut chars = inner.chars().peekable();
    loop {
        // Skip leading whitespace; an empty (or whitespace-only) map is valid.
        while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
            chars.next();
        }
        if chars.peek().is_none() {
            return Some(out);
        }
        let key = parse_json_string(&mut chars)?;
        skip_ws(&mut chars);
        if chars.next() != Some(':') {
            return None;
        }
        skip_ws(&mut chars);
        // Read the digits of an unsigned integer value.
        let mut num = String::new();
        while matches!(chars.peek(), Some(c) if c.is_ascii_digit()) {
            num.push(chars.next().unwrap());
        }
        let n: u64 = num.parse().ok()?;
        out.insert(key, n);
        skip_ws(&mut chars);
        match chars.next() {
            Some(',') => continue,
            None => return Some(out),
            Some(_) => return None,
        }
    }
}

fn skip_ws(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
        chars.next();
    }
}

/// Read one JSON string token (the leading `"` must be next). Unescapes the
/// escapes [`json_string`] produces. Returns `None` on a malformed token.
fn parse_json_string(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<String> {
    if chars.next() != Some('"') {
        return None;
    }
    let mut out = String::new();
    loop {
        match chars.next()? {
            '"' => return Some(out),
            '\\' => match chars.next()? {
                '"' => out.push('"'),
                '\\' => out.push('\\'),
                'n' => out.push('\n'),
                'r' => out.push('\r'),
                't' => out.push('\t'),
                _ => return None,
            },
            c => out.push(c),
        }
    }
}

/// Serialize the offset map as a compact JSON object. Keys are simple
/// `subscription_id` strings; the file is internal so we escape conservatively.
fn serialize_state(state: &HashMap<String, u64>) -> String {
    let mut parts: Vec<String> = state
        .iter()
        .map(|(k, v)| format!("{}:{}", json_string(k), v))
        .collect();
    parts.sort();
    format!("{{{}}}", parts.join(","))
}

fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

impl CheckpointStore for FileCheckpointStore {
    fn load(&self, subscription_id: &str) -> io::Result<Option<u64>> {
        let state = self.state.lock().unwrap();
        Ok(state.get(subscription_id).copied())
    }

    fn commit(&self, subscription_id: &str, offset: u64) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        state.insert(subscription_id.to_string(), offset);
        let data = serialize_state(&state);

        let dir = self
            .path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        std::fs::create_dir_all(&dir)?;

        // Write to a unique temp file, fsync, then atomically rename over the
        // target. A crash before the rename leaves the prior file intact.
        let tmp = dir.join(format!(".ckpt-{}.tmp", unique_suffix()));
        let write_and_rename = || -> io::Result<()> {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(data.as_bytes())?;
            f.sync_all()?;
            drop(f);
            std::fs::rename(&tmp, &self.path)
        };
        if let Err(e) = write_and_rename() {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
        Ok(())
    }
}

/// A process-unique suffix for temp checkpoint files (pid + monotonic counter).
fn unique_suffix() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{}-{}-{}", std::process::id(), nanos, n)
}

/// A single change delivered by [`crate::StateletClient::subscribe_committed`].
///
/// `offset` is the stable Raft log index (the resume key). `is_snapshot` is
/// `true` for synthetic `put` changes produced by a bootstrap `Scan` after the
/// requested offset was compacted away; those all carry the same
/// `snapshot_offset` and `term = 0` / `seq_in_entry = 0`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedChange {
    pub offset: u64,
    pub term: u64,
    pub seq_in_entry: u32,
    pub cf: u32,
    pub key: Vec<u8>,
    /// "put" | "delete" | "merge"
    pub op: String,
    pub value: Vec<u8>,
    pub is_snapshot: bool,
}

/// Configuration for [`crate::StateletClient::subscribe_committed`].
///
/// `cf` uses `Option<u32>` so that `None` means "the client's default CF" while
/// `Some(0)` selects CF 0 (matching the Python keyword default of `cf=None`).
pub struct SubscribeCommittedOptions<'a> {
    /// First Raft offset to deliver when no checkpoint applies (0 = live-only).
    pub from_offset: u64,
    pub cf: Option<u32>,
    pub key_prefix: Vec<u8>,
    pub include_values: bool,
    pub subscription_id: Option<String>,
    pub checkpoint: Option<&'a dyn CheckpointStore>,
    pub auto_commit: bool,
    pub shard_id: u64,
}

impl<'a> Default for SubscribeCommittedOptions<'a> {
    fn default() -> Self {
        Self {
            from_offset: 0,
            cf: None,
            key_prefix: Vec::new(),
            include_values: true,
            subscription_id: None,
            checkpoint: None,
            auto_commit: true,
            shard_id: 0,
        }
    }
}

/// One item received from the committed feed stream — the SDK-level mirror of
/// `proto::committed_feed_item::Item`.
#[derive(Debug, Clone)]
pub enum FeedItem {
    Change(CommittedChange),
    /// High-watermark offset advanced with no matching change.
    Heartbeat(u64),
    /// The requested offset fell below the compaction floor.
    Compacted {
        earliest_offset: u64,
        snapshot_offset: u64,
    },
}

impl FeedItem {
    /// Convert a wire `CommittedFeedItem` into the SDK-level [`FeedItem`].
    /// Returns `None` for an empty oneof.
    pub fn from_proto(item: proto::CommittedFeedItem) -> Option<FeedItem> {
        use proto::committed_feed_item::Item;
        match item.item? {
            Item::Change(c) => Some(FeedItem::Change(CommittedChange {
                offset: c.offset,
                term: c.term,
                seq_in_entry: c.seq_in_entry,
                cf: c.cf,
                key: c.key,
                op: c.op,
                value: c.value,
                is_snapshot: false,
            })),
            Item::Heartbeat(hw) => Some(FeedItem::Heartbeat(hw)),
            Item::Compacted(n) => Some(FeedItem::Compacted {
                earliest_offset: n.earliest_offset,
                snapshot_offset: n.snapshot_offset,
            }),
        }
    }
}

/// A source of committed-feed items for one server-streaming connection.
///
/// `recv` yields the next [`FeedItem`], `Ok(None)` on a clean end-of-stream, and
/// `Err` on a disconnect (which drives reconnect).
#[tonic::async_trait]
pub trait FeedStream: Send {
    async fn recv(&mut self) -> Result<Option<FeedItem>, tonic::Status>;
}

/// Transport abstraction used by the consumer driver: open a feed stream and
/// page a scan. The real [`crate::StateletClient`] implements this over gRPC;
/// tests provide a fake.
#[tonic::async_trait]
pub trait FeedTransport: Send {
    type Stream: FeedStream;

    /// Open a server-streaming committed feed from `from_offset`.
    async fn open_feed(
        &mut self,
        shard_id: u64,
        from_offset: u64,
        cf: u32,
        key_prefix: &[u8],
        include_values: bool,
    ) -> Result<Self::Stream, tonic::Status>;

    /// Page the scan; returns `(entries, next_cursor)` with `next_cursor = None`
    /// when there are no more results.
    async fn scan_page(
        &mut self,
        prefix: &[u8],
        cursor: Option<&[u8]>,
        limit: u32,
        cf: u32,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>), tonic::Status>;
}

/// Error returned by the consumer driver: either a transport error or an error
/// from the user's handler / checkpoint store.
#[derive(Debug)]
pub enum ConsumeError<E> {
    Transport(tonic::Status),
    Checkpoint(io::Error),
    Handler(E),
}

impl<E: std::fmt::Display> std::fmt::Display for ConsumeError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsumeError::Transport(s) => write!(f, "transport error: {s}"),
            ConsumeError::Checkpoint(e) => write!(f, "checkpoint error: {e}"),
            ConsumeError::Handler(e) => write!(f, "handler error: {e}"),
        }
    }
}

impl<E: std::fmt::Debug + std::fmt::Display> std::error::Error for ConsumeError<E> {}

/// Bounded exponential backoff for reconnect.
const BACKOFF_BASE: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Hook used by the driver to wait between reconnect attempts. The default
/// sleeps via tokio; tests override it to avoid real delays.
#[tonic::async_trait]
pub trait Sleeper: Send {
    async fn sleep(&mut self, dur: Duration);
}

/// Default tokio-backed [`Sleeper`].
pub struct TokioSleeper;

#[tonic::async_trait]
impl Sleeper for TokioSleeper {
    async fn sleep(&mut self, dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

/// Run the durable change-feed consumer driving the canonical Phase-5b
/// algorithm. This is generic over the [`FeedTransport`] so it is unit-testable
/// without a live server; [`crate::StateletClient::subscribe_committed`] is the
/// public entry point wrapping the gRPC transport.
///
/// `handler` is invoked for each [`CommittedChange`] in stable Raft-offset
/// order. Returning `Ok(true)` continues; `Ok(false)` stops the consumer
/// cleanly; `Err(e)` stops with [`ConsumeError::Handler`]. With
/// `auto_commit = true` each change's offset is committed *after* the handler
/// returns — giving at-least-once delivery.
pub async fn run_consumer<T, S, H, E>(
    transport: &mut T,
    sleeper: &mut S,
    opts: SubscribeCommittedOptions<'_>,
    default_cf: u32,
    mut handler: H,
) -> Result<(), ConsumeError<E>>
where
    T: FeedTransport,
    S: Sleeper,
    H: FnMut(CommittedChange) -> Result<bool, E>,
{
    let effective_cf = opts.cf.unwrap_or(default_cf);
    let do_commit = opts.checkpoint.is_some() && opts.subscription_id.is_some() && opts.auto_commit;
    let sub_id = opts.subscription_id.clone();

    let commit = |offset: u64| -> Result<(), ConsumeError<E>> {
        if do_commit {
            let ckpt = opts.checkpoint.unwrap();
            let sid = sub_id.as_deref().unwrap();
            ckpt.commit(sid, offset).map_err(ConsumeError::Checkpoint)?;
        }
        Ok(())
    };

    // Resolve the resume start: checkpoint+1 wins over from_offset.
    let mut next_offset = opts.from_offset;
    if let (Some(ckpt), Some(sid)) = (opts.checkpoint, sub_id.as_deref()) {
        if let Some(saved) = ckpt.load(sid).map_err(ConsumeError::Checkpoint)? {
            next_offset = saved + 1;
        }
    }

    // Track the highest offset observed so reconnect can resume.
    let mut last_offset = next_offset.saturating_sub(1);
    let mut backoff = BACKOFF_BASE;

    loop {
        let stream = transport
            .open_feed(
                opts.shard_id,
                next_offset,
                effective_cf,
                &opts.key_prefix,
                opts.include_values,
            )
            .await;
        let mut stream = match stream {
            Ok(s) => s,
            Err(_status) => {
                next_offset = last_offset + 1;
                sleeper.sleep(backoff).await;
                backoff = next_backoff(backoff);
                continue;
            }
        };

        let mut stream_err = false;
        let mut compacted = false;
        loop {
            let item = match stream.recv().await {
                Ok(Some(item)) => item,
                Ok(None) => {
                    // Clean end-of-stream: reconnect to keep tailing.
                    next_offset = last_offset + 1;
                    break;
                }
                Err(_status) => {
                    stream_err = true;
                    break;
                }
            };

            match item {
                FeedItem::Change(change) => {
                    let offset = change.offset;
                    match handler(change).map_err(ConsumeError::Handler)? {
                        true => {}
                        false => return Ok(()),
                    }
                    if offset > last_offset {
                        last_offset = offset;
                    }
                    next_offset = last_offset + 1;
                    commit(offset)?;
                }
                FeedItem::Heartbeat(hw) => {
                    // Filtered consumer matched nothing up to this watermark;
                    // advance + commit so a later resume skips the gap.
                    if hw > last_offset {
                        last_offset = hw;
                        next_offset = hw + 1;
                        commit(hw)?;
                    }
                }
                FeedItem::Compacted {
                    earliest_offset,
                    snapshot_offset,
                } => {
                    if snapshot_offset > 0 {
                        // Rebuild baseline from a full prefix scan, then resume
                        // strictly after the snapshot watermark.
                        let mut cursor: Option<Vec<u8>> = None;
                        loop {
                            let (entries, next) = transport
                                .scan_page(&opts.key_prefix, cursor.as_deref(), 500, effective_cf)
                                .await
                                .map_err(ConsumeError::Transport)?;
                            for (key, value) in entries {
                                let change = CommittedChange {
                                    offset: snapshot_offset,
                                    term: 0,
                                    seq_in_entry: 0,
                                    cf: effective_cf,
                                    key,
                                    op: "put".to_string(),
                                    value,
                                    is_snapshot: true,
                                };
                                match handler(change).map_err(ConsumeError::Handler)? {
                                    true => {}
                                    false => return Ok(()),
                                }
                            }
                            match next {
                                Some(c) => cursor = Some(c),
                                None => break,
                            }
                        }
                        if snapshot_offset > last_offset {
                            last_offset = snapshot_offset;
                        }
                        commit(snapshot_offset)?;
                        next_offset = snapshot_offset + 1;
                    } else {
                        // Old server: no state-machine watermark available;
                        // resume at the compaction floor (pre-Phase-5a).
                        next_offset = earliest_offset;
                        last_offset = last_offset.max(earliest_offset.saturating_sub(1));
                    }
                    compacted = true;
                    break;
                }
            }
        }

        if stream_err {
            // Disconnect: resume from the next unprocessed offset after a
            // bounded exponential backoff. at-least-once on reconnect.
            next_offset = last_offset + 1;
            sleeper.sleep(backoff).await;
            backoff = next_backoff(backoff);
        } else {
            // A clean stream end / compaction reconnect is not a failure; reset
            // backoff so transient cleans don't accumulate delay.
            let _ = compacted;
            backoff = BACKOFF_BASE;
        }
    }
}

fn next_backoff(cur: Duration) -> Duration {
    let next = cur.saturating_mul(2);
    if next > BACKOFF_MAX {
        BACKOFF_MAX
    } else {
        next
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for the durable change-feed consumer (CDC Phase 5c, #824).
    //!
    //! These drive [`run_consumer`] and [`FileCheckpointStore`] against a fake
    //! transport (no live server), mirroring the canonical Python tests (#823).

    use super::*;
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};

    // ── fakes ────────────────────────────────────────────────────────────

    fn change(offset: u64, key: &[u8], value: &[u8]) -> FeedItem {
        FeedItem::Change(CommittedChange {
            offset,
            term: 1,
            seq_in_entry: 0,
            cf: 0,
            key: key.to_vec(),
            op: "put".to_string(),
            value: value.to_vec(),
            is_snapshot: false,
        })
    }

    /// One scripted stream step: either an item or a disconnect error.
    enum Step {
        Item(FeedItem),
        Err,
    }

    struct FakeStream {
        script: Vec<Step>,
        pos: usize,
    }

    #[tonic::async_trait]
    impl FeedStream for FakeStream {
        async fn recv(&mut self) -> Result<Option<FeedItem>, tonic::Status> {
            if self.pos >= self.script.len() {
                return Ok(None); // clean end-of-stream
            }
            let step = &self.script[self.pos];
            self.pos += 1;
            match step {
                Step::Item(it) => Ok(Some(it.clone())),
                Step::Err => Err(tonic::Status::unavailable("disconnect")),
            }
        }
    }

    type ScanPage = (Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>);

    struct FakeTransport {
        streams: Vec<Vec<Step>>,
        scan_pages: Vec<ScanPage>,
        requests: Arc<StdMutex<Vec<u64>>>, // recorded from_offset per open_feed
        scan_calls: Arc<StdMutex<usize>>,
    }

    #[tonic::async_trait]
    impl FeedTransport for FakeTransport {
        type Stream = FakeStream;

        async fn open_feed(
            &mut self,
            _shard_id: u64,
            from_offset: u64,
            _cf: u32,
            _key_prefix: &[u8],
            _include_values: bool,
        ) -> Result<Self::Stream, tonic::Status> {
            self.requests.lock().unwrap().push(from_offset);
            let script = if self.streams.is_empty() {
                // Nothing more scripted: a consumer that keeps looping must not
                // spin forever (tests stop via the handler first).
                vec![Step::Err]
            } else {
                self.streams.remove(0)
            };
            Ok(FakeStream { script, pos: 0 })
        }

        async fn scan_page(
            &mut self,
            _prefix: &[u8],
            _cursor: Option<&[u8]>,
            _limit: u32,
            _cf: u32,
        ) -> Result<ScanPage, tonic::Status> {
            *self.scan_calls.lock().unwrap() += 1;
            if self.scan_pages.is_empty() {
                Ok((vec![], None))
            } else {
                Ok(self.scan_pages.remove(0))
            }
        }
    }

    /// A [`Sleeper`] that records call count and never actually sleeps.
    struct NoopSleeper {
        calls: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl Sleeper for NoopSleeper {
        async fn sleep(&mut self, _dur: Duration) {
            self.calls.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Build a transport + a handler that collects up to `n` changes then stops.
    async fn collect_n(
        streams: Vec<Vec<Step>>,
        scan_pages: Vec<ScanPage>,
        opts_builder: impl for<'a> FnOnce(&'a dyn CheckpointStore) -> SubscribeCommittedOptions<'a>,
        checkpoint: &dyn CheckpointStore,
        n: usize,
    ) -> (Vec<CommittedChange>, Vec<u64>, usize) {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let scan_calls = Arc::new(StdMutex::new(0usize));
        let mut transport = FakeTransport {
            streams,
            scan_pages,
            requests: requests.clone(),
            scan_calls: scan_calls.clone(),
        };
        let mut sleeper = NoopSleeper {
            calls: Arc::new(AtomicUsize::new(0)),
        };
        let collected = Arc::new(StdMutex::new(Vec::new()));
        let collected2 = collected.clone();
        let opts = opts_builder(checkpoint);
        let res: Result<(), ConsumeError<Infallible>> =
            run_consumer(&mut transport, &mut sleeper, opts, 0, move |ch| {
                let mut c = collected2.lock().unwrap();
                c.push(ch);
                Ok(c.len() < n)
            })
            .await;
        res.expect("consumer should stop cleanly");
        let out = collected.lock().unwrap().clone();
        let reqs = requests.lock().unwrap().clone();
        let scans = *scan_calls.lock().unwrap();
        (out, reqs, scans)
    }

    /// Create an isolated per-test directory and a checkpoint path inside it, so
    /// the "no leftover temp file" assertion never sees another test's files.
    fn tmp_ckpt(name: &str) -> (FileCheckpointStore, std::path::PathBuf) {
        let mut dir = std::env::temp_dir();
        dir.push(format!("statelet-cdc-test-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("ck.json");
        (FileCheckpointStore::open(&p).unwrap(), p)
    }

    // ── FileCheckpointStore ──────────────────────────────────────────────

    #[test]
    fn file_checkpoint_roundtrip() {
        let (store, p) = tmp_ckpt("roundtrip");
        assert_eq!(store.load("sub-a").unwrap(), None);
        store.commit("sub-a", 7).unwrap();
        store.commit("sub-b", 11).unwrap();
        assert_eq!(store.load("sub-a").unwrap(), Some(7));
        assert_eq!(store.load("sub-b").unwrap(), Some(11));
        // Re-open from disk: state survives.
        let store2 = FileCheckpointStore::open(&p).unwrap();
        assert_eq!(store2.load("sub-a").unwrap(), Some(7));
        assert_eq!(store2.load("sub-b").unwrap(), Some(11));
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn file_checkpoint_atomic_no_tmp_leftover() {
        let (store, p) = tmp_ckpt("atomic");
        store.commit("s", 3).unwrap();
        store.commit("s", 4).unwrap();
        let dir = p.parent().unwrap();
        let leftovers: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with(".ckpt-"))
            .collect();
        assert!(leftovers.is_empty(), "leftover temp files: {leftovers:?}");
        assert_eq!(
            FileCheckpointStore::open(&p).unwrap().load("s").unwrap(),
            Some(4)
        );
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn file_checkpoint_special_chars_in_subscription_id() {
        // A subscription_id with comma / colon / quote / backslash must
        // round-trip through disk without corrupting the rest of the map. A
        // naive comma/colon split would have dropped every entry on reload.
        let (store, p) = tmp_ckpt("special-chars");
        let weird = r#"tenant:42,topic="orders\path""#;
        store.commit(weird, 9).unwrap();
        store.commit("plain", 3).unwrap();
        let reopened = FileCheckpointStore::open(&p).unwrap();
        assert_eq!(reopened.load(weird).unwrap(), Some(9));
        assert_eq!(reopened.load("plain").unwrap(), Some(3));
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn file_checkpoint_corrupt_file_is_empty() {
        let (_unused, p) = tmp_ckpt("corrupt");
        std::fs::write(&p, b"{ this is not json").unwrap();
        let store = FileCheckpointStore::open(&p).unwrap();
        assert_eq!(store.load("s").unwrap(), None);
    }

    // ── start-offset resolution ──────────────────────────────────────────

    #[tokio::test]
    async fn start_from_checkpoint_plus_one() {
        let (ck, p) = tmp_ckpt("ckpt-plus-one");
        ck.commit("sub", 41).unwrap();
        let (_out, reqs, _) = collect_n(
            vec![vec![Step::Item(change(42, b"k", b""))]],
            vec![],
            |c| SubscribeCommittedOptions {
                subscription_id: Some("sub".to_string()),
                checkpoint: Some(c),
                ..Default::default()
            },
            &ck,
            1,
        )
        .await;
        assert_eq!(reqs[0], 42, "from_offset should be checkpoint(41)+1");
        let _ = std::fs::remove_file(&p);
    }

    #[tokio::test]
    async fn start_from_offset_when_no_checkpoint() {
        let (ck, p) = tmp_ckpt("from-offset");
        let (_out, reqs, _) = collect_n(
            vec![vec![Step::Item(change(100, b"k", b""))]],
            vec![],
            |c| SubscribeCommittedOptions {
                from_offset: 100,
                subscription_id: Some("sub".to_string()),
                checkpoint: Some(c),
                ..Default::default()
            },
            &ck,
            1,
        )
        .await;
        assert_eq!(reqs[0], 100);
        let _ = std::fs::remove_file(&p);
    }

    // ── change delivery + auto-commit ────────────────────────────────────

    #[tokio::test]
    async fn change_delivered_and_committed_after_handler() {
        let (ck, p) = tmp_ckpt("delivered");
        let (out, _reqs, _) = collect_n(
            vec![vec![
                Step::Item(change(5, b"a", b"v")),
                Step::Item(change(6, b"b", b"")),
            ]],
            vec![],
            |c| SubscribeCommittedOptions {
                subscription_id: Some("sub".to_string()),
                checkpoint: Some(c),
                ..Default::default()
            },
            &ck,
            2,
        )
        .await;
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].offset, 5);
        assert_eq!(out[0].key, b"a");
        assert_eq!(out[0].op, "put");
        assert_eq!(out[0].value, b"v");
        assert!(!out[0].is_snapshot);
        // Offset 5 committed after its handler ran (before the stop on 6).
        assert_eq!(ck.load("sub").unwrap(), Some(5));
        let _ = std::fs::remove_file(&p);
    }

    #[tokio::test]
    async fn no_commit_when_auto_commit_false() {
        let (ck, p) = tmp_ckpt("no-commit");
        collect_n(
            vec![vec![
                Step::Item(change(5, b"a", b"")),
                Step::Item(change(6, b"b", b"")),
            ]],
            vec![],
            |c| SubscribeCommittedOptions {
                subscription_id: Some("sub".to_string()),
                checkpoint: Some(c),
                auto_commit: false,
                ..Default::default()
            },
            &ck,
            2,
        )
        .await;
        assert_eq!(ck.load("sub").unwrap(), None);
        let _ = std::fs::remove_file(&p);
    }

    // ── heartbeat ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn heartbeat_advances_and_commits_without_delivery() {
        let (ck, p) = tmp_ckpt("heartbeat");
        let (out, _reqs, _) = collect_n(
            vec![vec![
                Step::Item(FeedItem::Heartbeat(50)),
                Step::Item(change(51, b"k", b"")),
            ]],
            vec![],
            |c| SubscribeCommittedOptions {
                subscription_id: Some("sub".to_string()),
                checkpoint: Some(c),
                ..Default::default()
            },
            &ck,
            1,
        )
        .await;
        assert_eq!(out[0].offset, 51, "heartbeat is not delivered");
        // Heartbeat committed its watermark before the change was delivered.
        assert_eq!(ck.load("sub").unwrap(), Some(50));
        let _ = std::fs::remove_file(&p);
    }

    // ── compaction bootstrap ─────────────────────────────────────────────

    #[tokio::test]
    async fn compacted_triggers_bootstrap_scan() {
        let (ck, p) = tmp_ckpt("compacted");
        let (out, reqs, scans) = collect_n(
            vec![
                vec![Step::Item(FeedItem::Compacted {
                    earliest_offset: 10,
                    snapshot_offset: 20,
                })],
                vec![Step::Item(change(21, b"live", b""))],
            ],
            vec![
                (
                    vec![(b"k1".to_vec(), b"v1".to_vec())],
                    Some(b"cursor1".to_vec()),
                ),
                (vec![(b"k2".to_vec(), b"v2".to_vec())], None),
            ],
            |c| SubscribeCommittedOptions {
                subscription_id: Some("sub".to_string()),
                checkpoint: Some(c),
                key_prefix: b"k".to_vec(),
                cf: Some(0),
                ..Default::default()
            },
            &ck,
            3,
        )
        .await;
        // Two synthetic snapshot puts then the live tail.
        assert!(out[0].is_snapshot && out[0].op == "put" && out[0].offset == 20);
        assert_eq!(
            (out[0].key.as_slice(), out[0].value.as_slice()),
            (b"k1".as_ref(), b"v1".as_ref())
        );
        assert!(out[1].is_snapshot && out[1].offset == 20 && out[1].key == b"k2");
        assert!(out[2].offset == 21 && !out[2].is_snapshot);
        assert_eq!(
            ck.load("sub").unwrap(),
            Some(20),
            "snapshot_offset committed"
        );
        assert_eq!(reqs[1], 21, "resume from snapshot_offset+1");
        assert_eq!(scans, 2, "two scan pages");
        let _ = std::fs::remove_file(&p);
    }

    #[tokio::test]
    async fn compacted_old_server_resumes_at_earliest() {
        let (ck, p) = tmp_ckpt("old-server");
        let (out, reqs, scans) = collect_n(
            vec![
                vec![Step::Item(FeedItem::Compacted {
                    earliest_offset: 99,
                    snapshot_offset: 0,
                })],
                vec![Step::Item(change(99, b"k", b""))],
            ],
            vec![],
            |c| SubscribeCommittedOptions {
                subscription_id: Some("sub".to_string()),
                checkpoint: Some(c),
                ..Default::default()
            },
            &ck,
            1,
        )
        .await;
        assert_eq!(out[0].offset, 99);
        assert_eq!(scans, 0, "old-server path must not bootstrap scan");
        assert_eq!(reqs[1], 99, "resume at earliest_offset");
        let _ = std::fs::remove_file(&p);
    }

    // ── reconnect ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn stream_error_reconnects_from_last_plus_one() {
        let (ck, p) = tmp_ckpt("reconnect");
        let (out, reqs, _) = collect_n(
            vec![
                vec![Step::Item(change(5, b"a", b"")), Step::Err],
                vec![Step::Item(change(6, b"b", b""))],
            ],
            vec![],
            |c| SubscribeCommittedOptions {
                subscription_id: Some("sub".to_string()),
                checkpoint: Some(c),
                ..Default::default()
            },
            &ck,
            2,
        )
        .await;
        assert_eq!(out[0].offset, 5);
        assert_eq!(out[1].offset, 6);
        assert_eq!(reqs[1], 6, "reconnect from last+1");
        let _ = std::fs::remove_file(&p);
    }
}
