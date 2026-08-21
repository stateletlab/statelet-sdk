"""Statelet Python client wrapping the Statelet gRPC service."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator, List, Optional

import json
import logging
import os
import tempfile
import time
import urllib.request

import grpc

try:
    # Python 3.8+: typing.Protocol
    from typing import Protocol, runtime_checkable
except ImportError:  # pragma: no cover - very old interpreters
    from typing_extensions import Protocol, runtime_checkable  # type: ignore

from statelet import statelet_pb2 as pb
from statelet import statelet_pb2_grpc as pb_grpc

_LOG = logging.getLogger("statelet.client")

def _conflict_policy_enum(policy: str) -> int:
    """Map a conflict-policy name to the ``ConflictPolicy`` enum used by the
    search RPCs (#783). The ``ResolveConflict`` RPC takes a free-form string
    instead, so this is only for ``text_graph_search`` / ``graph_query_edges``.

    Unknown names map to ``CONFLICT_POLICY_UNSPECIFIED`` (0) so the gateway
    falls back to its default rather than erroring. ``quorum`` has no search
    enum value (it is only meaningful for the explicit ``ResolveConflict`` RPC)
    and likewise maps to UNSPECIFIED here.
    """
    return {
        "": pb.CONFLICT_POLICY_UNSPECIFIED,
        "recency": pb.RECENCY,
        "trust": pb.TRUST,
        "confidence": pb.CONFIDENCE,
    }.get(policy.strip().lower(), pb.CONFLICT_POLICY_UNSPECIFIED)


def _graph_query_value(value: "pb.GraphQueryValue") -> Any:
    """Decode one ``GraphQueryValue`` union member to a plain Python value.

    ``JSON`` carries the hydrated ``ROLE_NodeProp`` blob for a whole node; it is
    parsed into a dict/list, falling back to the raw bytes when the payload is
    not valid JSON (an empty blob included) so nothing is silently dropped.
    """
    kind = value.kind
    if kind == pb.GraphQueryValue.INT:
        return value.int_value
    if kind == pb.GraphQueryValue.DOUBLE:
        return value.dbl_value
    if kind == pb.GraphQueryValue.STRING:
        return value.str_value
    if kind == pb.GraphQueryValue.BOOL:
        return value.bool_value
    if kind == pb.GraphQueryValue.JSON:
        try:
            return json.loads(value.json_value)
        except (ValueError, TypeError):
            return value.json_value
    return None


_DEFAULT_GRPC_MAX_MESSAGE_BYTES = 64 * 1024 * 1024
_DEFAULT_GRPC_CHANNEL_OPTIONS = (
    ("grpc.max_receive_message_length", _DEFAULT_GRPC_MAX_MESSAGE_BYTES),
    ("grpc.max_send_message_length", _DEFAULT_GRPC_MAX_MESSAGE_BYTES),
)


class _AuthInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    """Injects ``Authorization: Bearer <token>`` into every gRPC call."""

    def __init__(self, token: str) -> None:
        self._metadata = [("authorization", f"Bearer {token}")]

    def _add_metadata(self, client_call_details):
        metadata = list(client_call_details.metadata or [])
        metadata.extend(self._metadata)
        return client_call_details._replace(metadata=metadata)

    def intercept_unary_unary(self, continuation, client_call_details, request):
        return continuation(self._add_metadata(client_call_details), request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        return continuation(self._add_metadata(client_call_details), request)

    def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        return continuation(self._add_metadata(client_call_details), request_iterator)

    def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        return continuation(self._add_metadata(client_call_details), request_iterator)


# ── Durable change-feed (CDC) — issue #823 (CDC epic #692, Phase 5b) ──────────


@runtime_checkable
class CheckpointStore(Protocol):
    """Pluggable store for a consumer's last fully-processed CDC offset.

    Implements Kafka-style client-managed offsets: the consumer commits the
    offset *after* a change has been processed (at-least-once). Apps may supply
    a custom store that persists the offset transactionally with their own sink
    to achieve exactly-once-sink semantics.
    """

    def load(self, subscription_id: str) -> Optional[int]:
        """Return the last fully-processed offset for *subscription_id*.

        Returns ``None`` when no checkpoint has been committed yet.
        """
        ...

    def commit(self, subscription_id: str, offset: int) -> None:
        """Durably record *offset* as fully processed for *subscription_id*."""
        ...


class FileCheckpointStore:
    """Default :class:`CheckpointStore` backed by an atomically-written JSON file.

    The file maps ``subscription_id -> offset``. Each commit rewrites the file
    via a temp file + :func:`os.replace`, which is atomic on POSIX and Windows,
    so a crash between the write and the rename leaves the previous valid offset
    intact (never a torn/partial file).
    """

    def __init__(self, path: str) -> None:
        self.path = path
        self._state: dict = {}
        self._load_file()

    def _load_file(self) -> None:
        try:
            with open(self.path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                self._state = {str(k): int(v) for k, v in data.items()}
        except FileNotFoundError:
            self._state = {}
        except (ValueError, OSError):
            # Corrupt or unreadable file: start empty rather than crash. A prior
            # atomic commit would never produce this; treat it as "no checkpoint".
            self._state = {}

    def load(self, subscription_id: str) -> Optional[int]:
        return self._state.get(subscription_id)

    def commit(self, subscription_id: str, offset: int) -> None:
        self._state[subscription_id] = int(offset)
        directory = os.path.dirname(os.path.abspath(self.path)) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=directory, prefix=".ckpt-", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(self._state, fh)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise


@dataclass
class CommittedChange:
    """A single change delivered by :meth:`StateletClient.subscribe_committed`.

    ``offset`` is the stable Raft log index (the resume key). ``is_snapshot`` is
    ``True`` for synthetic ``put`` changes produced by a bootstrap ``Scan`` after
    the requested offset was compacted away; those all carry the same
    ``snapshot_offset`` and ``term=0``/``seq_in_entry=0``.
    """

    offset: int
    term: int
    seq_in_entry: int
    cf: int
    key: bytes
    op: str
    value: bytes
    is_snapshot: bool = False


@dataclass
class VectorSearchResult:
    """A single nearest-neighbor result.

    ``group_key`` carries the field-collapse group key when the search used
    ``group_field`` (see :meth:`StateletClient.vector_search`); it is an empty
    string when grouping is off.
    """

    id: int
    distance: float
    group_key: str = ""


@dataclass
class RerankSpec:
    """Optional second-stage reranker for :meth:`StateletClient.vector_search`.

    Mirrors Weaviate ``.with_additional({rerank: {property, query}})`` and
    Pinecone ``inference.rerank(model, query, documents, rank_fields)``:

    * ``model="cross-encoder"`` runs a loaded cross-encoder over passages
      hydrated from the KV store via ``passage_field`` (a key template with
      ``{id}`` / ``{index}`` tokens, e.g. ``"doc:{index}:{id}:text"``) using
      ``query_text``. Requires a reranker loaded on the gateway; otherwise the
      request auto-downgrades to score-fusion.
    * ``model="score-fusion"`` (the default) is LLM-free: it re-sorts the
      over-fetched window by ``signal_blend * norm_distance +
      (1 - signal_blend) * aux_signal``. With ``0 < signal_blend < 1`` on a
      quantized index the gateway fetches the full-precision vector and blends
      the exact distance (Qdrant prefetch→rescore lift); ``signal_blend = 1.0``
      is a pure re-sort.

    Set ``validate_only=True`` for a dry-run pre-flight: the gateway validates
    the spec (``passage_field`` template + reranker availability) and returns an
    empty success on a valid spec or an ``InvalidArgument`` error on a bad one,
    without executing the search. Prefer :meth:`StateletClient.rerank_validate`.
    """

    enabled: bool = True
    rerank_k: int = 0
    model: str = "score-fusion"
    passage_field: str = ""
    signal_blend: float = 0.0
    query_text: str = ""
    validate_only: bool = False

    def _to_proto(self) -> "pb.RerankSpec":
        spec = pb.RerankSpec(
            enabled=self.enabled,
            rerank_k=self.rerank_k,
            model=self.model,
            passage_field=self.passage_field,
            signal_blend=self.signal_blend,
            query_text=self.query_text,
        )
        # validate_only is an additive field (proto field 7). Set it only when
        # the generated stub already carries it so a client built against an
        # older `statelet_pb2` (not yet regenerated) keeps working.
        if self.validate_only:
            if not hasattr(spec, "validate_only"):
                raise RuntimeError(
                    "validate_only requested but the generated statelet_pb2 "
                    "lacks the field; regenerate the protobuf stub (make proto)."
                )
            spec.validate_only = True
        return spec


@dataclass
class VectorIndexConfig:
    """HNSW vector index configuration."""

    dim: int
    metric: str = "l2"  # "l2", "cosine", or "inner_product"
    m: int = 16
    m_max0: int = 0
    ef_construction: int = 200
    ef_search: int = 64

    def _to_proto(self) -> pb.VectorIndexConfig:
        metric_map = {
            "l2": pb.VECTOR_L2,
            "cosine": pb.VECTOR_COSINE,
            "inner_product": pb.VECTOR_INNER_PRODUCT,
        }
        return pb.VectorIndexConfig(
            dim=self.dim,
            metric=metric_map.get(self.metric, pb.VECTOR_L2),
            m=self.m,
            m_max0=self.m_max0,
            ef_construction=self.ef_construction,
            ef_search=self.ef_search,
        )


class StateletClient:
    """Synchronous gRPC client for Statelet.

    Usage::

        client = StateletClient("127.0.0.1:7379")
        client.put(b"hello", b"world")
        value = client.get(b"hello")   # b"world"
        client.delete(b"hello")
        client.close()

    Can also be used as a context manager::

        with StateletClient("127.0.0.1:7379") as client:
            client.put(b"key", b"value")
    """

    DEFAULT_PING_TIMEOUT_S = 5.0
    DEFAULT_KV_TIMEOUT_S = 10.0
    DEFAULT_GRAPH_ADMIN_TIMEOUT_S = 30.0
    DEFAULT_TEXT_GRAPH_PUT_TIMEOUT_S = 90.0
    DEFAULT_TEXT_GRAPH_SEARCH_TIMEOUT_S = 60.0

    def __init__(
        self,
        addr: str,
        *,
        cf: int = 0,
        token: Optional[str] = None,
        ping_timeout_s: float = DEFAULT_PING_TIMEOUT_S,
        kv_timeout_s: float = DEFAULT_KV_TIMEOUT_S,
        graph_admin_timeout_s: float = DEFAULT_GRAPH_ADMIN_TIMEOUT_S,
        text_graph_put_timeout_s: float = DEFAULT_TEXT_GRAPH_PUT_TIMEOUT_S,
        text_graph_search_timeout_s: float = DEFAULT_TEXT_GRAPH_SEARCH_TIMEOUT_S,
    ) -> None:
        """Connect to a Statelet node.

        Args:
            addr: gRPC address, e.g. ``"127.0.0.1:7379"``.
            cf: Default column family id (0 = default user CF).
            token: JWT token for authenticated gateways.
        """
        channel = grpc.insecure_channel(addr, options=_DEFAULT_GRPC_CHANNEL_OPTIONS)
        if token:
            interceptor = _AuthInterceptor(token)
            channel = grpc.intercept_channel(channel, interceptor)
        self._channel = channel
        self._stub = pb_grpc.StateletStub(self._channel)
        self._cf = cf
        self._ping_timeout_s = ping_timeout_s
        self._kv_timeout_s = kv_timeout_s
        self._graph_admin_timeout_s = graph_admin_timeout_s
        self._text_graph_put_timeout_s = text_graph_put_timeout_s
        self._text_graph_search_timeout_s = text_graph_search_timeout_s

    # ── context manager ─────────────────────────────────────────────────

    def __enter__(self) -> "StateletClient":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying gRPC channel."""
        self._channel.close()

    @staticmethod
    def login(mgmt_addr: str, username: str, password: str) -> str:
        """Authenticate against the gateway management API and return a JWT token.

        Args:
            mgmt_addr: Management HTTP address, e.g. ``"127.0.0.1:9380"``.
            username: Login username.
            password: Login password.

        Returns:
            JWT token string.
        """
        url = f"http://{mgmt_addr}/api/v1/auth/login"
        payload = json.dumps({"username": username, "password": password}).encode()
        req = urllib.request.Request(
            url, data=payload, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read())
        return body["token"]

    # ── KV operations ───────────────────────────────────────────────────

    def ping(self) -> str:
        """Liveness check. Returns ``"PONG"``."""
        resp = self._stub.Ping(pb.PingRequest(), timeout=self._ping_timeout_s)
        return resp.message

    def put(self, key: bytes, value: bytes, *, cf: Optional[int] = None) -> None:
        """Write a single key-value pair."""
        self._stub.Put(
            pb.PutRequest(cf=cf if cf is not None else self._cf, key=key, value=value),
            timeout=self._kv_timeout_s,
        )

    def get(self, key: bytes, *, cf: Optional[int] = None) -> Optional[bytes]:
        """Read the value for *key*. Returns ``None`` if not found."""
        resp = self._stub.Get(
            pb.GetRequest(cf=cf if cf is not None else self._cf, key=key),
            timeout=self._kv_timeout_s,
        )
        return resp.value if resp.found else None

    def delete(self, key: bytes, *, cf: Optional[int] = None) -> None:
        """Delete a key."""
        self._stub.Delete(
            pb.DeleteRequest(cf=cf if cf is not None else self._cf, key=key),
            timeout=self._kv_timeout_s,
        )

    def merge(self, key: bytes, value: bytes, *, cf: Optional[int] = None) -> None:
        """Merge an operand into the existing value."""
        self._stub.Merge(
            pb.MergeRequest(cf=cf if cf is not None else self._cf, key=key, value=value),
            timeout=self._kv_timeout_s,
        )

    def batch_write(self, entries: list) -> None:
        """Atomically apply a batch of writes.

        Each entry is a dict with keys: ``op`` (``"put"``, ``"delete"``, ``"merge"``),
        ``key`` (bytes), ``value`` (bytes, optional for delete), ``cf`` (int, optional).
        """
        op_map = {"put": pb.PUT, "delete": pb.DELETE, "merge": pb.MERGE}
        proto_entries = []
        for i, e in enumerate(entries):
            if not isinstance(e, dict):
                raise TypeError(
                    f"batch_write entries must be dicts like "
                    f'{{"op": "put", "key": b"k", "value": b"v"}}; '
                    f"entry {i} is {type(e).__name__}"
                )
            if e.get("op") not in op_map:
                raise ValueError(
                    f'entry {i}: "op" must be one of {sorted(op_map)}, got {e.get("op")!r}'
                )
            proto_entries.append(
                pb.WriteEntry(
                    cf=e.get("cf", self._cf),
                    op=op_map[e["op"]],
                    key=e["key"],
                    value=e.get("value", b""),
                )
            )
        self._stub.BatchWrite(
            pb.BatchWriteRequest(entries=proto_entries),
            timeout=self._kv_timeout_s,
        )

    def scan(
        self,
        prefix: bytes = b"",
        *,
        cursor: Optional[bytes] = None,
        limit: int = 100,
        cf: Optional[int] = None,
    ) -> tuple:
        """Scan keys with an optional prefix filter.

        Returns ``(entries, next_cursor)`` where *entries* is a list of
        ``(key, value)`` tuples and *next_cursor* is ``None`` when there are
        no more results.
        """
        resp = self._stub.Scan(
            pb.ScanRequest(
                cf=cf if cf is not None else self._cf,
                prefix=prefix,
                cursor=cursor or b"",
                limit=limit,
            ),
            timeout=self._kv_timeout_s,
        )
        entries = [(e.key, e.value) for e in resp.entries]
        next_cursor = resp.next_cursor if resp.next_cursor else None
        return entries, next_cursor

    def delete_by_prefix(self, prefix: bytes, *, cf: Optional[int] = None) -> int:
        """Delete all keys matching *prefix*. Returns the number of keys deleted."""
        resp = self._stub.DeleteByPrefix(
            pb.DeleteByPrefixRequest(
                cf=cf if cf is not None else self._cf,
                prefix=prefix,
            ),
            timeout=self._kv_timeout_s,
        )
        return resp.deleted

    # ── Durable change-feed (CDC) — issue #823 ──────────────────────────

    # Bounded exponential backoff for reconnect (seconds).
    _CDC_BACKOFF_BASE_S = 0.5
    _CDC_BACKOFF_MAX_S = 30.0

    def subscribe_committed(
        self,
        from_offset: int = 0,
        *,
        cf: Optional[int] = None,
        key_prefix: bytes = b"",
        include_values: bool = True,
        subscription_id: Optional[str] = None,
        checkpoint: Optional[CheckpointStore] = None,
        auto_commit: bool = True,
        shard_id: int = 1,
    ) -> Iterator[CommittedChange]:
        """Consume the durable, ordered, resumable committed change-feed (CDC).

        Requires an enterprise (statelet-ee) server build — the core build
        (including the ``statelet-lite`` PyPI binary) answers UNIMPLEMENTED,
        which this iterator raises immediately.

        Yields :class:`CommittedChange` items in stable Raft-offset order. Offsets
        are client-managed (Kafka-style): supply a ``subscription_id`` and a
        ``checkpoint`` store to resume across restarts. With ``auto_commit=True``
        (default) each change's ``offset`` is committed *after* it is yielded —
        i.e. once your loop body for that item completes — giving at-least-once
        delivery. Set ``auto_commit=False`` to commit offsets yourself via
        ``checkpoint.commit(subscription_id, change.offset)``.

        Start offset resolution: if both ``subscription_id`` and ``checkpoint``
        are given and a checkpoint exists, the stream starts at
        ``checkpoint.load(sid) + 1``; otherwise it starts at ``from_offset``.

        Resilience:
          * On disconnect / :class:`grpc.RpcError`, the stream reconnects from
            ``last_offset + 1`` (the server replays from the durable log) with
            bounded exponential backoff. The generator runs forever; break out
            of the consuming ``for`` loop to stop.
          * On a compaction notice (the requested offset fell below the
            compaction floor), the feed *bootstraps*: it pages a full
            :meth:`scan` over ``key_prefix`` (within ``cf``), emitting each
            ``(key, value)`` as a synthetic ``put`` ``CommittedChange`` with
            ``is_snapshot=True`` and ``offset=snapshot_offset``, commits
            ``snapshot_offset``, then resumes live-tail from
            ``snapshot_offset + 1``. Against an old server that reports
            ``snapshot_offset == 0`` it falls back to resuming at
            ``earliest_offset``.

        Args:
            from_offset: First Raft offset to deliver when no checkpoint applies
                (``0`` = live-only from the current commit index).
            cf: Column family filter (``None`` = the client's default CF;
                pass ``0`` explicitly only if that is your intent).
            key_prefix: Restrict the feed (and bootstrap scan) to this prefix.
            include_values: Include value bytes for put/merge changes.
            subscription_id: Logical consumer id used as the checkpoint key.
            checkpoint: A :class:`CheckpointStore` (e.g. :class:`FileCheckpointStore`).
            auto_commit: Commit each offset after the item is processed.
            shard_id: Target shard's id. Shard ids are numbered **from 1**;
                the default subscribes to shard 1 (the only shard on a fresh
                single-node deployment). There is no shard 0 — servers reject
                it, and on older servers it hung in the reconnect loop forever.
        """
        effective_cf = cf if cf is not None else self._cf
        do_commit = (
            checkpoint is not None
            and subscription_id is not None
            and auto_commit
        )

        # Resolve the resume start: checkpoint+1 wins over from_offset.
        next_offset = from_offset
        if checkpoint is not None and subscription_id is not None:
            saved = checkpoint.load(subscription_id)
            if saved is not None:
                next_offset = saved + 1

        # Track the highest offset we have observed so reconnect can resume.
        last_offset = next_offset - 1 if next_offset > 0 else 0
        backoff = self._CDC_BACKOFF_BASE_S
        retries = 0

        while True:
            try:
                request = pb.SubscribeCommittedRequest(
                    shard_id=shard_id,
                    from_offset=next_offset,
                    cf=effective_cf,
                    key_prefix=key_prefix,
                    include_values=include_values,
                )
                stream = self._stub.SubscribeCommitted(request)
                for item in stream:
                    kind = item.WhichOneof("item")
                    if kind == "change":
                        c = item.change
                        change = CommittedChange(
                            offset=c.offset,
                            term=c.term,
                            seq_in_entry=c.seq_in_entry,
                            cf=c.cf,
                            key=c.key,
                            op=c.op,
                            value=c.value,
                            is_snapshot=False,
                        )
                        yield change
                        last_offset = max(last_offset, c.offset)
                        next_offset = last_offset + 1
                        if do_commit:
                            checkpoint.commit(subscription_id, c.offset)
                    elif kind == "heartbeat":
                        # Filtered consumer matched nothing up to this watermark;
                        # advance + commit so a later resume skips the gap.
                        hw = item.heartbeat
                        if hw > last_offset:
                            last_offset = hw
                            next_offset = hw + 1
                            if do_commit:
                                checkpoint.commit(subscription_id, hw)
                    elif kind == "compacted":
                        snapshot_offset = item.compacted.snapshot_offset
                        earliest_offset = item.compacted.earliest_offset
                        if snapshot_offset > 0:
                            # Rebuild baseline from a full prefix scan, then
                            # resume strictly after the snapshot watermark.
                            yield from self._bootstrap_scan(
                                key_prefix, effective_cf, snapshot_offset
                            )
                            last_offset = max(last_offset, snapshot_offset)
                            if do_commit:
                                checkpoint.commit(subscription_id, snapshot_offset)
                            next_offset = snapshot_offset + 1
                        else:
                            # Old server: no state-machine watermark available;
                            # resume at the compaction floor (pre-5a behavior).
                            next_offset = earliest_offset
                            last_offset = max(last_offset, earliest_offset - 1)
                        break  # reconnect from the resolved next_offset
                else:
                    # Server closed the stream cleanly (no error). Reconnect from
                    # the next offset to continue tailing.
                    next_offset = last_offset + 1

                # A clean stream end / compaction reconnect is not a failure;
                # reset backoff so transient cleans don't accumulate delay.
                backoff = self._CDC_BACKOFF_BASE_S
                retries = 0
            except grpc.RpcError as e:
                # Only transient transport conditions are worth a silent
                # reconnect. Permanent conditions used to be swallowed here
                # too, which made a core (non-enterprise) server's
                # UNIMPLEMENTED look like an eternally-empty feed (API audit
                # 2026-08-20 item 6) — surface those immediately.
                _PERMANENT = (
                    grpc.StatusCode.UNIMPLEMENTED,
                    grpc.StatusCode.UNAUTHENTICATED,
                    grpc.StatusCode.PERMISSION_DENIED,
                    grpc.StatusCode.INVALID_ARGUMENT,
                    grpc.StatusCode.FAILED_PRECONDITION,
                    grpc.StatusCode.NOT_FOUND,
                )
                # A bare grpc.RpcError carries no status: code()/details()
                # live on the grpc.Call mixin, which real channel errors have
                # but plain RpcError subclasses (test fakes, some
                # interceptors) do not. Treat a status-less error as
                # transient — only a definite permanent code justifies
                # killing a feed that reconnection could revive.
                code = e.code() if callable(getattr(e, "code", None)) else None
                if code in _PERMANENT:
                    raise
                # Disconnect: resume from the next unprocessed offset after a
                # bounded exponential backoff. at-least-once on reconnect.
                # Never silently: an endless-retry condition (e.g. a shard id
                # the server keeps answering UNAVAILABLE for) used to hang
                # with zero output — log the first retry and every tenth.
                retries += 1
                if retries == 1 or retries % 10 == 0:
                    details = (
                        e.details()
                        if callable(getattr(e, "details", None))
                        else str(e)
                    )
                    _LOG.warning(
                        "subscribe_committed(shard_id=%s) reconnecting "
                        "(attempt %d, backoff %.1fs) after %s: %s",
                        shard_id,
                        retries,
                        backoff,
                        code,
                        details,
                    )
                next_offset = last_offset + 1
                time.sleep(backoff)
                backoff = min(backoff * 2.0, self._CDC_BACKOFF_MAX_S)

    def _bootstrap_scan(
        self, key_prefix: bytes, cf: int, snapshot_offset: int
    ) -> Iterator[CommittedChange]:
        """Page the whole prefix and emit synthetic snapshot ``put`` changes."""
        cursor: Optional[bytes] = None
        while True:
            entries, cursor = self.scan(
                key_prefix, cursor=cursor, limit=500, cf=cf
            )
            for key, value in entries:
                yield CommittedChange(
                    offset=snapshot_offset,
                    term=0,
                    seq_in_entry=0,
                    cf=cf,
                    key=key,
                    op="put",
                    value=value,
                    is_snapshot=True,
                )
            if cursor is None:
                break

    # ── Vector operations ───────────────────────────────────────────────

    def create_vector_index(self, name: str, config: VectorIndexConfig) -> None:
        """Create or reconfigure an HNSW vector index."""
        self._stub.CreateVectorIndex(
            pb.CreateVectorIndexRequest(index_name=name, config=config._to_proto()),
            timeout=self._graph_admin_timeout_s,
        )

    def drop_vector_index(self, name: str) -> None:
        """Drop an HNSW vector index."""
        self._stub.DropVectorIndex(
            pb.DropVectorIndexRequest(index_name=name),
            timeout=self._graph_admin_timeout_s,
        )

    def vector_put(self, index_name: str, vector_id: int, vector: List[float]) -> None:
        """Insert or update a vector in the index."""
        self._stub.VectorPut(
            pb.VectorPutRequest(index_name=index_name, vector_id=vector_id, vector=vector),
            timeout=self._kv_timeout_s,
        )

    def vector_delete(self, index_name: str, vector_id: int) -> None:
        """Remove a vector from the index."""
        self._stub.VectorDelete(
            pb.VectorDeleteRequest(index_name=index_name, vector_id=vector_id),
            timeout=self._kv_timeout_s,
        )

    def vector_search(
        self,
        index_name: str,
        query: List[float],
        k: int,
        *,
        ef_search: int = 0,
        mmr: bool = False,
        mmr_lambda: float = 0.0,
        mmr_pool: int = 0,
        rerank: Optional[RerankSpec] = None,
        group_field: str = "",
        group_size: int = 0,
        groups: int = 0,
        group_overfetch: int = 0,
        group_missing_as_own: bool = False,
    ) -> List[VectorSearchResult]:
        """Approximate nearest neighbor search.

        Set ``mmr=True`` to enable MMR (maximal marginal relevance) diversity
        reranking as a server-side post-step: the index is over-fetched and the
        top ``k`` are greedily reselected to balance query relevance against
        diversity, so RAG context-packing stops returning near-duplicates.

        ``mmr_lambda`` (0..1, ``0`` ⇒ default 0.5) is the relevance↔diversity
        trade-off (1 ⇒ plain top-k, 0 ⇒ pure diversity); ``mmr_pool`` (``0`` ⇒
        default 4) is the over-fetch multiplier (``fetch_k = k * mmr_pool``).
        The returned ``distance`` is always the original query distance; only
        the selected set and its order change.

        Set ``group_field`` to enable result grouping / field-collapse: return
        at most ``group_size`` (``0`` ⇒ 1, one-best-per-group) hits per distinct
        value of the payload attribute ``group_field``, for up to ``groups``
        (``0`` ⇒ ``k``) distinct group keys, ordered by ascending distance.
        ``group_overfetch`` (``0`` ⇒ default 4, capped server-side) is the
        candidate over-fetch multiplier. Each result's group value is surfaced
        on :attr:`VectorSearchResult.group_key`. Grouping is mutually exclusive
        with ``mmr`` (requesting both raises ``INVALID_ARGUMENT``). Grouping is
        exact on single-node / single-shard deployments.

        By default candidates missing ``group_field`` are dropped from grouped
        results. Set ``group_missing_as_own=True`` to instead return each
        missing-field candidate as its own singleton group (empty ``group_key``),
        counting against the ``groups`` cap.

        Pass a :class:`RerankSpec` as ``rerank`` to enable the optional
        second-stage reranker (cross-encoder or model-free score-fusion). See
        :class:`RerankSpec` and ``docs/reranking.md`` for the two models and
        ``signal_blend`` semantics.
        

        NOTE: ``mmr`` / ``mmr_lambda`` / ``mmr_pool`` / ``rerank`` are not yet
        implemented for unified vector indexes — the server answers
        UNIMPLEMENTED until the shared-vecidx consolidation lands.
        ``group_field`` / ``groups`` work.
        """
        req = pb.VectorSearchRequest(
            index_name=index_name, query=query, k=k, ef_search=ef_search
        )
        # MMR fields are additive proto fields (7-9). Set them only when the
        # generated stub already carries them so a client built against an older
        # `statelet_pb2` (not yet regenerated) keeps working with MMR off.
        if mmr or mmr_lambda or mmr_pool:
            if not hasattr(req, "mmr"):
                raise RuntimeError(
                    "MMR requested but the generated statelet_pb2 lacks the mmr "
                    "fields; regenerate the protobuf stub (make proto)."
                )
            req.mmr = mmr
            req.mmr_lambda = mmr_lambda
            req.mmr_pool = mmr_pool
        # rerank is additive proto field 10. Set it only when the generated stub
        # carries it so a client built against an older stub keeps working.
        if rerank is not None:
            if not hasattr(req, "rerank"):
                raise RuntimeError(
                    "rerank requested but the generated statelet_pb2 lacks the "
                    "RerankSpec field; regenerate the protobuf stub (make proto)."
                )
            req.rerank.CopyFrom(rerank._to_proto())
        # Grouping fields are additive proto fields (12-16). Set them only when
        # the generated stub already carries them so a client built against an
        # older `statelet_pb2` (not yet regenerated) keeps working (grouping off).
        if group_field or group_size or groups or group_overfetch or group_missing_as_own:
            if not hasattr(req, "group_field"):
                raise RuntimeError(
                    "grouping requested but the generated statelet_pb2 lacks the "
                    "group_field fields; regenerate the protobuf stub (make proto)."
                )
            req.group_field = group_field
            req.group_size = group_size
            req.groups = groups
            req.group_overfetch = group_overfetch
            # group_missing_as_own is additive proto field 16 (Phase 3); guard it
            # so an older stub without the field still works (drop behavior).
            if hasattr(req, "group_missing_as_own"):
                req.group_missing_as_own = group_missing_as_own
            elif group_missing_as_own:
                raise RuntimeError(
                    "group_missing_as_own requested but the generated statelet_pb2 "
                    "lacks the field; regenerate the protobuf stub (make proto)."
                )
        resp = self._stub.VectorSearch(req, timeout=self._text_graph_search_timeout_s)
        return [
            VectorSearchResult(
                id=r.id,
                distance=r.distance,
                group_key=getattr(r, "group_key", ""),
            )
            for r in resp.results
        ]

    def rerank_validate(
        self,
        index_name: str,
        rerank: RerankSpec,
    ) -> None:
        """Dry-run pre-flight validation of a :class:`RerankSpec`.

        Sends a ``validate_only`` :meth:`vector_search` that validates the
        ``passage_field`` template (and, for ``model="cross-encoder"``, that a
        reranker is loaded) without executing the search. Returns ``None`` when
        the spec is valid; raises :class:`grpc.RpcError` with
        ``INVALID_ARGUMENT`` / ``FAILED_PRECONDITION`` otherwise. Mirrors
        Weaviate's "property exists?" / Pinecone's "rank_fields valid?"
        pre-flight check.
        """
        spec = RerankSpec(
            enabled=True,
            rerank_k=rerank.rerank_k,
            model=rerank.model,
            passage_field=rerank.passage_field,
            signal_blend=rerank.signal_blend,
            query_text=rerank.query_text,
            validate_only=True,
        )
        req = pb.VectorSearchRequest(index_name=index_name, query=[], k=1)
        if not hasattr(req, "rerank"):
            raise RuntimeError(
                "rerank_validate requires a regenerated statelet_pb2 carrying the "
                "RerankSpec.validate_only field; run `make proto`."
            )
        req.rerank.CopyFrom(spec._to_proto())
        self._stub.VectorSearch(req, timeout=self._text_graph_search_timeout_s)

    def vector_get(self, index_name: str, vector_id: int) -> Optional[List[float]]:
        """Retrieve a stored vector by id. Returns ``None`` if not found."""
        resp = self._stub.VectorGet(
            pb.VectorGetRequest(index_name=index_name, vector_id=vector_id),
            timeout=self._kv_timeout_s,
        )
        return list(resp.vector) if resp.found else None

    # ── Graph operations ─────────────────────────────────────────────

    def drop_graph_index(self, name: str) -> None:
        """Drop a graph index and its column families."""
        self._stub.DropGraphIndex(
            pb.DropGraphIndexRequest(name=name),
            timeout=self._graph_admin_timeout_s,
        )

    def create_graph_index(
        self,
        name: str,
        dim: int = 0,
        metric: str = "cosine",
        m: int = 16,
        m_max0: int = 0,
        ef_construction: int = 200,
        ef_search: int = 64,
        quantizer: str = "",
        timeout_s: Optional[float] = None,
    ) -> None:
        """Create a graph index with HNSW + temporal edges.

        ``quantizer`` selects the in-list vector quantizer: ``"sq8"``,
        ``"pq"``, or ``"rabitq"``. Empty means the backend default
        (disk_hnsw -> sq8, spfresh -> pq). It is part of the graph's schema
        identity: re-creating with a different value is rejected.
        """
        self._stub.CreateGraphIndex(
            pb.CreateGraphIndexRequest(
                name=name,
                dim=dim,
                m=m,
                m_max0=m_max0 or m * 2,
                ef_construction=ef_construction,
                ef_search=ef_search,
                metric=metric,
                quantizer=quantizer,
            ),
            timeout=self._graph_admin_timeout_s if timeout_s is None else timeout_s,
        )

    def graph_add_node(
        self,
        graph_name: str,
        node_id: int,
        properties: bytes = b"",
        vector: Optional[List[float]] = None,
    ) -> None:
        """Add a node to a graph index."""
        self._stub.GraphAddNode(
            pb.GraphAddNodeRequest(
                graph_name=graph_name,
                node_id=node_id,
                properties=properties,
                vector=vector or [],
            ),
            timeout=self._kv_timeout_s,
        )

    def graph_add_edge(
        self,
        graph_name: str,
        src: int,
        dst: int,
        edge_type: str,
        valid_from: int = 0,
        valid_to: int = 0,
        properties: bytes = b"",
    ) -> None:
        """Add a temporal edge between two nodes."""
        self._stub.GraphAddEdge(
            pb.GraphAddEdgeRequest(
                graph_name=graph_name,
                src=src,
                dst=dst,
                edge_type=edge_type,
                valid_from=valid_from,
                valid_to=valid_to,
                properties=properties,
            ),
            timeout=self._kv_timeout_s,
        )

    def graph_search(
        self,
        graph_name: str,
        query: List[float],
        k: int,
        ef: int = 0,
    ) -> List["GraphSearchResult"]:
        """Search for nearest neighbor nodes in a graph index."""
        resp = self._stub.GraphSearch(
            pb.GraphSearchRequest(
                graph_name=graph_name,
                query=query,
                k=k,
                ef=ef,
            ),
            timeout=self._text_graph_search_timeout_s,
        )
        return [GraphSearchResult(node_id=r.node_id, distance=r.distance) for r in resp.results]

    def graph_get_node(
        self,
        graph_name: str,
        node_id: int,
        *,
        timeout_s: Optional[float] = None,
    ) -> "GraphNodeResult":
        """Retrieve a graph node by id."""
        resp = self._stub.GraphGetNode(
            pb.GraphGetNodeRequest(graph_name=graph_name, node_id=node_id),
            timeout=self._graph_admin_timeout_s if timeout_s is None else timeout_s,
        )
        return GraphNodeResult(found=resp.found, properties=resp.properties)

    def embed(
        self,
        texts: List[str],
        *,
        is_query: bool = False,
        timeout_s: Optional[float] = None,
    ) -> List[List[float]]:
        """Embed texts with the gateway's resident dense model, returning one
        vector per text (in order). Stores nothing — pair with graph_add_node /
        vector_put to write. ``is_query=True`` uses the query side of an
        asymmetric model (e5/gte); default embeds as a document/passage.
        """
        resp = self._stub.Embed(
            pb.EmbedRequest(texts=list(texts), is_query=is_query),
            timeout=self._graph_admin_timeout_s if timeout_s is None else timeout_s,
        )
        return [list(v.values) for v in resp.vectors]

    # ── Text + Graph (gateway-side embedding) ──────────────────────

    def text_graph_put(
        self,
        graph_name: str,
        text: str,
        *,
        node_id: int = 0,
        properties: bytes = b"",
        edge_target: int = 0,
        edge_type: str = "",
        edge_valid_from: int = 0,
        edge_valid_to: int = 0,
        skip_server_llm: bool = False,
        timeout_s: Optional[float] = None,
    ) -> int:
        """Insert text into a graph index via the gateway embedding pipeline.

        Returns the assigned node_id.
        """
        resp = self._stub.TextGraphPut(
            pb.TextGraphPutRequest(
                graph_name=graph_name,
                node_id=node_id,
                text=text,
                properties=properties,
                edge_target=edge_target,
                edge_type=edge_type,
                edge_valid_from=edge_valid_from,
                edge_valid_to=edge_valid_to,
                skip_server_llm=skip_server_llm,
            ),
            timeout=self._text_graph_put_timeout_s if timeout_s is None else timeout_s,
        )
        return resp.node_id

    def text_graph_search(
        self,
        graph_name: str,
        query: str,
        k: int = 5,
        ef: int = 0,
        *,
        skip_server_llm: bool = False,
        extra_queries: Optional[List[str]] = None,
        fact_k: int = 0,
        chunk_k: int = 0,
        granularities: Optional[List[str]] = None,
        rrf_k: int = 0,
        include_answer_bundle: bool = False,
        context_date: str = "",
        entity_consolidate: bool = False,
        conflict_policy: str = "",
        include_dissent: bool = False,
        timeout_s: Optional[float] = None,
    ) -> "TextGraphSearchResponse":
        """Search a graph index with a raw text query (gateway embeds it).

        Args:
            extra_queries: Optional caller-provided rewritten queries for better
                retrieval. Each is embedded and searched alongside the main query.
            fact_k: Max extracted-fact results (0 = server default, currently 7).
            chunk_k: Max raw-chunk results (0 = server default, currently 5).
            entity_consolidate: (#830 LongMemEval Phase 5d) When True, dereference
                each query entity term through its persisted ``gcanon:`` identity
                cluster and OR-expand retrieval over the cluster's surface forms, so
                evidence written under any surface in the cluster is recalled.
                Consolidated results carry a non-zero ``canonical_entity_id``.
                Honoured only when the gateway has entity resolution enabled
                (this flag or ``STATELET_ENTITY_RESOLVE``). Default False ⇒ legacy
                behaviour.
            conflict_policy: Conflict-as-data read-time re-rank policy (#783):
                ``""`` (graph/env default), ``"recency"``, ``"trust"``, or
                ``"confidence"``. When set, the gateway groups contradictory
                claims, arbitrates each set, and demotes losers below the winner
                (never dropping them) before truncating to ``k``. Ignored by
                older gateways/stubs that predate the feature.
            include_dissent: When True, ``conflict_resolutions_json`` carries the
                full per-loser detail (demoted_score, reason) for every demoted
                claim. No effect unless ``conflict_policy`` fires.
            include_answer_bundle: When True, ask the gateway to return compact
                answer-oriented JSON artifacts alongside the ranked results.
            context_date: Optional reference date for relative-time memory
                packing. This is sent separately from ``query`` so date tokens
                do not affect retrieval ranking.

        Returns a :class:`TextGraphSearchResponse` with ``results``,
        ``fact_results``, ``chunk_results``, optional answer bundle artifacts,
        and ``conflict_resolutions_json``.
        """
        # Build request — fact_k/chunk_k may not exist in older proto stubs
        req_kwargs = dict(
            graph_name=graph_name,
            query=query,
            k=k,
            ef=ef,
            skip_server_llm=skip_server_llm,
            extra_queries=extra_queries or [],
        )
        try:
            # Try setting new fields (requires updated proto stubs)
            req_kwargs["fact_k"] = fact_k
            req_kwargs["chunk_k"] = chunk_k
            # (#827 LongMemEval Phase 5a) Multi-granularity ingest + RRF fusion.
            if granularities:
                req_kwargs["granularities"] = list(granularities)
            if rrf_k:
                req_kwargs["rrf_k"] = rrf_k
            if include_answer_bundle:
                req_kwargs["include_answer_bundle"] = include_answer_bundle
            if context_date:
                req_kwargs["context_date"] = context_date
            # (#830 LongMemEval Phase 5d) Read-time entity consolidation.
            if entity_consolidate:
                req_kwargs["entity_consolidate"] = True
            # (#783 conflict-as-data Phase 3) read-time conflict re-rank.
            if conflict_policy:
                req_kwargs["conflict_policy"] = _conflict_policy_enum(conflict_policy)
            if include_dissent:
                req_kwargs["include_dissent"] = include_dissent
            req = pb.TextGraphSearchRequest(**req_kwargs)
        except (ValueError, TypeError):
            # Fall back to old proto without the optional fields above.
            for key in (
                "fact_k", "chunk_k", "granularities", "rrf_k",
                "include_answer_bundle", "context_date",
                "entity_consolidate", "conflict_policy", "include_dissent",
            ):
                req_kwargs.pop(key, None)
            req = pb.TextGraphSearchRequest(**req_kwargs)

        resp = self._stub.TextGraphSearch(
            req,
            timeout=self._text_graph_search_timeout_s if timeout_s is None else timeout_s,
        )
        def _convert(r):
            return TextGraphSearchResult(
                node_id=r.node_id, distance=r.distance, properties=r.properties,
                # (#830 Phase 5d) 0 when the result was not consolidated.
                canonical_entity_id=getattr(r, "canonical_entity_id", 0),
            )
        # Access fact_results/chunk_results dynamically (may not exist in old stubs)
        fact_list = list(getattr(resp, 'fact_results', []))
        chunk_list = list(getattr(resp, 'chunk_results', []))
        answer_bundle_json = getattr(resp, 'answer_bundle_json', b"") or None
        primary_answer_result_json = getattr(resp, 'primary_answer_result_json', b"") or None
        answer_results_json = list(getattr(resp, 'answer_results_json', []))
        # conflict_resolutions_json may be absent on a pre-#783 gateway/stub.
        conflict_json = getattr(resp, 'conflict_resolutions_json', "") or None
        # `memories`: plain-text reader block (gateway STATELET_READER_BLOCK).
        # getattr guards a pre-`memories` gateway/stub.
        memories = getattr(resp, 'memories', "") or ""
        return TextGraphSearchResponse(
            results=[_convert(r) for r in resp.results],
            fact_results=[_convert(r) for r in fact_list],
            chunk_results=[_convert(r) for r in chunk_list],
            answer_bundle_json=answer_bundle_json,
            primary_answer_result_json=primary_answer_result_json,
            answer_results_json=answer_results_json,
            conflict_resolutions_json=conflict_json,
            memories=memories,
        )

    def resolve_conflict(
        self,
        graph_name: str,
        node_id: int,
        *,
        policy: str = "",
        as_of: int = 0,
        timeout_s: Optional[float] = None,
    ) -> "ResolvedConflict":
        """Resolve the conflict set ``node_id`` belongs to (#810, gateway-only).

        Scans ``contradicts`` edges to gather every competing claim about the
        same entity, arbitrates them with ``policy`` (or the graph/env default
        when empty), and returns the authoritative claim plus the full dissent
        set (live + retired — claims are never dropped, only demoted).

        Args:
            node_id: ANY member of the conflict set.
            policy: ``""`` (graph default), ``"trust"``, ``"recency"``,
                ``"confidence"``, or ``"quorum"``. An unknown policy is warned
                about by the gateway and falls back to the default; the policy
                actually applied is returned as :attr:`ResolvedConflict.policy`.
            as_of: Bitemporal basis in ms; ``0`` = now.

        Returns a :class:`ResolvedConflict`. When the node has no properties row
        the response has ``found == False`` and empty winner/dissent.
        """
        resp = self._stub.ResolveConflict(
            pb.ResolveConflictRequest(
                graph_name=graph_name,
                node_id=node_id,
                policy=policy,
                as_of=as_of,
            ),
            timeout=self._text_graph_search_timeout_s if timeout_s is None else timeout_s,
        )
        return ResolvedConflict(
            found=resp.found,
            authoritative=resp.authoritative,
            dissenting=list(resp.dissenting),
            policy=resp.policy,
            score=resp.score,
            rationale=resp.rationale,
            truncated=resp.truncated,
            votes=[
                ConflictVote(
                    value=v.value,
                    weight=v.weight,
                    supporters=list(v.supporters),
                )
                for v in getattr(resp, "votes", [])
            ],
        )

    def resolve_entities(
        self,
        graph_name: str,
        queries: Optional[List[str]] = None,
        k: int = 0,
        threshold: float = 0.0,
        timeout_s: Optional[float] = None,
    ) -> "ResolveEntitiesResult":
        """(#828 LongMemEval Phase 5b) LLM-free entity-resolution candidate
        generation.

        Runs the gateway's blocking-then-scoring resolver (alias rule +
        entity-mention ANN nearest-neighbor + lexical/cosine) and returns the
        candidate clusters that surface the same real entity under different
        surface forms ("Bob"/"Robert"/"Mr. Smith") across sessions.

        Args:
            queries: Optional surface forms to resolve. When empty, every surface
                in the ``{graph}__entity`` mention index is scanned and clustered.
            k: ANN neighbors fetched per query surface (0 = server default).
            threshold: Similarity threshold override (0 = server default /
                ``STATELET_ENTITY_SIM_THRESHOLD``).

        Returns a :class:`ResolveEntitiesResult` with ``clusters`` and
        ``embedder_used`` (False ⇒ alias/lexical fallback only).
        """
        req = pb.ResolveEntitiesRequest(
            graph_name=graph_name,
            queries=list(queries or []),
            k=k,
            threshold=threshold,
        )
        resp = self._stub.ResolveEntities(
            req,
            timeout=self._text_graph_search_timeout_s if timeout_s is None else timeout_s,
        )
        clusters = [
            EntityCluster(
                canonical_id=c.canonical_id,
                canonical=c.canonical,
                members=[
                    EntityClusterMember(
                        surface=m.surface, method=m.method, score=m.score,
                    )
                    for m in c.members
                ],
            )
            for c in resp.clusters
        ]
        return ResolveEntitiesResult(
            clusters=clusters, embedder_used=resp.embedder_used,
        )

    def graph_query_edges(
        self,
        graph_name: str,
        node_id: int,
        edge_type: str = "",
        time_start: int = 0,
        time_end: int = 0,
        reverse: bool = False,
    ) -> List["GraphEdge"]:
        """Query temporal edges from a node."""
        resp = self._stub.GraphQueryEdges(
            pb.GraphQueryEdgesRequest(
                graph_name=graph_name,
                node_id=node_id,
                edge_type=edge_type,
                time_start=time_start,
                time_end=time_end,
                reverse=reverse,
            ),
            timeout=self._text_graph_search_timeout_s,
        )
        return [
            GraphEdge(
                src=e.src, dst=e.dst, edge_type=e.edge_type,
                valid_from=e.valid_from, valid_to=e.valid_to,
                properties=e.properties,
            )
            for e in resp.edges
        ]

    def graph_query(
        self,
        cypher: str,
        graph_name: str = "",
        max_rows: int = 0,
        as_of: int = 0,
        tx_as_of: int = 0,
        timeout_s: Optional[float] = None,
    ) -> "GraphQueryResult":
        """Run a read-only openCypher-subset query (the ``GraphQuery`` RPC).

        Gateway-only: the gateway parses and plans the query, then compiles it
        to engine traversal primitives. The subset covers ``MATCH`` path
        patterns, ``WHERE`` over node properties, ``RETURN`` / ``ORDER BY`` /
        ``LIMIT``, a bitemporal ``AS OF <valid>[, <tx>]`` clause and the
        retrieval procedures ``db.vectorSearch`` / ``db.hybridSearch`` /
        ``db.graphRag``. ``CREATE`` / ``MERGE`` are rejected.

        ``graph_name`` empty lets the gateway resolve the default graph.
        ``max_rows`` is a hard cap applied on top of any ``LIMIT`` in the query
        (0 = no extra cap). ``as_of`` / ``tx_as_of`` supply the bitemporal
        point-in-time out of band; an ``AS OF`` clause in the query text wins.

        Named query parameters (``$q``) parse but are not resolvable yet, so a
        vector-seeded procedure needs an inline literal —
        ``db.vectorSearch([0.1, 0.2, ...], 5)``.
        """
        resp = self._stub.GraphQuery(
            pb.GraphQueryRequest(
                graph_name=graph_name,
                cypher=cypher,
                max_rows=max_rows,
                as_of=as_of,
                tx_as_of=tx_as_of,
            ),
            timeout=self._text_graph_search_timeout_s if timeout_s is None else timeout_s,
        )
        return GraphQueryResult(
            columns=list(resp.columns),
            rows=[
                [_graph_query_value(v) for v in row.values] for row in resp.rows
            ],
            warnings=list(resp.warnings),
        )


@dataclass
class GraphSearchResult:
    """A graph vector search result."""

    node_id: int
    distance: float


@dataclass
class TextGraphSearchResult:
    """A text graph search result with hydrated properties."""

    node_id: int
    distance: float
    properties: bytes
    # (#830 LongMemEval Phase 5d) Canonical entity id when this result was
    # consolidated through a ``gcanon:`` identity cluster; 0 otherwise.
    canonical_entity_id: int = 0


@dataclass
class TextGraphSearchResponse:
    """Response from text_graph_search with per-type result buckets."""

    results: List[TextGraphSearchResult]
    fact_results: List[TextGraphSearchResult]
    chunk_results: List[TextGraphSearchResult]
    answer_bundle_json: Optional[bytes] = None
    primary_answer_result_json: Optional[bytes] = None
    answer_results_json: List[bytes] = field(default_factory=list)
    # Conflict-as-data resolution log (#783): JSON-encoded array of ResolvedClaim
    # objects, one per arbitrated conflict set. The proto field is a ``string``,
    # so this is a ``str`` (parse with ``json.loads``). ``None`` when no
    # conflict_policy fired, the graph was conflict-cold, or the gateway/stub
    # predates #783.
    conflict_resolutions_json: Optional[str] = None
    # Ready-to-read PLAIN-TEXT evidence block assembled by the gateway when
    # `STATELET_READER_BLOCK=1` (on by default). Empty string otherwise. Feed it
    # straight to an LLM reader — no client-side formatting needed.
    memories: str = ""


@dataclass
class ConflictVote:
    """A single normalised value's tally in a quorum-policy resolution (#810)."""

    value: str
    weight: float
    supporters: List[int]


@dataclass
class ResolvedConflict:
    """Result of :meth:`StateletClient.resolve_conflict` (#810).

    ``authoritative`` is the winning claim's node id; ``dissenting`` lists every
    other claim (live + retired) so audits can see the full conflict set. When
    ``found`` is False the conflict node had no properties row and the other
    fields are empty/zero.
    """

    found: bool
    authoritative: int
    dissenting: List[int]
    policy: str
    score: float
    rationale: str
    truncated: bool
    votes: List[ConflictVote]


@dataclass
class EntityClusterMember:
    """(#828) A surface form linked into an entity cluster, with the candidate
    method that linked it (``alias_rule`` | ``vector_nn`` | ``lexical``)."""

    surface: str
    method: str
    score: float


@dataclass
class EntityCluster:
    """(#828) A candidate cluster of same-entity surface forms."""

    canonical_id: int
    canonical: str
    members: List[EntityClusterMember]


@dataclass
class ResolveEntitiesResult:
    """(#828) Response from resolve_entities."""

    clusters: List[EntityCluster]
    embedder_used: bool


@dataclass
class GraphEdge:
    """A temporal edge between two graph nodes."""

    src: int
    dst: int
    edge_type: str
    valid_from: int
    valid_to: int
    properties: bytes


@dataclass
class GraphQueryResult:
    """Rows returned by :meth:`StateletClient.graph_query`.

    ``rows`` holds decoded Python values in ``columns`` order: ``None`` for
    NULL, ``int`` / ``float`` / ``str`` / ``bool`` for the scalar kinds, and the
    parsed node-property object for JSON. ``warnings`` is non-empty when the
    result may be incomplete (e.g. a label scan hit the per-shard frontier cap).
    """

    columns: List[str]
    rows: List[List[Any]]
    warnings: List[str] = field(default_factory=list)

    def dicts(self) -> List[dict]:
        """The rows as ``{column: value}`` dicts."""
        return [dict(zip(self.columns, row)) for row in self.rows]


@dataclass
class GraphNodeResult:
    """A graph node fetch result."""

    found: bool
    properties: bytes
