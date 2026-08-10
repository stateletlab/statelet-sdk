"""High-level Client and AsyncClient for Statelet.

These wrap both the KV service (Statelet) and the Agent State service
(AgentStateService) behind a single object with str-key convenience.

Usage::

    from statelet import Client

    db = Client("localhost:9379")
    db.put("key", b"value")
    value = db.get("key")

    # With auth (when GATEWAY_JWT_SECRET is set):
    db = Client("localhost:9379", username="admin", password="admin")
"""

from __future__ import annotations

import json
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Union

import grpc
import grpc.aio

from statelet import statelet_pb2 as pb
from statelet import statelet_pb2_grpc as pb_grpc

_DEFAULT_GRPC_MAX_MESSAGE_BYTES = 64 * 1024 * 1024
_DEFAULT_GRPC_CHANNEL_OPTIONS = (
    ("grpc.max_receive_message_length", _DEFAULT_GRPC_MAX_MESSAGE_BYTES),
    ("grpc.max_send_message_length", _DEFAULT_GRPC_MAX_MESSAGE_BYTES),
)


def _to_bytes(key: Union[str, bytes]) -> bytes:
    return key.encode("utf-8") if isinstance(key, str) else key


def _default_mgmt_addr(addr: str) -> str:
    """The management API address implied by a gateway address.

    The gateway serves gRPC on one port and the management HTTP API on the next
    one up — 9379 and 9380 by default. Deriving that by rewriting the literal
    ":9379" worked only on the default port: anywhere else the substring was
    absent, the replace did nothing, and login sent an HTTP request to the gRPC
    listener, which answers by resetting the connection. The traceback that
    produced named neither the port nor the cause.
    """
    host, sep, port = addr.rpartition(":")
    if not sep or not port.isdigit():
        return addr
    return f"{host}:{int(port) + 1}"


def _login(mgmt_addr: str, username: str, password: str) -> str:
    """Obtain a JWT token from the gateway management API."""
    url = f"http://{mgmt_addr}/api/v1/auth/login"
    payload = json.dumps({"username": username, "password": password}).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as resp:
        body = json.loads(resp.read())
    return body["token"]


class _AuthInterceptor(grpc.UnaryUnaryClientInterceptor, grpc.UnaryStreamClientInterceptor):
    """Injects ``Authorization: Bearer <token>`` into every gRPC call."""

    def __init__(self, token: str):
        self._metadata = [("authorization", f"Bearer {token}")]

    def intercept_unary_unary(self, continuation, client_call_details, request):
        new_details = _with_metadata(client_call_details, self._metadata)
        return continuation(new_details, request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        new_details = _with_metadata(client_call_details, self._metadata)
        return continuation(new_details, request)


def _with_metadata(client_call_details, extra_metadata):
    metadata = list(client_call_details.metadata or []) + extra_metadata
    return grpc._interceptor._ClientCallDetails(
        client_call_details.method, client_call_details.timeout,
        metadata, client_call_details.credentials,
        client_call_details.wait_for_ready, client_call_details.compression,
    )


class _AsyncAuthInterceptor(grpc.aio.UnaryUnaryClientInterceptor, grpc.aio.UnaryStreamClientInterceptor):
    """Async version of auth interceptor."""

    def __init__(self, metadata):
        self._metadata = metadata

    async def intercept_unary_unary(self, continuation, client_call_details, request):
        new_details = _with_metadata(client_call_details, self._metadata)
        return await continuation(new_details, request)

    async def intercept_unary_stream(self, continuation, client_call_details, request):
        new_details = _with_metadata(client_call_details, self._metadata)
        return await continuation(new_details, request)


# ── Result types ─────────────────────────────────────────────────────────────


@dataclass
class Step:
    """A causal step returned by get_step / traverse."""

    step_id: int = 0
    agent_id: str = ""
    step_type: str = ""
    branch_id: int = 0
    content: bytes = b""
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: int = 0

    @classmethod
    def _from_json(cls, raw: bytes) -> "Step":
        if not raw:
            return cls()
        d = json.loads(raw)
        return cls(
            step_id=d.get("step_id", 0),
            agent_id=d.get("agent_id", ""),
            step_type=d.get("step_type", ""),
            branch_id=d.get("branch_id", 0),
            content=d.get("content", "").encode() if isinstance(d.get("content"), str) else b"",
            metadata=d.get("metadata", {}),
            created_at=d.get("created_at", 0),
        )


@dataclass
class Edge:
    """A causal edge."""

    peer_step_id: int
    edge_type: str
    props: bytes = b""
    valid_from: int = 0
    valid_to: int = 0


@dataclass
class TraverseResult:
    """Result of a graph traversal."""

    steps: List[Step]
    edges: List[Edge]


@dataclass
class CausalChain:
    """A similar causal chain returned by find_similar_chains."""

    anchor: Step
    distance: float
    steps: List[Step]
    edges: List[Edge]


@dataclass
class BranchMeta:
    """Branch metadata."""

    id: int
    parent_id: int
    parent_snapshot_seq: int
    created_at: int
    status: str
    label: str


@dataclass
class CasPutResult:
    """Result of a compare-and-swap put."""

    success: bool
    new_seq: int = 0
    actual_seq: int = 0


@dataclass
class WatchEvent:
    """A single watch event from watch_prefix."""

    event_type: str
    cf: int
    key: bytes
    value: bytes
    seq: int


@dataclass
class MemoryIngestResult:
    """Result of a transactional memory ingest (#780, fenced in #784).

    ``action`` is one of ``"added"``, ``"deduplicated"``, ``"superseded"`` or
    ``"conflict"``. On ``"conflict"`` the optimistic transaction could not
    commit; ``fence_lost`` tells the two causes apart:

    - ``fence_lost == False`` (data conflict): a concurrent writer kept moving a
      read candidate / the id counter within the bounded retry budget. Nothing
      was written, ``committed`` is ``False``, and the caller should back off and
      retry.
    - ``fence_lost == True`` (lost lease, #784): the agent's lease was
      re-acquired/renewed by someone else after the ``fence`` it passed. This is
      terminal — retrying cannot reacquire the lease; re-``lease`` for a fresh
      fence. (Issued through :meth:`Client.with_lease`, this surfaces as a
      :class:`LeaseLost` exception instead.)
    """

    action: str
    fact_id: int = 0
    superseded: tuple = ()
    committed: bool = True
    conflict_fact_id: int = 0
    fence_lost: bool = False


@dataclass
class LeaseResult:
    """Result of a :meth:`Client.lease` / :meth:`Client.claim` call.

    ``acquired`` is ``True`` iff the caller now holds the key; on success
    ``fence`` is the fencing token to carry on subsequent fenced writes. On
    failure ``holder`` is the current holder and ``fence`` is *their* token.
    """

    acquired: bool
    holder: str = ""
    fence: int = 0


class LeaseLost(Exception):
    """Raised inside a :meth:`Client.with_lease` block when a fenced
    ``memory_ingest`` aborts because the lease moved past the held fence (the
    Kleppmann lost-lease window, #784). Terminal: re-acquire the lease to
    continue."""

    def __init__(self, scope: str, key: bytes):
        self.scope = scope
        self.key = key
        super().__init__(
            f"lease lost on key {key!r} (scope {scope!r}): the fenced ingest "
            f"aborted because the lease was re-acquired by another holder"
        )


_INGEST_ACTIONS = {0: "added", 1: "deduplicated", 2: "superseded", 3: "conflict"}


# ── Sync Client ──────────────────────────────────────────────────────────────


class Client:
    """High-level synchronous client for Statelet.

    Connects to the gateway (default ``localhost:9379``) and exposes both
    KV operations and Agent State operations with str-key convenience.

    Usage::

        from statelet import Client

        db = Client("localhost:9379")
        db.put("key", b"value")
        print(db.get("key"))          # b"value"

        step_id = db.add_step("agent-1", "Observe", content=b"saw something")
        db.add_edge(step_id, other_id, "Triggers")
        result = db.traverse(step_id, direction="forward", max_depth=3)
    """

    def __init__(
        self,
        addr: str = "localhost:9379",
        *,
        cf: int = 0,
        token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        mgmt_addr: Optional[str] = None,
    ) -> None:
        # Auto-login if username/password provided
        if token is None and username is not None:
            _mgmt = mgmt_addr or _default_mgmt_addr(addr)
            token = _login(_mgmt, username, password or "")
        if token:
            interceptor = _AuthInterceptor(token)
            self._channel = grpc.intercept_channel(
                grpc.insecure_channel(addr, options=_DEFAULT_GRPC_CHANNEL_OPTIONS),
                interceptor,
            )
        else:
            self._channel = grpc.insecure_channel(
                addr,
                options=_DEFAULT_GRPC_CHANNEL_OPTIONS,
            )
        self._kv = pb_grpc.StateletStub(self._channel)
        self._agent = pb_grpc.AgentStateServiceStub(self._channel)
        self._cf = cf

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def close(self) -> None:
        self._channel.close()

    # ── KV operations (str keys accepted) ────────────────────────────────

    def put(self, key: Union[str, bytes], value: bytes, *, cf: Optional[int] = None) -> None:
        self._kv.Put(pb.PutRequest(cf=cf if cf is not None else self._cf, key=_to_bytes(key), value=value))

    def get(self, key: Union[str, bytes], *, cf: Optional[int] = None) -> Optional[bytes]:
        resp = self._kv.Get(pb.GetRequest(cf=cf if cf is not None else self._cf, key=_to_bytes(key)))
        return resp.value if resp.found else None

    def delete(self, key: Union[str, bytes], *, cf: Optional[int] = None) -> None:
        self._kv.Delete(pb.DeleteRequest(cf=cf if cf is not None else self._cf, key=_to_bytes(key)))

    def scan(
        self,
        prefix: Union[str, bytes] = "",
        *,
        cursor: Optional[bytes] = None,
        limit: int = 100,
        cf: Optional[int] = None,
    ) -> tuple:
        """Returns ``(entries, next_cursor)`` where entries is ``[(key, value), ...]``."""
        resp = self._kv.Scan(
            pb.ScanRequest(
                cf=cf if cf is not None else self._cf,
                prefix=_to_bytes(prefix),
                cursor=cursor or b"",
                limit=limit,
            )
        )
        entries = [(e.key, e.value) for e in resp.entries]
        next_cursor = resp.next_cursor if resp.next_cursor else None
        return entries, next_cursor

    # ── Agent State: Causal Graph ────────────────────────────────────────

    def add_step(
        self,
        agent_id: str,
        step_type: str,
        *,
        content: bytes = b"",
        metadata: Optional[bytes] = None,
        embedding: Optional[List[float]] = None,
        branch_id: int = 0,
    ) -> int:
        """Add a causal step. Returns the step_id."""
        resp = self._agent.AddStep(
            pb.AgentAddStepRequest(
                agent_id=agent_id,
                step_type=step_type,
                branch_id=branch_id,
                content=content,
                metadata=metadata or b"",
                embedding=embedding or [],
            )
        )
        return resp.step_id

    def add_edge(
        self,
        src_step_id: int,
        dst_step_id: int,
        edge_type: str,
        *,
        props: bytes = b"",
        valid_from: int = 0,
        valid_to: int = 0,
    ) -> None:
        """Add a causal edge between two steps."""
        self._agent.AddEdge(
            pb.AgentAddEdgeRequest(
                src_step_id=src_step_id,
                dst_step_id=dst_step_id,
                edge_type=edge_type,
                props=props,
                valid_from=valid_from,
                valid_to=valid_to,
            )
        )

    def get_step(self, step_id: int) -> Optional[Step]:
        resp = self._agent.GetStep(pb.AgentGetStepRequest(step_id=step_id))
        if not resp.found:
            return None
        return Step._from_json(resp.step_json)

    def get_content(self, step_id: int) -> Optional[bytes]:
        resp = self._agent.GetContent(pb.AgentGetContentRequest(step_id=step_id))
        return resp.content if resp.found else None

    def get_edges(
        self,
        step_id: int,
        *,
        direction: str = "forward",
        edge_type: str = "",
        at_timestamp: int = 0,
        window_start: int = 0,
        window_end: int = 0,
    ) -> List[Edge]:
        resp = self._agent.GetEdges(
            pb.AgentGetEdgesRequest(
                step_id=step_id,
                direction=direction,
                edge_type=edge_type,
                at_timestamp=at_timestamp,
                window_start=window_start,
                window_end=window_end,
            )
        )
        return [
            Edge(peer_step_id=e.peer_step_id, edge_type=e.edge_type,
                 props=e.props, valid_from=e.valid_from, valid_to=e.valid_to)
            for e in resp.edges
        ]

    def traverse(
        self,
        start_step_id: int,
        *,
        direction: str = "forward",
        max_depth: int = 3,
    ) -> TraverseResult:
        """BFS traversal from a step. Returns steps and edges."""
        resp = self._agent.Traverse(
            pb.AgentTraverseRequest(
                start_step_id=start_step_id,
                direction=direction,
                max_depth=max_depth,
            )
        )
        steps = [Step._from_json(s) for s in resp.step_jsons]
        edges = [
            Edge(peer_step_id=e.peer_step_id, edge_type=e.edge_type,
                 props=e.props, valid_from=e.valid_from, valid_to=e.valid_to)
            for e in resp.edges
        ]
        return TraverseResult(steps=steps, edges=edges)

    def find_similar_chains(
        self,
        query_embedding: List[float],
        k: int = 5,
        chain_depth: int = 3,
        ef: int = 0,
    ) -> List[CausalChain]:
        """Find causal chains anchored on steps similar to the query embedding."""
        resp = self._agent.FindSimilarChains(
            pb.AgentFindSimilarChainsRequest(
                query_embedding=query_embedding,
                k=k,
                chain_depth=chain_depth,
                ef=ef,
            )
        )
        chains = []
        for c in resp.chains:
            chains.append(CausalChain(
                anchor=Step._from_json(c.anchor_step_json),
                distance=c.distance,
                steps=[Step._from_json(s) for s in c.step_jsons],
                edges=[
                    Edge(peer_step_id=e.peer_step_id, edge_type=e.edge_type,
                         props=e.props, valid_from=e.valid_from, valid_to=e.valid_to)
                    for e in c.edges
                ],
            ))
        return chains

    # ── Agent State: Branching (fork) ────────────────────────────────────

    def fork(self, label: str, *, parent_branch_id: int = 0) -> int:
        """Fork a new branch. Returns the branch_id."""
        resp = self._agent.Fork(
            pb.AgentForkRequest(label=label, parent_branch_id=parent_branch_id)
        )
        return resp.branch_id

    def merge_branch(self, branch_id: int) -> None:
        self._agent.MergeBranch(pb.AgentMergeBranchRequest(branch_id=branch_id))

    def discard_branch(self, branch_id: int) -> None:
        self._agent.DiscardBranch(pb.AgentDiscardBranchRequest(branch_id=branch_id))

    def list_branches(self) -> List[BranchMeta]:
        resp = self._agent.ListBranches(pb.AgentListBranchesRequest())
        return [
            BranchMeta(
                id=b.id, parent_id=b.parent_id,
                parent_snapshot_seq=b.parent_snapshot_seq,
                created_at=b.created_at, status=b.status, label=b.label,
            )
            for b in resp.branches
        ]

    def branch_put(self, branch_id: int, key: Union[str, bytes], value: bytes, *, cf: int = 0) -> None:
        self._agent.BranchPut(
            pb.AgentBranchPutRequest(branch_id=branch_id, cf=cf, key=_to_bytes(key), value=value)
        )

    def branch_get(self, branch_id: int, key: Union[str, bytes], *, cf: int = 0) -> Optional[bytes]:
        resp = self._agent.BranchGet(
            pb.AgentBranchGetRequest(branch_id=branch_id, cf=cf, key=_to_bytes(key))
        )
        return resp.value if resp.found else None

    # ── Agent State: Reactive state ──────────────────────────────────────

    def cas_put(
        self,
        key: Union[str, bytes],
        expected_seq: int,
        new_value: bytes,
        *,
        cf: int = 0,
    ) -> CasPutResult:
        """Compare-and-swap put. Returns success status and version info."""
        resp = self._agent.CasPut(
            pb.AgentCasPutRequest(
                cf=cf, key=_to_bytes(key),
                expected_seq=expected_seq, new_value=new_value,
            )
        )
        return CasPutResult(success=resp.success, new_seq=resp.new_seq, actual_seq=resp.actual_seq)

    def memory_ingest(
        self,
        scope: str,
        content: str,
        *,
        candidates: Optional[Iterable[Tuple[int, float]]] = None,
        provenance_steps: Optional[Iterable[int]] = None,
        embedding_id: Optional[int] = None,
        dedup_threshold: float = 0.97,
        supersede_threshold: float = 0.80,
        author_agent_id: str = "",
        confidence: float = 0.0,
        run_id: str = "",
        fence: int = 0,
        lease_key: Optional[Union[str, bytes]] = None,
    ) -> MemoryIngestResult:
        """Transactionally ingest a fact (#780), optionally fence-gated (#784).

        Dedup / create-with-provenance / supersede candidates all commit as ONE
        atomic, snapshot-isolated batch on the owning data node. ``candidates``
        are ``(existing_fact_id, cosine_similarity)`` pairs the caller already
        retrieved (ANN over the scope index) and scope-filtered.

        When ``fence`` is non-zero, the write is gated by the lease ``fence``
        token on ``lease_key`` (the bytes passed to :meth:`lease`/:meth:`claim`):
        if the lease moved past ``fence`` the commit aborts with
        ``action == "conflict"`` and ``fence_lost == True`` — nothing is written.
        :meth:`with_lease` injects ``fence``/``lease_key`` automatically and
        raises :class:`LeaseLost` on that outcome.
        """
        cand_msgs = [
            pb.AgentIngestCandidate(fact_id=fid, sim=sim)
            for (fid, sim) in (candidates or [])
        ]
        # An active with_lease() block injects its held fence/key unless the
        # caller passed an explicit fence.
        held = getattr(self, "_held_lease", None)
        if fence == 0 and held is not None:
            fence = held["fence"]
            lease_key = held["key"]
        req = pb.AgentMemoryIngestRequest(
            scope=scope,
            content=content,
            candidates=cand_msgs,
            provenance_steps=list(provenance_steps or []),
            dedup_threshold=dedup_threshold,
            supersede_threshold=supersede_threshold,
            author_agent_id=author_agent_id,
            confidence=confidence,
            run_id=run_id,
            fence=fence,
            lease_key=_to_bytes(lease_key) if lease_key is not None else b"",
        )
        if embedding_id is not None:
            req.embedding_id = embedding_id
        resp = self._agent.MemoryIngest(req)
        result = MemoryIngestResult(
            action=_INGEST_ACTIONS.get(resp.action, "unknown"),
            fact_id=resp.fact_id,
            superseded=tuple(resp.superseded),
            committed=resp.committed,
            conflict_fact_id=resp.conflict_fact_id,
            fence_lost=resp.fence_lost,
        )
        if held is not None and result.fence_lost:
            raise LeaseLost(scope, req.lease_key)
        return result

    # ── Agent State: coordination (leases / fences, #752 + #784) ─────────

    def claim(self, key: Union[str, bytes], agent_id: str) -> LeaseResult:
        """Acquire ``key`` for ``agent_id`` iff unheld (no expiry). On success
        the returned ``fence`` is the fencing token for downstream fenced
        writes."""
        resp = self._agent.Claim(
            pb.AgentClaimRequest(key=_to_bytes(key), agent_id=agent_id)
        )
        return LeaseResult(acquired=resp.acquired, holder=resp.holder, fence=resp.fence)

    def lease(self, key: Union[str, bytes], agent_id: str, ttl_ms: int) -> LeaseResult:
        """Acquire ``key`` for ``agent_id`` with a ``ttl_ms`` TTL; an un-renewed
        lease auto-expires. Returns the fencing token on success."""
        resp = self._agent.Lease(
            pb.AgentLeaseRequest(key=_to_bytes(key), agent_id=agent_id, ttl_ms=ttl_ms)
        )
        return LeaseResult(acquired=resp.acquired, holder=resp.holder, fence=resp.fence)

    def renew(self, key: Union[str, bytes], agent_id: str, fence: int, ttl_ms: int) -> LeaseResult:
        """Extend the lease on ``key`` iff ``fence`` still matches the holder's
        token. NOTE: a successful renew ADVANCES the fence — adopt the returned
        ``fence`` for subsequent fenced writes or they will abort (#784)."""
        resp = self._agent.Renew(
            pb.AgentRenewRequest(
                key=_to_bytes(key), agent_id=agent_id, fence=fence, ttl_ms=ttl_ms
            )
        )
        return LeaseResult(acquired=resp.acquired, holder=resp.holder, fence=resp.fence)

    def release(self, key: Union[str, bytes], fence: int) -> bool:
        """Drop the lease on ``key`` iff ``fence`` matches. Returns ``True`` if
        released."""
        resp = self._agent.Release(
            pb.AgentReleaseRequest(key=_to_bytes(key), fence=fence)
        )
        return resp.released

    @contextmanager
    def with_lease(
        self,
        key: Union[str, bytes],
        agent_id: str,
        ttl_ms: int,
        *,
        release_on_exit: bool = True,
    ):
        """Acquire a lease on ``key`` and auto-thread its fence into every
        :meth:`memory_ingest` issued inside the block (#784).

        Yields the acquired :class:`LeaseResult`. While the block is active, any
        ``memory_ingest`` that does not pass an explicit ``fence`` is fenced on
        this lease; if the lease is lost mid-block (re-acquired by another
        holder past the held fence) the offending ingest raises
        :class:`LeaseLost`. On exit the lease is released (unless
        ``release_on_exit`` is ``False``).

        Raises ``RuntimeError`` if the lease could not be acquired (already held
        by another agent)."""
        lr = self.lease(key, agent_id, ttl_ms)
        if not lr.acquired:
            raise RuntimeError(
                f"with_lease: key {key!r} is held by {lr.holder!r}"
            )
        key_bytes = _to_bytes(key)
        prev = getattr(self, "_held_lease", None)
        self._held_lease = {"key": key_bytes, "fence": lr.fence, "agent_id": agent_id}
        try:
            yield lr
        finally:
            held_fence = self._held_lease["fence"]
            self._held_lease = prev
            if release_on_exit:
                try:
                    self.release(key_bytes, held_fence)
                except grpc.RpcError:
                    pass

    def watch_prefix(
        self,
        agent_id: str,
        prefix: Union[str, bytes],
        *,
        cf: int = 0,
    ) -> Iterator[WatchEvent]:
        """Subscribe to write events matching a prefix. Yields WatchEvent."""
        stream = self._agent.WatchPrefix(
            pb.AgentWatchPrefixRequest(agent_id=agent_id, cf=cf, prefix=_to_bytes(prefix))
        )
        for event in stream:
            yield WatchEvent(
                event_type=event.event_type, cf=event.cf,
                key=event.key, value=event.value, seq=event.seq,
            )

    # ── Agent State: Temporal edges ──────────────────────────────────────

    def expire_edge(self, src_step_id: int, dst_step_id: int, edge_type: str, expire_at: int) -> None:
        self._agent.ExpireEdge(
            pb.AgentExpireEdgeRequest(
                src_step_id=src_step_id, dst_step_id=dst_step_id,
                edge_type=edge_type, expire_at=expire_at,
            )
        )

    def edge_history(self, src_step_id: int, dst_step_id: int, edge_type: str) -> list:
        resp = self._agent.EdgeHistory(
            pb.AgentEdgeHistoryRequest(
                src_step_id=src_step_id, dst_step_id=dst_step_id, edge_type=edge_type,
            )
        )
        return [
            {"src": e.src, "dst": e.dst, "edge_type": e.edge_type,
             "valid_from": e.valid_from, "valid_to": e.valid_to, "props": e.props}
            for e in resp.edges
        ]


# ── Async Client ─────────────────────────────────────────────────────────────


class AsyncClient:
    """Async version of Client, using ``grpc.aio``.

    Usage::

        from statelet import AsyncClient

        async with AsyncClient("localhost:9379") as db:
            await db.put("key", b"value")
            value = await db.get("key")
    """

    def __init__(
        self,
        addr: str = "localhost:9379",
        *,
        cf: int = 0,
        token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        mgmt_addr: Optional[str] = None,
    ) -> None:
        if token is None and username is not None:
            _mgmt = mgmt_addr or _default_mgmt_addr(addr)
            token = _login(_mgmt, username, password or "")
        if token:
            metadata = [("authorization", f"Bearer {token}")]
            self._channel = grpc.aio.insecure_channel(
                addr,
                interceptors=[_AsyncAuthInterceptor(metadata)],
                options=_DEFAULT_GRPC_CHANNEL_OPTIONS,
            )
        else:
            self._channel = grpc.aio.insecure_channel(
                addr,
                options=_DEFAULT_GRPC_CHANNEL_OPTIONS,
            )
        self._kv = pb_grpc.StateletStub(self._channel)
        self._agent = pb_grpc.AgentStateServiceStub(self._channel)
        self._cf = cf

    async def __aenter__(self) -> "AsyncClient":
        return self

    async def __aexit__(self, *_exc) -> None:
        await self.close()

    async def close(self) -> None:
        await self._channel.close()

    # ── KV operations ────────────────────────────────────────────────────

    async def put(self, key: Union[str, bytes], value: bytes, *, cf: Optional[int] = None) -> None:
        await self._kv.Put(pb.PutRequest(cf=cf if cf is not None else self._cf, key=_to_bytes(key), value=value))

    async def get(self, key: Union[str, bytes], *, cf: Optional[int] = None) -> Optional[bytes]:
        resp = await self._kv.Get(pb.GetRequest(cf=cf if cf is not None else self._cf, key=_to_bytes(key)))
        return resp.value if resp.found else None

    async def delete(self, key: Union[str, bytes], *, cf: Optional[int] = None) -> None:
        await self._kv.Delete(pb.DeleteRequest(cf=cf if cf is not None else self._cf, key=_to_bytes(key)))

    async def scan(
        self,
        prefix: Union[str, bytes] = "",
        *,
        cursor: Optional[bytes] = None,
        limit: int = 100,
        cf: Optional[int] = None,
    ) -> tuple:
        resp = await self._kv.Scan(
            pb.ScanRequest(
                cf=cf if cf is not None else self._cf,
                prefix=_to_bytes(prefix),
                cursor=cursor or b"",
                limit=limit,
            )
        )
        entries = [(e.key, e.value) for e in resp.entries]
        next_cursor = resp.next_cursor if resp.next_cursor else None
        return entries, next_cursor

    # ── Agent State: Causal Graph ────────────────────────────────────────

    async def add_step(
        self,
        agent_id: str,
        step_type: str,
        *,
        content: bytes = b"",
        metadata: Optional[bytes] = None,
        embedding: Optional[List[float]] = None,
        branch_id: int = 0,
    ) -> int:
        resp = await self._agent.AddStep(
            pb.AgentAddStepRequest(
                agent_id=agent_id, step_type=step_type, branch_id=branch_id,
                content=content, metadata=metadata or b"",
                embedding=embedding or [],
            )
        )
        return resp.step_id

    async def add_edge(
        self,
        src_step_id: int,
        dst_step_id: int,
        edge_type: str,
        *,
        props: bytes = b"",
        valid_from: int = 0,
        valid_to: int = 0,
    ) -> None:
        await self._agent.AddEdge(
            pb.AgentAddEdgeRequest(
                src_step_id=src_step_id, dst_step_id=dst_step_id,
                edge_type=edge_type, props=props,
                valid_from=valid_from, valid_to=valid_to,
            )
        )

    async def get_step(self, step_id: int) -> Optional[Step]:
        resp = await self._agent.GetStep(pb.AgentGetStepRequest(step_id=step_id))
        if not resp.found:
            return None
        return Step._from_json(resp.step_json)

    async def get_content(self, step_id: int) -> Optional[bytes]:
        resp = await self._agent.GetContent(pb.AgentGetContentRequest(step_id=step_id))
        return resp.content if resp.found else None

    async def get_edges(
        self,
        step_id: int,
        *,
        direction: str = "forward",
        edge_type: str = "",
        at_timestamp: int = 0,
        window_start: int = 0,
        window_end: int = 0,
    ) -> List[Edge]:
        resp = await self._agent.GetEdges(
            pb.AgentGetEdgesRequest(
                step_id=step_id, direction=direction, edge_type=edge_type,
                at_timestamp=at_timestamp, window_start=window_start, window_end=window_end,
            )
        )
        return [
            Edge(peer_step_id=e.peer_step_id, edge_type=e.edge_type,
                 props=e.props, valid_from=e.valid_from, valid_to=e.valid_to)
            for e in resp.edges
        ]

    async def traverse(
        self,
        start_step_id: int,
        *,
        direction: str = "forward",
        max_depth: int = 3,
    ) -> TraverseResult:
        resp = await self._agent.Traverse(
            pb.AgentTraverseRequest(
                start_step_id=start_step_id, direction=direction, max_depth=max_depth,
            )
        )
        steps = [Step._from_json(s) for s in resp.step_jsons]
        edges = [
            Edge(peer_step_id=e.peer_step_id, edge_type=e.edge_type,
                 props=e.props, valid_from=e.valid_from, valid_to=e.valid_to)
            for e in resp.edges
        ]
        return TraverseResult(steps=steps, edges=edges)

    async def find_similar_chains(
        self,
        query_embedding: List[float],
        k: int = 5,
        chain_depth: int = 3,
        ef: int = 0,
    ) -> List[CausalChain]:
        resp = await self._agent.FindSimilarChains(
            pb.AgentFindSimilarChainsRequest(
                query_embedding=query_embedding, k=k, chain_depth=chain_depth, ef=ef,
            )
        )
        chains = []
        for c in resp.chains:
            chains.append(CausalChain(
                anchor=Step._from_json(c.anchor_step_json),
                distance=c.distance,
                steps=[Step._from_json(s) for s in c.step_jsons],
                edges=[
                    Edge(peer_step_id=e.peer_step_id, edge_type=e.edge_type,
                         props=e.props, valid_from=e.valid_from, valid_to=e.valid_to)
                    for e in c.edges
                ],
            ))
        return chains

    # ── Agent State: Branching ───────────────────────────────────────────

    async def fork(self, label: str, *, parent_branch_id: int = 0) -> int:
        resp = await self._agent.Fork(
            pb.AgentForkRequest(label=label, parent_branch_id=parent_branch_id)
        )
        return resp.branch_id

    async def merge_branch(self, branch_id: int) -> None:
        await self._agent.MergeBranch(pb.AgentMergeBranchRequest(branch_id=branch_id))

    async def discard_branch(self, branch_id: int) -> None:
        await self._agent.DiscardBranch(pb.AgentDiscardBranchRequest(branch_id=branch_id))

    async def list_branches(self) -> List[BranchMeta]:
        resp = await self._agent.ListBranches(pb.AgentListBranchesRequest())
        return [
            BranchMeta(
                id=b.id, parent_id=b.parent_id,
                parent_snapshot_seq=b.parent_snapshot_seq,
                created_at=b.created_at, status=b.status, label=b.label,
            )
            for b in resp.branches
        ]

    async def branch_put(self, branch_id: int, key: Union[str, bytes], value: bytes, *, cf: int = 0) -> None:
        await self._agent.BranchPut(
            pb.AgentBranchPutRequest(branch_id=branch_id, cf=cf, key=_to_bytes(key), value=value)
        )

    async def branch_get(self, branch_id: int, key: Union[str, bytes], *, cf: int = 0) -> Optional[bytes]:
        resp = await self._agent.BranchGet(
            pb.AgentBranchGetRequest(branch_id=branch_id, cf=cf, key=_to_bytes(key))
        )
        return resp.value if resp.found else None

    # ── Agent State: Reactive state ──────────────────────────────────────

    async def cas_put(
        self,
        key: Union[str, bytes],
        expected_seq: int,
        new_value: bytes,
        *,
        cf: int = 0,
    ) -> CasPutResult:
        resp = await self._agent.CasPut(
            pb.AgentCasPutRequest(
                cf=cf, key=_to_bytes(key),
                expected_seq=expected_seq, new_value=new_value,
            )
        )
        return CasPutResult(success=resp.success, new_seq=resp.new_seq, actual_seq=resp.actual_seq)

    async def memory_ingest(
        self,
        scope: str,
        content: str,
        *,
        candidates: Optional[Iterable[Tuple[int, float]]] = None,
        provenance_steps: Optional[Iterable[int]] = None,
        embedding_id: Optional[int] = None,
        dedup_threshold: float = 0.97,
        supersede_threshold: float = 0.80,
        author_agent_id: str = "",
        confidence: float = 0.0,
        run_id: str = "",
        fence: int = 0,
        lease_key: Optional[Union[str, bytes]] = None,
    ) -> MemoryIngestResult:
        """Transactionally ingest a fact (#780), optionally fence-gated (#784).
        See ``Client.memory_ingest``."""
        cand_msgs = [
            pb.AgentIngestCandidate(fact_id=fid, sim=sim)
            for (fid, sim) in (candidates or [])
        ]
        req = pb.AgentMemoryIngestRequest(
            scope=scope,
            content=content,
            candidates=cand_msgs,
            provenance_steps=list(provenance_steps or []),
            dedup_threshold=dedup_threshold,
            supersede_threshold=supersede_threshold,
            author_agent_id=author_agent_id,
            confidence=confidence,
            run_id=run_id,
            fence=fence,
            lease_key=_to_bytes(lease_key) if lease_key is not None else b"",
        )
        if embedding_id is not None:
            req.embedding_id = embedding_id
        resp = await self._agent.MemoryIngest(req)
        return MemoryIngestResult(
            action=_INGEST_ACTIONS.get(resp.action, "unknown"),
            fact_id=resp.fact_id,
            superseded=tuple(resp.superseded),
            committed=resp.committed,
            conflict_fact_id=resp.conflict_fact_id,
            fence_lost=resp.fence_lost,
        )

    # ── Agent State: coordination (leases / fences, #752 + #784) ─────────

    async def claim(self, key: Union[str, bytes], agent_id: str) -> LeaseResult:
        """Acquire ``key`` for ``agent_id`` iff unheld. See ``Client.claim``."""
        resp = await self._agent.Claim(
            pb.AgentClaimRequest(key=_to_bytes(key), agent_id=agent_id)
        )
        return LeaseResult(acquired=resp.acquired, holder=resp.holder, fence=resp.fence)

    async def lease(self, key: Union[str, bytes], agent_id: str, ttl_ms: int) -> LeaseResult:
        """TTL-bound acquire of ``key``. See ``Client.lease``."""
        resp = await self._agent.Lease(
            pb.AgentLeaseRequest(key=_to_bytes(key), agent_id=agent_id, ttl_ms=ttl_ms)
        )
        return LeaseResult(acquired=resp.acquired, holder=resp.holder, fence=resp.fence)

    async def renew(self, key: Union[str, bytes], agent_id: str, fence: int, ttl_ms: int) -> LeaseResult:
        """Extend the lease (advances the fence). See ``Client.renew``."""
        resp = await self._agent.Renew(
            pb.AgentRenewRequest(
                key=_to_bytes(key), agent_id=agent_id, fence=fence, ttl_ms=ttl_ms
            )
        )
        return LeaseResult(acquired=resp.acquired, holder=resp.holder, fence=resp.fence)

    async def release(self, key: Union[str, bytes], fence: int) -> bool:
        """Drop the lease iff ``fence`` matches. See ``Client.release``."""
        resp = await self._agent.Release(
            pb.AgentReleaseRequest(key=_to_bytes(key), fence=fence)
        )
        return resp.released

    async def watch_prefix(
        self,
        agent_id: str,
        prefix: Union[str, bytes],
        *,
        cf: int = 0,
    ):
        """Async iterator of WatchEvent."""
        stream = self._agent.WatchPrefix(
            pb.AgentWatchPrefixRequest(agent_id=agent_id, cf=cf, prefix=_to_bytes(prefix))
        )
        async for event in stream:
            yield WatchEvent(
                event_type=event.event_type, cf=event.cf,
                key=event.key, value=event.value, seq=event.seq,
            )

    # ── Agent State: Temporal edges ──────────────────────────────────────

    async def expire_edge(self, src_step_id: int, dst_step_id: int, edge_type: str, expire_at: int) -> None:
        await self._agent.ExpireEdge(
            pb.AgentExpireEdgeRequest(
                src_step_id=src_step_id, dst_step_id=dst_step_id,
                edge_type=edge_type, expire_at=expire_at,
            )
        )

    async def edge_history(self, src_step_id: int, dst_step_id: int, edge_type: str) -> list:
        resp = await self._agent.EdgeHistory(
            pb.AgentEdgeHistoryRequest(
                src_step_id=src_step_id, dst_step_id=dst_step_id, edge_type=edge_type,
            )
        )
        return [
            {"src": e.src, "dst": e.dst, "edge_type": e.edge_type,
             "valid_from": e.valid_from, "valid_to": e.valid_to, "props": e.props}
            for e in resp.edges
        ]
