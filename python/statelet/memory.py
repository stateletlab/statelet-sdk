"""High-level agent memory API built on Statelet's KV + Vector + Graph engines.

Usage::

    from statelet import AgentMemory

    memory = AgentMemory()
    # Store an observation
    obs_id = memory.observe("user prefers dark mode", metadata={"source": "settings"})
    # Link two observations causally
    memory.link(cause_id, effect_id, relation="caused")
    # Recall similar memories
    results = memory.recall("what does the user prefer?", k=5)
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from statelet.client import StateletClient


# Default names for internal indexes.
_GRAPH_NAME = "agent_memory"
_VECTOR_INDEX = "agent_memory_vectors"
_KV_CF = 1  # user column family


@dataclass
class MemoryEntry:
    """A single memory entry returned by recall."""

    id: int
    text: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    distance: float = 0.0
    timestamp: float = 0.0


class AgentMemory:
    """High-level agent memory backed by Statelet.

    Provides three primitives:

    * **observe** — store a text observation (KV + optional vector + graph node)
    * **link** — create a causal/temporal edge between two observations
    * **recall** — retrieve similar past observations via vector search + graph traversal

    Args:
        addr: Statelet gRPC address (default ``"127.0.0.1:9379"``).
        agent_id: Namespace for this agent's memories.
        embed_fn: Optional callable ``(text) -> List[float]`` for embedding.
            If not provided, vector search is disabled and recall falls back to
            prefix scan on KV.
        dim: Embedding dimensionality (required if *embed_fn* is provided).
        auto_init: Automatically create graph + vector indexes on first use.
        token: JWT for authenticated gateways — passed straight to the
            underlying :class:`StateletClient`. Obtain one with
            :meth:`StateletClient.login` (or pass *username*/*password* below
            to log in automatically). Without it, a gateway running with auth
            enabled rejects every call with ``UNAUTHENTICATED``.
        username / password / mgmt_addr: Convenience auto-login: when *token*
            is not given but *username* is, logs in against the management API
            (default ``<host>:9380``) and uses the returned JWT.
    """

    def __init__(
        self,
        addr: str = "127.0.0.1:9379",
        agent_id: str = "default",
        embed_fn=None,
        dim: int = 0,
        auto_init: bool = True,
        *,
        token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        mgmt_addr: Optional[str] = None,
    ):
        if token is None and username is not None:
            host = addr.rsplit(":", 1)[0]
            token = StateletClient.login(
                mgmt_addr or f"{host}:9380", username, password or ""
            )
        self._client = StateletClient(addr, cf=_KV_CF, token=token)
        self._agent_id = agent_id
        self._embed_fn = embed_fn
        self._dim = dim
        self._initialized = False
        self._auto_init = auto_init

    def _ensure_init(self):
        """Lazily create graph + vector indexes."""
        if self._initialized:
            return
        if not self._auto_init:
            self._initialized = True
            return
        try:
            self._client.create_graph_index(
                _GRAPH_NAME, dim=self._dim, metric="cosine",
            )
        except Exception:
            pass  # already exists
        if self._embed_fn and self._dim > 0:
            try:
                from statelet.client import VectorIndexConfig
                self._client.create_vector_index(
                    _VECTOR_INDEX,
                    VectorIndexConfig(dim=self._dim, metric="cosine"),
                )
            except Exception:
                pass  # already exists
        self._initialized = True

    def observe(
        self,
        text: str,
        metadata: Optional[Dict[str, Any]] = None,
        observation_id: Optional[int] = None,
    ) -> int:
        """Store a text observation.

        Returns the observation ID (monotonic microsecond timestamp).
        """
        self._ensure_init()
        obs_id = observation_id or int(time.time() * 1_000_000)
        ts = int(time.time())

        # Build properties JSON.
        props = {
            "agent_id": self._agent_id,
            "text": text,
            "ts": ts,
            **(metadata or {}),
        }
        props_bytes = json.dumps(props).encode()

        # 1. Store in KV for durable lookup.
        kv_key = f"mem:{self._agent_id}:{obs_id}".encode()
        self._client.put(kv_key, props_bytes)

        # 2. Add graph node (with optional embedding vector).
        embedding = []
        if self._embed_fn:
            embedding = self._embed_fn(text)
        try:
            self._client.graph_add_node(_GRAPH_NAME, obs_id, props_bytes, embedding)
        except Exception:
            pass  # graph not available — KV is the fallback

        # 3. Store embedding in vector index for search.
        if embedding:
            try:
                self._client.vector_put(_VECTOR_INDEX, obs_id, embedding)
            except Exception:
                pass  # vector index not available

        return obs_id

    def link(
        self,
        cause_id: int,
        effect_id: int,
        relation: str = "caused",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Create a causal/temporal edge between two observations."""
        self._ensure_init()
        ts = int(time.time())
        props = json.dumps(metadata or {}).encode()
        try:
            self._client.graph_add_edge(
                _GRAPH_NAME, cause_id, effect_id, relation,
                valid_from=ts, properties=props,
            )
        except Exception:
            # Fallback: store edge in KV.
            edge_key = f"mem_edge:{self._agent_id}:{cause_id}:{effect_id}".encode()
            edge_val = json.dumps({
                "src": cause_id, "dst": effect_id,
                "relation": relation, "ts": ts,
                **(metadata or {}),
            }).encode()
            self._client.put(edge_key, edge_val)

    def recall(
        self,
        query: str,
        k: int = 5,
    ) -> List[MemoryEntry]:
        """Retrieve the most relevant past observations.

        Uses vector search if an embedding function is available,
        otherwise falls back to a KV prefix scan (most recent).
        """
        self._ensure_init()

        if self._embed_fn:
            return self._recall_vector(query, k)
        return self._recall_kv(k)

    def _recall_vector(self, query: str, k: int) -> List[MemoryEntry]:
        """Vector similarity search + KV hydration."""
        query_vec = self._embed_fn(query)
        try:
            results = self._client.vector_search(_VECTOR_INDEX, query_vec, k)
        except Exception:
            return self._recall_kv(k)

        entries = []
        for r in results:
            kv_key = f"mem:{self._agent_id}:{r.id}".encode()
            raw = self._client.get(kv_key)
            if raw:
                props = json.loads(raw)
                entries.append(MemoryEntry(
                    id=r.id,
                    text=props.get("text", ""),
                    metadata={k: v for k, v in props.items() if k not in ("text", "ts", "agent_id")},
                    distance=r.distance,
                    timestamp=props.get("ts", 0),
                ))
        return entries

    def _recall_kv(self, k: int) -> List[MemoryEntry]:
        """Fallback: scan recent memories from KV."""
        prefix = f"mem:{self._agent_id}:".encode()
        kvs, _ = self._client.scan(prefix, limit=k)
        entries = []
        for key, val in kvs:
            obs_id = int(key.decode().split(":")[-1])
            props = json.loads(val)
            entries.append(MemoryEntry(
                id=obs_id,
                text=props.get("text", ""),
                metadata={k_: v for k_, v in props.items() if k_ not in ("text", "ts", "agent_id")},
                timestamp=props.get("ts", 0),
            ))
        return entries

    def get_edges(
        self,
        node_id: int,
        edge_type: str = "",
        reverse: bool = False,
    ) -> list:
        """Query edges from a memory node (causal chain traversal)."""
        try:
            return self._client.graph_query_edges(
                _GRAPH_NAME, node_id, edge_type=edge_type, reverse=reverse,
            )
        except Exception:
            return []

    def close(self):
        """Close the underlying connection."""
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()
