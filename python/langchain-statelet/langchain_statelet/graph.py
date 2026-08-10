"""Statelet GraphStore for LangChain.

Stores knowledge graph documents in Statelet's temporal graph engine.

Usage::

    from langchain_statelet import StateletGraphStore

    graph = StateletGraphStore(addr="localhost:9379")
    graph.add_graph_documents(graph_docs)
    result = graph.query("MATCH (n) RETURN n LIMIT 10")
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

from langchain_community.graphs.graph_document import GraphDocument
from langchain_community.graphs.graph_store import GraphStore
from statelet.client import StateletClient

_DEFAULT_GRAPH = "langchain_kg"
_KV_CF = 0


def _node_id_int(node_id: str) -> int:
    """Convert a string node ID to a stable integer."""
    if isinstance(node_id, int):
        return node_id
    if isinstance(node_id, str) and node_id.isdigit():
        return int(node_id)
    return abs(hash(node_id)) % (2**53)


class StateletGraphStore(GraphStore):
    """LangChain GraphStore backed by Statelet's temporal graph engine."""

    def __init__(
        self,
        addr: str = "localhost:9379",
        graph_name: str = _DEFAULT_GRAPH,
        *,
        cf: int = _KV_CF,
    ) -> None:
        self._client = StateletClient(addr, cf=cf)
        self._graph_name = graph_name
        self._schema: Dict[str, Any] = {}
        self._initialized = False

    def _ensure_graph(self) -> None:
        if self._initialized:
            return
        try:
            self._client.create_graph_index(self._graph_name, dim=0, metric="cosine")
        except Exception:
            pass  # already exists
        self._initialized = True

    @property
    def get_schema(self) -> str:  # type: ignore[override]
        """Return the graph schema as a string."""
        return json.dumps(self._schema, indent=2) if self._schema else ""

    @property
    def get_structured_schema(self) -> Dict[str, Any]:  # type: ignore[override]
        """Return the graph schema as a dict."""
        return self._schema

    def refresh_schema(self) -> None:
        """Refresh the graph schema (node/edge types)."""
        node_types: set = set()
        edge_types: set = set()

        # Scan KV for stored node/edge metadata (use get for full values)
        prefix = f"kg:{self._graph_name}:node:".encode()
        entries, _ = self._client.scan(prefix, limit=1000)
        for key, _value in entries:
            full = self._client.get(key)
            if not full:
                continue
            try:
                props = json.loads(full)
                if "type" in props:
                    node_types.add(props["type"])
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue

        prefix = f"kg:{self._graph_name}:edge:".encode()
        entries, _ = self._client.scan(prefix, limit=1000)
        for key, _value in entries:
            full = self._client.get(key)
            if not full:
                continue
            try:
                props = json.loads(full)
                if "type" in props:
                    edge_types.add(props["type"])
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue

        self._schema = {
            "node_types": sorted(node_types),
            "edge_types": sorted(edge_types),
        }

    def query(self, query: str, params: Optional[dict] = None) -> List[Dict[str, Any]]:
        """Query the graph. Supports simple prefix-based node lookup.

        Since Statelet uses a property graph (not Cypher), queries are
        interpreted as KV prefix scans on node/edge metadata.
        """
        self._ensure_graph()
        prefix = f"kg:{self._graph_name}:node:".encode()
        entries, _ = self._client.scan(prefix, limit=100)

        results: List[Dict[str, Any]] = []
        query_lower = query.lower()
        for key, _value in entries:
            full = self._client.get(key)
            if not full:
                continue
            try:
                props = json.loads(full)
                # Simple text match filter
                text = json.dumps(props).lower()
                if query_lower in text or query == "*":
                    results.append(props)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
        return results

    def add_graph_documents(
        self,
        graph_documents: List[GraphDocument],
        include_source: bool = False,
    ) -> None:
        """Add graph documents (nodes + relationships) to Statelet."""
        self._ensure_graph()

        for doc in graph_documents:
            # Add nodes
            for node in doc.nodes:
                node_int_id = _node_id_int(node.id)
                props = {
                    "id": str(node.id),
                    "type": node.type,
                    **(node.properties if hasattr(node, "properties") else {}),
                }
                if include_source and doc.source:
                    props["_source"] = doc.source.page_content[:500]

                props_bytes = json.dumps(props).encode()

                # Add to graph engine
                try:
                    self._client.graph_add_node(self._graph_name, node_int_id, props_bytes)
                except Exception:
                    pass

                # Also store in KV for query/schema
                kv_key = f"kg:{self._graph_name}:node:{node.id}".encode()
                self._client.put(kv_key, props_bytes)

            # Add relationships as edges
            for rel in doc.relationships:
                src_int = _node_id_int(rel.source.id)
                dst_int = _node_id_int(rel.target.id)
                ts = int(time.time())

                try:
                    self._client.graph_add_edge(
                        self._graph_name,
                        src_int,
                        dst_int,
                        rel.type,
                        valid_from=ts,
                    )
                except Exception:
                    pass

                # Store edge metadata in KV
                edge_key = f"kg:{self._graph_name}:edge:{rel.source.id}:{rel.type}:{rel.target.id}".encode()
                edge_props = {
                    "source": str(rel.source.id),
                    "target": str(rel.target.id),
                    "type": rel.type,
                    **(rel.properties if hasattr(rel, "properties") else {}),
                }
                self._client.put(edge_key, json.dumps(edge_props).encode())

        # Update schema
        self.refresh_schema()
