# Statelet Python SDK

Agent memory with KV, vector search, and temporal causal graphs — in one database.

## Quick Start

```bash
pip install statelet-sdk
```

`statelet-sdk` is the client on its own. `pip install statelet` gets you the
same library plus the server binaries, and is the better choice if you also
want to run a node locally.

### High-Level Client

```python
from statelet import Client

# No auth (local dev without GATEWAY_JWT_SECRET)
db = Client("localhost:9379")

# With auth (auto-login, fetches JWT from gateway management API)
db = Client("localhost:9379", username="admin", password="admin")

# With a pre-obtained token
db = Client("localhost:9379", token="eyJ...")

# KV — keys can be str or bytes
db.put("key", b"value")
print(db.get("key"))          # b"value"
db.delete("key")
```

### Agent State: Causal Graph

```python
from statelet import Client

db = Client("localhost:9379", username="admin", password="admin")

# Add steps to the causal graph
s1 = db.add_step("agent-1", "Observe", content=b"user clicked buy")
s2 = db.add_step("agent-1", "Think",   content=b"should confirm order")
s3 = db.add_step("agent-1", "Act",     content=b"sent confirmation email")

# Link them causally
db.add_edge(s1, s2, "Triggers")
db.add_edge(s2, s3, "Triggers")

# Traverse the chain
result = db.traverse(s1, direction="forward", max_depth=5)
for step in result.steps:
    print(f"  [{step.step_type}] {step.content}")

# Find similar causal chains by embedding
chains = db.find_similar_chains(query_embedding=[0.1] * 128, k=3, chain_depth=3)
```

### Agent State: Branching (Fork)

```python
# Fork a speculative branch
branch_id = db.fork("experiment-a")

# Write to the branch (isolated from main timeline)
db.branch_put(branch_id, "config:model", b"gpt-4o")
val = db.branch_get(branch_id, "config:model")

# Merge back or discard
db.merge_branch(branch_id)
# db.discard_branch(branch_id)

# List all branches
for b in db.list_branches():
    print(f"  branch {b.id}: {b.label} ({b.status})")
```

### Agent State: Reactive State

```python
# Compare-and-swap for optimistic concurrency
result = db.cas_put("counter", expected_seq=0, new_value=b"1")
if result.success:
    print(f"written, new version: {result.new_seq}")
else:
    print(f"conflict, current version: {result.actual_seq}")

# Watch for changes (streaming)
for event in db.watch_prefix("agent-1", "state:"):
    print(f"  {event.event_type}: {event.key} = {event.value}")
```

### Async Client

```python
import asyncio
from statelet import AsyncClient

async def main():
    async with AsyncClient("localhost:9379", username="admin", password="admin") as db:
        await db.put("key", b"value")
        print(await db.get("key"))

        step_id = await db.add_step("agent-1", "Observe", content=b"hello")
        step = await db.get_step(step_id)
        print(step)

asyncio.run(main())
```

### Agent Memory (High-Level 3-Primitive API)

```python
from statelet import AgentMemory

memory = AgentMemory("127.0.0.1:9379")

# Store observations
obs1 = memory.observe("user prefers dark mode", metadata={"source": "settings"})
obs2 = memory.observe("user switched to light mode after update")

# Link them causally
memory.link(obs1, obs2, relation="caused")

# Recall relevant memories
results = memory.recall("what theme does the user prefer?", k=5)
for r in results:
    print(f"  [{r.id}] {r.text} (distance={r.distance:.3f})")
```

### Low-Level Client (bytes keys, direct gRPC)

```python
from statelet import StateletClient, VectorIndexConfig

with StateletClient("127.0.0.1:9379") as client:
    # KV
    client.put(b"key", b"value")
    print(client.get(b"key"))  # b"value"

    # Vector search
    client.create_vector_index("my_index", VectorIndexConfig(dim=768, metric="cosine"))
    client.vector_put("my_index", 1, [0.1] * 768)
    results = client.vector_search("my_index", [0.1] * 768, k=5)

    # Graph
    client.create_graph_index("my_graph", dim=768, metric="cosine")
    client.graph_add_node("my_graph", node_id=1, properties=b'{"type":"event"}')
    client.graph_add_edge("my_graph", src=1, dst=2, edge_type="caused", valid_from=1700000000)
```

### Declarative graph query (openCypher subset)

Read-only pattern matching over the temporal graph, served by the gateway's
`GraphQuery` RPC: `MATCH` path patterns, `WHERE` on node properties,
`RETURN` / `ORDER BY` / `LIMIT`, a bitemporal `AS OF <valid>[, <tx>]` clause and
the retrieval procedures `db.vectorSearch` / `db.hybridSearch` / `db.graphRag`.
`CREATE` / `MERGE` are rejected.

```python
from statelet import StateletClient

with StateletClient("127.0.0.1:9379") as client:
    res = client.graph_query(
        "MATCH (m {id: 42})-[:supersedes]->(old) RETURN m, old LIMIT 10",
        graph_name="my_graph",
    )
    print(res.columns)          # ["m", "old"]
    for row in res.dicts():     # column-keyed rows
        print(row["m"])         # node properties, JSON-decoded
    print(res.warnings)         # non-empty ⇒ the result may be incomplete

    # Time travel + vector-seeded expansion (inline query vector: named
    # parameters like $q parse but are not resolvable yet).
    res = client.graph_query(
        "CALL db.vectorSearch([0.1, 0.2, 0.3], 5) YIELD node, score "
        "RETURN node, score",
        graph_name="my_graph",
        as_of=1737000000000,
    )
```

### Reranking (optional second stage)

A first-class, optional second-stage reranker over an over-fetched candidate
window — the analogue of Weaviate `.with_additional({rerank})` and Pinecone
`inference.rerank`. See [`docs/reranking.md`](../../docs/reranking.md).

```python
from statelet import StateletClient, RerankSpec

with StateletClient("127.0.0.1:9379") as client:
    # Cross-encoder: hydrate passage text via the {id}/{index} template + rescore.
    results = client.vector_search(
        "my_index", [0.1] * 768, k=5,
        rerank=RerankSpec(
            model="cross-encoder",
            passage_field="doc:{index}:{id}:text",
            query_text="capital of France",
        ),
    )

    # Score-fusion prefetch->rescore: blend the exact full-precision distance.
    results = client.vector_search(
        "my_index", [0.1] * 768, k=5,
        rerank=RerankSpec(model="score-fusion", signal_blend=0.7),
    )

    # Dry-run pre-flight validation (no search executed; raises on a bad spec).
    client.rerank_validate(
        "my_index",
        RerankSpec(
            model="cross-encoder",
            passage_field="doc:{index}:{id}:text",
            query_text="q",
        ),
    )
```

### Conflict-as-data (authority resolution)

When competing claims about the same entity coexist (linked by `contradicts`
edges), the gateway can arbitrate which claim is authoritative at read time
without ever dropping the minority claims (epic #694).

```python
from statelet import StateletClient

with StateletClient("127.0.0.1:9379") as client:
    # Resolve the conflict set that any member node belongs to.
    res = client.resolve_conflict("my_graph", node_id=42, policy="trust")
    if res.found:
        print(res.authoritative)   # winning claim node id
        print(res.dissenting)      # every other claim (live + retired)
        print(res.policy, res.score, res.rationale)

    # Or re-rank a text search so the authoritative claim floats to the top
    # while dissent stays retrievable (requires the Phase 3 gateway).
    out = client.text_graph_search(
        "my_graph", "current employer", conflict_policy="trust", include_dissent=True,
    )
    print(out.conflict_resolutions_json)  # per-set resolution log (or None)
```

The `eval/conflict_resolution_eval.py` harness measures authoritative-claim
accuracy and dissent recall over this surface against the pool-degrade baseline.

## API Reference

### `Client` (recommended)

| Method | Description |
|--------|-------------|
| `put(key, value)` | Write a KV pair (str or bytes key) |
| `get(key)` | Read a value (returns `None` if not found) |
| `delete(key)` | Delete a key |
| `scan(prefix, limit=100)` | Scan keys by prefix |
| `add_step(agent_id, step_type, ...)` | Add a causal step |
| `add_edge(src, dst, edge_type)` | Add a causal edge |
| `get_step(step_id)` | Get step metadata |
| `get_content(step_id)` | Get step content |
| `get_edges(step_id, direction=...)` | Query edges |
| `traverse(start, direction=..., max_depth=...)` | BFS graph traversal |
| `find_similar_chains(embedding, k=...)` | Vector-anchored chain search |
| `fork(label)` | Create a speculative branch |
| `merge_branch(branch_id)` | Merge branch to main |
| `discard_branch(branch_id)` | Discard branch |
| `branch_put(branch_id, key, value)` | Write to branch |
| `branch_get(branch_id, key)` | Read from branch |
| `cas_put(key, expected_seq, new_value)` | Compare-and-swap |
| `watch_prefix(agent_id, prefix)` | Stream write events |
| `expire_edge(src, dst, type, at)` | Expire a temporal edge |
| `edge_history(src, dst, type)` | Get edge version history |

### `AsyncClient`

Same API as `Client`, all methods are `async`.

## Why Statelet?

| | Redis + Pinecone + Neo4j | mem0 | Statelet |
|---|---|---|---|
| Deploy | 3 clusters | depends on external DBs | **1 process** |
| Causal reasoning | DIY | flat triplets | **native temporal causal graph** |
| Vector + graph query | 3 round-trips | not supported | **1 query** |
| Streaming writes | Pinecone rebuilds index | depends on backend | **SPFreshLSM incremental** |

## Requirements

- Statelet server running (see [statelet.com](https://statelet.com))
- Python >= 3.9
