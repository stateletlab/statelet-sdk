# Statelet C++ SDK

C++ gRPC client for the [Statelet](https://github.com/stateletlab/statelet) distributed key-value store.

## Prerequisites

- CMake 3.20+
- C++17 compiler
- gRPC and Protobuf installed (e.g. via `brew install grpc protobuf` or vcpkg)

## Build

```bash
mkdir build && cd build
cmake ..
make
```

## Usage

```cpp
#include "statelet/client.h"

int main() {
    statelet::Client client("127.0.0.1:7379");

    // Ping
    printf("%s\n", client.ping().c_str());

    // KV operations
    client.put("hello", "world");
    auto val = client.get("hello");
    if (val) printf("got: %s\n", val->c_str());
    client.del("hello");

    // Batch write
    client.batch_write({
        {statelet::WriteOpType::Put, 0, "k1", "v1"},
        {statelet::WriteOpType::Delete, 0, "k3", ""},
    });

    // Vector operations
    statelet::VectorIndexConfig cfg;
    cfg.dim = 128;
    cfg.metric = ::statelet::v1::VECTOR_COSINE;
    client.create_vector_index("embeddings", cfg);

    std::vector<float> vec(128, 0.1f);
    client.vector_put("embeddings", 1, vec);

    auto results = client.vector_search("embeddings", vec, 5);
    for (const auto& r : results) {
        printf("id=%llu distance=%.4f\n", (unsigned long long)r.id, r.distance);
    }

    client.drop_vector_index("embeddings");
}
```

## Agent state

The causal graph, branches, reactive state, coordination leases, temporal edges
and the prefix watch, wrapping `AgentStateService`. These are served by the
**gateway** (default `127.0.0.1:9379`) — a data node answers the KV and vector
surface only.

Calls that yield an id take an out-parameter and return the `grpc::Status`;
plain reads return `std::optional` / an empty vector.

```cpp
statelet::Client db("127.0.0.1:9379");

// Causal graph: steps and edges.
uint64_t observed = 0;
statelet::AddStepOptions opts;
opts.content = "user asked about pricing";
opts.scope = statelet::MemoryScope::Team;   // world (default) | team | private
opts.scope_owner = "team-a";
db.add_step("agent-1", statelet::StepType::Observe, opts, &observed);

uint64_t acted = 0;
db.add_step("agent-1", statelet::StepType::Act, &acted);
db.add_edge(observed, acted, statelet::EdgeType::Triggers);

auto walked = db.traverse(observed, statelet::Direction::Forward, 3);
for (const auto& step : walked.steps) {
    printf("step=%llu type=%s\n", (unsigned long long)step.id, step.step_type.c_str());
}

// Edges of one step, optionally filtered by type and valid time.
statelet::GetEdgesOptions edge_opts;
edge_opts.direction = statelet::Direction::Backward;   // Both is traverse-only
edge_opts.edge_type = statelet::EdgeType::Informs;
auto edges = db.get_edges(acted, edge_opts);

// Coordination: a lease returns the fencing token to carry on fenced writes.
statelet::LeaseResult held;
db.lease("job:42", "agent-1", 30000, &held);
if (held.acquired) {
    bool released = false;
    db.release("job:42", held.fence, &released);
}

// Watch a key prefix; returning false from the callback ends the stream.
db.watch_prefix("agent-1", 0, "state:", [](const statelet::WatchEvent& event) {
    printf("%s %s seq=%llu\n", event.event_type.c_str(), event.key.c_str(),
           (unsigned long long)event.seq);
    return true;
});
```

`StepType` / `EdgeType` / `Direction` / `MemoryScope` are enums over the exact
strings the server parses (it is case-sensitive and rejects anything else).
Results keep the raw wire string — `Step::step_type`, `Edge::edge_type` — so an
unknown newer kind is never lost; map it with `step_type_from_string()` /
`edge_type_from_string()`.

A step arrives as JSON, which `parse_step()` decodes into `Step`. It is
best-effort and total: an unknown member is skipped, a missing one keeps its
default (an untagged step decodes as `MemoryScope::World`, matching the
server), and `Step::raw_json` always carries the exact bytes received.

The value types and that decoder carry no gRPC dependency, so they build and
test standalone:

```bash
c++ -std=c++17 -I include tests/agent_types_test.cpp src/agent_types.cpp -o t && ./t
```

## Declarative graph query (openCypher subset)

Read-only pattern matching over the temporal graph, served by the gateway's
`GraphQuery` RPC: `MATCH` path patterns, `WHERE` on node properties,
`RETURN` / `ORDER BY` / `LIMIT`, a bitemporal `AS OF <valid>[, <tx>]` clause and
the retrieval procedures `db.vectorSearch` / `db.hybridSearch` / `db.graphRag`.
`CREATE` / `MERGE` are rejected.

```cpp
statelet::GraphQueryOptions opts;
opts.graph_name = "my_graph";

statelet::GraphQueryResult res;
grpc::Status st = client.graph_query(
    "MATCH (m {id: 42})-[:supersedes]->(old) RETURN m, old LIMIT 10", opts, &res);
if (!st.ok()) { /* handle */ }

for (const auto& row : res.rows) {
    for (const auto& v : row) {
        if (v.kind == statelet::GraphValueKind::Json) {
            std::cout << v.json_value << "\n";  // node properties JSON
        }
    }
}
// res.warnings non-empty => the result may be incomplete.

// Time travel + vector-seeded expansion (inline query vector: named
// parameters like $q parse but are not resolvable yet).
opts.as_of = 1737000000000ULL;
client.graph_query(
    "CALL db.vectorSearch([0.1, 0.2, 0.3], 5) YIELD node, score RETURN node, score",
    opts, &res);
```

The two-argument overload `graph_query(cypher, &res)` uses the gateway defaults.

## Reranking (optional second stage)

A first-class, optional second-stage reranker over an over-fetched candidate
window — the analogue of Weaviate `.with_additional({rerank})` and Pinecone
`inference.rerank`. See [`docs/reranking.md`](../../docs/reranking.md).

```cpp
// Cross-encoder: hydrate passage text via the {id}/{index} template + rescore.
statelet::RerankSpec rr;
rr.model = "cross-encoder";
rr.passage_field = "doc:{index}:{id}:text";
rr.query_text = "capital of France";
auto results = client.vector_search("embeddings", vec, 5, 0, rr);

// Score-fusion prefetch->rescore: blend the exact full-precision distance.
statelet::RerankSpec sf;
sf.model = "score-fusion";
sf.signal_blend = 0.7f;
results = client.vector_search("embeddings", vec, 5, 0, sf);

// Dry-run pre-flight validation (no search executed; status carries the error).
grpc::Status st = client.rerank_validate("embeddings", rr);
```
