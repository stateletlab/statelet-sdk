# Statelet Go SDK

Go gRPC client for the [Statelet](https://github.com/stateletlab/statelet) distributed key-value store.

## Install

```bash
go get github.com/stateletlab/statelet-sdk/go@latest
```

This directory is its own Go module nested in a polyglot repository, so its
releases are tagged `go/vX.Y.Z` rather than `vX.Y.Z` — that prefix is what the
Go module proxy looks for.

> **Moved.** Through v0.1.x this module lived at
> `github.com/stateletlab/statelet/sdk/go`. The import path is now
> `github.com/stateletlab/statelet-sdk/go/...`; nothing else about the API
> changed, so updating the import lines is the whole migration.

## Prerequisites for regenerating stubs

The generated `statelet/proto/*.pb.go` files are committed, so consumers need
none of this. It is only required after the root proto changes.

- Go 1.21+
- `protoc` with `protoc-gen-go` and `protoc-gen-go-grpc` plugins

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

## Generate protobuf stubs

```bash
make proto
```

## Usage

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/stateletlab/statelet-sdk/go/statelet"
    pb "github.com/stateletlab/statelet-sdk/go/statelet/proto"
)

func main() {
    client, err := statelet.NewClient("127.0.0.1:7379")
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    ctx := context.Background()

    // Ping
    msg, _ := client.Ping(ctx)
    fmt.Println(msg) // "PONG"

    // KV operations
    client.Put(ctx, []byte("hello"), []byte("world"))
    val, _ := client.Get(ctx, []byte("hello"))
    fmt.Printf("got: %s\n", val)
    client.Delete(ctx, []byte("hello"))

    // Batch write
    client.BatchWrite(ctx, []statelet.WriteOp{
        {Op: pb.WriteOp_PUT, Key: []byte("k1"), Value: []byte("v1")},
        {Op: pb.WriteOp_PUT, Key: []byte("k2"), Value: []byte("v2")},
        {Op: pb.WriteOp_DELETE, Key: []byte("k3")},
    })

    // Vector operations
    client.CreateVectorIndex(ctx, "embeddings", statelet.VectorIndexConfig{
        Dim:    128,
        Metric: pb.VectorDistanceMetric_VECTOR_COSINE,
    })

    vec := make([]float32, 128)
    for i := range vec {
        vec[i] = 0.1
    }
    client.VectorPut(ctx, "embeddings", 1, vec)

    results, _ := client.VectorSearch(ctx, "embeddings", vec, 5, 0)
    for _, r := range results {
        fmt.Printf("id=%d distance=%.4f\n", r.ID, r.Distance)
    }

    client.DropVectorIndex(ctx, "embeddings")
}
```

## Agent state

The causal graph, branches, reactive state, coordination leases, temporal edges
and the prefix watch, wrapping `AgentStateService`. These are served by the
**gateway** (default `127.0.0.1:9379`) — a data node answers the KV and vector
surface only.

```go
db, _ := statelet.NewClient("127.0.0.1:9379")
defer db.Close()

// Causal graph: steps and edges.
observed, _ := db.AddStep(ctx, "agent-1", statelet.StepObserve, &statelet.AddStepOptions{
    Content:    []byte("user asked about pricing"),
    Scope:      statelet.ScopeTeam, // world (default) | team | private
    ScopeOwner: "team-a",
})
acted, _ := db.AddStep(ctx, "agent-1", statelet.StepAct, nil)
db.AddEdge(ctx, observed, acted, statelet.EdgeTriggers, nil)

walked, _ := db.Traverse(ctx, observed, statelet.Forward, 3)
for _, step := range walked.Steps {
    fmt.Printf("step=%d type=%s agent=%s\n", step.ID, step.Type, step.AgentID)
}

// Edges of one step, optionally filtered by type and valid time.
edges, _ := db.GetEdges(ctx, acted, &statelet.GetEdgesOptions{
    Direction: statelet.Backward, // Both is Traverse-only; GetEdges rejects it
    Type:      statelet.EdgeInforms,
})

// Coordination: a lease returns the fencing token to carry on fenced writes.
held, _ := db.Lease(ctx, []byte("job:42"), "agent-1", 30_000)
if held.Acquired {
    defer db.Release(ctx, []byte("job:42"), held.Fence)
}

// Watch a key prefix. Return statelet.ErrWatchStop to end the stream cleanly.
db.WatchPrefix(ctx, "agent-1", 0, []byte("state:"), func(ev statelet.WatchEvent) error {
    fmt.Printf("%s %s seq=%d\n", ev.EventType, ev.Key, ev.Seq)
    return nil
})
```

`StepType`, `EdgeType`, `Direction` and `MemoryScope` are string types over the
exact values the server parses — it is case-sensitive and rejects anything else,
so prefer the constants (`StepObserve`, `EdgeDerivedFrom`, …) and use `Valid()`
before sending a value you built yourself.

A step arrives as JSON: `Step` decodes it (including the `metadata` byte array
and the nested scope tag), and `Step.Raw` keeps the exact bytes received, so a
field added server-side stays reachable before this SDK grows to match.

Unlike `SubscribeCommitted`, `WatchPrefix` is a live tail with no offsets: it
replays no history and does not reconnect, so a dropped stream surfaces as an
error rather than silently resuming with a gap.

## Declarative graph query (openCypher subset)

Read-only pattern matching over the temporal graph, served by the gateway's
`GraphQuery` RPC: `MATCH` path patterns, `WHERE` on node properties,
`RETURN` / `ORDER BY` / `LIMIT`, a bitemporal `AS OF <valid>[, <tx>]` clause and
the retrieval procedures `db.vectorSearch` / `db.hybridSearch` / `db.graphRag`.
`CREATE` / `MERGE` are rejected.

```go
res, err := client.GraphQuery(ctx,
    "MATCH (m {id: 42})-[:supersedes]->(old) RETURN m, old LIMIT 10",
    &statelet.GraphQueryOptions{GraphName: "my_graph"})
if err != nil {
    log.Fatal(err)
}
fmt.Println(res.Columns) // [m old]
for _, row := range res.Maps() {
    fmt.Println(row["m"]) // []byte — node properties JSON
}
fmt.Println(res.Warnings) // non-empty ⇒ the result may be incomplete

// Time travel + vector-seeded expansion (inline query vector: named
// parameters like $q parse but are not resolvable yet).
res, err = client.GraphQuery(ctx,
    "CALL db.vectorSearch([0.1, 0.2, 0.3], 5) YIELD node, score RETURN node, score",
    &statelet.GraphQueryOptions{GraphName: "my_graph", AsOf: 1737000000000})
```

Pass `nil` options for the gateway defaults. Each cell is a `GraphValue` —
switch on `Kind`, or call `Interface()` for the natural Go value
(`nil` / `int64` / `float64` / `string` / `bool` / `[]byte`).

## Reranking (optional second stage)

A first-class, optional second-stage reranker over an over-fetched candidate
window — the analogue of Weaviate `.with_additional({rerank})` and Pinecone
`inference.rerank`. See [`docs/reranking.md`](../../docs/reranking.md).

```go
// Cross-encoder: hydrate passage text via the {id}/{index} template + rescore.
results, _ := client.VectorSearchReranked(ctx, "embeddings", vec, 5, 0,
    &statelet.RerankSpec{
        Model:        "cross-encoder",
        PassageField: "doc:{index}:{id}:text",
        QueryText:    "capital of France",
    })

// Score-fusion prefetch->rescore: blend the exact full-precision distance.
results, _ = client.VectorSearchReranked(ctx, "embeddings", vec, 5, 0,
    &statelet.RerankSpec{Model: "score-fusion", SignalBlend: 0.7})

// Dry-run pre-flight validation (no search executed; returns an error on a bad spec).
err := client.RerankValidate(ctx, "embeddings", statelet.RerankSpec{
    Model:        "cross-encoder",
    PassageField: "doc:{index}:{id}:text",
    QueryText:    "q",
})
```

## Durable change-feed (CDC)

`SubscribeCommitted` consumes the durable, ordered, resumable committed
change-feed with Kafka-style client-managed offsets. Supply a `SubscriptionID`
and a `CheckpointStore` (the default `FileCheckpointStore` persists offsets
atomically) to resume across restarts. With `AutoCommit`, each change's offset is
committed *after* your handler returns `nil`, giving at-least-once delivery — so
your handler must be idempotent.

```go
ckpt, _ := statelet.NewFileCheckpointStore("/var/lib/myapp/cdc.json")

err := client.SubscribeCommitted(ctx, statelet.SubscribeCommittedOptions{
    SubscriptionID: "my-consumer",
    Checkpoint:     ckpt,
    AutoCommit:     true,
    KeyPrefix:      []byte("orders/"),
    IncludeValues:  true,
}, func(ch statelet.CommittedChange) error {
    // Process the change idempotently (offset is the stable resume key).
    fmt.Printf("offset=%d op=%s key=%s snapshot=%v\n",
        ch.Offset, ch.Op, ch.Key, ch.IsSnapshot)
    return nil // returning a non-nil error stops the consumer
})
```

The consumer transparently reconnects from `last_offset+1` on disconnect, and on
a compaction notice it bootstraps a baseline via a paged `Scan` (each entry
delivered as a synthetic `put` with `IsSnapshot=true`) before resuming the live
tail from `snapshot_offset+1` — no gap.
