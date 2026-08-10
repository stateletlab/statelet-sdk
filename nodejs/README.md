# Statelet Node.js SDK

TypeScript-first Node.js client for the [Statelet](https://github.com/stateletlab/statelet) distributed key-value store.

This SDK mirrors the structure of the Java client: one main `StateletClient` class, builder helpers for batch writes and vector index configuration, and a small, direct gRPC-based API. The source is written in TypeScript and compiles to `dist/`.

The proto is loaded at runtime from `proto/statelet.proto` inside the package, which is shipped in the published tarball. That copy is generated from the repository-root [`proto/statelet.proto`](../proto/statelet.proto) by [`scripts/sync-proto.sh`](../scripts/sync-proto.sh) — edit the root file, never the copy.

## Install

```bash
npm install @grpc/grpc-js @grpc/proto-loader
```

If you want to use this package directly from the repo:

```bash
cd nodejs
npm install
npm run build
```

## Usage

```ts
import {
  StateletClient,
  WriteEntryBuilder,
  VectorIndexConfigBuilder,
  VectorDistanceMetric,
} from "statelet-client";

async function main() {
  const client = new StateletClient("127.0.0.1", 7379);
  await client.waitForReady();

  console.log(await client.ping()); // "PONG"

  await client.put("hello", "world");
  const value = await client.get("hello");
  console.log(value?.toString("utf8"));

  await client.batchWrite([
    WriteEntryBuilder.put("k1", "v1"),
    WriteEntryBuilder.put("k2", "v2"),
    WriteEntryBuilder.delete("k3"),
  ]);

  const config = new VectorIndexConfigBuilder(128)
    .metric(VectorDistanceMetric.VECTOR_COSINE)
    .efSearch(96);

  await client.createVectorIndex("embeddings", config);
  await client.vectorPut("embeddings", 1n, new Array(128).fill(0.1));

  const results = await client.vectorSearch(
    "embeddings",
    new Array(128).fill(0.15),
    5
  );

  for (const result of results) {
    console.log(`id=${result.id} distance=${result.distance}`);
  }

  await client.dropVectorIndex("embeddings");
  client.close();
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
```

## Declarative graph query (openCypher subset)

Read-only pattern matching over the temporal graph, served by the gateway's
`GraphQuery` RPC: `MATCH` path patterns, `WHERE` on node properties,
`RETURN` / `ORDER BY` / `LIMIT`, a bitemporal `AS OF <valid>[, <tx>]` clause and
the retrieval procedures `db.vectorSearch` / `db.hybridSearch` / `db.graphRag`.
`CREATE` / `MERGE` are rejected.

```ts
import { StateletClient, graphRowsToObjects } from "statelet-client";

const res = await client.graphQuery(
  "MATCH (m {id: 42})-[:supersedes]->(old) RETURN m, old LIMIT 10",
  { graphName: "my_graph" }
);

console.log(res.columns); // ["m", "old"]
for (const row of graphRowsToObjects(res)) {
  console.log(row.m); // Buffer — node properties JSON
}
console.log(res.warnings); // non-empty ⇒ the result may be incomplete

// Time travel + vector-seeded expansion (inline query vector: named
// parameters like $q parse but are not resolvable yet).
await client.graphQuery(
  "CALL db.vectorSearch([0.1, 0.2, 0.3], 5) YIELD node, score RETURN node, score",
  { graphName: "my_graph", asOf: 1737000000000n }
);
```

Each cell is a `GraphValue` — switch on `kind`, or call `graphValueToJs(value)`
for the natural JS value (`null` / `bigint` / `number` / `string` / `boolean` /
`Buffer`).

## Reranking (optional second stage)

A first-class, optional second-stage reranker over an over-fetched candidate
window — the analogue of Weaviate `.with_additional({rerank})` and Pinecone
`inference.rerank`. See [`docs/reranking.md`](../../docs/reranking.md).

```ts
import { RerankSpec } from "statelet-client";

// Cross-encoder: hydrate passage text via the {id}/{index} template + rescore.
let results = await client.vectorSearch(
  "embeddings",
  new Array(128).fill(0.15),
  5,
  0,
  undefined,
  { model: "cross-encoder", passageField: "doc:{index}:{id}:text", queryText: "capital of France" }
);

// Score-fusion prefetch->rescore: blend the exact full-precision distance.
results = await client.vectorSearch(
  "embeddings",
  new Array(128).fill(0.15),
  5,
  0,
  undefined,
  { model: "score-fusion", signalBlend: 0.7 }
);

// Dry-run pre-flight validation (no search executed; rejects on a bad spec).
await client.rerankValidate("embeddings", {
  model: "cross-encoder",
  passageField: "doc:{index}:{id}:text",
  queryText: "q",
});
```

## Managed Server

For local integration, the SDK can also start and stop a standalone `raft_engine` process for you.

```ts
import { StateletServer } from "statelet-client";

async function main() {
  const server = await StateletServer.startStandalone({
    repoRoot: "/path/to/Statelet",
    dbPath: "/tmp/statelet-node-demo",
    grpcAddr: "127.0.0.1:7379",
    env: {
      METRICS_ADDR: "127.0.0.1:19091",
    },
  });

  const client = server.createClient();
  await client.put("hello", "world");
  console.log((await client.get("hello"))?.toString("utf8"));
  client.close();

  await server.stop();
}
```

Notes:

- This uses a separate process, not in-process embedding.
- The expected binary is `target/debug/raft_engine` by default.
- Build it first with `cargo build --features data-node --bin raft_engine`.
- Build the SDK itself with `npm run build`.
- If you run multiple local servers, set a unique `METRICS_ADDR` for each process.

## Constructor Forms

```ts
new StateletClient("127.0.0.1", 7379);
new StateletClient("127.0.0.1:7379");
new StateletClient("127.0.0.1:7379", { defaultCf: 2 });
```

## Notes

- Keys and values accept `Buffer`, `Uint8Array`, or UTF-8 `string`.
- Vector IDs are returned as `bigint` to avoid losing `uint64` precision.
- The client also includes `scan`, `deleteByPrefix`, `vectorBatchPut`, `vectorBatchDelete`, `vectorTrain`, `vectorSample`, and `getNodeStats`.
- `StateletServer.startStandalone()` manages a local standalone `raft_engine` child process.
- For authenticated gateway deployments, pass default gRPC metadata in the constructor:

```ts
const client = new StateletClient("127.0.0.1:9379", {
  metadata: {
    authorization: "Bearer <jwt>",
  },
});
```
