# Statelet Java SDK

Java client for the [Statelet](https://github.com/stateletlab/statelet) distributed key-value store.

## Prerequisites

- Java 17+
- Maven 3.8+

## Build

```bash
# src/main/proto/statelet.proto is a vendored copy of the repository-root proto,
# refreshed by ../scripts/sync-proto.sh. It is committed, so this step is only
# needed after the root proto changes.
../scripts/sync-proto.sh

# Build with Maven (generates protobuf stubs automatically)
mvn clean compile
```

## Usage

```java
import ai.statelet.client.StateletClient;
import ai.statelet.client.StateletClient.*;

import java.util.List;

try (StateletClient client = new StateletClient("127.0.0.1", 7379)) {
    // Ping
    System.out.println(client.ping()); // "PONG"

    // KV operations
    client.put("hello".getBytes(), "world".getBytes());
    byte[] value = client.get("hello".getBytes()).orElse(null);
    client.delete("hello".getBytes());

    // Batch write
    client.batchWrite(List.of(
        WriteEntryBuilder.put("k1".getBytes(), "v1".getBytes()),
        WriteEntryBuilder.put("k2".getBytes(), "v2".getBytes()),
        WriteEntryBuilder.delete("k3".getBytes())
    ));

    // Vector operations
    var config = new VectorIndexConfigBuilder(128)
        .metric(statelet.Statelet.VectorDistanceMetric.VECTOR_COSINE);
    client.createVectorIndex("embeddings", config);

    client.vectorPut("embeddings", 1, List.of(0.1f, 0.2f, /* ... */ 0.3f));

    List<VectorResult> results = client.vectorSearch("embeddings",
        List.of(0.15f, /* ... */ 0.25f), 5);
    for (VectorResult r : results) {
        System.out.printf("id=%d distance=%.4f%n", r.id(), r.distance());
    }

    client.dropVectorIndex("embeddings");
}
```

## Declarative graph query (openCypher subset)

Read-only pattern matching over the temporal graph, served by the gateway's
`GraphQuery` RPC: `MATCH` path patterns, `WHERE` on node properties,
`RETURN` / `ORDER BY` / `LIMIT`, a bitemporal `AS OF <valid>[, <tx>]` clause and
the retrieval procedures `db.vectorSearch` / `db.hybridSearch` / `db.graphRag`.
`CREATE` / `MERGE` are rejected.

```java
import ai.statelet.client.StateletClient.GraphQueryOptions;
import ai.statelet.client.StateletClient.GraphQueryResult;
import ai.statelet.client.StateletClient.GraphValue;

GraphQueryResult res = client.graphQuery(
    "MATCH (m {id: 42})-[:supersedes]->(old) RETURN m, old LIMIT 10",
    new GraphQueryOptions("my_graph"));

System.out.println(res.columns());   // [m, old]
for (List<GraphValue> row : res.rows()) {
    System.out.println(row.get(0).asObject());  // byte[] — node properties JSON
}
System.out.println(res.warnings());  // non-empty ⇒ the result may be incomplete

// Time travel + vector-seeded expansion (inline query vector: named
// parameters like $q parse but are not resolvable yet).
res = client.graphQuery(
    "CALL db.vectorSearch([0.1, 0.2, 0.3], 5) YIELD node, score RETURN node, score",
    new GraphQueryOptions("my_graph", 0, 1737000000000L, 0L));
```

`client.graphQuery(cypher)` uses the gateway defaults. Each cell is a
`GraphValue` — switch on `kind()`, or call `asObject()` for the natural Java
value (`null` / `Long` / `Double` / `String` / `Boolean` / `byte[]`).

## Reranking (optional second stage)

A first-class, optional second-stage reranker over an over-fetched candidate
window — the analogue of Weaviate `.with_additional({rerank})` and Pinecone
`inference.rerank`. See [`docs/reranking.md`](../../docs/reranking.md).

```java
import statelet.Statelet.RerankSpec;

// Cross-encoder: hydrate passage text via the {id}/{index} template + rescore.
List<VectorResult> results = client.vectorSearch("embeddings", query, 5, 0,
    RerankSpec.newBuilder()
        .setModel("cross-encoder")
        .setPassageField("doc:{index}:{id}:text")
        .setQueryText("capital of France")
        .build());

// Score-fusion prefetch->rescore: blend the exact full-precision distance.
results = client.vectorSearch("embeddings", query, 5, 0,
    RerankSpec.newBuilder().setModel("score-fusion").setSignalBlend(0.7f).build());

// Dry-run pre-flight validation (no search executed; throws on a bad spec).
client.rerankValidate("embeddings", RerankSpec.newBuilder()
    .setModel("cross-encoder")
    .setPassageField("doc:{index}:{id}:text")
    .setQueryText("q")
    .build());
```
