# Statelet Rust SDK

Async Rust gRPC client for the [Statelet](https://github.com/stateletlab/statelet) distributed key-value store.

## Add to Cargo.toml

```toml
[dependencies]
statelet-client = { git = "https://github.com/stateletlab/statelet-sdk", branch = "main" }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

## Usage

```rust
use statelet_client::{StateletClient, VectorIndexConfig, WriteOp};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = StateletClient::connect("http://127.0.0.1:7379").await?;

    // Ping
    println!("{}", client.ping().await?);

    // KV operations
    client.put(b"hello", b"world", None).await?;
    if let Some(value) = client.get(b"hello", None).await? {
        println!("got: {}", String::from_utf8_lossy(&value));
    }
    client.delete(b"hello", None).await?;

    // Batch write
    client.batch_write(vec![
        WriteOp::Put { cf: 0, key: b"k1".to_vec(), value: b"v1".to_vec() },
        WriteOp::Put { cf: 0, key: b"k2".to_vec(), value: b"v2".to_vec() },
        WriteOp::Delete { cf: 0, key: b"k3".to_vec() },
    ]).await?;

    // Vector operations
    let config = VectorIndexConfig { dim: 128, metric: 1, ..Default::default() };
    client.create_vector_index("embeddings", config).await?;
    client.vector_put("embeddings", 1, vec![0.1; 128]).await?;

    let results = client.vector_search("embeddings", vec![0.15; 128], 5, None).await?;
    for r in &results {
        println!("id={} distance={:.4}", r.id, r.distance);
    }

    client.drop_vector_index("embeddings").await?;
    Ok(())
}
```

## Declarative graph query (openCypher subset)

Read-only pattern matching over the temporal graph, served by the gateway's
`GraphQuery` RPC: `MATCH` path patterns, `WHERE` on node properties,
`RETURN` / `ORDER BY` / `LIMIT`, a bitemporal `AS OF <valid>[, <tx>]` clause and
the retrieval procedures `db.vectorSearch` / `db.hybridSearch` / `db.graphRag`.
`CREATE` / `MERGE` are rejected.

```rust
use statelet_client::{GraphQueryOptions, GraphValue, StateletClient};

let res = client
    .graph_query(
        "MATCH (m {id: 42})-[:supersedes]->(old) RETURN m, old LIMIT 10",
        GraphQueryOptions {
            graph_name: "my_graph".to_string(),
            ..Default::default()
        },
    )
    .await?;

println!("{:?}", res.columns); // ["m", "old"]
for row in &res.rows {
    if let Some(GraphValue::Json(props)) = row.first() {
        println!("{}", String::from_utf8_lossy(props));
    }
}
println!("{:?}", res.warnings); // non-empty ⇒ the result may be incomplete

// Time travel + vector-seeded expansion (inline query vector: named
// parameters like $q parse but are not resolvable yet).
let res = client
    .graph_query(
        "CALL db.vectorSearch([0.1, 0.2, 0.3], 5) YIELD node, score \
         RETURN node, score",
        GraphQueryOptions { as_of: 1_737_000_000_000, ..Default::default() },
    )
    .await?;
```

## Reranking (optional second stage)

A first-class, optional second-stage reranker over an over-fetched candidate
window — the analogue of Weaviate `.with_additional({rerank})` and Pinecone
`inference.rerank`. See [`docs/reranking.md`](../../docs/reranking.md).

```rust
use statelet_client::proto::RerankSpec;

// Cross-encoder: hydrate passage text via the {id}/{index} template and rescore.
let reranked = client
    .vector_search_reranked(
        "embeddings",
        vec![0.15; 128],
        5,
        None,
        Some(RerankSpec {
            enabled: true,
            model: "cross-encoder".into(),
            passage_field: "doc:{index}:{id}:text".into(),
            query_text: "capital of France".into(),
            ..Default::default()
        }),
    )
    .await?;

// Score-fusion prefetch->rescore: blend the exact full-precision distance.
let blended = client
    .vector_search_reranked(
        "embeddings",
        vec![0.15; 128],
        5,
        None,
        Some(RerankSpec {
            enabled: true,
            model: "score-fusion".into(),
            signal_blend: 0.7,
            ..Default::default()
        }),
    )
    .await?;

// Dry-run pre-flight validation of a spec (no search executed).
client
    .rerank_validate(
        "embeddings",
        RerankSpec {
            model: "cross-encoder".into(),
            passage_field: "doc:{index}:{id}:text".into(),
            query_text: "q".into(),
            ..Default::default()
        },
    )
    .await?;
```

## Durable change-feed (CDC)

`subscribe_committed` consumes the durable, ordered, resumable committed
change-feed with Kafka-style client-managed offsets. Supply a `subscription_id`
and a `CheckpointStore` (the default `FileCheckpointStore` persists offsets
atomically) to resume across restarts. With `auto_commit`, each change's offset
is committed *after* your handler returns `Ok(true)`, giving at-least-once
delivery — so your handler must be idempotent.

```rust
use statelet_client::{FileCheckpointStore, SubscribeCommittedOptions};

let ckpt = FileCheckpointStore::open("/var/lib/myapp/cdc.json")?;
let opts = SubscribeCommittedOptions {
    subscription_id: Some("my-consumer".to_string()),
    checkpoint: Some(&ckpt),
    auto_commit: true,
    key_prefix: b"orders/".to_vec(),
    include_values: true,
    ..Default::default()
};

client.subscribe_committed(opts, |ch| {
    // Process the change idempotently (offset is the stable resume key).
    println!("offset={} op={} snapshot={}", ch.offset, ch.op, ch.is_snapshot);
    Ok::<_, std::convert::Infallible>(true) // Ok(false) stops cleanly; Err(e) stops with an error
}).await?;
```

The consumer transparently reconnects from `last_offset + 1` on disconnect, and
on a compaction notice it bootstraps a baseline via a paged `scan` (each entry
delivered as a synthetic `put` with `is_snapshot = true`) before resuming the
live tail from `snapshot_offset + 1` — no gap.
