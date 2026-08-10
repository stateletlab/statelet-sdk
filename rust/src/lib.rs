//! Statelet Rust SDK — async gRPC client.
//!
//! # Example
//!
//! ```no_run
//! use statelet_client::StateletClient;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut client = StateletClient::connect("http://127.0.0.1:7379").await?;
//!
//!     // Ping
//!     println!("{}", client.ping().await?);
//!
//!     // KV operations
//!     client.put(b"hello", b"world", None).await?;
//!     if let Some(value) = client.get(b"hello", None).await? {
//!         println!("got: {:?}", value);
//!     }
//!     client.delete(b"hello", None).await?;
//!     Ok(())
//! }
//! ```

pub mod proto {
    tonic::include_proto!("statelet.v1");
}

pub mod cdc;
pub use cdc::{
    CheckpointStore, CommittedChange, ConsumeError, FeedItem, FeedStream, FeedTransport,
    FileCheckpointStore, SubscribeCommittedOptions,
};

use proto::statelet_client::StateletClient as GrpcClient;
use tonic::transport::Channel;

/// A single nearest-neighbor search result.
#[derive(Debug, Clone)]
pub struct VectorSearchResult {
    pub id: u64,
    pub distance: f32,
    /// Field-collapse group key (epic #1427). Empty unless the search set
    /// [`GroupSpec::field`]; otherwise the candidate's `group_field` payload
    /// value, rendered to its canonical string. Re-bucket on this to present
    /// results grouped.
    pub group_key: String,
}

/// Result grouping / field-collapse options for [`StateletClient::vector_search_grouped`]
/// (epic #1427). Collapse results to at most `group_size` hits per distinct
/// value of the payload field `field`, returning up to `groups` distinct group
/// keys, ordered by ascending distance.
#[derive(Debug, Clone, Default)]
pub struct GroupSpec {
    /// Payload field to group by (empty ⇒ grouping off).
    pub field: String,
    /// Max hits per group (`0` ⇒ 1, one-best-per-group).
    pub group_size: u32,
    /// Number of distinct group keys to return (`0` ⇒ falls back to `k`).
    pub groups: u32,
    /// Candidate over-fetch multiplier (`0` ⇒ default 4, capped server-side).
    pub overfetch: u32,
    /// When `true`, candidates missing `field` are returned as their own
    /// singleton group (empty `group_key`) instead of being dropped (default).
    pub missing_as_own: bool,
}

/// HNSW vector index configuration.
#[derive(Debug, Clone)]
pub struct VectorIndexConfig {
    pub dim: u32,
    pub metric: i32, // 0=L2, 1=Cosine, 2=InnerProduct
    pub m: u32,
    pub m_max0: u32,
    pub ef_construction: u32,
    pub ef_search: u32,
}

impl Default for VectorIndexConfig {
    fn default() -> Self {
        Self {
            dim: 128,
            metric: 0,
            m: 16,
            m_max0: 0,
            ef_construction: 200,
            ef_search: 64,
        }
    }
}

/// Batch write operation.
pub enum WriteOp {
    Put {
        cf: u32,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        cf: u32,
        key: Vec<u8>,
    },
    Merge {
        cf: u32,
        key: Vec<u8>,
        value: Vec<u8>,
    },
}

/// Out-of-band knobs for [`StateletClient::graph_query`]. The default means
/// "let the gateway decide": the default graph, no extra row cap, and no
/// temporal filter on either axis.
#[derive(Debug, Clone, Default)]
pub struct GraphQueryOptions {
    /// Graph index to query (empty ⇒ the gateway's default graph).
    pub graph_name: String,
    /// Hard cap on returned rows regardless of any `LIMIT` in the query
    /// (`0` ⇒ no extra cap; a parsed `LIMIT` still applies).
    pub max_rows: u32,
    /// Valid-time the query is evaluated against, in ms (`0` ⇒ current). An
    /// `AS OF` clause in the query text overrides it.
    pub as_of: u64,
    /// Transaction-time the query is evaluated against, in ms (`0` ⇒ current).
    pub tx_as_of: u64,
}

/// One projected column value. `Json` carries the hydrated `ROLE_NodeProp`
/// blob for a whole node, verbatim.
#[derive(Debug, Clone, PartialEq)]
pub enum GraphValue {
    Null,
    Int(i64),
    Double(f64),
    Str(String),
    Bool(bool),
    Json(Vec<u8>),
}

impl GraphValue {
    /// Decode the wire union, reading the member the `kind` tag selects. An
    /// unknown tag (a newer server) decodes to [`GraphValue::Null`].
    fn from_proto(value: proto::GraphQueryValue) -> Self {
        use proto::graph_query_value::Kind;
        match Kind::try_from(value.kind) {
            Ok(Kind::Int) => GraphValue::Int(value.int_value),
            Ok(Kind::Double) => GraphValue::Double(value.dbl_value),
            Ok(Kind::String) => GraphValue::Str(value.str_value),
            Ok(Kind::Bool) => GraphValue::Bool(value.bool_value),
            Ok(Kind::Json) => GraphValue::Json(value.json_value),
            Ok(Kind::Null) | Err(_) => GraphValue::Null,
        }
    }
}

/// The projected result set of a [`StateletClient::graph_query`].
///
/// `warnings` is non-empty when the result may be incomplete — e.g. a label
/// scan hit the per-shard frontier cap, so the anchor set was truncated.
#[derive(Debug, Clone, Default)]
pub struct GraphQueryResult {
    /// `RETURN` column names, in projection order.
    pub columns: Vec<String>,
    /// Result rows, each in `columns` order.
    pub rows: Vec<Vec<GraphValue>>,
    /// Non-fatal query warnings.
    pub warnings: Vec<String>,
}

/// Async gRPC client for Statelet.
pub struct StateletClient {
    inner: GrpcClient<Channel>,
    default_cf: u32,
}

impl StateletClient {
    /// Connect to a Statelet node.
    pub async fn connect(addr: &str) -> Result<Self, tonic::transport::Error> {
        let inner = GrpcClient::connect(addr.to_string()).await?;
        Ok(Self {
            inner,
            default_cf: 0,
        })
    }

    /// Set the default column family id.
    pub fn set_default_cf(&mut self, cf: u32) {
        self.default_cf = cf;
    }

    // ── KV operations ───────────────────────────────────────────────

    /// Liveness check. Returns "PONG".
    pub async fn ping(&mut self) -> Result<String, tonic::Status> {
        let resp = self.inner.ping(proto::PingRequest {}).await?;
        Ok(resp.into_inner().message)
    }

    /// Write a single key-value pair.
    pub async fn put(
        &mut self,
        key: &[u8],
        value: &[u8],
        cf: Option<u32>,
    ) -> Result<(), tonic::Status> {
        self.inner
            .put(proto::PutRequest {
                cf: cf.unwrap_or(self.default_cf),
                key: key.to_vec(),
                value: value.to_vec(),
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    /// Read the value for a key. Returns `None` if not found.
    pub async fn get(
        &mut self,
        key: &[u8],
        cf: Option<u32>,
    ) -> Result<Option<Vec<u8>>, tonic::Status> {
        let resp = self
            .inner
            .get(proto::GetRequest {
                cf: cf.unwrap_or(self.default_cf),
                key: key.to_vec(),
                ..Default::default()
            })
            .await?
            .into_inner();
        Ok(if resp.found { Some(resp.value) } else { None })
    }

    /// Delete a key.
    pub async fn delete(&mut self, key: &[u8], cf: Option<u32>) -> Result<(), tonic::Status> {
        self.inner
            .delete(proto::DeleteRequest {
                cf: cf.unwrap_or(self.default_cf),
                key: key.to_vec(),
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    /// Merge an operand into the existing value.
    pub async fn merge(
        &mut self,
        key: &[u8],
        value: &[u8],
        cf: Option<u32>,
    ) -> Result<(), tonic::Status> {
        self.inner
            .merge(proto::MergeRequest {
                cf: cf.unwrap_or(self.default_cf),
                key: key.to_vec(),
                value: value.to_vec(),
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    /// Atomically apply a batch of write operations.
    pub async fn batch_write(&mut self, ops: Vec<WriteOp>) -> Result<(), tonic::Status> {
        let entries = ops
            .into_iter()
            .map(|op| match op {
                WriteOp::Put { cf, key, value } => proto::WriteEntry {
                    cf,
                    op: proto::WriteOp::Put as i32,
                    key,
                    value,
                    ..Default::default()
                },
                WriteOp::Delete { cf, key } => proto::WriteEntry {
                    cf,
                    op: proto::WriteOp::Delete as i32,
                    key,
                    value: vec![],
                    ..Default::default()
                },
                WriteOp::Merge { cf, key, value } => proto::WriteEntry {
                    cf,
                    op: proto::WriteOp::Merge as i32,
                    key,
                    value,
                    ..Default::default()
                },
            })
            .collect();
        self.inner
            .batch_write(proto::BatchWriteRequest {
                entries,
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    /// Scan keys with an optional prefix filter. Returns entries and next cursor.
    pub async fn scan(
        &mut self,
        prefix: &[u8],
        cursor: Option<&[u8]>,
        limit: u32,
        cf: Option<u32>,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>), tonic::Status> {
        let resp = self
            .inner
            .scan(proto::ScanRequest {
                cf: cf.unwrap_or(self.default_cf),
                prefix: prefix.to_vec(),
                cursor: cursor.unwrap_or(&[]).to_vec(),
                limit,
                ..Default::default()
            })
            .await?
            .into_inner();
        let entries = resp.entries.into_iter().map(|e| (e.key, e.value)).collect();
        let next = if resp.next_cursor.is_empty() {
            None
        } else {
            Some(resp.next_cursor)
        };
        Ok((entries, next))
    }

    /// Delete all keys matching a prefix. Returns the number of keys deleted.
    pub async fn delete_by_prefix(
        &mut self,
        prefix: &[u8],
        cf: Option<u32>,
    ) -> Result<u32, tonic::Status> {
        let resp = self
            .inner
            .delete_by_prefix(proto::DeleteByPrefixRequest {
                cf: cf.unwrap_or(self.default_cf),
                prefix: prefix.to_vec(),
                ..Default::default()
            })
            .await?
            .into_inner();
        Ok(resp.deleted)
    }

    // ── Vector operations ───────────────────────────────────────────

    /// Create or reconfigure an HNSW vector index.
    pub async fn create_vector_index(
        &mut self,
        name: &str,
        config: VectorIndexConfig,
    ) -> Result<(), tonic::Status> {
        self.inner
            .create_vector_index(proto::CreateVectorIndexRequest {
                index_name: name.to_string(),
                config: Some(proto::VectorIndexConfig {
                    dim: config.dim,
                    metric: config.metric,
                    m: config.m,
                    m_max0: config.m_max0,
                    ef_construction: config.ef_construction,
                    ef_search: config.ef_search,
                    ..Default::default()
                }),
            })
            .await?;
        Ok(())
    }

    /// Drop an HNSW vector index.
    pub async fn drop_vector_index(&mut self, name: &str) -> Result<(), tonic::Status> {
        self.inner
            .drop_vector_index(proto::DropVectorIndexRequest {
                index_name: name.to_string(),
            })
            .await?;
        Ok(())
    }

    /// Insert or update a vector.
    pub async fn vector_put(
        &mut self,
        index_name: &str,
        vector_id: u64,
        vector: Vec<f32>,
    ) -> Result<(), tonic::Status> {
        self.inner
            .vector_put(proto::VectorPutRequest {
                index_name: index_name.to_string(),
                vector_id,
                vector,
                attributes: Default::default(),
            })
            .await?;
        Ok(())
    }

    /// Remove a vector from the index.
    pub async fn vector_delete(
        &mut self,
        index_name: &str,
        vector_id: u64,
    ) -> Result<(), tonic::Status> {
        self.inner
            .vector_delete(proto::VectorDeleteRequest {
                index_name: index_name.to_string(),
                vector_id,
            })
            .await?;
        Ok(())
    }

    /// Approximate nearest neighbor search.
    pub async fn vector_search(
        &mut self,
        index_name: &str,
        query: Vec<f32>,
        k: u32,
        ef_search: Option<u32>,
    ) -> Result<Vec<VectorSearchResult>, tonic::Status> {
        self.vector_search_reranked(index_name, query, k, ef_search, None)
            .await
    }

    /// Approximate nearest neighbor search with an optional second-stage
    /// reranker.
    ///
    /// Pass a [`proto::RerankSpec`] to enable the cross-encoder or model-free
    /// score-fusion rerank over an over-fetched candidate window — the analogue
    /// of Weaviate `.with_additional({rerank})` / Pinecone `inference.rerank`.
    /// `None` ⇒ no rerank (identical to [`Self::vector_search`]). See
    /// `docs/reranking.md` for the two models and `signal_blend` semantics.
    pub async fn vector_search_reranked(
        &mut self,
        index_name: &str,
        query: Vec<f32>,
        k: u32,
        ef_search: Option<u32>,
        rerank: Option<proto::RerankSpec>,
    ) -> Result<Vec<VectorSearchResult>, tonic::Status> {
        let resp = self
            .inner
            .vector_search(proto::VectorSearchRequest {
                index_name: index_name.to_string(),
                query,
                k,
                ef_search: ef_search.unwrap_or(0),
                filter: None,
                query_payload: None, // single-vector ANN path (no multi-vector MaxSim)
                mmr: false,          // MMR diversity rerank off (omit ⇒ default behavior)
                mmr_lambda: 0.0,
                mmr_pool: 0,
                rerank, // optional second-stage rerank
                planner_override: 0, // 0 ⇒ let the server pick the plan
                group_field: String::new(), // grouping off (see vector_search_grouped)
                group_size: 0,
                groups: 0,
                group_overfetch: 0,
                group_missing_as_own: false,
            })
            .await?
            .into_inner();
        Ok(resp
            .results
            .into_iter()
            .map(|r| VectorSearchResult {
                id: r.id,
                distance: r.distance,
                group_key: r.group_key,
            })
            .collect())
    }

    /// Approximate nearest neighbor search with result grouping / field-collapse
    /// (epic #1427).
    ///
    /// Collapse results to at most [`GroupSpec::group_size`] hits per distinct
    /// value of [`GroupSpec::field`], returning up to [`GroupSpec::groups`]
    /// distinct group keys (each result's value surfaced on
    /// [`VectorSearchResult::group_key`]). Grouping is exact on single-shard
    /// deployments and best-effort across shards (tune via [`GroupSpec::overfetch`]).
    /// Grouping is mutually exclusive with MMR. The analogue of Qdrant
    /// `query_groups` / Weaviate `groupBy` / Milvus `grouping_field`.
    pub async fn vector_search_grouped(
        &mut self,
        index_name: &str,
        query: Vec<f32>,
        k: u32,
        ef_search: Option<u32>,
        group: GroupSpec,
    ) -> Result<Vec<VectorSearchResult>, tonic::Status> {
        let resp = self
            .inner
            .vector_search(proto::VectorSearchRequest {
                index_name: index_name.to_string(),
                query,
                k,
                ef_search: ef_search.unwrap_or(0),
                filter: None,
                query_payload: None,
                mmr: false,
                mmr_lambda: 0.0,
                mmr_pool: 0,
                rerank: None,
                planner_override: 0,
                group_field: group.field,
                group_size: group.group_size,
                groups: group.groups,
                group_overfetch: group.overfetch,
                group_missing_as_own: group.missing_as_own,
            })
            .await?
            .into_inner();
        Ok(resp
            .results
            .into_iter()
            .map(|r| VectorSearchResult {
                id: r.id,
                distance: r.distance,
                group_key: r.group_key,
            })
            .collect())
    }

    /// Dry-run pre-flight validation of a [`proto::RerankSpec`].
    ///
    /// Issues a `validate_only` vector search that validates the
    /// `passage_field` template (and, for `model = "cross-encoder"`, that a
    /// reranker is loaded on the gateway) without executing the search.
    /// Returns `Ok(())` when the spec is valid; the underlying
    /// `InvalidArgument` / `FailedPrecondition` [`tonic::Status`] otherwise.
    /// Mirrors Weaviate's "property exists?" / Pinecone's "rank_fields valid?"
    /// pre-flight.
    pub async fn rerank_validate(
        &mut self,
        index_name: &str,
        mut rerank: proto::RerankSpec,
    ) -> Result<(), tonic::Status> {
        rerank.enabled = true;
        rerank.validate_only = true;
        self.inner
            .vector_search(proto::VectorSearchRequest {
                index_name: index_name.to_string(),
                query: Vec::new(),
                k: 1,
                ef_search: 0,
                filter: None,
                query_payload: None,
                mmr: false,
                mmr_lambda: 0.0,
                mmr_pool: 0,
                rerank: Some(rerank),
                planner_override: 0,
                group_field: String::new(),
                group_size: 0,
                groups: 0,
                group_overfetch: 0,
                group_missing_as_own: false,
            })
            .await?;
        Ok(())
    }

    /// Retrieve a stored vector by id.
    pub async fn vector_get(
        &mut self,
        index_name: &str,
        vector_id: u64,
    ) -> Result<Option<Vec<f32>>, tonic::Status> {
        let resp = self
            .inner
            .vector_get(proto::VectorGetRequest {
                index_name: index_name.to_string(),
                vector_id,
            })
            .await?
            .into_inner();
        Ok(if resp.found { Some(resp.vector) } else { None })
    }

    // ── Declarative graph query (openCypher subset) ─────────────────────

    /// Run a read-only openCypher-subset query.
    ///
    /// Gateway-only: the gateway parses and plans the query, then compiles it to
    /// engine traversal primitives. The subset covers `MATCH` path patterns,
    /// `WHERE` over node properties, `RETURN` / `ORDER BY` / `LIMIT`, a
    /// bitemporal `AS OF <valid>[, <tx>]` clause, and the retrieval procedures
    /// `db.vectorSearch` / `db.hybridSearch` / `db.graphRag`. `CREATE` / `MERGE`
    /// are rejected.
    ///
    /// [`GraphQueryOptions::default()`] means "let the gateway decide": the
    /// default graph, no extra row cap and no temporal filter on either axis.
    ///
    /// Named query parameters (`$q`) parse but are not resolvable yet, so a
    /// vector-seeded procedure needs an inline literal —
    /// `db.vectorSearch([0.1, 0.2, ...], 5)`.
    pub async fn graph_query(
        &mut self,
        cypher: &str,
        options: GraphQueryOptions,
    ) -> Result<GraphQueryResult, tonic::Status> {
        let resp = self
            .inner
            .graph_query(proto::GraphQueryRequest {
                graph_name: options.graph_name,
                cypher: cypher.to_string(),
                max_rows: options.max_rows,
                as_of: options.as_of,
                tx_as_of: options.tx_as_of,
            })
            .await?
            .into_inner();
        Ok(GraphQueryResult {
            columns: resp.columns,
            rows: resp
                .rows
                .into_iter()
                .map(|row| row.values.into_iter().map(GraphValue::from_proto).collect())
                .collect(),
            warnings: resp.warnings,
        })
    }

    // ── Durable change-feed (CDC) — issue #824 ──────────────────────────

    /// Consume the durable, ordered, resumable committed change-feed (CDC).
    ///
    /// Invokes `handler` for each [`cdc::CommittedChange`] in stable Raft-offset
    /// order, driving the canonical Phase-5b algorithm: client-managed offsets
    /// (supply `subscription_id` + `checkpoint` to resume across restarts),
    /// bootstrap-on-`compacted` via a paged [`Self::scan`], heartbeat-advances-
    /// checkpoint, reconnect-on-disconnect from `last_offset + 1`, and
    /// at-least-once delivery (with `auto_commit`, each offset is committed
    /// *after* `handler` returns `Ok(true)`).
    ///
    /// `handler` returns `Ok(true)` to continue, `Ok(false)` to stop cleanly, or
    /// `Err(e)` to stop with [`cdc::ConsumeError::Handler`]. The future runs
    /// until the handler stops it (the live tail never ends on its own).
    pub async fn subscribe_committed<H, E>(
        &mut self,
        opts: cdc::SubscribeCommittedOptions<'_>,
        handler: H,
    ) -> Result<(), cdc::ConsumeError<E>>
    where
        H: FnMut(cdc::CommittedChange) -> Result<bool, E>,
    {
        let default_cf = self.default_cf;
        let mut sleeper = cdc::TokioSleeper;
        cdc::run_consumer(self, &mut sleeper, opts, default_cf, handler).await
    }
}

/// A live gRPC committed-feed stream, wrapping `tonic::Streaming`.
pub struct GrpcFeedStream {
    inner: tonic::Streaming<proto::CommittedFeedItem>,
}

#[tonic::async_trait]
impl cdc::FeedStream for GrpcFeedStream {
    async fn recv(&mut self) -> Result<Option<cdc::FeedItem>, tonic::Status> {
        match self.inner.message().await? {
            Some(item) => Ok(cdc::FeedItem::from_proto(item)),
            None => Ok(None),
        }
    }
}

#[tonic::async_trait]
impl cdc::FeedTransport for StateletClient {
    type Stream = GrpcFeedStream;

    async fn open_feed(
        &mut self,
        shard_id: u64,
        from_offset: u64,
        cf: u32,
        key_prefix: &[u8],
        include_values: bool,
    ) -> Result<Self::Stream, tonic::Status> {
        let resp = self
            .inner
            .subscribe_committed(proto::SubscribeCommittedRequest {
                shard_id,
                from_offset,
                cf,
                key_prefix: key_prefix.to_vec(),
                include_values,
            })
            .await?;
        Ok(GrpcFeedStream {
            inner: resp.into_inner(),
        })
    }

    async fn scan_page(
        &mut self,
        prefix: &[u8],
        cursor: Option<&[u8]>,
        limit: u32,
        cf: u32,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>), tonic::Status> {
        self.scan(prefix, cursor, limit, Some(cf)).await
    }
}

#[cfg(test)]
mod graph_query_tests {
    use super::*;

    fn value(kind: proto::graph_query_value::Kind) -> proto::GraphQueryValue {
        proto::GraphQueryValue {
            kind: kind as i32,
            int_value: 42,
            dbl_value: 0.5,
            str_value: "knows".to_string(),
            bool_value: true,
            json_value: br#"{"name":"ada"}"#.to_vec(),
        }
    }

    #[test]
    fn decodes_every_value_kind() {
        use proto::graph_query_value::Kind;
        assert_eq!(GraphValue::from_proto(value(Kind::Null)), GraphValue::Null);
        assert_eq!(
            GraphValue::from_proto(value(Kind::Int)),
            GraphValue::Int(42)
        );
        assert_eq!(
            GraphValue::from_proto(value(Kind::Double)),
            GraphValue::Double(0.5)
        );
        assert_eq!(
            GraphValue::from_proto(value(Kind::String)),
            GraphValue::Str("knows".to_string())
        );
        assert_eq!(
            GraphValue::from_proto(value(Kind::Bool)),
            GraphValue::Bool(true)
        );
        assert_eq!(
            GraphValue::from_proto(value(Kind::Json)),
            GraphValue::Json(br#"{"name":"ada"}"#.to_vec())
        );
    }

    #[test]
    fn unknown_kind_from_a_newer_server_decodes_to_null() {
        let mut v = value(proto::graph_query_value::Kind::Int);
        v.kind = 99;
        assert_eq!(GraphValue::from_proto(v), GraphValue::Null);
    }

    #[test]
    fn default_options_leave_every_knob_at_the_server_default() {
        let o = GraphQueryOptions::default();
        assert!(o.graph_name.is_empty());
        assert_eq!((o.max_rows, o.as_of, o.tx_as_of), (0, 0, 0));
    }
}
