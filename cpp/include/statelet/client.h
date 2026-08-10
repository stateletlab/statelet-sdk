#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "statelet.grpc.pb.h"
#include "statelet/agent_types.h"

namespace statelet {

/// A single nearest-neighbor result.
struct VectorSearchResult {
    uint64_t id;
    float distance;
    /// Field-collapse group key (epic #1427). Empty unless the search set a
    /// GroupSpec; otherwise the candidate's group_field payload value rendered to
    /// its canonical string. Re-bucket on this to present grouped results.
    std::string group_key;
};

/// Result grouping / field-collapse options for vector_search_grouped
/// (epic #1427): collapse results to at most group_size hits per distinct value
/// of the payload field, returning up to groups distinct group keys, ordered by
/// ascending distance. The analogue of Qdrant query_groups / Weaviate groupBy /
/// Milvus grouping_field.
struct GroupSpec {
    std::string field;             // payload field to group by ("" => grouping off)
    uint32_t group_size = 0;       // max hits per group (0 => 1, one-best-per-group)
    uint32_t groups = 0;           // distinct group keys to return (0 => falls back to k)
    uint32_t overfetch = 0;        // candidate over-fetch multiplier (0 => default 4)
    bool missing_as_own = false;   // missing-field rows as own singleton group (else dropped)
};

/// Optional second-stage reranker for vector_search.
///
/// Mirrors Weaviate `.with_additional({rerank:{property,query}})` and Pinecone
/// `inference.rerank(model, query, documents, rank_fields)`:
///   * model "cross-encoder" runs a loaded cross-encoder over passages hydrated
///     from the KV store via passage_field (a key template with {id}/{index}
///     tokens, e.g. "doc:{index}:{id}:text") using query_text; requires a
///     reranker on the gateway, else auto-downgrades to score-fusion.
///   * model "score-fusion" (the default, "" => "score-fusion") is LLM-free and
///     re-sorts by signal_blend*norm_distance + (1-signal_blend)*aux_signal.
///     With 0 < signal_blend < 1 on a quantized index the gateway blends the
///     exact distance (Qdrant prefetch->rescore lift); 1.0 is a pure re-sort.
/// See docs/reranking.md.
struct RerankSpec {
    uint32_t rerank_k = 0;
    std::string model;          // "" => "score-fusion"
    std::string passage_field;  // e.g. "doc:{index}:{id}:text"
    float signal_blend = 0.0f;
    std::string query_text;
};

/// HNSW vector index configuration.
struct VectorIndexConfig {
    uint32_t dim = 128;
    ::statelet::v1::VectorDistanceMetric metric = ::statelet::v1::VECTOR_L2;
    uint32_t m = 16;
    uint32_t m_max0 = 0;
    uint32_t ef_construction = 200;
    uint32_t ef_search = 64;
};

/// Which member of a GraphValue is meaningful. The values match the wire
/// numbering of GraphQueryValue::Kind; they are mirrored here so callers never
/// touch the generated enum (whose NULL member protoc renames to NULL_, the
/// macro being taken).
enum class GraphValueKind { Null = 0, Int = 1, Double = 2, String = 3, Bool = 4, Json = 5 };

/// One projected column value of a graph query. Exactly one member is
/// meaningful, selected by kind; json_value carries the hydrated ROLE_NodeProp
/// blob for a whole node, verbatim.
struct GraphValue {
    GraphValueKind kind = GraphValueKind::Null;
    int64_t int_value = 0;
    double dbl_value = 0.0;
    std::string str_value;
    bool bool_value = false;
    std::string json_value;
};

/// Out-of-band knobs for graph_query. The defaults mean "let the gateway
/// decide": the default graph, no extra row cap, no temporal filter.
struct GraphQueryOptions {
    /// Graph index to query ("" => the gateway's default graph).
    std::string graph_name;
    /// Hard cap on returned rows regardless of any LIMIT in the query
    /// (0 => no extra cap; a parsed LIMIT still applies).
    uint32_t max_rows = 0;
    /// Bitemporal point-in-time in ms (0 => current on that axis). An
    /// `AS OF <valid>[, <tx>]` clause in the query text overrides these.
    uint64_t as_of = 0;
    uint64_t tx_as_of = 0;
};

/// The projected result set of a graph query. warnings is non-empty when the
/// result may be incomplete — e.g. a label scan hit the per-shard frontier cap.
struct GraphQueryResult {
    std::vector<std::string> columns;              // RETURN names, projection order
    std::vector<std::vector<GraphValue>> rows;     // each row in columns order
    std::vector<std::string> warnings;
};

/// Batch write operation type.
enum class WriteOpType { Put, Delete, Merge };

/// A single entry within a batch write.
struct WriteEntry {
    WriteOpType op;
    uint32_t cf;
    std::string key;
    std::string value;  // empty for Delete
};

/// Synchronous gRPC client for Statelet.
///
/// One Client speaks both services on the connection: the KV/vector surface
/// (Statelet) and the agent-state surface (AgentStateService).
///
/// Usage:
///   statelet::Client client("127.0.0.1:7379");
///   client.put("hello", "world");
///   auto val = client.get("hello");
///   client.del("hello");
class Client {
public:
    /// Connect to a Statelet node.
    ///
    /// Agent-state calls are served by the gateway (default "127.0.0.1:9379");
    /// a data node answers the KV and vector surface only.
    explicit Client(const std::string& addr, uint32_t default_cf = 0);

    // ── KV operations ───────────────────────────────────────────────

    /// Liveness check. Returns "PONG".
    std::string ping();

    /// Write a single key-value pair.
    grpc::Status put(const std::string& key, const std::string& value);
    grpc::Status put(uint32_t cf, const std::string& key, const std::string& value);

    /// Read the value for a key. Returns nullopt if not found.
    std::optional<std::string> get(const std::string& key);
    std::optional<std::string> get(uint32_t cf, const std::string& key);

    /// Delete a key.
    grpc::Status del(const std::string& key);
    grpc::Status del(uint32_t cf, const std::string& key);

    /// Merge an operand into the existing value.
    grpc::Status merge(const std::string& key, const std::string& value);
    grpc::Status merge(uint32_t cf, const std::string& key, const std::string& value);

    /// Atomically apply a batch of writes.
    grpc::Status batch_write(const std::vector<WriteEntry>& entries);

    // ── Vector operations ───────────────────────────────────────────

    /// Create or reconfigure an HNSW vector index.
    grpc::Status create_vector_index(const std::string& name, const VectorIndexConfig& config);

    /// Drop an HNSW vector index.
    grpc::Status drop_vector_index(const std::string& name);

    /// Insert or update a vector.
    grpc::Status vector_put(const std::string& index_name, uint64_t vector_id,
                            const std::vector<float>& vec);

    /// Remove a vector from the index.
    grpc::Status vector_delete(const std::string& index_name, uint64_t vector_id);

    /// Approximate nearest neighbor search.
    std::vector<VectorSearchResult> vector_search(const std::string& index_name,
                                                   const std::vector<float>& query,
                                                   uint32_t k, uint32_t ef_search = 0);

    /// Approximate nearest neighbor search with an optional second-stage
    /// reranker (cross-encoder or model-free score-fusion).
    std::vector<VectorSearchResult> vector_search(const std::string& index_name,
                                                   const std::vector<float>& query,
                                                   uint32_t k, uint32_t ef_search,
                                                   const RerankSpec& rerank);

    /// Approximate nearest neighbor search with result grouping / field-collapse
    /// (epic #1427). See GroupSpec. Grouping is mutually exclusive with MMR; it
    /// is exact on single-shard deployments and best-effort across shards (tune
    /// via group.overfetch). Each result's group value is surfaced on
    /// VectorSearchResult::group_key.
    std::vector<VectorSearchResult> vector_search_grouped(const std::string& index_name,
                                                          const std::vector<float>& query,
                                                          uint32_t k, uint32_t ef_search,
                                                          const GroupSpec& group);

    /// Dry-run pre-flight validation of a RerankSpec. Issues a validate_only
    /// vector search that validates the passage_field template (and, for
    /// model="cross-encoder", reranker availability) without executing the
    /// search. Returns the gRPC status (OK when valid; INVALID_ARGUMENT /
    /// FAILED_PRECONDITION otherwise). Mirrors Weaviate's "property exists?" /
    /// Pinecone's "rank_fields valid?" pre-flight.
    grpc::Status rerank_validate(const std::string& index_name,
                                 const RerankSpec& rerank);

    /// Retrieve a stored vector by id. Returns nullopt if not found.
    std::optional<std::vector<float>> vector_get(const std::string& index_name,
                                                  uint64_t vector_id);

    // ── Declarative graph query (openCypher subset) ─────────────────

    /// Run a read-only openCypher-subset query, writing the projected rows to
    /// out and returning the gRPC status.
    ///
    /// Gateway-only: the gateway parses and plans the query, then compiles it
    /// to engine traversal primitives. The subset covers MATCH path patterns,
    /// WHERE over node properties, RETURN / ORDER BY / LIMIT, a bitemporal
    /// `AS OF <valid>[, <tx>]` clause, and the retrieval procedures
    /// db.vectorSearch / db.hybridSearch / db.graphRag. CREATE / MERGE are
    /// rejected.
    ///
    /// Named query parameters ($q) parse but are not resolvable yet, so a
    /// vector-seeded procedure needs an inline literal:
    /// db.vectorSearch([0.1, 0.2, ...], 5).
    grpc::Status graph_query(const std::string& cypher, const GraphQueryOptions& options,
                             GraphQueryResult* out);

    /// Run a graph query with the default options.
    grpc::Status graph_query(const std::string& cypher, GraphQueryResult* out);

    // ── Agent state: causal graph ───────────────────────────────────
    //
    // Mirrors the canonical Python client (sdk/python/statelet's
    // high_level.Client). Calls that yield an id take an out-parameter and
    // return the gRPC status; plain reads return nullopt / an empty vector.

    /// Append a causal step; writes its id to step_id.
    grpc::Status add_step(const std::string& agent_id, StepType type,
                          const AddStepOptions& options, uint64_t* step_id);

    /// Append a causal step with no content, metadata or embedding.
    grpc::Status add_step(const std::string& agent_id, StepType type, uint64_t* step_id);

    /// Link two steps.
    grpc::Status add_edge(uint64_t src_step_id, uint64_t dst_step_id, EdgeType type,
                          const AddEdgeOptions& options = {});

    /// Read one step. Returns nullopt when it does not exist or the caller's
    /// scope does not admit it.
    std::optional<Step> get_step(uint64_t step_id);

    /// Read a step's content blob. Returns nullopt if not found.
    std::optional<std::string> get_content(uint64_t step_id);

    /// List the edges incident to a step (forward and unfiltered by default).
    std::vector<Edge> get_edges(uint64_t step_id, const GetEdgesOptions& options = {});

    /// Walk the causal graph breadth-first from a step, up to max_depth hops,
    /// returning every step and edge reached.
    TraverseResult traverse(uint64_t start_step_id,
                            Direction direction = Direction::Forward,
                            uint32_t max_depth = 3);

    /// Return the k causal chains whose anchor step is nearest the query
    /// embedding, each walked chain_depth hops. ef 0 = the index default.
    std::vector<CausalChain> find_similar_chains(const std::vector<float>& query_embedding,
                                                 uint32_t k = 5, uint32_t chain_depth = 3,
                                                 uint32_t ef = 0);

    // ── Agent state: branches (fork) ────────────────────────────────

    /// Open a branch off parent_branch_id (0 = the main timeline).
    grpc::Status fork(const std::string& label, uint64_t parent_branch_id, uint64_t* branch_id);

    /// Merge a branch back into its parent.
    grpc::Status merge_branch(uint64_t branch_id);

    /// Throw a branch away without merging it.
    grpc::Status discard_branch(uint64_t branch_id);

    /// List every known branch and its status.
    std::vector<BranchMeta> list_branches();

    /// Write a key inside a branch's overlay.
    grpc::Status branch_put(uint64_t branch_id, uint32_t cf, const std::string& key,
                            const std::string& value);

    /// Read a key as of a branch, falling back to the parent snapshot.
    std::optional<std::string> branch_get(uint64_t branch_id, uint32_t cf,
                                          const std::string& key);

    // ── Agent state: reactive state ─────────────────────────────────

    /// Write new_value only if the key is still at expected_seq. A failed CAS
    /// is not an error: it returns OK with result->success false and the
    /// current actual_seq.
    grpc::Status cas_put(uint32_t cf, const std::string& key, uint64_t expected_seq,
                         const std::string& new_value, CasPutResult* result);

    /// Transactionally ingest a fact (#780), optionally fence-gated (#784).
    ///
    /// Dedup, create-with-provenance and supersede all commit as ONE atomic,
    /// snapshot-isolated batch on the owning data node. A conflict is not an
    /// error: it returns OK with result->action Conflict and committed false.
    grpc::Status memory_ingest(const std::string& scope, const std::string& content,
                               const MemoryIngestOptions& options, MemoryIngestResult* result);

    // ── Agent state: coordination (claims, leases, fences) ──────────

    /// Take key for agent_id iff it is unheld, with no expiry.
    grpc::Status claim(const std::string& key, const std::string& agent_id, LeaseResult* result);

    /// Take key for agent_id with a TTL; an un-renewed lease auto-expires.
    /// ttl_ms 0 makes it a plain claim.
    grpc::Status lease(const std::string& key, const std::string& agent_id, uint64_t ttl_ms,
                       LeaseResult* result);

    /// Extend the lease on key iff fence still matches the holder's token.
    /// NOTE: a successful renew ADVANCES the fence — adopt the returned fence
    /// for subsequent fenced writes or they abort (#784).
    grpc::Status renew(const std::string& key, const std::string& agent_id, uint64_t fence,
                       uint64_t ttl_ms, LeaseResult* result);

    /// Drop the lease on key iff fence matches; reports whether it was dropped.
    grpc::Status release(const std::string& key, uint64_t fence, bool* released);

    // ── Agent state: temporal edges ─────────────────────────────────

    /// Close an edge's valid time at expire_at, leaving the earlier revision
    /// readable through edge_history() and as-of queries.
    grpc::Status expire_edge(uint64_t src_step_id, uint64_t dst_step_id, EdgeType type,
                             uint64_t expire_at);

    /// Every bitemporal revision of one edge.
    std::vector<TemporalEdge> edge_history(uint64_t src_step_id, uint64_t dst_step_id,
                                           EdgeType type);

    // ── Agent state: prefix watch ───────────────────────────────────

    /// Stream every write whose key starts with prefix in cf, calling on_event
    /// for each one until it returns false or the server ends the stream.
    ///
    /// This is a live tail with no offsets: it does not replay history and it
    /// does not reconnect, so a dropped stream surfaces as a non-OK status and
    /// the caller decides whether re-watching is safe. Stopping via on_event
    /// cancels the RPC and still returns OK.
    grpc::Status watch_prefix(const std::string& agent_id, uint32_t cf,
                              const std::string& prefix,
                              const std::function<bool(const WatchEvent&)>& on_event);

private:
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<::statelet::v1::Statelet::Stub> stub_;
    std::unique_ptr<::statelet::v1::AgentStateService::Stub> agent_stub_;
    uint32_t default_cf_;
};

}  // namespace statelet
