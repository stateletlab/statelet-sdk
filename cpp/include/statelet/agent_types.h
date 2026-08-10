// Copyright 2024 Statelet Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

// Plain value types for the agent-state surface (causal graph, branches,
// reactive state, leases, temporal edges, prefix watch).
//
// Deliberately free of any gRPC / protobuf include: the wire mapping lives in
// src/agent_client.cpp, so these types — and the step-JSON decoder — can be
// compiled and tested without the generated stubs.

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace statelet {

// ── Enumerations carried on the wire as strings ─────────────────────────────

/// The kind of a causal step. The server parses these names exactly; anything
/// else is rejected with INVALID_ARGUMENT.
enum class StepType { Observe, Think, Act, Tool, Result };

std::string to_string(StepType type);
std::optional<StepType> step_type_from_string(const std::string& name);

/// The kind of a causal edge.
enum class EdgeType {
    Triggers,     ///< A directly causes B.
    Informs,      ///< A provides context to B.
    Branches,     ///< Fork point: A branches into B.
    Merges,       ///< Join point: A merges back from B.
    Supersedes,   ///< Fact A retired fact B (B's valid_to is closed).
    DerivedFrom,  ///< Provenance: fact A was derived from episode/step B.
    Contradicts,  ///< Asserted conflict: both kept, the as-of filter decides.
};

std::string to_string(EdgeType type);
std::optional<EdgeType> edge_type_from_string(const std::string& name);

/// Which way an edge query or traversal walks.
enum class Direction {
    Forward,   ///< src -> dst.
    Backward,  ///< dst -> src.
    Both,      ///< Either way. Accepted by traverse() only; get_edges() rejects it.
};

std::string to_string(Direction direction);

/// A step's query-time access boundary (issue #697).
enum class MemoryScope {
    World,    ///< Visible to every authenticated agent in the tenant (default).
    Team,     ///< Visible to holders of the owning team's grant; owner = team id.
    Private,  ///< Visible only to the owning agent; owner = agent id.
};

std::string to_string(MemoryScope scope);
std::optional<MemoryScope> memory_scope_from_string(const std::string& name);

// ── Result types ────────────────────────────────────────────────────────────

/// One field-level ACL entry over a step's metadata JSON.
struct FieldRule {
    /// RFC-6901 pointer into the step's metadata JSON (e.g. "/raw_reasoning").
    std::string json_pointer;
    /// Minimum scope a caller must satisfy to see the field; otherwise it is
    /// stripped from the returned metadata.
    MemoryScope min_scope = MemoryScope::World;
};

/// A node in the causal DAG, decoded from the server's JSON encoding.
///
/// `step_type` stays the raw wire string so an unknown (newer) kind is never
/// lost; map it with step_type_from_string(). `raw_json` carries the exact
/// bytes the server sent, so a field added server-side is reachable before this
/// struct grows to match.
struct Step {
    uint64_t id = 0;
    std::string agent_id;
    std::string step_type;
    /// Milliseconds since the Unix epoch.
    uint64_t timestamp = 0;
    /// 0 on the main timeline.
    uint64_t branch_id = 0;
    /// Empty when the step carries no embedding.
    std::optional<uint64_t> embedding_id;
    /// Arbitrary caller bytes (JSON or msgpack), held as raw bytes.
    std::string metadata;
    MemoryScope scope = MemoryScope::World;
    /// Team id for Team, agent id for Private, empty for World.
    std::string scope_owner;
    std::vector<FieldRule> field_acl;
    std::string raw_json;
};

/// Decode the server's JSON encoding of a causal step.
///
/// Best-effort and total: a member that is missing or of an unexpected shape
/// leaves its field at the default (an untagged step decodes as World, matching
/// the server's serde default) rather than failing the whole decode. Returns
/// false only when `json` is not a JSON object at all; `out->raw_json` is set
/// either way.
bool parse_step(const std::string& json, Step* out);

/// One causal edge as seen from the queried step.
struct Edge {
    /// The step at the other end: dst for a forward query, src for a backward
    /// one.
    uint64_t peer_step_id = 0;
    /// Raw wire name; map it with edge_type_from_string().
    std::string edge_type;
    std::string props;
    /// Valid-time bounds in ms. valid_to 0 means the edge is still open.
    uint64_t valid_from = 0;
    uint64_t valid_to = 0;
};

/// The sub-graph a traversal reached.
struct TraverseResult {
    std::vector<Step> steps;
    std::vector<Edge> edges;
};

/// One chain returned by find_similar_chains: the anchor step the query
/// embedding matched, plus the sub-graph walked from it.
struct CausalChain {
    Step anchor;
    float distance = 0.0f;
    std::vector<Step> steps;
    std::vector<Edge> edges;
};

/// One fork branch.
struct BranchMeta {
    uint64_t id = 0;
    uint64_t parent_id = 0;
    uint64_t parent_snapshot_seq = 0;
    uint64_t created_at = 0;
    /// "Active", "Merged" or "Discarded".
    std::string status;
    std::string label;
};

/// The outcome of a compare-and-swap put.
struct CasPutResult {
    bool success = false;
    /// The new logical version, set on success.
    uint64_t new_seq = 0;
    /// The current logical version, set on conflict.
    uint64_t actual_seq = 0;
};

/// The outcome of claim / lease / renew.
///
/// `acquired` is true iff the caller now holds the key, and `fence` is then the
/// fencing token to carry on subsequent fenced writes. On failure `holder` is
/// the current holder and `fence` is *their* token.
struct LeaseResult {
    bool acquired = false;
    std::string holder;
    uint64_t fence = 0;
};

/// One revision of an edge in its bitemporal history.
struct TemporalEdge {
    uint64_t src = 0;
    uint64_t dst = 0;
    std::string edge_type;
    uint64_t valid_from = 0;
    uint64_t valid_to = 0;
    std::string props;
    /// Transaction time: when the revision became believed, and when it was
    /// superseded. 0 means "always known" / "still believed".
    uint64_t tx_from = 0;
    uint64_t tx_to = 0;
    /// Empty when the author is unknown.
    std::string author_agent_id;
};

/// How memory_ingest resolved an incoming fact.
///
/// The order is load-bearing: these map onto the wire's numeric action codes
/// (0=Added, 1=Deduplicated, 2=Superseded, 3=Conflict).
enum class IngestAction {
    Added,          ///< The fact was new and was created.
    Deduplicated,   ///< A near-duplicate already existed and was reused.
    Superseded,     ///< It replaced existing facts, whose valid time was closed.
    Conflict,       ///< The transaction could not commit; nothing was written.
};

std::string to_string(IngestAction action);

/// One existing fact the caller already retrieved (ANN over the scope index,
/// scope-filtered) and its cosine similarity to the incoming content.
struct IngestCandidate {
    uint64_t fact_id = 0;
    float sim = 0.0f;
};

/// Optional arguments of memory_ingest.
struct MemoryIngestOptions {
    std::vector<IngestCandidate> candidates;
    /// DerivedFrom episode steps recorded as provenance.
    std::vector<uint64_t> provenance_steps;
    /// Empty when the fact carries no embedding.
    std::optional<uint64_t> embedding_id;
    /// A candidate at or above this similarity is treated as a duplicate.
    float dedup_threshold = 0.97f;
    /// A candidate between this and dedup_threshold is superseded by the
    /// incoming fact.
    float supersede_threshold = 0.80f;
    std::string author_agent_id;
    float confidence = 0.0f;
    std::string run_id;
    /// Gates the write on a lease (#784). When non-zero, lease_key must be the
    /// exact bytes passed to lease()/claim(): if the lease moved past `fence`
    /// the commit aborts with Conflict and fence_lost true, writing nothing.
    uint64_t fence = 0;
    std::string lease_key;
};

/// How a fact was resolved.
struct MemoryIngestResult {
    IngestAction action = IngestAction::Added;
    /// The resulting fact: newly created, or the deduped existing one.
    uint64_t fact_id = 0;
    /// The facts whose valid time this ingest closed.
    std::vector<uint64_t> superseded;
    /// False exactly when action is Conflict.
    bool committed = true;
    /// On a data conflict, the candidate whose version moved; 0 on a lost fence.
    uint64_t conflict_fact_id = 0;
    /// Separates the two conflict causes. False means a concurrent writer kept
    /// moving a read candidate within the retry budget — back off and retry.
    /// True means the lease was re-acquired past the fence passed (#784), which
    /// is terminal: take a fresh lease for a new fence.
    bool fence_lost = false;
};

/// A single write observed by watch_prefix.
struct WatchEvent {
    /// "put" or "delete".
    std::string event_type;
    uint32_t cf = 0;
    std::string key;
    std::string value;
    uint64_t seq = 0;
};

// ── Request options ─────────────────────────────────────────────────────────

/// Optional arguments of add_step.
struct AddStepOptions {
    std::string content;
    std::string metadata;
    std::vector<float> embedding;
    /// 0 for the main timeline.
    uint64_t branch_id = 0;
    MemoryScope scope = MemoryScope::World;
    /// Team id for Team, agent id for Private.
    std::string scope_owner;
    /// Optional JSON [{json_pointer, min_scope}] array.
    std::string field_acl_json;
};

/// Optional arguments of add_edge.
struct AddEdgeOptions {
    std::string props;
    /// 0 means "now".
    uint64_t valid_from = 0;
    /// 0 means the edge never expires.
    uint64_t valid_to = 0;
    /// Records which agent's write created this belief; set it to make the edge
    /// visible to per-agent belief queries.
    std::string author_agent_id;
};

/// Narrowing options of get_edges.
struct GetEdgesOptions {
    /// The server accepts Forward or Backward here — Both is rejected with
    /// INVALID_ARGUMENT.
    Direction direction = Direction::Forward;
    /// Filters by edge type; unset returns every type.
    std::optional<EdgeType> edge_type;
    /// Returns the edges valid at that instant (0 = no filter).
    uint64_t at_timestamp = 0;
    /// Both > 0, with at_timestamp 0, returns the edges overlapping the window.
    /// A window query requires Direction::Forward.
    uint64_t window_start = 0;
    uint64_t window_end = 0;
};

}  // namespace statelet
