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

#include "statelet/client.h"

// Wire mapping for the agent-state surface (AgentStateService): the causal
// graph, branches, reactive state, coordination leases, temporal edges and the
// prefix watch. The value types and the step-JSON decoding live in
// agent_types.{h,cpp}, which stay free of any generated-stub dependency.

namespace statelet {

// The public SDK structs share several names with the generated messages
// (WriteEntry, RerankSpec, …), so the generated namespace is aliased rather
// than imported.
namespace pb = ::statelet::v1;

namespace {

Edge decode_edge(const pb::AgentEdgeProto& src) {
    Edge edge;
    edge.peer_step_id = src.peer_step_id();
    edge.edge_type = src.edge_type();
    edge.props = src.props();
    edge.valid_from = src.valid_from();
    edge.valid_to = src.valid_to();
    return edge;
}

template <typename Edges>
std::vector<Edge> decode_edges(const Edges& src) {
    std::vector<Edge> edges;
    edges.reserve(static_cast<size_t>(src.size()));
    for (const auto& e : src) {
        edges.push_back(decode_edge(e));
    }
    return edges;
}

template <typename Jsons>
std::vector<Step> decode_steps(const Jsons& src) {
    std::vector<Step> steps;
    steps.reserve(static_cast<size_t>(src.size()));
    for (const auto& json : src) {
        Step step;
        parse_step(json, &step);
        steps.push_back(std::move(step));
    }
    return steps;
}

}  // namespace

// ── Causal graph ────────────────────────────────────────────────────────────

grpc::Status Client::add_step(const std::string& agent_id, StepType type,
                              const AddStepOptions& options, uint64_t* step_id) {
    grpc::ClientContext ctx;
    pb::AgentAddStepRequest req;
    req.set_agent_id(agent_id);
    req.set_step_type(to_string(type));
    req.set_branch_id(options.branch_id);
    req.set_content(options.content);
    req.set_metadata(options.metadata);
    for (float f : options.embedding) {
        req.add_embedding(f);
    }
    req.set_scope(to_string(options.scope));
    req.set_scope_owner(options.scope_owner);
    req.set_field_acl_json(options.field_acl_json);
    pb::AgentAddStepResponse resp;
    grpc::Status status = agent_stub_->AddStep(&ctx, req, &resp);
    if (status.ok() && step_id != nullptr) {
        *step_id = resp.step_id();
    }
    return status;
}

grpc::Status Client::add_step(const std::string& agent_id, StepType type, uint64_t* step_id) {
    return add_step(agent_id, type, AddStepOptions{}, step_id);
}

grpc::Status Client::add_edge(uint64_t src_step_id, uint64_t dst_step_id, EdgeType type,
                              const AddEdgeOptions& options) {
    grpc::ClientContext ctx;
    pb::AgentAddEdgeRequest req;
    req.set_src_step_id(src_step_id);
    req.set_dst_step_id(dst_step_id);
    req.set_edge_type(to_string(type));
    req.set_props(options.props);
    req.set_valid_from(options.valid_from);
    req.set_valid_to(options.valid_to);
    req.set_author_agent_id(options.author_agent_id);
    pb::AgentAddEdgeResponse resp;
    return agent_stub_->AddEdge(&ctx, req, &resp);
}

std::optional<Step> Client::get_step(uint64_t step_id) {
    grpc::ClientContext ctx;
    pb::AgentGetStepRequest req;
    req.set_step_id(step_id);
    pb::AgentGetStepResponse resp;
    if (!agent_stub_->GetStep(&ctx, req, &resp).ok() || !resp.found()) {
        return std::nullopt;
    }
    Step step;
    parse_step(resp.step_json(), &step);
    return step;
}

std::optional<std::string> Client::get_content(uint64_t step_id) {
    grpc::ClientContext ctx;
    pb::AgentGetContentRequest req;
    req.set_step_id(step_id);
    pb::AgentGetContentResponse resp;
    if (!agent_stub_->GetContent(&ctx, req, &resp).ok() || !resp.found()) {
        return std::nullopt;
    }
    return resp.content();
}

std::vector<Edge> Client::get_edges(uint64_t step_id, const GetEdgesOptions& options) {
    grpc::ClientContext ctx;
    pb::AgentGetEdgesRequest req;
    req.set_step_id(step_id);
    req.set_direction(to_string(options.direction));
    if (options.edge_type.has_value()) {
        req.set_edge_type(to_string(*options.edge_type));
    }
    req.set_at_timestamp(options.at_timestamp);
    req.set_window_start(options.window_start);
    req.set_window_end(options.window_end);
    pb::AgentGetEdgesResponse resp;
    if (!agent_stub_->GetEdges(&ctx, req, &resp).ok()) {
        return {};
    }
    return decode_edges(resp.edges());
}

TraverseResult Client::traverse(uint64_t start_step_id, Direction direction, uint32_t max_depth) {
    grpc::ClientContext ctx;
    pb::AgentTraverseRequest req;
    req.set_start_step_id(start_step_id);
    req.set_direction(to_string(direction));
    req.set_max_depth(max_depth);
    pb::AgentTraverseResponse resp;
    if (!agent_stub_->Traverse(&ctx, req, &resp).ok()) {
        return {};
    }
    TraverseResult result;
    result.steps = decode_steps(resp.step_jsons());
    result.edges = decode_edges(resp.edges());
    return result;
}

std::vector<CausalChain> Client::find_similar_chains(const std::vector<float>& query_embedding,
                                                     uint32_t k, uint32_t chain_depth,
                                                     uint32_t ef) {
    grpc::ClientContext ctx;
    pb::AgentFindSimilarChainsRequest req;
    for (float f : query_embedding) {
        req.add_query_embedding(f);
    }
    req.set_k(k);
    req.set_chain_depth(chain_depth);
    req.set_ef(ef);
    pb::AgentFindSimilarChainsResponse resp;
    if (!agent_stub_->FindSimilarChains(&ctx, req, &resp).ok()) {
        return {};
    }
    std::vector<CausalChain> chains;
    chains.reserve(static_cast<size_t>(resp.chains_size()));
    for (const auto& c : resp.chains()) {
        CausalChain chain;
        parse_step(c.anchor_step_json(), &chain.anchor);
        chain.distance = c.distance();
        chain.steps = decode_steps(c.step_jsons());
        chain.edges = decode_edges(c.edges());
        chains.push_back(std::move(chain));
    }
    return chains;
}

// ── Branches (fork) ─────────────────────────────────────────────────────────

grpc::Status Client::fork(const std::string& label, uint64_t parent_branch_id,
                          uint64_t* branch_id) {
    grpc::ClientContext ctx;
    pb::AgentForkRequest req;
    req.set_label(label);
    req.set_parent_branch_id(parent_branch_id);
    pb::AgentForkResponse resp;
    grpc::Status status = agent_stub_->Fork(&ctx, req, &resp);
    if (status.ok() && branch_id != nullptr) {
        *branch_id = resp.branch_id();
    }
    return status;
}

grpc::Status Client::merge_branch(uint64_t branch_id) {
    grpc::ClientContext ctx;
    pb::AgentMergeBranchRequest req;
    req.set_branch_id(branch_id);
    pb::AgentMergeBranchResponse resp;
    return agent_stub_->MergeBranch(&ctx, req, &resp);
}

grpc::Status Client::discard_branch(uint64_t branch_id) {
    grpc::ClientContext ctx;
    pb::AgentDiscardBranchRequest req;
    req.set_branch_id(branch_id);
    pb::AgentDiscardBranchResponse resp;
    return agent_stub_->DiscardBranch(&ctx, req, &resp);
}

std::vector<BranchMeta> Client::list_branches() {
    grpc::ClientContext ctx;
    pb::AgentListBranchesRequest req;
    pb::AgentListBranchesResponse resp;
    if (!agent_stub_->ListBranches(&ctx, req, &resp).ok()) {
        return {};
    }
    std::vector<BranchMeta> branches;
    branches.reserve(static_cast<size_t>(resp.branches_size()));
    for (const auto& b : resp.branches()) {
        BranchMeta meta;
        meta.id = b.id();
        meta.parent_id = b.parent_id();
        meta.parent_snapshot_seq = b.parent_snapshot_seq();
        meta.created_at = b.created_at();
        meta.status = b.status();
        meta.label = b.label();
        branches.push_back(std::move(meta));
    }
    return branches;
}

grpc::Status Client::branch_put(uint64_t branch_id, uint32_t cf, const std::string& key,
                                const std::string& value) {
    grpc::ClientContext ctx;
    pb::AgentBranchPutRequest req;
    req.set_branch_id(branch_id);
    req.set_cf(cf);
    req.set_key(key);
    req.set_value(value);
    pb::AgentBranchPutResponse resp;
    return agent_stub_->BranchPut(&ctx, req, &resp);
}

std::optional<std::string> Client::branch_get(uint64_t branch_id, uint32_t cf,
                                              const std::string& key) {
    grpc::ClientContext ctx;
    pb::AgentBranchGetRequest req;
    req.set_branch_id(branch_id);
    req.set_cf(cf);
    req.set_key(key);
    pb::AgentBranchGetResponse resp;
    if (!agent_stub_->BranchGet(&ctx, req, &resp).ok() || !resp.found()) {
        return std::nullopt;
    }
    return resp.value();
}

// ── Reactive state ──────────────────────────────────────────────────────────

grpc::Status Client::cas_put(uint32_t cf, const std::string& key, uint64_t expected_seq,
                             const std::string& new_value, CasPutResult* result) {
    grpc::ClientContext ctx;
    pb::AgentCasPutRequest req;
    req.set_cf(cf);
    req.set_key(key);
    req.set_expected_seq(expected_seq);
    req.set_new_value(new_value);
    pb::AgentCasPutResponse resp;
    grpc::Status status = agent_stub_->CasPut(&ctx, req, &resp);
    if (status.ok() && result != nullptr) {
        result->success = resp.success();
        result->new_seq = resp.new_seq();
        result->actual_seq = resp.actual_seq();
    }
    return status;
}

grpc::Status Client::memory_ingest(const std::string& scope, const std::string& content,
                                   const MemoryIngestOptions& options,
                                   MemoryIngestResult* result) {
    grpc::ClientContext ctx;
    pb::AgentMemoryIngestRequest req;
    req.set_scope(scope);
    req.set_content(content);
    for (const auto& candidate : options.candidates) {
        auto* c = req.add_candidates();
        c->set_fact_id(candidate.fact_id);
        c->set_sim(candidate.sim);
    }
    for (uint64_t step : options.provenance_steps) {
        req.add_provenance_steps(step);
    }
    if (options.embedding_id.has_value()) {
        req.set_embedding_id(*options.embedding_id);
    }
    req.set_dedup_threshold(options.dedup_threshold);
    req.set_supersede_threshold(options.supersede_threshold);
    req.set_author_agent_id(options.author_agent_id);
    req.set_confidence(options.confidence);
    req.set_run_id(options.run_id);
    req.set_fence(options.fence);
    req.set_lease_key(options.lease_key);

    pb::AgentMemoryIngestResponse resp;
    grpc::Status status = agent_stub_->MemoryIngest(&ctx, req, &resp);
    if (!status.ok()) {
        return status;
    }
    if (resp.action() > static_cast<uint32_t>(IngestAction::Conflict)) {
        return grpc::Status(grpc::StatusCode::INTERNAL,
                            "statelet: unknown ingest action " + std::to_string(resp.action()));
    }
    if (result != nullptr) {
        result->action = static_cast<IngestAction>(resp.action());
        result->fact_id = resp.fact_id();
        result->superseded.assign(resp.superseded().begin(), resp.superseded().end());
        result->committed = resp.committed();
        result->conflict_fact_id = resp.conflict_fact_id();
        result->fence_lost = resp.fence_lost();
    }
    return status;
}

// ── Coordination: claims, leases, fences ────────────────────────────────────

grpc::Status Client::claim(const std::string& key, const std::string& agent_id,
                           LeaseResult* result) {
    grpc::ClientContext ctx;
    pb::AgentClaimRequest req;
    req.set_key(key);
    req.set_agent_id(agent_id);
    pb::AgentClaimResponse resp;
    grpc::Status status = agent_stub_->Claim(&ctx, req, &resp);
    if (status.ok() && result != nullptr) {
        result->acquired = resp.acquired();
        result->holder = resp.holder();
        result->fence = resp.fence();
    }
    return status;
}

grpc::Status Client::lease(const std::string& key, const std::string& agent_id, uint64_t ttl_ms,
                           LeaseResult* result) {
    grpc::ClientContext ctx;
    pb::AgentLeaseRequest req;
    req.set_key(key);
    req.set_agent_id(agent_id);
    req.set_ttl_ms(ttl_ms);
    pb::AgentLeaseResponse resp;
    grpc::Status status = agent_stub_->Lease(&ctx, req, &resp);
    if (status.ok() && result != nullptr) {
        result->acquired = resp.acquired();
        result->holder = resp.holder();
        result->fence = resp.fence();
    }
    return status;
}

grpc::Status Client::renew(const std::string& key, const std::string& agent_id, uint64_t fence,
                           uint64_t ttl_ms, LeaseResult* result) {
    grpc::ClientContext ctx;
    pb::AgentRenewRequest req;
    req.set_key(key);
    req.set_agent_id(agent_id);
    req.set_fence(fence);
    req.set_ttl_ms(ttl_ms);
    pb::AgentRenewResponse resp;
    grpc::Status status = agent_stub_->Renew(&ctx, req, &resp);
    if (status.ok() && result != nullptr) {
        result->acquired = resp.acquired();
        result->holder = resp.holder();
        result->fence = resp.fence();
    }
    return status;
}

grpc::Status Client::release(const std::string& key, uint64_t fence, bool* released) {
    grpc::ClientContext ctx;
    pb::AgentReleaseRequest req;
    req.set_key(key);
    req.set_fence(fence);
    pb::AgentReleaseResponse resp;
    grpc::Status status = agent_stub_->Release(&ctx, req, &resp);
    if (status.ok() && released != nullptr) {
        *released = resp.released();
    }
    return status;
}

// ── Temporal edges ──────────────────────────────────────────────────────────

grpc::Status Client::expire_edge(uint64_t src_step_id, uint64_t dst_step_id, EdgeType type,
                                 uint64_t expire_at) {
    grpc::ClientContext ctx;
    pb::AgentExpireEdgeRequest req;
    req.set_src_step_id(src_step_id);
    req.set_dst_step_id(dst_step_id);
    req.set_edge_type(to_string(type));
    req.set_expire_at(expire_at);
    pb::AgentExpireEdgeResponse resp;
    return agent_stub_->ExpireEdge(&ctx, req, &resp);
}

std::vector<TemporalEdge> Client::edge_history(uint64_t src_step_id, uint64_t dst_step_id,
                                               EdgeType type) {
    grpc::ClientContext ctx;
    pb::AgentEdgeHistoryRequest req;
    req.set_src_step_id(src_step_id);
    req.set_dst_step_id(dst_step_id);
    req.set_edge_type(to_string(type));
    pb::AgentEdgeHistoryResponse resp;
    if (!agent_stub_->EdgeHistory(&ctx, req, &resp).ok()) {
        return {};
    }
    std::vector<TemporalEdge> edges;
    edges.reserve(static_cast<size_t>(resp.edges_size()));
    for (const auto& e : resp.edges()) {
        TemporalEdge edge;
        edge.src = e.src();
        edge.dst = e.dst();
        edge.edge_type = e.edge_type();
        edge.valid_from = e.valid_from();
        edge.valid_to = e.valid_to();
        edge.props = e.props();
        edge.tx_from = e.tx_from();
        edge.tx_to = e.tx_to();
        edge.author_agent_id = e.author_agent_id();
        edges.push_back(std::move(edge));
    }
    return edges;
}

// ── Prefix watch ────────────────────────────────────────────────────────────

grpc::Status Client::watch_prefix(const std::string& agent_id, uint32_t cf,
                                  const std::string& prefix,
                                  const std::function<bool(const WatchEvent&)>& on_event) {
    grpc::ClientContext ctx;
    pb::AgentWatchPrefixRequest req;
    req.set_agent_id(agent_id);
    req.set_cf(cf);
    req.set_prefix(prefix);

    std::unique_ptr<grpc::ClientReader<pb::AgentWatchEventProto>> reader(
        agent_stub_->WatchPrefix(&ctx, req));

    bool stopped_by_caller = false;
    pb::AgentWatchEventProto proto;
    while (reader->Read(&proto)) {
        WatchEvent event;
        event.event_type = proto.event_type();
        event.cf = proto.cf();
        event.key = proto.key();
        event.value = proto.value();
        event.seq = proto.seq();
        if (!on_event(event)) {
            stopped_by_caller = true;
            // Cancelling is what ends an open server stream; Finish() then
            // reports CANCELLED, which is this caller's own doing, not a fault.
            ctx.TryCancel();
            break;
        }
    }
    grpc::Status status = reader->Finish();
    if (stopped_by_caller) {
        return grpc::Status::OK;
    }
    return status;
}

}  // namespace statelet
