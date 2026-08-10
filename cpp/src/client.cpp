#include "statelet/client.h"

namespace statelet {

// The generated messages live in ::statelet::v1, which unqualified lookup from
// this (enclosing) namespace does not reach. They are pulled in one by one
// rather than with a using-directive because four of them — WriteEntry,
// VectorIndexConfig, RerankSpec, VectorSearchResult — share a name with a
// public SDK struct, and a directive would make every such use ambiguous.
using ::statelet::v1::BatchWriteRequest;
using ::statelet::v1::BatchWriteResponse;
using ::statelet::v1::CreateVectorIndexRequest;
using ::statelet::v1::CreateVectorIndexResponse;
using ::statelet::v1::DeleteRequest;
using ::statelet::v1::DeleteResponse;
using ::statelet::v1::DropVectorIndexRequest;
using ::statelet::v1::DropVectorIndexResponse;
using ::statelet::v1::GetRequest;
using ::statelet::v1::GetResponse;
using ::statelet::v1::GraphQueryRequest;
using ::statelet::v1::GraphQueryResponse;
using ::statelet::v1::MergeRequest;
using ::statelet::v1::MergeResponse;
using ::statelet::v1::PingRequest;
using ::statelet::v1::PingResponse;
using ::statelet::v1::PutRequest;
using ::statelet::v1::PutResponse;
using ::statelet::v1::VectorDeleteRequest;
using ::statelet::v1::VectorDeleteResponse;
using ::statelet::v1::VectorGetRequest;
using ::statelet::v1::VectorGetResponse;
using ::statelet::v1::VectorPutRequest;
using ::statelet::v1::VectorPutResponse;
using ::statelet::v1::VectorSearchRequest;
using ::statelet::v1::VectorSearchResponse;

Client::Client(const std::string& addr, uint32_t default_cf)
    : channel_(grpc::CreateChannel(addr, grpc::InsecureChannelCredentials())),
      stub_(::statelet::v1::Statelet::NewStub(channel_)),
      agent_stub_(::statelet::v1::AgentStateService::NewStub(channel_)),
      default_cf_(default_cf) {}

// ── KV operations ───────────────────────────────────────────────────────────

std::string Client::ping() {
    grpc::ClientContext ctx;
    PingRequest req;
    PingResponse resp;
    stub_->Ping(&ctx, req, &resp);
    return resp.message();
}

grpc::Status Client::put(const std::string& key, const std::string& value) {
    return put(default_cf_, key, value);
}

grpc::Status Client::put(uint32_t cf, const std::string& key, const std::string& value) {
    grpc::ClientContext ctx;
    PutRequest req;
    req.set_cf(cf);
    req.set_key(key);
    req.set_value(value);
    PutResponse resp;
    return stub_->Put(&ctx, req, &resp);
}

std::optional<std::string> Client::get(const std::string& key) {
    return get(default_cf_, key);
}

std::optional<std::string> Client::get(uint32_t cf, const std::string& key) {
    grpc::ClientContext ctx;
    GetRequest req;
    req.set_cf(cf);
    req.set_key(key);
    GetResponse resp;
    stub_->Get(&ctx, req, &resp);
    if (resp.found()) {
        return resp.value();
    }
    return std::nullopt;
}

grpc::Status Client::del(const std::string& key) {
    return del(default_cf_, key);
}

grpc::Status Client::del(uint32_t cf, const std::string& key) {
    grpc::ClientContext ctx;
    DeleteRequest req;
    req.set_cf(cf);
    req.set_key(key);
    DeleteResponse resp;
    return stub_->Delete(&ctx, req, &resp);
}

grpc::Status Client::merge(const std::string& key, const std::string& value) {
    return merge(default_cf_, key, value);
}

grpc::Status Client::merge(uint32_t cf, const std::string& key, const std::string& value) {
    grpc::ClientContext ctx;
    MergeRequest req;
    req.set_cf(cf);
    req.set_key(key);
    req.set_value(value);
    MergeResponse resp;
    return stub_->Merge(&ctx, req, &resp);
}

grpc::Status Client::batch_write(const std::vector<WriteEntry>& entries) {
    grpc::ClientContext ctx;
    BatchWriteRequest req;
    for (const auto& e : entries) {
        auto* entry = req.add_entries();
        entry->set_cf(e.cf == 0 ? default_cf_ : e.cf);
        switch (e.op) {
            case WriteOpType::Put:    entry->set_op(::statelet::v1::PUT); break;
            case WriteOpType::Delete: entry->set_op(::statelet::v1::DELETE); break;
            case WriteOpType::Merge:  entry->set_op(::statelet::v1::MERGE); break;
        }
        entry->set_key(e.key);
        entry->set_value(e.value);
    }
    BatchWriteResponse resp;
    return stub_->BatchWrite(&ctx, req, &resp);
}

// ── Vector operations ───────────────────────────────────────────────────────

grpc::Status Client::create_vector_index(const std::string& name,
                                          const VectorIndexConfig& config) {
    grpc::ClientContext ctx;
    CreateVectorIndexRequest req;
    req.set_index_name(name);
    auto* cfg = req.mutable_config();
    cfg->set_dim(config.dim);
    cfg->set_metric(config.metric);
    cfg->set_m(config.m);
    cfg->set_m_max0(config.m_max0);
    cfg->set_ef_construction(config.ef_construction);
    cfg->set_ef_search(config.ef_search);
    CreateVectorIndexResponse resp;
    return stub_->CreateVectorIndex(&ctx, req, &resp);
}

grpc::Status Client::drop_vector_index(const std::string& name) {
    grpc::ClientContext ctx;
    DropVectorIndexRequest req;
    req.set_index_name(name);
    DropVectorIndexResponse resp;
    return stub_->DropVectorIndex(&ctx, req, &resp);
}

grpc::Status Client::vector_put(const std::string& index_name, uint64_t vector_id,
                                 const std::vector<float>& vec) {
    grpc::ClientContext ctx;
    VectorPutRequest req;
    req.set_index_name(index_name);
    req.set_vector_id(vector_id);
    for (float f : vec) {
        req.add_vector(f);
    }
    VectorPutResponse resp;
    return stub_->VectorPut(&ctx, req, &resp);
}

grpc::Status Client::vector_delete(const std::string& index_name, uint64_t vector_id) {
    grpc::ClientContext ctx;
    VectorDeleteRequest req;
    req.set_index_name(index_name);
    req.set_vector_id(vector_id);
    VectorDeleteResponse resp;
    return stub_->VectorDelete(&ctx, req, &resp);
}

std::vector<VectorSearchResult> Client::vector_search(const std::string& index_name,
                                                       const std::vector<float>& query,
                                                       uint32_t k, uint32_t ef_search) {
    grpc::ClientContext ctx;
    VectorSearchRequest req;
    req.set_index_name(index_name);
    for (float f : query) {
        req.add_query(f);
    }
    req.set_k(k);
    req.set_ef_search(ef_search);
    VectorSearchResponse resp;
    stub_->VectorSearch(&ctx, req, &resp);

    std::vector<VectorSearchResult> results;
    results.reserve(resp.results_size());
    for (const auto& r : resp.results()) {
        results.push_back({r.id(), r.distance(), r.group_key()});
    }
    return results;
}

namespace {
// Populate a generated ::statelet::v1::RerankSpec from the public RerankSpec struct.
void fill_rerank(::statelet::v1::RerankSpec* dst, const RerankSpec& src, bool validate_only) {
    dst->set_enabled(true);
    dst->set_rerank_k(src.rerank_k);
    dst->set_model(src.model);
    dst->set_passage_field(src.passage_field);
    dst->set_signal_blend(src.signal_blend);
    dst->set_query_text(src.query_text);
    dst->set_validate_only(validate_only);
}
}  // namespace

std::vector<VectorSearchResult> Client::vector_search(const std::string& index_name,
                                                       const std::vector<float>& query,
                                                       uint32_t k, uint32_t ef_search,
                                                       const RerankSpec& rerank) {
    grpc::ClientContext ctx;
    VectorSearchRequest req;
    req.set_index_name(index_name);
    for (float f : query) {
        req.add_query(f);
    }
    req.set_k(k);
    req.set_ef_search(ef_search);
    fill_rerank(req.mutable_rerank(), rerank, false);
    VectorSearchResponse resp;
    stub_->VectorSearch(&ctx, req, &resp);

    std::vector<VectorSearchResult> results;
    results.reserve(resp.results_size());
    for (const auto& r : resp.results()) {
        results.push_back({r.id(), r.distance(), r.group_key()});
    }
    return results;
}

std::vector<VectorSearchResult> Client::vector_search_grouped(const std::string& index_name,
                                                              const std::vector<float>& query,
                                                              uint32_t k, uint32_t ef_search,
                                                              const GroupSpec& group) {
    grpc::ClientContext ctx;
    VectorSearchRequest req;
    req.set_index_name(index_name);
    for (float f : query) {
        req.add_query(f);
    }
    req.set_k(k);
    req.set_ef_search(ef_search);
    req.set_group_field(group.field);
    req.set_group_size(group.group_size);
    req.set_groups(group.groups);
    req.set_group_overfetch(group.overfetch);
    req.set_group_missing_as_own(group.missing_as_own);
    VectorSearchResponse resp;
    stub_->VectorSearch(&ctx, req, &resp);

    std::vector<VectorSearchResult> results;
    results.reserve(resp.results_size());
    for (const auto& r : resp.results()) {
        results.push_back({r.id(), r.distance(), r.group_key()});
    }
    return results;
}

grpc::Status Client::rerank_validate(const std::string& index_name,
                                     const RerankSpec& rerank) {
    grpc::ClientContext ctx;
    VectorSearchRequest req;
    req.set_index_name(index_name);
    req.set_k(1);
    fill_rerank(req.mutable_rerank(), rerank, true);
    VectorSearchResponse resp;
    return stub_->VectorSearch(&ctx, req, &resp);
}

std::optional<std::vector<float>> Client::vector_get(const std::string& index_name,
                                                      uint64_t vector_id) {
    grpc::ClientContext ctx;
    VectorGetRequest req;
    req.set_index_name(index_name);
    req.set_vector_id(vector_id);
    VectorGetResponse resp;
    stub_->VectorGet(&ctx, req, &resp);
    if (resp.found()) {
        return std::vector<float>(resp.vector().begin(), resp.vector().end());
    }
    return std::nullopt;
}

// ── Declarative graph query (openCypher subset) ─────────────────────────────

namespace {

/// Decode one wire value. The kind tag is compared numerically so this file
/// never names the generated NULL_ member (protoc renames it: NULL is a macro).
GraphValue decode_graph_value(const ::statelet::v1::GraphQueryValue& v) {
    GraphValue out;
    switch (static_cast<int>(v.kind())) {
        case static_cast<int>(GraphValueKind::Int):
            out.kind = GraphValueKind::Int;
            out.int_value = v.int_value();
            break;
        case static_cast<int>(GraphValueKind::Double):
            out.kind = GraphValueKind::Double;
            out.dbl_value = v.dbl_value();
            break;
        case static_cast<int>(GraphValueKind::String):
            out.kind = GraphValueKind::String;
            out.str_value = v.str_value();
            break;
        case static_cast<int>(GraphValueKind::Bool):
            out.kind = GraphValueKind::Bool;
            out.bool_value = v.bool_value();
            break;
        case static_cast<int>(GraphValueKind::Json):
            out.kind = GraphValueKind::Json;
            out.json_value = v.json_value();
            break;
        default:
            // NULL, or an unknown tag from a newer server.
            out.kind = GraphValueKind::Null;
            break;
    }
    return out;
}

}  // namespace

grpc::Status Client::graph_query(const std::string& cypher, const GraphQueryOptions& options,
                                 GraphQueryResult* out) {
    grpc::ClientContext ctx;
    GraphQueryRequest req;
    req.set_graph_name(options.graph_name);
    req.set_cypher(cypher);
    req.set_max_rows(options.max_rows);
    req.set_as_of(options.as_of);
    req.set_tx_as_of(options.tx_as_of);
    GraphQueryResponse resp;
    grpc::Status status = stub_->GraphQuery(&ctx, req, &resp);
    if (!status.ok() || out == nullptr) {
        return status;
    }

    out->columns.assign(resp.columns().begin(), resp.columns().end());
    out->warnings.assign(resp.warnings().begin(), resp.warnings().end());
    out->rows.clear();
    out->rows.reserve(resp.rows_size());
    for (const auto& row : resp.rows()) {
        std::vector<GraphValue> values;
        values.reserve(row.values_size());
        for (const auto& v : row.values()) {
            values.push_back(decode_graph_value(v));
        }
        out->rows.push_back(std::move(values));
    }
    return status;
}

grpc::Status Client::graph_query(const std::string& cypher, GraphQueryResult* out) {
    return graph_query(cypher, GraphQueryOptions{}, out);
}

}  // namespace statelet
