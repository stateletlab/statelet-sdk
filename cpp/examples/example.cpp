#include <cstdio>
#include <vector>

#include "statelet/client.h"

int main() {
    statelet::Client client("127.0.0.1:7379");

    // Ping
    printf("ping: %s\n", client.ping().c_str());

    // KV operations
    client.put("hello", "world");
    auto val = client.get("hello");
    if (val) {
        printf("got: %s\n", val->c_str());
    }
    client.del("hello");

    // Batch write
    client.batch_write({
        {statelet::WriteOpType::Put, 0, "k1", "v1"},
        {statelet::WriteOpType::Put, 0, "k2", "v2"},
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
        printf("id=%llu distance=%.4f\n", static_cast<unsigned long long>(r.id), r.distance);
    }

    client.drop_vector_index("embeddings");

    // Agent state: causal graph. Served by the gateway (default :9379), so
    // point the client there rather than at a data node.
    statelet::Client gateway("127.0.0.1:9379");

    uint64_t observed = 0;
    statelet::AddStepOptions step_options;
    step_options.content = "saw something";
    gateway.add_step("agent-1", statelet::StepType::Observe, step_options, &observed);

    uint64_t acted = 0;
    gateway.add_step("agent-1", statelet::StepType::Act, &acted);
    gateway.add_edge(observed, acted, statelet::EdgeType::Triggers);

    auto walked = gateway.traverse(observed, statelet::Direction::Forward, 3);
    for (const auto& step : walked.steps) {
        printf("step=%llu type=%s agent=%s\n", static_cast<unsigned long long>(step.id),
               step.step_type.c_str(), step.agent_id.c_str());
    }

    // Watch a key prefix; stop after the first event.
    gateway.watch_prefix("agent-1", 0, "state:", [](const statelet::WatchEvent& event) {
        printf("watch %s %s seq=%llu\n", event.event_type.c_str(), event.key.c_str(),
               static_cast<unsigned long long>(event.seq));
        return false;  // false ends the watch
    });

    return 0;
}
