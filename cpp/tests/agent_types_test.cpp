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

// Tests for the wire-name conversions and the step-JSON decoder. These need no
// gRPC stubs and no server, so they build standalone:
//
//   c++ -std=c++17 -I include tests/agent_types_test.cpp src/agent_types.cpp

#include "statelet/agent_types.h"

#include <cstdio>
#include <string>

namespace {

int failures = 0;

void check(bool ok, const char* what) {
    if (!ok) {
        std::printf("FAIL: %s\n", what);
        ++failures;
    }
}

void test_enum_round_trip() {
    check(statelet::to_string(statelet::StepType::Observe) == "Observe", "StepType::Observe name");
    check(statelet::to_string(statelet::StepType::Result) == "Result", "StepType::Result name");
    check(statelet::step_type_from_string("Tool") == statelet::StepType::Tool, "parse Tool");
    // Step/edge names are case-sensitive server-side.
    check(!statelet::step_type_from_string("tool").has_value(), "reject lowercase step type");

    check(statelet::to_string(statelet::EdgeType::DerivedFrom) == "DerivedFrom", "EdgeType name");
    check(statelet::edge_type_from_string("Contradicts") == statelet::EdgeType::Contradicts,
          "parse Contradicts");
    check(!statelet::edge_type_from_string("Causes").has_value(), "reject unknown edge type");

    check(statelet::to_string(statelet::Direction::Backward) == "backward", "Direction name");
    check(statelet::to_string(statelet::MemoryScope::Private) == "private", "MemoryScope name");
    check(statelet::memory_scope_from_string("team") == statelet::MemoryScope::Team, "parse team");
}

void test_parse_full_step() {
    const std::string json = R"({
        "id": 11,
        "agent_id": "agent-1",
        "step_type": "Act",
        "timestamp": 1712345678000,
        "branch_id": 2,
        "embedding_id": 99,
        "metadata": [123, 34, 109, 34, 58, 49, 125],
        "scope": {
            "scope": "private",
            "owner": "agent-1",
            "field_acl": [
                {"json_pointer": "/cot", "min_scope": "team"},
                {"json_pointer": "/raw", "min_scope": "private"}
            ]
        }
    })";

    statelet::Step step;
    check(statelet::parse_step(json, &step), "parse_step succeeds");
    check(step.id == 11, "id");
    check(step.agent_id == "agent-1", "agent_id");
    check(step.step_type == "Act", "step_type");
    check(step.timestamp == 1712345678000ULL, "timestamp");
    check(step.branch_id == 2, "branch_id");
    check(step.embedding_id.has_value() && *step.embedding_id == 99, "embedding_id");
    // metadata is a Vec<u8> on the wire: an array of numbers, not base64.
    check(step.metadata == R"({"m":1})", "metadata byte array");
    check(step.scope == statelet::MemoryScope::Private, "scope");
    check(step.scope_owner == "agent-1", "scope owner");
    check(step.field_acl.size() == 2, "field_acl size");
    check(step.field_acl.size() == 2 && step.field_acl[0].json_pointer == "/cot" &&
              step.field_acl[0].min_scope == statelet::MemoryScope::Team,
          "field_acl[0]");
    check(step.field_acl.size() == 2 && step.field_acl[1].min_scope == statelet::MemoryScope::Private,
          "field_acl[1]");
    check(step.raw_json == json, "raw json retained");
}

void test_parse_untagged_step() {
    // A step written before scope tagging carries no `scope` member and must
    // decode as world-readable, and a null embedding_id must stay empty.
    const std::string json =
        R"({"id":1,"agent_id":"a","step_type":"Tool","timestamp":0,)"
        R"("branch_id":0,"embedding_id":null,"metadata":[]})";

    statelet::Step step;
    check(statelet::parse_step(json, &step), "parse untagged step");
    check(step.scope == statelet::MemoryScope::World, "untagged scope defaults to world");
    check(step.scope_owner.empty(), "untagged owner empty");
    check(!step.embedding_id.has_value(), "null embedding_id");
    check(step.metadata.empty(), "empty metadata");
    check(step.field_acl.empty(), "no field acl");
}

void test_parse_escapes() {
    const std::string json =
        R"({"id":2,"agent_id":"a\"b\\c\ndé😀","step_type":"Think",)"
        R"("timestamp":1,"branch_id":0,"embedding_id":null,"metadata":[0,255],)"
        R"("scope":{"scope":"team","owner":"t\/1","field_acl":[]}})";

    statelet::Step step;
    check(statelet::parse_step(json, &step), "parse escaped step");
    check(step.agent_id == "a\"b\\c\nd\xc3\xa9\xf0\x9f\x98\x80", "string escapes and \\u");
    check(step.scope_owner == "t/1", "escaped solidus");
    check(step.metadata.size() == 2, "metadata size");
    check(step.metadata.size() == 2 && static_cast<unsigned char>(step.metadata[1]) == 255,
          "metadata byte value");
    check(step.scope == statelet::MemoryScope::Team, "scope team");
}

void test_parse_rejects_non_object() {
    statelet::Step step;
    check(!statelet::parse_step("", &step), "empty input rejected");
    check(!statelet::parse_step("[1,2]", &step), "array rejected");
    check(!statelet::parse_step("{\"id\":1", &step), "truncated object rejected");
    // Even on rejection the raw bytes stay reachable.
    check(step.raw_json == "{\"id\":1", "raw json kept on failure");
}

// Unknown members (a field added server-side) must be skipped, whatever their
// shape, without disturbing the members around them.
void test_parse_skips_unknown_members() {
    const std::string json =
        R"({"unknown_obj":{"a":[1,{"b":"}"}],"c":"x"},"id":7,)"
        R"("unknown_arr":[[],{},"s,tr","]"],"agent_id":"z","step_type":"Act",)"
        R"("unknown_num":-1.5e3,"unknown_bool":true,"timestamp":5,"branch_id":0,)"
        R"("embedding_id":null,"metadata":[65],"scope":{"scope":"world","owner":"","field_acl":[]}})";

    statelet::Step step;
    check(statelet::parse_step(json, &step), "parse with unknown members");
    check(step.id == 7, "id past unknown object");
    check(step.agent_id == "z", "agent_id past unknown array");
    check(step.timestamp == 5, "timestamp past unknown scalars");
    check(step.metadata == "A", "metadata after unknown members");
}

}  // namespace

int main() {
    test_enum_round_trip();
    test_parse_full_step();
    test_parse_untagged_step();
    test_parse_escapes();
    test_parse_rejects_non_object();
    test_parse_skips_unknown_members();

    if (failures == 0) {
        std::printf("ok: agent_types tests passed\n");
        return 0;
    }
    std::printf("%d check(s) failed\n", failures);
    return 1;
}
