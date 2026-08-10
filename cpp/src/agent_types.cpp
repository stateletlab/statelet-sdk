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

#include "statelet/agent_types.h"

// Wire-name conversions plus the decoder for the server's JSON step encoding.
//
// The engine serializes a CausalStep with serde, so `metadata` (a Vec<u8>)
// arrives as an array of numbers and the scope tag is a nested object:
//
//   {"id":11,"agent_id":"a","step_type":"Act","timestamp":17…,"branch_id":0,
//    "embedding_id":null,"metadata":[123,125],
//    "scope":{"scope":"world","owner":"","field_acl":[]}}
//
// Rather than take on a JSON dependency for that one shape, this file carries a
// small scanner: it walks members without materializing a document, and skips
// any value it does not care about.

namespace statelet {

std::string to_string(StepType type) {
    switch (type) {
        case StepType::Observe: return "Observe";
        case StepType::Think:   return "Think";
        case StepType::Act:     return "Act";
        case StepType::Tool:    return "Tool";
        case StepType::Result:  return "Result";
    }
    return "Observe";
}

std::optional<StepType> step_type_from_string(const std::string& name) {
    if (name == "Observe") return StepType::Observe;
    if (name == "Think")   return StepType::Think;
    if (name == "Act")     return StepType::Act;
    if (name == "Tool")    return StepType::Tool;
    if (name == "Result")  return StepType::Result;
    return std::nullopt;
}

std::string to_string(EdgeType type) {
    switch (type) {
        case EdgeType::Triggers:    return "Triggers";
        case EdgeType::Informs:     return "Informs";
        case EdgeType::Branches:    return "Branches";
        case EdgeType::Merges:      return "Merges";
        case EdgeType::Supersedes:  return "Supersedes";
        case EdgeType::DerivedFrom: return "DerivedFrom";
        case EdgeType::Contradicts: return "Contradicts";
    }
    return "Triggers";
}

std::optional<EdgeType> edge_type_from_string(const std::string& name) {
    if (name == "Triggers")    return EdgeType::Triggers;
    if (name == "Informs")     return EdgeType::Informs;
    if (name == "Branches")    return EdgeType::Branches;
    if (name == "Merges")      return EdgeType::Merges;
    if (name == "Supersedes")  return EdgeType::Supersedes;
    if (name == "DerivedFrom") return EdgeType::DerivedFrom;
    if (name == "Contradicts") return EdgeType::Contradicts;
    return std::nullopt;
}

std::string to_string(Direction direction) {
    switch (direction) {
        case Direction::Forward:  return "forward";
        case Direction::Backward: return "backward";
        case Direction::Both:     return "both";
    }
    return "forward";
}

std::string to_string(MemoryScope scope) {
    switch (scope) {
        case MemoryScope::World:   return "world";
        case MemoryScope::Team:    return "team";
        case MemoryScope::Private: return "private";
    }
    return "world";
}

std::string to_string(IngestAction action) {
    switch (action) {
        case IngestAction::Added:        return "added";
        case IngestAction::Deduplicated: return "deduplicated";
        case IngestAction::Superseded:   return "superseded";
        case IngestAction::Conflict:     return "conflict";
    }
    return "added";
}

std::optional<MemoryScope> memory_scope_from_string(const std::string& name) {
    if (name == "world")   return MemoryScope::World;
    if (name == "team")    return MemoryScope::Team;
    if (name == "private") return MemoryScope::Private;
    return std::nullopt;
}

// ── Minimal JSON scanner ────────────────────────────────────────────────────

namespace {

constexpr size_t kBad = std::string::npos;

size_t skip_ws(const std::string& s, size_t i) {
    while (i < s.size() && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r')) {
        ++i;
    }
    return i;
}

/// Index just past the string literal at s[i] == '"', without unescaping.
size_t skip_string(const std::string& s, size_t i) {
    if (i >= s.size() || s[i] != '"') return kBad;
    ++i;
    while (i < s.size()) {
        if (s[i] == '\\') {
            i += 2;
            continue;
        }
        if (s[i] == '"') return i + 1;
        ++i;
    }
    return kBad;
}

size_t skip_value(const std::string& s, size_t i);

/// Index just past the object/array whose opening bracket is at s[i].
size_t skip_container(const std::string& s, size_t i, char open, char close) {
    ++i;  // past the opening bracket
    i = skip_ws(s, i);
    if (i < s.size() && s[i] == close) return i + 1;
    while (i < s.size()) {
        if (open == '{') {
            i = skip_string(s, skip_ws(s, i));
            if (i == kBad) return kBad;
            i = skip_ws(s, i);
            if (i >= s.size() || s[i] != ':') return kBad;
            ++i;
        }
        i = skip_value(s, i);
        if (i == kBad) return kBad;
        i = skip_ws(s, i);
        if (i >= s.size()) return kBad;
        if (s[i] == ',') {
            ++i;
            continue;
        }
        if (s[i] == close) return i + 1;
        return kBad;
    }
    return kBad;
}

size_t skip_value(const std::string& s, size_t i) {
    i = skip_ws(s, i);
    if (i >= s.size()) return kBad;
    if (s[i] == '"') return skip_string(s, i);
    if (s[i] == '{') return skip_container(s, i, '{', '}');
    if (s[i] == '[') return skip_container(s, i, '[', ']');
    // Number / true / false / null: run to the next structural character.
    size_t j = i;
    while (j < s.size() && s[j] != ',' && s[j] != '}' && s[j] != ']' &&
           s[j] != ' ' && s[j] != '\t' && s[j] != '\n' && s[j] != '\r') {
        ++j;
    }
    return j == i ? kBad : j;
}

void append_utf8(std::string* out, uint32_t cp) {
    if (cp < 0x80) {
        out->push_back(static_cast<char>(cp));
    } else if (cp < 0x800) {
        out->push_back(static_cast<char>(0xC0 | (cp >> 6)));
        out->push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else if (cp < 0x10000) {
        out->push_back(static_cast<char>(0xE0 | (cp >> 12)));
        out->push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out->push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else {
        out->push_back(static_cast<char>(0xF0 | (cp >> 18)));
        out->push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
        out->push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out->push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    }
}

bool hex4(const std::string& s, size_t i, uint32_t* out) {
    if (i + 4 > s.size()) return false;
    uint32_t v = 0;
    for (size_t k = 0; k < 4; ++k) {
        const char c = s[i + k];
        v <<= 4;
        if (c >= '0' && c <= '9') {
            v |= static_cast<uint32_t>(c - '0');
        } else if (c >= 'a' && c <= 'f') {
            v |= static_cast<uint32_t>(c - 'a' + 10);
        } else if (c >= 'A' && c <= 'F') {
            v |= static_cast<uint32_t>(c - 'A' + 10);
        } else {
            return false;
        }
    }
    *out = v;
    return true;
}

/// Unescape the string literal at `i`; returns the index just past it.
size_t parse_string(const std::string& s, size_t i, std::string* out) {
    i = skip_ws(s, i);
    if (i >= s.size() || s[i] != '"') return kBad;
    ++i;
    out->clear();
    while (i < s.size()) {
        const char c = s[i];
        if (c == '"') return i + 1;
        if (c != '\\') {
            out->push_back(c);
            ++i;
            continue;
        }
        if (i + 1 >= s.size()) return kBad;
        const char esc = s[i + 1];
        i += 2;
        switch (esc) {
            case '"':  out->push_back('"');  break;
            case '\\': out->push_back('\\'); break;
            case '/':  out->push_back('/');  break;
            case 'b':  out->push_back('\b'); break;
            case 'f':  out->push_back('\f'); break;
            case 'n':  out->push_back('\n'); break;
            case 'r':  out->push_back('\r'); break;
            case 't':  out->push_back('\t'); break;
            case 'u': {
                uint32_t cp = 0;
                if (!hex4(s, i, &cp)) return kBad;
                i += 4;
                // A high surrogate pairs with the \uXXXX that follows it.
                if (cp >= 0xD800 && cp <= 0xDBFF && i + 6 <= s.size() &&
                    s[i] == '\\' && s[i + 1] == 'u') {
                    uint32_t low = 0;
                    if (hex4(s, i + 2, &low) && low >= 0xDC00 && low <= 0xDFFF) {
                        cp = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
                        i += 6;
                    }
                }
                append_utf8(out, cp);
                break;
            }
            default:
                return kBad;
        }
    }
    return kBad;
}

bool parse_uint(const std::string& s, size_t i, uint64_t* out) {
    i = skip_ws(s, i);
    if (i >= s.size() || s[i] < '0' || s[i] > '9') return false;
    uint64_t v = 0;
    while (i < s.size() && s[i] >= '0' && s[i] <= '9') {
        v = v * 10 + static_cast<uint64_t>(s[i] - '0');
        ++i;
    }
    *out = v;
    return true;
}

/// Invokes fn(key, value_start) for every member of the object at `obj`.
template <typename Fn>
bool for_each_member(const std::string& s, size_t obj, Fn fn) {
    size_t i = skip_ws(s, obj);
    if (i >= s.size() || s[i] != '{') return false;
    i = skip_ws(s, i + 1);
    if (i < s.size() && s[i] == '}') return true;
    while (i < s.size()) {
        std::string key;
        i = parse_string(s, i, &key);
        if (i == kBad) return false;
        i = skip_ws(s, i);
        if (i >= s.size() || s[i] != ':') return false;
        const size_t value_start = skip_ws(s, i + 1);
        const size_t value_end = skip_value(s, value_start);
        if (value_end == kBad) return false;
        fn(key, value_start);
        i = skip_ws(s, value_end);
        if (i >= s.size()) return false;
        if (s[i] == ',') {
            i = skip_ws(s, i + 1);
            continue;
        }
        return s[i] == '}';
    }
    return false;
}

/// Invokes fn(value_start) for every element of the array at `arr`.
template <typename Fn>
bool for_each_element(const std::string& s, size_t arr, Fn fn) {
    size_t i = skip_ws(s, arr);
    if (i >= s.size() || s[i] != '[') return false;
    i = skip_ws(s, i + 1);
    if (i < s.size() && s[i] == ']') return true;
    while (i < s.size()) {
        const size_t value_end = skip_value(s, i);
        if (value_end == kBad) return false;
        fn(i);
        i = skip_ws(s, value_end);
        if (i >= s.size()) return false;
        if (s[i] == ',') {
            i = skip_ws(s, i + 1);
            continue;
        }
        return s[i] == ']';
    }
    return false;
}

void parse_scope_tag(const std::string& s, size_t obj, Step* out) {
    for_each_member(s, obj, [&](const std::string& key, size_t value) {
        if (key == "scope") {
            std::string name;
            if (parse_string(s, value, &name) != kBad) {
                if (auto scope = memory_scope_from_string(name)) out->scope = *scope;
            }
        } else if (key == "owner") {
            parse_string(s, value, &out->scope_owner);
        } else if (key == "field_acl") {
            for_each_element(s, value, [&](size_t element) {
                FieldRule rule;
                const bool ok = for_each_member(s, element, [&](const std::string& k, size_t v) {
                    if (k == "json_pointer") {
                        parse_string(s, v, &rule.json_pointer);
                    } else if (k == "min_scope") {
                        std::string name;
                        if (parse_string(s, v, &name) != kBad) {
                            if (auto scope = memory_scope_from_string(name)) rule.min_scope = *scope;
                        }
                    }
                });
                if (ok) out->field_acl.push_back(std::move(rule));
            });
        }
    });
}

}  // namespace

bool parse_step(const std::string& json, Step* out) {
    *out = Step{};
    out->raw_json = json;
    if (json.empty()) return false;
    return for_each_member(json, 0, [&](const std::string& key, size_t value) {
        if (key == "id") {
            parse_uint(json, value, &out->id);
        } else if (key == "agent_id") {
            parse_string(json, value, &out->agent_id);
        } else if (key == "step_type") {
            parse_string(json, value, &out->step_type);
        } else if (key == "timestamp") {
            parse_uint(json, value, &out->timestamp);
        } else if (key == "branch_id") {
            parse_uint(json, value, &out->branch_id);
        } else if (key == "embedding_id") {
            uint64_t id = 0;
            if (parse_uint(json, value, &id)) out->embedding_id = id;
        } else if (key == "metadata") {
            for_each_element(json, value, [&](size_t element) {
                uint64_t byte = 0;
                if (parse_uint(json, element, &byte)) {
                    out->metadata.push_back(static_cast<char>(byte & 0xFF));
                }
            });
        } else if (key == "scope") {
            // An untagged step (written before scope tagging) has no member
            // here and keeps the World default, matching the server's serde.
            parse_scope_tag(json, value, out);
        }
    });
}

}  // namespace statelet
