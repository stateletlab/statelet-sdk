# Copyright 2026 Statelet Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for the LangGraph-style Statelet runtime adapter (#1752)."""

import sys
from pathlib import Path

# Make the SDK importable when run from the repo without installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from statelet.langgraph_adapter import (  # noqa: E402
    InMemoryRuntimeStateStore,
    LangGraphStateAdapter,
)


def _checkpoint(checkpoint_id="ckpt-1", *, parent=None):
    return {
        "v": 1,
        "id": checkpoint_id,
        "parent_checkpoint_id": parent,
        "channel_values": {
            "messages": [
                {"role": "user", "content": "ship phase 11"},
                {"role": "assistant", "content": "mapped to runtime state"},
            ],
            "plan": ["map checkpoint", "persist snapshot"],
            "memory": [
                {
                    "id": "fact:phase",
                    "text": "phase 11 validates framework integration",
                    "confidence": 0.82,
                    "metadata": {"source": "test"},
                }
            ],
            "context": {"repo": "statelet-release"},
        },
        "channel_versions": {"messages": 2, "plan": 1},
        "versions_seen": {"agent": {"messages": 1}},
        "pending_sends": [{"node": "review"}],
    }


def test_put_maps_checkpoint_to_runtime_records():
    store = InMemoryRuntimeStateStore()
    adapter = LangGraphStateAdapter(store, actor_id="test-runner")

    config = adapter.put(
        {"configurable": {"thread_id": "thread-a", "branch_id": 7}},
        _checkpoint(),
        {"source": "langgraph"},
        {"plan": 2},
    )

    assert config["configurable"]["checkpoint_id"] == "ckpt-1"
    run_id = store.get_or_create_run_id("thread-a")

    events = store.events[run_id]
    assert [event.kind for event in events] == ["Context", "State"]
    assert events[0].payload["metadata"]["framework"] == "langgraph"
    assert events[1].causal_parent_seq == events[0].seq
    assert events[1].payload["channel_versions"]["plan"] == 2

    projection = store.get_projection(run_id, "langgraph.state")
    assert projection["checkpoint"]["channel_values"]["plan"] == [
        "map checkpoint",
        "persist snapshot",
    ]
    snapshot = store.latest_snapshot(run_id, "langgraph.state")
    assert snapshot is not None
    assert snapshot.checkpoint_id == "ckpt-1"
    assert len(snapshot.state_hash) == 64

    context = store.context_versions[run_id][0]
    assert context.context_id == "langgraph:thread-a"
    assert context.branch_id == 7
    assert set(context.segments) == {"messages", "context"}

    memory = store.memory_records[run_id][0]
    assert memory.memory_id == "fact:phase"
    assert memory.confidence == 0.82
    assert memory.metadata["source"] == "test"


def test_get_tuple_restores_latest_state_from_projection():
    store = InMemoryRuntimeStateStore()
    adapter = LangGraphStateAdapter(store)
    adapter.put({"configurable": {"thread_id": "thread-a"}}, _checkpoint("ckpt-1"))
    adapter.put(
        {"configurable": {"thread_id": "thread-a", "checkpoint_id": "ckpt-1"}},
        _checkpoint("ckpt-2", parent="ckpt-1"),
    )

    item = adapter.get_tuple({"configurable": {"thread_id": "thread-a"}})

    assert item is not None
    assert item.config["configurable"]["checkpoint_id"] == "ckpt-2"
    assert item.parent_config["configurable"]["checkpoint_id"] == "ckpt-1"
    assert item.checkpoint["channel_values"]["messages"][0]["content"] == (
        "ship phase 11"
    )


def test_import_export_roundtrips_representative_state_shape():
    store = InMemoryRuntimeStateStore()
    adapter = LangGraphStateAdapter(store)
    config = {"configurable": {"thread_id": "thread-a"}}
    adapter.put(config, _checkpoint("ckpt-original"), {"step": "original"})

    exported = adapter.export_state(config)
    imported_config = adapter.import_state(
        {"configurable": {"thread_id": "thread-b"}},
        exported,
    )

    assert imported_config["configurable"]["thread_id"] == "thread-b"
    restored = adapter.get_tuple(imported_config)
    assert restored is not None
    assert restored.checkpoint == exported["checkpoint"]
    assert restored.metadata["step"] == "original"
    assert store.get_or_create_run_id("thread-a") != store.get_or_create_run_id("thread-b")


def test_specific_checkpoint_lookup_uses_checkpoint_id():
    store = InMemoryRuntimeStateStore()
    adapter = LangGraphStateAdapter(store)
    config = {"configurable": {"thread_id": "thread-a"}}
    first = _checkpoint("ckpt-1")
    second = _checkpoint("ckpt-2", parent="ckpt-1")
    second["channel_values"]["plan"] = ["new plan"]
    adapter.put(config, first)
    adapter.put(
        {"configurable": {"thread_id": "thread-a", "checkpoint_id": "ckpt-1"}},
        second,
    )

    item = adapter.get_tuple(
        {"configurable": {"thread_id": "thread-a", "checkpoint_id": "ckpt-1"}}
    )

    assert item is not None
    assert item.checkpoint["id"] == "ckpt-1"
    assert item.checkpoint["channel_values"]["plan"] == [
        "map checkpoint",
        "persist snapshot",
    ]
