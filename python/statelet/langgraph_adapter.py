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

"""LangGraph-style adapter for Statelet's agent runtime data model.

The adapter is intentionally dependency-free: it follows the shape of
LangGraph's checkpointer/state-store contract without importing LangGraph. A
real LangGraph application can wrap it from a small optional package, while the
core SDK and crate keep their existing dependency footprint.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
import hashlib
import json
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol


JsonMap = Dict[str, Any]
RuntimeEventKind = str


def _now_ms() -> int:
    return int(time.time() * 1000)


def _canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode(
        "utf-8"
    )


def _state_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _copy_map(value: Optional[Mapping[str, Any]]) -> JsonMap:
    return deepcopy(dict(value or {}))


def _configurable(config: Optional[Mapping[str, Any]]) -> JsonMap:
    raw = dict(config or {})
    configurable = raw.get("configurable", raw)
    if not isinstance(configurable, Mapping):
        raise TypeError("config.configurable must be a mapping")
    return dict(configurable)


def _required_str(configurable: Mapping[str, Any], key: str) -> str:
    value = configurable.get(key)
    if value is None or value == "":
        raise ValueError(f"missing required LangGraph config field: {key}")
    return str(value)


def _checkpoint_id(checkpoint: Mapping[str, Any], fallback: str) -> str:
    value = checkpoint.get("id") or checkpoint.get("checkpoint_id") or fallback
    return str(value)


def _extract_channel_values(checkpoint: Mapping[str, Any]) -> JsonMap:
    values = checkpoint.get("channel_values", {})
    if not isinstance(values, Mapping):
        raise TypeError("checkpoint.channel_values must be a mapping")
    return _copy_map(values)


@dataclass(frozen=True)
class RuntimeEventRecord:
    """Adapter-level representation of one Statelet runtime event."""

    run_id: int
    seq: int
    kind: RuntimeEventKind
    payload: JsonMap
    actor_id: str = ""
    scope: str = ""
    branch_id: int = 0
    causal_parent_seq: Optional[int] = None
    ts: int = field(default_factory=_now_ms)


@dataclass(frozen=True)
class RuntimeSnapshotRecord:
    """Durable reducer-state snapshot produced from a framework checkpoint."""

    run_id: int
    checkpoint_id: str
    reducer_id: str
    event_seq: int
    state_hash: str
    payload: JsonMap
    branch_id: int = 0
    scope: str = ""
    created_at: int = field(default_factory=_now_ms)


@dataclass(frozen=True)
class RuntimeContextVersionRecord:
    """Versioned context segments mapped from framework state channels."""

    run_id: int
    context_id: str
    version: int
    checkpoint_id: str
    event_seq: int
    segments: JsonMap
    branch_id: int = 0


@dataclass(frozen=True)
class RuntimeMemoryRecord:
    """Structured memory refs surfaced from checkpoint channel values."""

    run_id: int
    memory_id: str
    value: Any
    source_event_seq: int
    checkpoint_id: str
    confidence: float = 1.0
    metadata: JsonMap = field(default_factory=dict)


@dataclass(frozen=True)
class LangGraphCheckpointTuple:
    """Minimal checkpoint tuple compatible with LangGraph-style callers."""

    config: JsonMap
    checkpoint: JsonMap
    metadata: JsonMap
    parent_config: Optional[JsonMap] = None


class RuntimeStateStore(Protocol):
    """Storage protocol used by :class:`LangGraphStateAdapter`.

    Production wiring can back this protocol with Statelet's runtime-event,
    reducer-projection, snapshot, context-version and memory-lineage APIs. The
    included in-memory implementation is for tests and local prototypes only.
    """

    def get_or_create_run_id(self, thread_id: str) -> int:
        ...

    def append_event(
        self,
        run_id: int,
        kind: RuntimeEventKind,
        payload: Mapping[str, Any],
        *,
        actor_id: str = "",
        scope: str = "",
        branch_id: int = 0,
        causal_parent_seq: Optional[int] = None,
    ) -> RuntimeEventRecord:
        ...

    def put_projection(
        self,
        run_id: int,
        reducer_id: str,
        checkpoint_id: str,
        payload: Mapping[str, Any],
    ) -> None:
        ...

    def get_projection(
        self,
        run_id: int,
        reducer_id: str,
        checkpoint_id: Optional[str] = None,
    ) -> Optional[JsonMap]:
        ...

    def put_snapshot(self, snapshot: RuntimeSnapshotRecord) -> None:
        ...

    def latest_snapshot(
        self,
        run_id: int,
        reducer_id: str,
        checkpoint_id: Optional[str] = None,
    ) -> Optional[RuntimeSnapshotRecord]:
        ...

    def put_context_version(self, record: RuntimeContextVersionRecord) -> None:
        ...

    def put_memory_record(self, record: RuntimeMemoryRecord) -> None:
        ...


class InMemoryRuntimeStateStore:
    """Small runtime-store implementation used by tests and examples."""

    def __init__(self) -> None:
        self._run_ids: Dict[str, int] = {}
        self._next_run_id = 1
        self.events: Dict[int, List[RuntimeEventRecord]] = {}
        self.projections: Dict[tuple[int, str, str], JsonMap] = {}
        self._latest_projection: Dict[tuple[int, str], str] = {}
        self.snapshots: Dict[int, List[RuntimeSnapshotRecord]] = {}
        self.context_versions: Dict[int, List[RuntimeContextVersionRecord]] = {}
        self.memory_records: Dict[int, List[RuntimeMemoryRecord]] = {}

    def get_or_create_run_id(self, thread_id: str) -> int:
        if thread_id not in self._run_ids:
            self._run_ids[thread_id] = self._next_run_id
            self._next_run_id += 1
        return self._run_ids[thread_id]

    def append_event(
        self,
        run_id: int,
        kind: RuntimeEventKind,
        payload: Mapping[str, Any],
        *,
        actor_id: str = "",
        scope: str = "",
        branch_id: int = 0,
        causal_parent_seq: Optional[int] = None,
    ) -> RuntimeEventRecord:
        seq = len(self.events.setdefault(run_id, [])) + 1
        event = RuntimeEventRecord(
            run_id=run_id,
            seq=seq,
            kind=kind,
            payload=_copy_map(payload),
            actor_id=actor_id,
            scope=scope,
            branch_id=branch_id,
            causal_parent_seq=causal_parent_seq,
        )
        self.events[run_id].append(event)
        return event

    def put_projection(
        self,
        run_id: int,
        reducer_id: str,
        checkpoint_id: str,
        payload: Mapping[str, Any],
    ) -> None:
        key = (run_id, reducer_id, checkpoint_id)
        self.projections[key] = _copy_map(payload)
        self._latest_projection[(run_id, reducer_id)] = checkpoint_id

    def get_projection(
        self,
        run_id: int,
        reducer_id: str,
        checkpoint_id: Optional[str] = None,
    ) -> Optional[JsonMap]:
        if checkpoint_id is None:
            checkpoint_id = self._latest_projection.get((run_id, reducer_id))
        if checkpoint_id is None:
            return None
        value = self.projections.get((run_id, reducer_id, checkpoint_id))
        return _copy_map(value)

    def put_snapshot(self, snapshot: RuntimeSnapshotRecord) -> None:
        self.snapshots.setdefault(snapshot.run_id, []).append(snapshot)

    def latest_snapshot(
        self,
        run_id: int,
        reducer_id: str,
        checkpoint_id: Optional[str] = None,
    ) -> Optional[RuntimeSnapshotRecord]:
        candidates = [
            item
            for item in self.snapshots.get(run_id, [])
            if item.reducer_id == reducer_id
            and (checkpoint_id is None or item.checkpoint_id == checkpoint_id)
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda item: item.event_seq)

    def put_context_version(self, record: RuntimeContextVersionRecord) -> None:
        self.context_versions.setdefault(record.run_id, []).append(record)

    def put_memory_record(self, record: RuntimeMemoryRecord) -> None:
        self.memory_records.setdefault(record.run_id, []).append(record)


class LangGraphStateAdapter:
    """Map LangGraph checkpoints into Statelet runtime records.

    Mapping:
    - `put()` appends a `Context` event for checkpoint metadata and a `State`
      event for channel values.
    - Channel values are stored as the `langgraph.state` reducer projection and
      as a snapshot so `get_tuple()` can restore the latest state without event
      replay.
    - Message/context/file/tool channels become context-version segments.
    - `memory` / `memories` / `facts` channels become memory-lineage records.
    """

    reducer_id = "langgraph.state"

    def __init__(self, store: RuntimeStateStore, *, actor_id: str = "langgraph") -> None:
        self.store = store
        self.actor_id = actor_id

    def put(
        self,
        config: Mapping[str, Any],
        checkpoint: Mapping[str, Any],
        metadata: Optional[Mapping[str, Any]] = None,
        new_versions: Optional[Mapping[str, Any]] = None,
    ) -> JsonMap:
        configurable = _configurable(config)
        thread_id = _required_str(configurable, "thread_id")
        checkpoint_id = _checkpoint_id(checkpoint, f"{_now_ms()}")
        parent_id = checkpoint.get("parent_checkpoint_id") or configurable.get(
            "checkpoint_id"
        )
        branch_id = int(configurable.get("branch_id", 0) or 0)
        scope = str(configurable.get("scope", thread_id))
        run_id = self.store.get_or_create_run_id(thread_id)

        normalized = self._normalize_checkpoint(
            checkpoint,
            checkpoint_id=checkpoint_id,
            parent_checkpoint_id=str(parent_id) if parent_id else None,
            new_versions=new_versions,
        )
        meta = _copy_map(metadata)
        meta.setdefault("framework", "langgraph")
        meta.setdefault("thread_id", thread_id)

        context_event = self.store.append_event(
            run_id,
            "Context",
            {
                "checkpoint_id": checkpoint_id,
                "parent_checkpoint_id": parent_id,
                "metadata": meta,
            },
            actor_id=self.actor_id,
            scope=scope,
            branch_id=branch_id,
        )
        state_event = self.store.append_event(
            run_id,
            "State",
            {
                "checkpoint_id": checkpoint_id,
                "parent_checkpoint_id": parent_id,
                "channel_values": normalized["channel_values"],
                "channel_versions": normalized["channel_versions"],
                "versions_seen": normalized["versions_seen"],
                "pending_sends": normalized["pending_sends"],
            },
            actor_id=self.actor_id,
            scope=scope,
            branch_id=branch_id,
            causal_parent_seq=context_event.seq,
        )

        state_payload = {
            "thread_id": thread_id,
            "checkpoint_id": checkpoint_id,
            "parent_checkpoint_id": parent_id,
            "checkpoint": normalized,
            "metadata": meta,
        }
        self.store.put_projection(run_id, self.reducer_id, checkpoint_id, state_payload)
        self.store.put_snapshot(
            RuntimeSnapshotRecord(
                run_id=run_id,
                checkpoint_id=checkpoint_id,
                reducer_id=self.reducer_id,
                event_seq=state_event.seq,
                state_hash=_state_hash(state_payload),
                payload=state_payload,
                branch_id=branch_id,
                scope=scope,
            )
        )
        self._write_context_segments(
            run_id,
            thread_id,
            checkpoint_id,
            state_event.seq,
            normalized,
            branch_id,
        )
        self._write_memory_records(run_id, checkpoint_id, state_event.seq, normalized)

        out = dict(config)
        out_configurable = dict(out.get("configurable", {}))
        out_configurable.update({"thread_id": thread_id, "checkpoint_id": checkpoint_id})
        out["configurable"] = out_configurable
        return out

    def get_tuple(self, config: Mapping[str, Any]) -> Optional[LangGraphCheckpointTuple]:
        configurable = _configurable(config)
        thread_id = _required_str(configurable, "thread_id")
        run_id = self.store.get_or_create_run_id(thread_id)
        checkpoint_id = configurable.get("checkpoint_id")
        projection = self.store.get_projection(
            run_id,
            self.reducer_id,
            str(checkpoint_id) if checkpoint_id else None,
        )
        if projection is None:
            snapshot = self.store.latest_snapshot(
                run_id,
                self.reducer_id,
                str(checkpoint_id) if checkpoint_id else None,
            )
            if snapshot is None:
                return None
            projection = _copy_map(snapshot.payload)

        restored_checkpoint = _copy_map(projection["checkpoint"])
        restored_id = restored_checkpoint["id"]
        parent_id = restored_checkpoint.get("parent_checkpoint_id")
        restored_config = {
            "configurable": {
                **configurable,
                "thread_id": thread_id,
                "checkpoint_id": restored_id,
            }
        }
        parent_config = (
            {
                "configurable": {
                    **configurable,
                    "thread_id": thread_id,
                    "checkpoint_id": parent_id,
                }
            }
            if parent_id
            else None
        )
        return LangGraphCheckpointTuple(
            config=restored_config,
            checkpoint=restored_checkpoint,
            metadata=_copy_map(projection.get("metadata")),
            parent_config=parent_config,
        )

    def export_state(self, config: Mapping[str, Any]) -> JsonMap:
        item = self.get_tuple(config)
        if item is None:
            raise KeyError("no checkpoint found for LangGraph config")
        return {
            "config": item.config,
            "checkpoint": item.checkpoint,
            "metadata": item.metadata,
            "parent_config": item.parent_config,
        }

    def import_state(
        self,
        config: Mapping[str, Any],
        state: Mapping[str, Any],
    ) -> JsonMap:
        checkpoint = state.get("checkpoint")
        if not isinstance(checkpoint, Mapping):
            raise ValueError("imported state must contain a checkpoint mapping")
        metadata = state.get("metadata")
        return self.put(
            config,
            checkpoint,
            metadata if isinstance(metadata, Mapping) else None,
        )

    def list_events(self, config: Mapping[str, Any]) -> List[RuntimeEventRecord]:
        configurable = _configurable(config)
        thread_id = _required_str(configurable, "thread_id")
        store = self.store
        if not isinstance(store, InMemoryRuntimeStateStore):
            raise TypeError("list_events is only available for InMemoryRuntimeStateStore")
        return list(store.events.get(store.get_or_create_run_id(thread_id), []))

    def _normalize_checkpoint(
        self,
        checkpoint: Mapping[str, Any],
        *,
        checkpoint_id: str,
        parent_checkpoint_id: Optional[str],
        new_versions: Optional[Mapping[str, Any]],
    ) -> JsonMap:
        channel_versions = _copy_map(checkpoint.get("channel_versions"))
        if new_versions:
            channel_versions.update(_copy_map(new_versions))
        return {
            "v": int(checkpoint.get("v", 1) or 1),
            "id": checkpoint_id,
            "ts": checkpoint.get("ts", _now_ms()),
            "parent_checkpoint_id": parent_checkpoint_id,
            "channel_values": _extract_channel_values(checkpoint),
            "channel_versions": channel_versions,
            "versions_seen": _copy_map(checkpoint.get("versions_seen")),
            "pending_sends": deepcopy(list(checkpoint.get("pending_sends", []))),
        }

    def _write_context_segments(
        self,
        run_id: int,
        thread_id: str,
        checkpoint_id: str,
        event_seq: int,
        checkpoint: Mapping[str, Any],
        branch_id: int,
    ) -> None:
        values = checkpoint["channel_values"]
        segments = {
            key: deepcopy(values[key])
            for key in ("messages", "context", "files", "tool_outputs", "memory_refs")
            if key in values
        }
        if not segments:
            return
        self.store.put_context_version(
            RuntimeContextVersionRecord(
                run_id=run_id,
                context_id=f"langgraph:{thread_id}",
                version=event_seq,
                checkpoint_id=checkpoint_id,
                event_seq=event_seq,
                segments=segments,
                branch_id=branch_id,
            )
        )

    def _write_memory_records(
        self,
        run_id: int,
        checkpoint_id: str,
        event_seq: int,
        checkpoint: Mapping[str, Any],
    ) -> None:
        values = checkpoint["channel_values"]
        for key in ("memory", "memories", "facts"):
            if key not in values:
                continue
            for memory_id, value, metadata in self._iter_memories(key, values[key]):
                self.store.put_memory_record(
                    RuntimeMemoryRecord(
                        run_id=run_id,
                        memory_id=memory_id,
                        value=deepcopy(value),
                        source_event_seq=event_seq,
                        checkpoint_id=checkpoint_id,
                        confidence=float(metadata.pop("confidence", 1.0)),
                        metadata=metadata,
                    )
                )

    def _iter_memories(self, key: str, raw: Any) -> Iterable[tuple[str, Any, JsonMap]]:
        if isinstance(raw, Mapping):
            if "id" in raw and ("value" in raw or "text" in raw):
                memory_id = str(raw["id"])
                value = raw.get("value", raw.get("text"))
                metadata = _copy_map(raw.get("metadata"))
                if "confidence" in raw:
                    metadata["confidence"] = raw["confidence"]
                yield memory_id, value, metadata
            else:
                for name, value in raw.items():
                    yield str(name), value, {"channel": key}
            return
        if isinstance(raw, list):
            for idx, item in enumerate(raw):
                if isinstance(item, Mapping):
                    memory_id = str(item.get("id", f"{key}:{idx}"))
                    value = item.get("value", item.get("text", item))
                    metadata = _copy_map(item.get("metadata"))
                    if "confidence" in item:
                        metadata["confidence"] = item["confidence"]
                    yield memory_id, value, metadata
                else:
                    yield f"{key}:{idx}", item, {"channel": key}
            return
        yield key, raw, {"channel": key}


__all__ = [
    "InMemoryRuntimeStateStore",
    "LangGraphCheckpointTuple",
    "LangGraphStateAdapter",
    "RuntimeContextVersionRecord",
    "RuntimeEventRecord",
    "RuntimeMemoryRecord",
    "RuntimeSnapshotRecord",
    "RuntimeStateStore",
]
