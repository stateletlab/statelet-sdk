"""Unit tests for the durable change-feed consumer (CDC Phase 5b, #823).

These drive ``StateletClient.subscribe_committed`` and ``FileCheckpointStore``
against a fake ``Statelet`` stub (no live server). They assert:

  * ``FileCheckpointStore`` round-trips and writes atomically (a crash between
    write and replace leaves the prior valid offset intact).
  * start offset resolves to ``checkpoint+1`` when a checkpoint exists, else
    ``from_offset``.
  * ``change`` items are yielded and (auto_commit) committed *after* yield.
  * ``heartbeat`` advances + commits the high-watermark without yielding.
  * a ``compacted`` notice triggers a bootstrap ``scan`` that emits synthetic
    snapshot ``put`` changes, commits ``snapshot_offset``, then resumes at
    ``snapshot_offset + 1``.
  * an old-server ``snapshot_offset == 0`` resumes at ``earliest_offset``.
  * a ``grpc.RpcError`` mid-stream reconnects from ``last_offset + 1``.
"""

import os
import sys
from pathlib import Path

# Make the SDK importable when run from the repo without installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import grpc  # noqa: E402
import pytest  # noqa: E402

from statelet import statelet_pb2 as pb  # noqa: E402
from statelet.client import (  # noqa: E402
    StateletClient,
    CheckpointStore,
    CommittedChange,
    FileCheckpointStore,
)


# ── fakes ────────────────────────────────────────────────────────────────────


def _change_item(offset, key, op="put", value=b"", cf=0, term=1, seq=0):
    return pb.CommittedFeedItem(
        change=pb.CommittedChangeProto(
            offset=offset, term=term, seq_in_entry=seq,
            cf=cf, key=key, op=op, value=value,
        )
    )


def _heartbeat_item(hw):
    return pb.CommittedFeedItem(heartbeat=hw)


def _compacted_item(earliest, snapshot):
    return pb.CommittedFeedItem(
        compacted=pb.CompactedNotice(
            earliest_offset=earliest, snapshot_offset=snapshot
        )
    )


class _RpcError(grpc.RpcError):
    pass


class FakeStub:
    """Replays a scripted list of streams; records SubscribeCommitted requests.

    ``streams`` is a list of "stream scripts"; each script is a list of items
    where an item is either a ``CommittedFeedItem`` or an exception instance to
    raise while iterating. ``scan_pages`` is a list of ``(entries, next_cursor)``
    tuples returned by successive ``Scan`` calls.
    """

    def __init__(self, streams, scan_pages=None):
        self._streams = list(streams)
        self.requests = []
        self._scan_pages = list(scan_pages or [])
        self.scan_calls = []

    def SubscribeCommitted(self, request):
        self.requests.append(request)
        if not self._streams:
            # Nothing more scripted: emit an RpcError so a consumer that keeps
            # looping doesn't spin forever in a test (the test breaks out first).
            raise _RpcError()
        script = self._streams.pop(0)

        def _gen():
            for item in script:
                if isinstance(item, BaseException):
                    raise item
                yield item

        return _gen()

    def Scan(self, request, timeout=None):
        self.scan_calls.append(request)
        if self._scan_pages:
            entries, next_cursor = self._scan_pages.pop(0)
        else:
            entries, next_cursor = [], b""
        return pb.ScanResponse(
            entries=[pb.ScanEntry(key=k, value=v) for k, v in entries],
            next_cursor=next_cursor or b"",
        )


def _client_with(stub):
    client = StateletClient.__new__(StateletClient)
    client._stub = stub
    client._cf = 0
    client._kv_timeout_s = 10.0
    return client


def _take(it, n):
    out = []
    for _ in range(n):
        out.append(next(it))
    return out


# ── FileCheckpointStore ──────────────────────────────────────────────────────


def test_file_checkpoint_roundtrip(tmp_path):
    p = str(tmp_path / "ck.json")
    store = FileCheckpointStore(p)
    assert store.load("sub-a") is None
    store.commit("sub-a", 7)
    store.commit("sub-b", 11)
    assert store.load("sub-a") == 7
    assert store.load("sub-b") == 11
    # Re-open from disk: state survives.
    store2 = FileCheckpointStore(p)
    assert store2.load("sub-a") == 7
    assert store2.load("sub-b") == 11


def test_file_checkpoint_atomic_no_tmp_leftover(tmp_path):
    p = str(tmp_path / "ck.json")
    store = FileCheckpointStore(p)
    store.commit("s", 3)
    store.commit("s", 4)
    # Only the real file remains; no leftover ".ckpt-*.tmp" temp files.
    leftovers = [f for f in os.listdir(tmp_path) if f.startswith(".ckpt-")]
    assert leftovers == []
    assert FileCheckpointStore(p).load("s") == 4


def test_file_checkpoint_crash_before_replace_keeps_prior(tmp_path, monkeypatch):
    p = str(tmp_path / "ck.json")
    store = FileCheckpointStore(p)
    store.commit("s", 5)

    # Simulate a crash *between* the temp write and the os.replace.
    def boom(_src, _dst):
        raise OSError("simulated crash before replace")

    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError):
        store.commit("s", 6)
    monkeypatch.undo()

    # The file on disk still holds the prior valid offset (5), not a torn 6.
    assert FileCheckpointStore(p).load("s") == 5
    # And the failed commit left no temp file behind.
    leftovers = [f for f in os.listdir(tmp_path) if f.startswith(".ckpt-")]
    assert leftovers == []


def test_file_checkpoint_corrupt_file_is_treated_as_empty(tmp_path):
    p = tmp_path / "ck.json"
    p.write_text("{ this is not json")
    store = FileCheckpointStore(str(p))
    assert store.load("s") is None


# ── start-offset resolution ──────────────────────────────────────────────────


def test_start_from_checkpoint_plus_one(tmp_path):
    ck = FileCheckpointStore(str(tmp_path / "ck.json"))
    ck.commit("sub", 41)
    stub = FakeStub(streams=[[_change_item(42, b"k")]])
    client = _client_with(stub)
    it = client.subscribe_committed(
        from_offset=0, subscription_id="sub", checkpoint=ck
    )
    _take(it, 1)
    assert stub.requests[0].from_offset == 42  # checkpoint(41) + 1


def test_start_from_offset_when_no_checkpoint(tmp_path):
    ck = FileCheckpointStore(str(tmp_path / "ck.json"))
    stub = FakeStub(streams=[[_change_item(100, b"k")]])
    client = _client_with(stub)
    it = client.subscribe_committed(
        from_offset=100, subscription_id="sub", checkpoint=ck
    )
    _take(it, 1)
    assert stub.requests[0].from_offset == 100


# ── change delivery + auto-commit ────────────────────────────────────────────


def test_change_yielded_and_committed_after_yield(tmp_path):
    ck = FileCheckpointStore(str(tmp_path / "ck.json"))
    stub = FakeStub(streams=[[_change_item(5, b"a", value=b"v")]])
    client = _client_with(stub)
    it = client.subscribe_committed(subscription_id="sub", checkpoint=ck)
    change = next(it)
    assert isinstance(change, CommittedChange)
    assert (change.offset, change.key, change.op, change.value) == (5, b"a", "put", b"v")
    assert change.is_snapshot is False
    # Commit happens on the *next* iteration step (after the body runs).
    assert ck.load("sub") is None
    # Advance past it (next stream raises -> consumer would block on backoff,
    # so just assert the commit landed by stepping the generator once more in a
    # way that triggers the post-yield commit): pull the second change.
    stub._streams = [[_change_item(6, b"b")]]
    next(it)
    assert ck.load("sub") == 5


def test_no_commit_when_auto_commit_false(tmp_path):
    ck = FileCheckpointStore(str(tmp_path / "ck.json"))
    stub = FakeStub(streams=[[_change_item(5, b"a"), _change_item(6, b"b")]])
    client = _client_with(stub)
    it = client.subscribe_committed(
        subscription_id="sub", checkpoint=ck, auto_commit=False
    )
    _take(it, 2)
    assert ck.load("sub") is None  # consumer must commit manually


# ── heartbeat ────────────────────────────────────────────────────────────────


def test_heartbeat_advances_and_commits_without_yield(tmp_path):
    ck = FileCheckpointStore(str(tmp_path / "ck.json"))
    stub = FakeStub(streams=[[_heartbeat_item(50), _change_item(51, b"k")]])
    client = _client_with(stub)
    it = client.subscribe_committed(subscription_id="sub", checkpoint=ck)
    change = next(it)  # heartbeat is consumed silently; first yield is the change
    assert change.offset == 51
    # Heartbeat committed its watermark before the change was yielded.
    assert ck.load("sub") == 50


# ── compaction bootstrap ─────────────────────────────────────────────────────


def test_compacted_triggers_bootstrap_scan(tmp_path):
    ck = FileCheckpointStore(str(tmp_path / "ck.json"))
    # Stream 1: compacted notice. Then a paged scan rebuilds 2 keys. Stream 2:
    # live tail resumes after the snapshot offset.
    stub = FakeStub(
        streams=[
            [_compacted_item(earliest=10, snapshot=20)],
            [_change_item(21, b"live")],
        ],
        scan_pages=[
            ([(b"k1", b"v1")], b"cursor1"),
            ([(b"k2", b"v2")], b""),
        ],
    )
    client = _client_with(stub)
    it = client.subscribe_committed(
        subscription_id="sub", checkpoint=ck, key_prefix=b"k"
    )
    snap1 = next(it)
    snap2 = next(it)
    assert snap1.is_snapshot and snap1.op == "put" and snap1.offset == 20
    assert (snap1.key, snap1.value) == (b"k1", b"v1")
    assert snap2.is_snapshot and snap2.offset == 20
    assert (snap2.key, snap2.value) == (b"k2", b"v2")
    # Resume tail strictly after the snapshot watermark. Stepping past the last
    # snapshot item runs the post-bootstrap commit(snapshot_offset) and then
    # reconnects from snapshot_offset+1.
    live = next(it)
    assert live.offset == 21 and live.is_snapshot is False
    assert ck.load("sub") == 20  # snapshot_offset committed before the resume
    assert stub.requests[1].from_offset == 21


def test_compacted_old_server_resumes_at_earliest(tmp_path):
    ck = FileCheckpointStore(str(tmp_path / "ck.json"))
    stub = FakeStub(
        streams=[
            [_compacted_item(earliest=99, snapshot=0)],
            [_change_item(99, b"k")],
        ]
    )
    client = _client_with(stub)
    it = client.subscribe_committed(subscription_id="sub", checkpoint=ck)
    change = next(it)
    assert change.offset == 99
    # No bootstrap scan on old-server path.
    assert stub.scan_calls == []
    assert stub.requests[1].from_offset == 99  # earliest_offset


# ── reconnect ────────────────────────────────────────────────────────────────


def test_rpc_error_reconnects_from_last_plus_one(tmp_path, monkeypatch):
    import statelet.client as client_mod

    # Avoid real sleeps during backoff.
    monkeypatch.setattr(client_mod.time, "sleep", lambda _s: None)
    ck = FileCheckpointStore(str(tmp_path / "ck.json"))
    stub = FakeStub(
        streams=[
            [_change_item(5, b"a"), _RpcError()],
            [_change_item(6, b"b")],
        ]
    )
    client = _client_with(stub)
    it = client.subscribe_committed(subscription_id="sub", checkpoint=ck)
    first = next(it)   # offset 5, then the stream raises mid-iteration
    second = next(it)  # reconnect, offset 6
    assert first.offset == 5 and second.offset == 6
    # Reconnect resumed strictly after the last delivered offset.
    assert stub.requests[1].from_offset == 6


# ── protocol conformance ─────────────────────────────────────────────────────


def test_file_checkpoint_store_is_a_checkpoint_store(tmp_path):
    store = FileCheckpointStore(str(tmp_path / "ck.json"))
    assert isinstance(store, CheckpointStore)
