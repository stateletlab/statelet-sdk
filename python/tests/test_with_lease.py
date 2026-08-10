"""Unit tests for the fence-gated ``with_lease()`` ergonomics (#784).

These drive ``Client.with_lease`` / ``memory_ingest`` against a fake agent stub
(no live server), asserting that the held lease fence is auto-injected into every
ingest, that a lost lease surfaces as ``LeaseLost``, and that ``fence == 0``
(no active block) leaves the request unfenced.
"""

import sys
from pathlib import Path

# Make the SDK importable when run from the repo without installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest  # noqa: E402

from statelet import statelet_pb2 as pb  # noqa: E402
from statelet.high_level import Client, LeaseLost  # noqa: E402


class FakeAgent:
    """Records the requests it receives and replays scripted responses."""

    def __init__(self):
        self.ingests = []
        self.leased = []
        self.released = []
        # next MemoryIngest returns this (default: a clean Added).
        self.next_ingest_resp = pb.AgentMemoryIngestResponse(
            action=0, fact_id=1, committed=True
        )

    def Lease(self, req):
        self.leased.append(req)
        return pb.AgentLeaseResponse(acquired=True, holder=req.agent_id, fence=42)

    def Release(self, req):
        self.released.append(req)
        return pb.AgentReleaseResponse(released=True)

    def MemoryIngest(self, req):
        self.ingests.append(req)
        return self.next_ingest_resp


def _client_with(agent):
    c = Client.__new__(Client)  # bypass channel setup
    c._agent = agent
    return c


def test_with_lease_injects_fence_and_key():
    agent = FakeAgent()
    c = _client_with(agent)
    with c.with_lease(b"c:user:k", "agentA", 60_000) as lease:
        assert lease.acquired and lease.fence == 42
        res = c.memory_ingest("user", "a fact")
        assert res.action == "added"
    # The ingest carried the held fence + claim key automatically.
    assert len(agent.ingests) == 1
    assert agent.ingests[0].fence == 42
    assert agent.ingests[0].lease_key == b"c:user:k"
    # And the lease was released on block exit with the held fence.
    assert agent.released and agent.released[0].fence == 42


def test_with_lease_raises_lease_lost_on_fence_loss():
    agent = FakeAgent()
    agent.next_ingest_resp = pb.AgentMemoryIngestResponse(
        action=3, committed=False, fence_lost=True, conflict_fact_id=0
    )
    c = _client_with(agent)
    with pytest.raises(LeaseLost) as ei:
        with c.with_lease(b"c:user:k", "agentA", 60_000):
            c.memory_ingest("user", "stale write")
    assert ei.value.key == b"c:user:k"
    # The lease is still released on the way out (finally).
    assert agent.released


def test_data_conflict_inside_lease_is_not_lease_lost():
    agent = FakeAgent()
    agent.next_ingest_resp = pb.AgentMemoryIngestResponse(
        action=3, committed=False, fence_lost=False, conflict_fact_id=7
    )
    c = _client_with(agent)
    with c.with_lease(b"c:user:k", "agentA", 60_000):
        res = c.memory_ingest("user", "contended")
    # A data conflict surfaces as a result, NOT a LeaseLost exception.
    assert res.action == "conflict"
    assert not res.fence_lost
    assert res.conflict_fact_id == 7


def test_ingest_outside_lease_is_unfenced():
    agent = FakeAgent()
    c = _client_with(agent)
    c.memory_ingest("user", "plain fact")
    assert agent.ingests[0].fence == 0
    assert agent.ingests[0].lease_key == b""


def test_explicit_fence_overrides_held_lease():
    agent = FakeAgent()
    c = _client_with(agent)
    with c.with_lease(b"c:user:k", "agentA", 60_000):
        c.memory_ingest("user", "f", fence=99, lease_key=b"c:other:z")
    assert agent.ingests[0].fence == 99
    assert agent.ingests[0].lease_key == b"c:other:z"
