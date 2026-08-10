"""Unit tests for the conflict-as-data SDK surface (#786 / #810 / #783).

These drive ``StateletClient.resolve_conflict`` and the ``text_graph_search``
conflict kwargs against a fake gRPC stub (no live server), asserting that every
proto field is mapped onto the typed dataclasses and that the optional-field
guard does not raise on a stub that predates the Phase 3 search fields.
"""

import sys
from pathlib import Path

# Make the SDK importable when run from the repo without installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest  # noqa: E402

from statelet import statelet_pb2 as pb  # noqa: E402
from statelet.client import (  # noqa: E402
    StateletClient,
    ResolvedConflict,
    ConflictVote,
    TextGraphSearchResponse,
    _conflict_policy_enum,
)


def _client(stub):
    """An ``StateletClient`` with its stub swapped for a fake (no channel)."""
    c = StateletClient.__new__(StateletClient)
    c._stub = stub
    c._text_graph_search_timeout_s = 5.0
    return c


class FakeStub:
    """Records the last request and replays scripted conflict responses."""

    def __init__(self, *, search_resp=None, resolve_resp=None):
        self.last_resolve = None
        self.last_search = None
        self._resolve_resp = resolve_resp
        self._search_resp = search_resp or pb.TextGraphSearchResponse()

    def ResolveConflict(self, req, timeout=None):
        self.last_resolve = req
        return self._resolve_resp

    def TextGraphSearch(self, req, timeout=None):
        self.last_search = req
        return self._search_resp


# ── Stub-presence assertions (catch a forgotten `make proto`) ────────────────


def test_stubs_have_conflict_surface():
    assert hasattr(pb, "ResolveConflictRequest")
    assert hasattr(pb, "ResolveConflictResponse")
    assert hasattr(pb, "ConflictVote")
    assert hasattr(pb, "ConflictPolicy")
    from statelet import statelet_pb2_grpc as g
    assert hasattr(g.StateletStub, "__init__")
    search_fields = {f.name for f in pb.TextGraphSearchRequest().DESCRIPTOR.fields}
    assert {"conflict_policy", "include_dissent"} <= search_fields
    resp_fields = {f.name for f in pb.TextGraphSearchResponse().DESCRIPTOR.fields}
    assert "conflict_resolutions_json" in resp_fields


# ── Policy-name → search enum mapping ────────────────────────────────────────


def test_conflict_policy_enum_mapping():
    assert _conflict_policy_enum("") == pb.CONFLICT_POLICY_UNSPECIFIED
    assert _conflict_policy_enum("recency") == pb.RECENCY
    assert _conflict_policy_enum("RECENCY") == pb.RECENCY
    assert _conflict_policy_enum("trust") == pb.TRUST
    assert _conflict_policy_enum("confidence") == pb.CONFIDENCE
    # quorum / unknown fall back to UNSPECIFIED (search has no quorum value).
    assert _conflict_policy_enum("quorum") == pb.CONFLICT_POLICY_UNSPECIFIED
    assert _conflict_policy_enum("bogus") == pb.CONFLICT_POLICY_UNSPECIFIED


# ── resolve_conflict field mapping ───────────────────────────────────────────


def test_resolve_conflict_maps_all_fields():
    resp = pb.ResolveConflictResponse(
        found=True,
        authoritative=42,
        dissenting=[7, 9],
        policy="trust",
        score=0.9,
        rationale="trust(author=admin)=0.90",
        truncated=True,
        votes=[
            pb.ConflictVote(value="Globex", weight=0.9, supporters=[42]),
            pb.ConflictVote(value="Acme", weight=0.6, supporters=[7]),
        ],
    )
    stub = FakeStub(resolve_resp=resp)
    c = _client(stub)
    rc = c.resolve_conflict("g", 7, policy="trust", as_of=123)

    assert isinstance(rc, ResolvedConflict)
    assert rc.found is True
    assert rc.authoritative == 42
    assert rc.dissenting == [7, 9]
    assert rc.policy == "trust"
    assert rc.score == pytest.approx(0.9)
    assert rc.rationale == "trust(author=admin)=0.90"
    assert rc.truncated is True
    assert len(rc.votes) == 2
    assert isinstance(rc.votes[0], ConflictVote)
    assert rc.votes[0].value == "Globex"
    assert rc.votes[0].weight == pytest.approx(0.9)
    assert rc.votes[0].supporters == [42]

    # Request fields propagate verbatim.
    assert stub.last_resolve.graph_name == "g"
    assert stub.last_resolve.node_id == 7
    assert stub.last_resolve.policy == "trust"
    assert stub.last_resolve.as_of == 123


def test_resolve_conflict_not_found():
    stub = FakeStub(resolve_resp=pb.ResolveConflictResponse(found=False))
    rc = _client(stub).resolve_conflict("g", 99)
    assert rc.found is False
    assert rc.authoritative == 0
    assert rc.dissenting == []
    assert rc.votes == []


# ── text_graph_search conflict kwargs ────────────────────────────────────────


def test_text_graph_search_sets_conflict_kwargs():
    resp = pb.TextGraphSearchResponse(
        results=[pb.TextGraphSearchResult(node_id=1, distance=0.1)],
        conflict_resolutions_json='[{"winner":1,"losers":[2]}]',
    )
    stub = FakeStub(search_resp=resp)
    c = _client(stub)
    out = c.text_graph_search("g", "q", conflict_policy="trust", include_dissent=True)

    assert stub.last_search.conflict_policy == pb.TRUST
    assert stub.last_search.include_dissent is True
    assert isinstance(out, TextGraphSearchResponse)
    assert out.conflict_resolutions_json == '[{"winner":1,"losers":[2]}]'
    assert out.results[0].node_id == 1


def test_text_graph_search_no_conflict_returns_none_json():
    stub = FakeStub(search_resp=pb.TextGraphSearchResponse())
    out = _client(stub).text_graph_search("g", "q")
    # Default (empty) conflict policy: no JSON, kwarg left at enum default.
    assert out.conflict_resolutions_json is None
    assert stub.last_search.conflict_policy == pb.CONFLICT_POLICY_UNSPECIFIED
    assert stub.last_search.include_dissent is False


def test_text_graph_search_guard_tolerates_old_request_message():
    """The optional-field guard must not raise when the request message lacks the
    conflict fields (a stub built from a pre-#783 proto). We simulate that with a
    request class whose constructor rejects the new kwargs."""

    class OldRequest:
        def __init__(self, **kw):
            for bad in ("conflict_policy", "include_dissent", "fact_k", "chunk_k"):
                if bad in kw:
                    raise ValueError(f"unknown field {bad}")
            self.__dict__.update(kw)

    import statelet.client as client_mod

    real_req = client_mod.pb.TextGraphSearchRequest
    real_search = client_mod.pb.TextGraphSearchRequest
    try:
        client_mod.pb.TextGraphSearchRequest = OldRequest
        stub = FakeStub(search_resp=pb.TextGraphSearchResponse())
        out = _client(stub).text_graph_search(
            "g", "q", conflict_policy="trust", include_dissent=True
        )
        # Fell back to the old request — call still succeeds, no conflict json.
        assert isinstance(out, TextGraphSearchResponse)
        assert not hasattr(stub.last_search, "conflict_policy")
    finally:
        client_mod.pb.TextGraphSearchRequest = real_req
        assert real_search is real_req
