# Copyright 2024 Statelet Contributors
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

"""Unit tests for the vector-search result-grouping SDK surface (#1433).

These drive ``StateletClient.vector_search`` with the ``group_field`` /
``group_size`` / ``groups`` / ``group_overfetch`` kwargs against a fake gRPC
stub (no live server), asserting that the grouping fields are mapped onto the
request and that ``group_key`` is surfaced on the typed result dataclass.
"""

import sys
from pathlib import Path

# Make the SDK importable when run from the repo without installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from statelet import statelet_pb2 as pb  # noqa: E402
from statelet.client import StateletClient, VectorSearchResult  # noqa: E402


def _client(stub):
    """An ``StateletClient`` with its stub swapped for a fake (no channel)."""
    c = StateletClient.__new__(StateletClient)
    c._stub = stub
    c._text_graph_search_timeout_s = 5.0
    return c


class FakeStub:
    """Records the last request and replays a scripted search response."""

    def __init__(self, search_resp=None):
        self.last_search = None
        self._search_resp = search_resp or pb.VectorSearchResponse()

    def VectorSearch(self, req, timeout=None):  # noqa: N802 (gRPC method name)
        self.last_search = req
        return self._search_resp


def test_grouping_kwargs_map_onto_request():
    stub = FakeStub()
    client = _client(stub)
    client.vector_search(
        "idx",
        [1.0, 0.0, 0.0, 0.0],
        k=10,
        group_field="doc_id",
        group_size=2,
        groups=3,
        group_overfetch=8,
    )
    req = stub.last_search
    assert req.group_field == "doc_id"
    assert req.group_size == 2
    assert req.groups == 3
    assert req.group_overfetch == 8


def test_grouping_off_leaves_fields_default():
    stub = FakeStub()
    client = _client(stub)
    client.vector_search("idx", [1.0, 0.0, 0.0, 0.0], k=5)
    req = stub.last_search
    assert req.group_field == ""
    assert req.group_size == 0
    assert req.groups == 0
    assert req.group_overfetch == 0
    assert req.group_missing_as_own is False


def test_group_missing_as_own_maps_onto_request():
    """Phase 3 (#1435): the additive group_missing_as_own toggle round-trips."""
    stub = FakeStub()
    client = _client(stub)
    client.vector_search(
        "idx",
        [1.0, 0.0, 0.0, 0.0],
        k=10,
        group_field="doc_id",
        group_missing_as_own=True,
    )
    req = stub.last_search
    assert req.group_field == "doc_id"
    assert req.group_missing_as_own is True

    # Default leaves it off (drop behavior unchanged from Phase 1).
    client.vector_search("idx", [1.0, 0.0, 0.0, 0.0], k=10, group_field="doc_id")
    assert stub.last_search.group_missing_as_own is False


def test_group_key_surfaced_on_results():
    resp = pb.VectorSearchResponse(
        results=[
            pb.VectorSearchResult(id=1, distance=0.1, group_key="docA"),
            pb.VectorSearchResult(id=2, distance=0.2, group_key="docB"),
        ]
    )
    client = _client(FakeStub(search_resp=resp))
    out = client.vector_search(
        "idx", [1.0, 0.0, 0.0, 0.0], k=2, group_field="doc_id"
    )
    assert out == [
        VectorSearchResult(id=1, distance=out[0].distance, group_key="docA"),
        VectorSearchResult(id=2, distance=out[1].distance, group_key="docB"),
    ]
    assert [r.group_key for r in out] == ["docA", "docB"]


def test_group_key_defaults_empty_when_grouping_off():
    resp = pb.VectorSearchResponse(
        results=[pb.VectorSearchResult(id=1, distance=0.1)]
    )
    client = _client(FakeStub(search_resp=resp))
    out = client.vector_search("idx", [1.0, 0.0, 0.0, 0.0], k=1)
    assert out[0].group_key == ""
