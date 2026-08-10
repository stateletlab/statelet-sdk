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

"""Unit tests for the openCypher ``GraphQuery`` SDK surface.

These drive ``StateletClient.graph_query`` against a fake gRPC stub (no live
server), asserting that every request field reaches the wire and that each
``GraphQueryValue`` union kind decodes to the right Python value.
"""

import sys
from pathlib import Path

# Make the SDK importable when run from the repo without installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest  # noqa: E402

from statelet import statelet_pb2 as pb  # noqa: E402
from statelet.client import StateletClient, GraphQueryResult  # noqa: E402


def _client(stub):
    """A ``StateletClient`` with its stub swapped for a fake (no channel)."""
    c = StateletClient.__new__(StateletClient)
    c._stub = stub
    c._text_graph_search_timeout_s = 5.0
    return c


class FakeStub:
    """Records the last request and replays one scripted response."""

    def __init__(self, resp=None):
        self.last_request = None
        self.last_timeout = None
        self._resp = resp if resp is not None else pb.GraphQueryResponse()

    def GraphQuery(self, req, timeout=None):
        self.last_request = req
        self.last_timeout = timeout
        return self._resp


def test_stubs_have_graph_query_surface():
    """Catches a forgotten ``make proto`` after a proto change."""
    assert hasattr(pb, "GraphQueryRequest")
    assert hasattr(pb, "GraphQueryResponse")
    assert hasattr(pb, "GraphQueryRow")
    assert hasattr(pb, "GraphQueryValue")
    from statelet import statelet_pb2_grpc as g
    assert hasattr(g.StateletStub, "__init__")
    req_fields = {f.name for f in pb.GraphQueryRequest().DESCRIPTOR.fields}
    assert {"graph_name", "cypher", "max_rows", "as_of", "tx_as_of"} <= req_fields


def test_graph_query_sends_every_request_field():
    stub = FakeStub()
    c = _client(stub)
    c.graph_query(
        "MATCH (a)-[:knows]->(b) RETURN b LIMIT 10",
        graph_name="g",
        max_rows=25,
        as_of=1737000000000,
        tx_as_of=1737000000001,
        timeout_s=2.5,
    )
    req = stub.last_request
    assert req.graph_name == "g"
    assert req.cypher == "MATCH (a)-[:knows]->(b) RETURN b LIMIT 10"
    assert req.max_rows == 25
    assert req.as_of == 1737000000000
    assert req.tx_as_of == 1737000000001
    assert stub.last_timeout == pytest.approx(2.5)


def test_graph_query_defaults_leave_server_policy_alone():
    """An empty graph name / zero temporal fields mean "let the gateway decide"."""
    stub = FakeStub()
    c = _client(stub)
    c.graph_query("MATCH (n) RETURN n")
    req = stub.last_request
    assert req.graph_name == ""
    assert req.max_rows == 0
    assert req.as_of == 0
    assert req.tx_as_of == 0
    # No explicit timeout falls back to the client's graph-search timeout.
    assert stub.last_timeout == pytest.approx(5.0)


def test_graph_query_decodes_every_value_kind():
    V = pb.GraphQueryValue
    resp = pb.GraphQueryResponse(
        columns=["nul", "i", "d", "s", "b", "j"],
        rows=[
            pb.GraphQueryRow(
                values=[
                    V(kind=V.NULL),
                    V(kind=V.INT, int_value=42),
                    V(kind=V.DOUBLE, dbl_value=0.5),
                    V(kind=V.STRING, str_value="knows"),
                    V(kind=V.BOOL, bool_value=True),
                    V(kind=V.JSON, json_value=b'{"name":"ada"}'),
                ]
            )
        ],
        warnings=["label scan truncated at the frontier cap"],
    )
    result = _client(FakeStub(resp)).graph_query("MATCH (n) RETURN n")

    assert isinstance(result, GraphQueryResult)
    assert result.columns == ["nul", "i", "d", "s", "b", "j"]
    assert result.warnings == ["label scan truncated at the frontier cap"]
    assert result.rows == [[None, 42, 0.5, "knows", True, {"name": "ada"}]]
    assert result.dicts() == [
        {"nul": None, "i": 42, "d": 0.5, "s": "knows", "b": True,
         "j": {"name": "ada"}}
    ]


def test_graph_query_keeps_unparseable_json_as_bytes():
    """A non-JSON blob is surfaced raw rather than dropped."""
    V = pb.GraphQueryValue
    resp = pb.GraphQueryResponse(
        columns=["n"],
        rows=[pb.GraphQueryRow(values=[V(kind=V.JSON, json_value=b"\xff\x00")])],
    )
    result = _client(FakeStub(resp)).graph_query("MATCH (n) RETURN n")
    assert result.rows == [[b"\xff\x00"]]


def test_graph_query_empty_result_is_not_an_error():
    result = _client(FakeStub()).graph_query("MATCH (n) RETURN n")
    assert result.columns == []
    assert result.rows == []
    assert result.warnings == []
    assert result.dicts() == []
