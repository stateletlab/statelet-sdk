"""Integration tests for StateletGraphStore.

Requires Statelet running on localhost:9379 and langchain-community installed.
"""

import uuid

import pytest

try:
    from langchain_community.graphs.graph_document import (
        GraphDocument,
        Node,
        Relationship,
    )
except ImportError:
    pytest.skip("langchain-community not installed", allow_module_level=True)

from langchain_core.documents import Document

from langchain_statelet import StateletGraphStore


@pytest.fixture
def graph():
    name = f"test_kg_{uuid.uuid4().hex[:8]}"
    g = StateletGraphStore(addr="localhost:9379", graph_name=name)
    yield g


def test_add_and_query(graph):
    source_doc = Document(page_content="Alice knows Bob")
    alice = Node(id="alice", type="Person", properties={"name": "Alice"})
    bob = Node(id="bob", type="Person", properties={"name": "Bob"})
    rel = Relationship(source=alice, target=bob, type="KNOWS")

    graph_doc = GraphDocument(nodes=[alice, bob], relationships=[rel], source=source_doc)
    graph.add_graph_documents([graph_doc], include_source=True)

    results = graph.query("*")
    assert len(results) >= 2

    # Check that both nodes are present
    ids = {r["id"] for r in results}
    assert "alice" in ids
    assert "bob" in ids


def test_schema(graph):
    alice = Node(id="a1", type="Person")
    company = Node(id="c1", type="Company")
    rel = Relationship(source=alice, target=company, type="WORKS_AT")
    graph_doc = GraphDocument(nodes=[alice, company], relationships=[rel], source=Document(page_content=""))
    graph.add_graph_documents([graph_doc])

    schema = graph.get_structured_schema
    assert "Person" in schema.get("node_types", [])
    assert "Company" in schema.get("node_types", [])
    assert "WORKS_AT" in schema.get("edge_types", [])
