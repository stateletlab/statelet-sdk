"""Integration tests for StateletVectorStore.

Requires Statelet running on localhost:9379.
"""

import pytest
from langchain_core.documents import Document

from langchain_statelet import StateletVectorStore


class FakeEmbeddings:
    """Deterministic embeddings for testing."""

    def embed_documents(self, texts):
        return [[float(ord(c)) / 200.0 for c in t[:8].ljust(8)] for t in texts]

    def embed_query(self, text):
        return [float(ord(c)) / 200.0 for c in text[:8].ljust(8)]


@pytest.fixture
def store():
    s = StateletVectorStore(
        embedding=FakeEmbeddings(),
        addr="localhost:9379",
        index_name="test_langchain_vs",
    )
    yield s


def test_add_and_search(store):
    ids = store.add_texts(
        ["the cat sat on the mat", "the dog ran in the park"],
        metadatas=[{"source": "a"}, {"source": "b"}],
    )
    assert len(ids) == 2

    results = store.similarity_search("the cat", k=2)
    assert len(results) > 0
    assert isinstance(results[0], Document)
    assert results[0].page_content != ""


def test_search_with_score(store):
    store.add_texts(["alpha beta gamma"])
    results = store.similarity_search_with_score("alpha", k=1)
    assert len(results) > 0
    doc, score = results[0]
    assert isinstance(doc, Document)
    assert isinstance(score, float)


def test_delete(store):
    ids = store.add_texts(["to be deleted"])
    assert store.delete(ids) is True


def test_from_texts():
    vs = StateletVectorStore.from_texts(
        ["hello", "world"],
        embedding=FakeEmbeddings(),
        addr="localhost:9379",
        index_name="test_langchain_from_texts",
    )
    results = vs.similarity_search("hello", k=1)
    assert len(results) > 0
