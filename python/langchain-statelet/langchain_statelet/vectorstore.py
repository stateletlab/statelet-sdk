"""Statelet VectorStore for LangChain.

Stores document embeddings in Statelet's HNSW vector index and metadata in KV.

Usage::

    from langchain_statelet import StateletVectorStore
    from langchain_openai import OpenAIEmbeddings

    vs = StateletVectorStore(embedding=OpenAIEmbeddings(), addr="localhost:9379")
    vs.add_texts(["The cat sat on the mat"], metadatas=[{"source": "test"}])
    docs = vs.similarity_search("cat", k=2)
"""

from __future__ import annotations

import json
import time
from typing import Any, Iterable, List, Optional, Type

from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore
from statelet.client import StateletClient, VectorIndexConfig

_DEFAULT_INDEX = "langchain"
_KV_CF = 0


class StateletVectorStore(VectorStore):
    """LangChain VectorStore backed by Statelet's HNSW index + KV store."""

    def __init__(
        self,
        embedding: Embeddings,
        addr: str = "localhost:9379",
        index_name: str = _DEFAULT_INDEX,
        *,
        cf: int = _KV_CF,
        metric: str = "cosine",
        **kwargs: Any,
    ) -> None:
        self._embedding = embedding
        self._client = StateletClient(addr, cf=cf)
        self._index_name = index_name
        self._cf = cf
        self._metric = metric
        self._initialized = False

    @property
    def embeddings(self) -> Embeddings:
        return self._embedding

    def _ensure_index(self, dim: int) -> None:
        if self._initialized:
            return
        try:
            self._client.create_vector_index(
                self._index_name,
                VectorIndexConfig(dim=dim, metric=self._metric),
            )
        except Exception:
            pass  # already exists
        self._initialized = True

    def add_texts(
        self,
        texts: Iterable[str],
        metadatas: Optional[List[dict]] = None,
        *,
        ids: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> List[str]:
        """Embed texts and add to the vector store. Returns list of IDs."""
        text_list = list(texts)
        vectors = self._embedding.embed_documents(text_list)
        if vectors:
            self._ensure_index(len(vectors[0]))

        meta_list = metadatas or [{}] * len(text_list)
        result_ids: List[str] = []

        for i, (text, vec, meta) in enumerate(zip(text_list, vectors, meta_list)):
            if ids and i < len(ids):
                doc_id = int(ids[i]) if ids[i].isdigit() else abs(hash(ids[i])) % (2**53)
            else:
                doc_id = int(time.time() * 1_000_000) + i

            # Store vector
            self._client.vector_put(self._index_name, doc_id, vec)

            # Store document content + metadata in KV
            payload = {"text": text, "metadata": meta}
            kv_key = f"lc:{self._index_name}:{doc_id}".encode()
            self._client.put(kv_key, json.dumps(payload).encode())

            result_ids.append(str(doc_id))

        return result_ids

    def similarity_search(self, query: str, k: int = 4, **kwargs: Any) -> List[Document]:
        """Search for documents similar to the query string."""
        query_vec = self._embedding.embed_query(query)
        return self.similarity_search_by_vector(query_vec, k=k, **kwargs)

    def similarity_search_by_vector(
        self, embedding: List[float], k: int = 4, **kwargs: Any
    ) -> List[Document]:
        """Search for documents by embedding vector."""
        self._ensure_index(len(embedding))
        results = self._client.vector_search(self._index_name, embedding, k)

        docs: List[Document] = []
        for r in results:
            kv_key = f"lc:{self._index_name}:{r.id}".encode()
            raw = self._client.get(kv_key)
            if raw:
                payload = json.loads(raw)
                meta = payload.get("metadata", {})
                meta["_distance"] = r.distance
                meta["_id"] = str(r.id)
                docs.append(Document(page_content=payload.get("text", ""), metadata=meta))
        return docs

    def similarity_search_with_score(self, query: str, k: int = 4, **kwargs: Any) -> List[tuple]:
        """Return documents with their similarity scores."""
        query_vec = self._embedding.embed_query(query)
        self._ensure_index(len(query_vec))
        results = self._client.vector_search(self._index_name, query_vec, k)

        docs_with_scores: List[tuple] = []
        for r in results:
            kv_key = f"lc:{self._index_name}:{r.id}".encode()
            raw = self._client.get(kv_key)
            if raw:
                payload = json.loads(raw)
                meta = payload.get("metadata", {})
                meta["_id"] = str(r.id)
                doc = Document(page_content=payload.get("text", ""), metadata=meta)
                docs_with_scores.append((doc, r.distance))
        return docs_with_scores

    def delete(self, ids: Optional[List[str]] = None, **kwargs: Any) -> Optional[bool]:
        """Delete documents by IDs."""
        if not ids:
            return False
        for doc_id_str in ids:
            doc_id = int(doc_id_str)
            try:
                self._client.vector_delete(self._index_name, doc_id)
            except Exception:
                pass
            kv_key = f"lc:{self._index_name}:{doc_id}".encode()
            try:
                self._client.delete(kv_key)
            except Exception:
                pass
        return True

    @classmethod
    def from_texts(
        cls: Type["StateletVectorStore"],
        texts: list[str],
        embedding: Embeddings,
        metadatas: Optional[list[dict]] = None,
        *,
        ids: Optional[list[str]] = None,
        **kwargs: Any,
    ) -> "StateletVectorStore":
        """Create an StateletVectorStore from a list of texts."""
        store = cls(embedding=embedding, **kwargs)
        store.add_texts(texts, metadatas=metadatas, ids=ids)
        return store
