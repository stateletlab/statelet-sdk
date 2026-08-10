"""LangChain integration for Statelet.

Provides VectorStore, ChatMessageHistory, and GraphStore backed by a single
self-hosted Statelet engine (KV + Vector + Graph).

Usage::

    from langchain_statelet import StateletVectorStore

    vs = StateletVectorStore(embedding=my_embeddings, addr="localhost:9379")
    vs.add_texts(["hello world"])
    docs = vs.similarity_search("hello", k=3)
"""

from langchain_statelet.vectorstore import StateletVectorStore
from langchain_statelet.chat_history import StateletChatMessageHistory

__all__ = [
    "StateletVectorStore",
    "StateletChatMessageHistory",
]

try:
    from langchain_statelet.graph import StateletGraphStore  # noqa: F401

    __all__.append("StateletGraphStore")
except ImportError:
    pass  # langchain-community not installed

try:
    from langchain_statelet.memory import StateletMemory  # noqa: F401

    __all__.append("StateletMemory")
except ImportError:
    pass
