# langchain-statelet

LangChain integration for [Statelet](https://github.com/stateletlab/statelet) — VectorStore, ChatHistory, and GraphStore backed by a single self-hosted engine.

## Install

```bash
pip install langchain-statelet
```

For GraphStore support:
```bash
pip install langchain-statelet[graph]
```

## Prerequisites

Statelet must be running locally:

```bash
curl -sL https://github.com/stateletlab/statelet/releases/download/v0.1.1/install.sh | bash
```

## Usage

### VectorStore

```python
from langchain_statelet import StateletVectorStore
from langchain_openai import OpenAIEmbeddings

vectorstore = StateletVectorStore(
    embedding=OpenAIEmbeddings(),
    addr="localhost:9379",
    index_name="my_docs",
)

# Add documents
vectorstore.add_texts(
    ["The cat sat on the mat", "The dog ran in the park"],
    metadatas=[{"source": "a"}, {"source": "b"}],
)

# Search
docs = vectorstore.similarity_search("cat", k=2)

# Use as retriever in a RAG chain
retriever = vectorstore.as_retriever()
```

### Chat Message History

```python
from langchain_statelet import StateletChatMessageHistory

history = StateletChatMessageHistory(session_id="user-123")

history.add_user_message("hello")
history.add_ai_message("hi there")

print(history.messages)  # [HumanMessage(...), AIMessage(...)]

history.clear()
```

### Graph Store

```python
from langchain_statelet import StateletGraphStore
from langchain_community.graphs.graph_document import GraphDocument, Node, Relationship
from langchain_core.documents import Document

graph = StateletGraphStore(addr="localhost:9379", graph_name="knowledge")

alice = Node(id="alice", type="Person", properties={"name": "Alice"})
bob = Node(id="bob", type="Person", properties={"name": "Bob"})
rel = Relationship(source=alice, target=bob, type="KNOWS")

graph.add_graph_documents([
    GraphDocument(nodes=[alice, bob], relationships=[rel], source=Document(page_content=""))
])

results = graph.query("*")
schema = graph.get_structured_schema
```

## Why Statelet?

Statelet is the only single backend that covers all three LangChain storage needs:

| Need | Pinecone | Neo4j | Redis | Statelet |
|------|----------|-------|-------|---------|
| VectorStore | Yes | No | Yes | Yes |
| GraphStore | No | Yes | No | Yes |
| ChatHistory | No | No | Yes | Yes |
| Self-hosted | No | Partial | Yes | Yes |
| Single engine | - | - | - | Yes |

Zero extra API keys. Apache-2.0. Self-hosted.
