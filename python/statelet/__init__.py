"""Statelet Python SDK — gRPC client for the Statelet distributed database."""

from statelet.client import (
    StateletClient,
    VectorSearchResult,
    VectorIndexConfig,
    RerankSpec,
    GraphSearchResult,
    GraphEdge,
    GraphQueryResult,
    TextGraphSearchResult,
    TextGraphSearchResponse,
    ResolvedConflict,
    ConflictVote,
    EntityClusterMember,
    EntityCluster,
    ResolveEntitiesResult,
    CheckpointStore,
    FileCheckpointStore,
    CommittedChange,
)
from statelet.memory import AgentMemory
from statelet.high_level import (
    Client,
    AsyncClient,
    Step,
    Edge,
    TraverseResult,
    CausalChain,
    BranchMeta,
    CasPutResult,
    WatchEvent,
    MemoryIngestResult,
    LeaseResult,
    LeaseLost,
)
from statelet.langgraph_adapter import (
    InMemoryRuntimeStateStore,
    LangGraphCheckpointTuple,
    LangGraphStateAdapter,
    RuntimeContextVersionRecord,
    RuntimeEventRecord,
    RuntimeMemoryRecord,
    RuntimeSnapshotRecord,
    RuntimeStateStore,
)

__all__ = [
    # High-level API (documented)
    "Client",
    "AsyncClient",
    "Step",
    "Edge",
    "TraverseResult",
    "CausalChain",
    "BranchMeta",
    "CasPutResult",
    "WatchEvent",
    "MemoryIngestResult",
    "LeaseResult",
    "LeaseLost",
    # Framework adapters
    "InMemoryRuntimeStateStore",
    "LangGraphCheckpointTuple",
    "LangGraphStateAdapter",
    "RuntimeContextVersionRecord",
    "RuntimeEventRecord",
    "RuntimeMemoryRecord",
    "RuntimeSnapshotRecord",
    "RuntimeStateStore",
    # Low-level client
    "StateletClient",
    "VectorSearchResult",
    "VectorIndexConfig",
    "RerankSpec",
    "GraphSearchResult",
    "GraphEdge",
    "GraphQueryResult",
    "TextGraphSearchResult",
    "TextGraphSearchResponse",
    "ResolvedConflict",
    "ConflictVote",
    "EntityClusterMember",
    "EntityCluster",
    "ResolveEntitiesResult",
    # Durable change-feed (CDC)
    "CheckpointStore",
    "FileCheckpointStore",
    "CommittedChange",
    # Agent memory
    "AgentMemory",
]
__version__ = "0.1.6"
