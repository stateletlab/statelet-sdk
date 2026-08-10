"""Statelet Memory for LangChain.

Wraps Statelet's AgentMemory as a LangChain BaseMemory for use in chains.

Usage::

    from langchain_statelet import StateletMemory
    from langchain.chains import ConversationChain

    memory = StateletMemory(embed_fn=my_embed_fn, dim=384)
    chain = ConversationChain(llm=llm, memory=memory)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from langchain_core.memory import BaseMemory
from statelet.memory import AgentMemory


class StateletMemory(BaseMemory):
    """LangChain memory backed by Statelet's AgentMemory (KV + Vector + Graph)."""

    memory_key: str = "history"
    input_key: Optional[str] = None
    output_key: Optional[str] = None
    return_messages: bool = False
    k: int = 5

    # Private attributes
    _agent_memory: AgentMemory = None  # type: ignore

    model_config = {"arbitrary_types_allowed": True}

    def __init__(
        self,
        addr: str = "localhost:9379",
        agent_id: str = "langchain",
        embed_fn: Any = None,
        dim: int = 0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._agent_memory = AgentMemory(
            addr=addr,
            agent_id=agent_id,
            embed_fn=embed_fn,
            dim=dim,
        )

    @property
    def memory_variables(self) -> List[str]:
        return [self.memory_key]

    def load_memory_variables(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Search relevant memories based on the latest input."""
        # Extract query from inputs
        query = ""
        if self.input_key and self.input_key in inputs:
            query = str(inputs[self.input_key])
        elif inputs:
            query = str(next(iter(inputs.values())))

        if not query:
            return {self.memory_key: ""}

        entries = self._agent_memory.recall(query, k=self.k)
        if not entries:
            return {self.memory_key: ""}

        text = "\n".join(f"- {e.text}" for e in entries)
        return {self.memory_key: text}

    def save_context(self, inputs: Dict[str, Any], outputs: Dict[str, str]) -> None:
        """Save the input/output pair as a memory observation."""
        input_val = ""
        if self.input_key and self.input_key in inputs:
            input_val = str(inputs[self.input_key])
        elif inputs:
            input_val = str(next(iter(inputs.values())))

        output_val = ""
        if self.output_key and self.output_key in outputs:
            output_val = str(outputs[self.output_key])
        elif outputs:
            output_val = str(next(iter(outputs.values())))

        if input_val:
            self._agent_memory.observe(
                f"User: {input_val}",
                metadata={"role": "user"},
            )
        if output_val:
            self._agent_memory.observe(
                f"AI: {output_val}",
                metadata={"role": "ai"},
            )

    def clear(self) -> None:
        """Clear is a no-op for Statelet memory (memories are persistent)."""
        pass
