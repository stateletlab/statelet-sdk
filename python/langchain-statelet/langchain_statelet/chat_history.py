"""Statelet ChatMessageHistory for LangChain.

Stores conversation history in Statelet KV with prefix-based retrieval.

Usage::

    from langchain_statelet import StateletChatMessageHistory

    history = StateletChatMessageHistory(session_id="user-123")
    history.add_user_message("hello")
    history.add_ai_message("hi there")
    print(history.messages)
"""

from __future__ import annotations

import json
import time
from typing import List

from langchain_core.chat_history import BaseChatMessageHistory
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
)
from statelet.client import StateletClient

_KV_CF = 0


class StateletChatMessageHistory(BaseChatMessageHistory):
    """Chat message history stored in Statelet KV."""

    def __init__(
        self,
        session_id: str,
        addr: str = "localhost:9379",
        *,
        cf: int = _KV_CF,
    ) -> None:
        self._session_id = session_id
        self._client = StateletClient(addr, cf=cf)
        self._cf = cf
        self._seq = 0

    @property
    def _prefix(self) -> bytes:
        return f"chat:{self._session_id}:".encode()

    _MESSAGE_CLS = {
        "human": HumanMessage,
        "ai": AIMessage,
        "system": SystemMessage,
    }

    @property
    def messages(self) -> List[BaseMessage]:  # type: ignore[override]
        """Retrieve all messages for this session."""
        entries, _ = self._client.scan(self._prefix, limit=10000)
        msgs: List[BaseMessage] = []
        for key, _value in entries:
            full_value = self._client.get(key)
            if not full_value:
                continue
            try:
                data = json.loads(full_value)
                msg_type = data.get("type", "human")
                content = data.get("content", "")
                cls = self._MESSAGE_CLS.get(msg_type, HumanMessage)
                msgs.append(cls(content=content))
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
        return msgs

    def add_message(self, message: BaseMessage) -> None:
        """Add a message to the session history."""
        ts = int(time.time() * 1_000_000)
        key = f"chat:{self._session_id}:{ts:020d}".encode()
        value = json.dumps({"type": message.type, "content": message.content}).encode()
        self._client.put(key, value)

    def clear(self) -> None:
        """Delete all messages for this session."""
        self._client.delete_by_prefix(self._prefix)
