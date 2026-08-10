"""Integration tests for StateletChatMessageHistory.

Requires Statelet running on localhost:9379.
"""

import uuid

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from langchain_statelet import StateletChatMessageHistory


@pytest.fixture
def history():
    session_id = f"test-{uuid.uuid4().hex[:8]}"
    h = StateletChatMessageHistory(session_id=session_id, addr="localhost:9379")
    yield h
    h.clear()


def test_add_and_retrieve(history):
    history.add_user_message("hello")
    history.add_ai_message("hi there")

    msgs = history.messages
    assert len(msgs) == 2
    assert isinstance(msgs[0], HumanMessage)
    assert isinstance(msgs[1], AIMessage)
    assert msgs[0].content == "hello"
    assert msgs[1].content == "hi there"


def test_clear(history):
    history.add_user_message("to be cleared")
    assert len(history.messages) >= 1

    history.clear()
    assert len(history.messages) == 0


def test_multiple_messages(history):
    for i in range(5):
        history.add_user_message(f"message {i}")

    msgs = history.messages
    assert len(msgs) == 5
