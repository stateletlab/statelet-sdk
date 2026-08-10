"""The management address a Client derives when none is supplied."""

import pytest

from statelet.high_level import _default_mgmt_addr


@pytest.mark.parametrize(
    "addr, expected",
    [
        # the default pairing the gateway actually serves
        ("127.0.0.1:9379", "127.0.0.1:9380"),
        ("localhost:9379", "localhost:9380"),
        # any other port has to follow the same rule. The previous
        # implementation rewrote the literal ":9379" and so left these
        # untouched, pointing login's HTTP request at the gRPC listener.
        ("127.0.0.1:19379", "127.0.0.1:19380"),
        ("gw.internal:1234", "gw.internal:1235"),
        # a port that merely contains 9379 must not be mistaken for it
        ("127.0.0.1:29379", "127.0.0.1:29380"),
        # IPv6 literals keep their brackets
        ("[::1]:9379", "[::1]:9380"),
        # nothing to derive from; hand it back rather than guess
        ("gateway.internal", "gateway.internal"),
    ],
)
def test_default_mgmt_addr(addr, expected):
    assert _default_mgmt_addr(addr) == expected
