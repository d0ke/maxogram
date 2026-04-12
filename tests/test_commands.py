from __future__ import annotations

from maxogram.services.commands import parse_command


def test_parse_bridge_command_with_bot_suffix():
    parsed = parse_command("/bridge@MaxogramBot confirm 123456")

    assert parsed is not None
    assert parsed.root == "/bridge"
    assert parsed.action == "confirm"
    assert parsed.args == "123456"


def test_parse_nick_command():
    parsed = parse_command("/nick set Alice Bob")

    assert parsed is not None
    assert parsed.root == "/nick"
    assert parsed.action == "set"
    assert parsed.args == "Alice Bob"


def test_non_command_returns_none():
    assert parse_command("hello") is None
