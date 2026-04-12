from __future__ import annotations

from maxogram.domain import EventType, OutboxAction, Platform
from maxogram.services.dedup import (
    canonical_dedup_key,
    max_update_key,
    outbox_dedup_key,
    stable_json_hash,
)


def test_stable_json_hash_ignores_key_order():
    assert stable_json_hash({"b": 2, "a": 1}) == stable_json_hash({"a": 1, "b": 2})


def test_max_update_key_prefers_top_level_update_id():
    assert max_update_key({"update_id": 10}) == "10"


def test_max_update_key_uses_raw_fingerprint_without_update_id():
    raw = {
        "update_type": "message_created",
        "timestamp": 1_700_000_000_000,
        "message": {
            "recipient": {"chat_id": 100},
            "sender": {"user_id": 42},
            "body": {"mid": "m1", "seq": 7, "text": "hello"},
        },
    }

    assert max_update_key(raw) == stable_json_hash(raw)


def test_max_update_key_differs_for_create_and_edit_with_same_mid_seq_and_timestamp():
    created = {
        "update_type": "message_created",
        "timestamp": 1_700_000_000_000,
        "message": {
            "recipient": {"chat_id": 100},
            "sender": {"user_id": 42},
            "body": {"mid": "m1", "seq": 7, "text": "Test1"},
        },
    }
    edited = {
        "update_type": "message_edited",
        "timestamp": 1_700_000_000_000,
        "message": {
            "recipient": {"chat_id": 100},
            "sender": {"user_id": 42},
            "body": {"mid": "m1", "seq": 7, "text": "Test2"},
        },
    }

    assert max_update_key(created) != max_update_key(edited)


def test_dedup_keys_are_deterministic():
    canonical = canonical_dedup_key(
        Platform.TELEGRAM,
        "chat",
        "msg",
        EventType.MESSAGE_CREATED,
    )
    outbox = outbox_dedup_key(
        "bridge",
        Platform.TELEGRAM,
        "chat",
        "msg",
        Platform.MAX,
        OutboxAction.SEND,
    )
    assert canonical == "telegram:chat:msg:message.created"
    assert outbox == "bridge:telegram:chat:msg:max:send"


def test_outbox_dedup_key_changes_when_edit_version_changes():
    first = outbox_dedup_key(
        "bridge",
        Platform.MAX,
        "chat",
        "msg",
        Platform.TELEGRAM,
        OutboxAction.EDIT,
        7,
    )
    second = outbox_dedup_key(
        "bridge",
        Platform.MAX,
        "chat",
        "msg",
        Platform.TELEGRAM,
        OutboxAction.EDIT,
        8,
    )

    assert first != second
