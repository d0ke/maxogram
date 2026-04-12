from __future__ import annotations

import hashlib
import json
from typing import Any

from maxogram.domain import EventType, OutboxAction, Platform


def stable_json_hash(value: dict[str, Any]) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def max_update_key(raw: dict[str, Any]) -> str:
    if raw.get("update_id") is not None:
        return str(raw["update_id"])
    return stable_json_hash(raw)


def canonical_dedup_key(
    platform: Platform,
    chat_id: str,
    message_id: str,
    event_type: EventType,
    version: str | int | None = None,
) -> str:
    base = f"{platform.value}:{chat_id}:{message_id}:{event_type.value}"
    return f"{base}:{version}" if version is not None else base


def outbox_dedup_key(
    bridge_id: object,
    src_platform: Platform,
    src_chat_id: str,
    src_message_id: str,
    dst_platform: Platform,
    action: OutboxAction,
    version: str | int | None = None,
) -> str:
    parts = [
        str(bridge_id),
        src_platform.value,
        src_chat_id,
        src_message_id,
        dst_platform.value,
        action.value,
    ]
    if version is not None:
        parts.append(str(version))
    return ":".join(parts)


def partition_key(
    bridge_id: object,
    src_platform: Platform,
    dst_platform: Platform,
) -> str:
    return f"{bridge_id}:{src_platform.value}_to_{dst_platform.value}"
