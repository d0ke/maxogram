from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

from maxogram.db.models import MessageMapping
from maxogram.db.repositories import Repository
from maxogram.domain import EventType, Platform, UserIdentity
from maxogram.services.dedup import stable_json_hash
from maxogram.services.normalization import NormalizedUpdate, normalize_update
from maxogram.workers.normalizer import NormalizerWorker


class FakeRepository(Repository):
    def __init__(
        self,
        *,
        source_mapping: MessageMapping | None = None,
        destination_mapping: MessageMapping | None = None,
    ) -> None:
        self.source_mapping = source_mapping
        self.destination_mapping = destination_mapping
        self.source_calls: list[tuple[uuid.UUID, Platform, str, str]] = []
        self.destination_calls: list[tuple[uuid.UUID, Platform, str, str]] = []

    async def get_alias(
        self,
        bridge_id: uuid.UUID,
        platform: Platform,
        user_id: str,
    ) -> str | None:
        _ = bridge_id, platform, user_id
        return None

    async def find_mapping_by_source(
        self,
        bridge_id: uuid.UUID,
        src_platform: Platform,
        src_chat_id: str,
        src_message_id: str,
    ) -> MessageMapping | None:
        self.source_calls.append(
            (bridge_id, src_platform, src_chat_id, src_message_id)
        )
        return self.source_mapping

    async def find_mapping_by_destination(
        self,
        bridge_id: uuid.UUID,
        dst_platform: Platform,
        dst_chat_id: str,
        dst_message_id: str,
    ) -> MessageMapping | None:
        self.destination_calls.append(
            (bridge_id, dst_platform, dst_chat_id, dst_message_id)
        )
        return self.destination_mapping

    async def list_destination_message_ids(
        self,
        bridge_id: uuid.UUID,
        src_platform: Platform,
        src_chat_id: str,
        src_message_id: str,
    ) -> list[str]:
        _ = bridge_id, src_platform, src_chat_id, src_message_id
        if self.source_mapping is None:
            return []
        return [self.source_mapping.dst_message_id]


class ProcessingRepository(FakeRepository):
    def __init__(
        self,
        *,
        source_mapping: MessageMapping | None = None,
        destination_mapping: MessageMapping | None = None,
        dst_platform: Platform = Platform.TELEGRAM,
        dst_chat_id: str = "-100",
    ) -> None:
        super().__init__(
            source_mapping=source_mapping,
            destination_mapping=destination_mapping,
        )
        self.bridge = SimpleNamespace(bridge_id=uuid.uuid4())
        self.dst_chat = SimpleNamespace(platform=dst_platform, chat_id=dst_chat_id)
        self.pending_mutations: list[dict[str, Any]] = []
        self.enqueued: list[dict[str, Any]] = []
        self.identities: list[dict[str, Any]] = []
        self.canonical_events: list[dict[str, Any]] = []

    async def upsert_identity(
        self,
        platform: Platform,
        user_id: str,
        **kwargs: Any,
    ) -> None:
        self.identities.append(
            {"platform": platform, "user_id": user_id, **kwargs}
        )

    async def find_bridge_by_chat(
        self,
        platform: Platform,
        chat_id: str,
        *,
        include_paused: bool = False,
    ) -> Any:
        _ = platform, chat_id, include_paused
        return self.bridge

    async def find_other_chat(
        self,
        bridge_id: uuid.UUID,
        source_platform: Platform,
    ) -> Any:
        _ = bridge_id, source_platform
        return self.dst_chat

    async def insert_canonical_event(self, **kwargs: Any) -> uuid.UUID:
        self.canonical_events.append(kwargs)
        return uuid.uuid4()

    async def insert_pending_mutation(self, **kwargs: Any) -> None:
        self.pending_mutations.append(kwargs)

    async def enqueue_outbox(self, **kwargs: Any) -> uuid.UUID:
        self.enqueued.append(kwargs)
        return uuid.uuid4()


class GroupedProcessingRepository(ProcessingRepository):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.buffers: dict[str, Any] = {}
        self.buffer_members: dict[uuid.UUID, list[Any]] = {}

    async def buffer_telegram_media_group_update(
        self,
        *,
        chat_id: str,
        media_group_id: str,
        group_key: str,
        message_id: str,
        raw_message: dict[str, Any],
        flush_after: datetime,
    ) -> None:
        buffer = self.buffers.get(group_key)
        if buffer is None:
            buffer = SimpleNamespace(
                buffer_id=uuid.uuid4(),
                chat_id=chat_id,
                media_group_id=media_group_id,
                group_key=group_key,
                anchor_message_id=message_id,
                pending_flush=True,
                has_flushed=False,
                flush_after=flush_after,
            )
            self.buffers[group_key] = buffer
            self.buffer_members[buffer.buffer_id] = []
        else:
            buffer.pending_flush = True
            buffer.flush_after = flush_after
        members = self.buffer_members[buffer.buffer_id]
        existing = next(
            (item for item in members if item.message_id == message_id),
            None,
        )
        if existing is None:
            members.append(
                SimpleNamespace(
                    message_id=message_id,
                    raw_message=raw_message,
                )
            )
        else:
            existing.raw_message = raw_message

    async def claim_flushable_telegram_media_groups(self, limit: int) -> list[Any]:
        ready = [
            buffer
            for buffer in self.buffers.values()
            if buffer.pending_flush
        ]
        return ready[:limit]

    async def list_telegram_media_group_members(self, buffer_id: uuid.UUID) -> list[Any]:
        return list(self.buffer_members[buffer_id])

    async def mark_telegram_media_group_flushed(self, buffer: Any) -> None:
        buffer.pending_flush = False
        buffer.has_flushed = True


def make_worker() -> NormalizerWorker:
    return NormalizerWorker(
        database=None,  # type: ignore[arg-type]
        clients={},
        command_processor=None,  # type: ignore[arg-type]
        stop_event=asyncio.Event(),
        idle_seconds=0,
    )


def make_mapping(*, src_message_id: str, dst_message_id: str) -> MessageMapping:
    return cast(
        MessageMapping,
        SimpleNamespace(
            src_message_id=src_message_id,
            dst_message_id=dst_message_id,
        ),
    )


def make_normalized(
    platform: Platform,
    *,
    chat_id: str,
    message_id: str,
    reply_to_message_id: str | None = None,
    text: str | None = "hello",
    formatted_html: str | None = None,
    payload: dict[str, Any] | None = None,
    event_version: str | int | None = None,
) -> NormalizedUpdate:
    return NormalizedUpdate(
        platform=platform,
        event_type=EventType.MESSAGE_CREATED,
        dedup_key=f"{platform.value}:{message_id}",
        chat_id=chat_id,
        user_id="42",
        message_id=message_id,
        event_version=event_version,
        text=text,
        formatted_html=formatted_html,
        happened_at=datetime.now(UTC),
        identity=UserIdentity(
            platform=platform,
            user_id="42",
            first_name="Alice",
        ),
        reply_to_message_id=reply_to_message_id,
        payload=payload,
    )


def make_supported_media_payload(
    kind: str,
    *,
    source_platform: str = "telegram",
    text_hint: str | None = None,
) -> dict[str, Any]:
    resolved_text_hint = text_hint or f"[{kind}]"
    source = (
        {"file_id": "file-id"}
        if source_platform == "telegram"
        else {"url": f"https://example.invalid/{kind}.bin"}
    )
    return {
        "media": {
            "supported": True,
            "kind": kind,
            "text_hint": resolved_text_hint,
            "payload": {
                "source_platform": source_platform,
                "kind": kind,
                "placeholder": resolved_text_hint,
                "filename": f"{kind}.bin",
                "identity": f"{source_platform}:{kind}:id:file-id",
                "source": source,
            },
        }
    }


@pytest.mark.asyncio
async def test_build_payload_replies_to_destination_mapping_for_local_source_message():
    bridge_id = uuid.uuid4()
    repo = FakeRepository(
        source_mapping=make_mapping(
            src_message_id="tg-parent",
            dst_message_id="max-parent",
        )
    )
    worker = make_worker()

    payload = await worker._build_payload(
        repo,
        make_normalized(
            Platform.TELEGRAM,
            chat_id="-100",
            message_id="200",
            reply_to_message_id="100",
        ),
        bridge_id,
    )

    assert payload["reply_to_message_id"] == "max-parent"
    assert "[reply to" not in str(payload["text"])
    assert repo.destination_calls == []


@pytest.mark.asyncio
async def test_build_payload_replies_to_source_message_for_mirrored_destination_message(
):
    bridge_id = uuid.uuid4()
    repo = FakeRepository(
        destination_mapping=make_mapping(
            src_message_id="max-parent",
            dst_message_id="tg-mirror-parent",
        )
    )
    worker = make_worker()

    payload = await worker._build_payload(
        repo,
        make_normalized(
            Platform.TELEGRAM,
            chat_id="-100",
            message_id="200",
            reply_to_message_id="150",
        ),
        bridge_id,
    )

    assert payload["reply_to_message_id"] == "max-parent"
    assert "[reply to" not in str(payload["text"])
    assert len(repo.source_calls) == 1
    assert len(repo.destination_calls) == 1


@pytest.mark.asyncio
async def test_build_payload_resolves_native_reply_from_max_to_telegram():
    bridge_id = uuid.uuid4()
    repo = FakeRepository(
        destination_mapping=make_mapping(
            src_message_id="tg-parent",
            dst_message_id="max-mirror-parent",
        )
    )
    worker = make_worker()

    payload = await worker._build_payload(
        repo,
        make_normalized(
            Platform.MAX,
            chat_id="100",
            message_id="mid-child",
            reply_to_message_id="mid-mirror-parent",
        ),
        bridge_id,
    )

    assert payload["reply_to_message_id"] == "tg-parent"
    assert "[reply to" not in str(payload["text"])
    assert len(repo.source_calls) == 1
    assert len(repo.destination_calls) == 1


@pytest.mark.asyncio
async def test_build_payload_keeps_reply_hint_when_mapping_is_missing():
    bridge_id = uuid.uuid4()
    repo = FakeRepository()
    worker = make_worker()

    payload = await worker._build_payload(
        repo,
        make_normalized(
            Platform.MAX,
            chat_id="100",
            message_id="mid-child",
            reply_to_message_id="mid-parent",
        ),
        bridge_id,
    )

    assert payload["reply_to_message_id"] is None
    assert "[reply to mid-parent]" in str(payload["text"])
    assert len(repo.source_calls) == 1
    assert len(repo.destination_calls) == 1


@pytest.mark.asyncio
async def test_build_payload_uses_media_caption_and_fallback_text():
    bridge_id = uuid.uuid4()
    repo = FakeRepository()
    worker = make_worker()

    payload = await worker._build_payload(
        repo,
        make_normalized(
            Platform.TELEGRAM,
            chat_id="-100",
            message_id="201",
            text=None,
            payload={
                "media": {
                    "supported": True,
                    "kind": "image",
                    "text_hint": "[photo]",
                    "payload": {
                        "source_platform": "telegram",
                        "kind": "image",
                        "placeholder": "[photo]",
                        "filename": "photo.jpg",
                        "identity": "telegram:image:id:file-id",
                        "source": {"file_id": "file-id"},
                    },
                }
            },
        ),
        bridge_id,
    )

    assert payload["text"] == "Alice:"
    assert payload["fallback_text"] == "Alice: [photo]"
    assert payload["has_media"] is True
    assert payload["media_kind"] == "image"
    media = cast(dict[str, Any], payload["media"])
    assert media["source"]["file_id"] == "file-id"
    assert media["identity"] == "telegram:image:id:file-id"


@pytest.mark.asyncio
async def test_build_payload_uses_audio_label_for_telegram_audio_caption():
    bridge_id = uuid.uuid4()
    repo = FakeRepository()
    worker = make_worker()

    payload = await worker._build_payload(
        repo,
        make_normalized(
            Platform.TELEGRAM,
            chat_id="-100",
            message_id="202-audio",
            text="caption",
            formatted_html="<b>caption</b>",
            payload=make_supported_media_payload("audio"),
        ),
        bridge_id,
    )

    assert payload["text_plain"] == ""
    assert payload["text_html"] is None
    assert payload["fallback_text"] == "🔊 Alice\ncaption"
    assert payload["post_send_text_plain"] == "🔊 Alice\ncaption"
    assert payload["post_send_text_html"] == "🔊 Alice\n<b>caption</b>"
    assert payload["media_kind"] == "audio"


@pytest.mark.asyncio
async def test_build_payload_uses_audio_label_for_telegram_voice_without_text():
    bridge_id = uuid.uuid4()
    repo = FakeRepository()
    worker = make_worker()

    payload = await worker._build_payload(
        repo,
        make_normalized(
            Platform.TELEGRAM,
            chat_id="-100",
            message_id="203-voice",
            text=None,
            payload=make_supported_media_payload("voice"),
        ),
        bridge_id,
    )

    assert payload["text_plain"] == ""
    assert payload["text_html"] is None
    assert payload["fallback_text"] == "🔊 Alice"
    assert payload["post_send_text_plain"] == "🔊 Alice"
    assert "post_send_text_html" not in payload
    assert payload["media_kind"] == "voice"


@pytest.mark.asyncio
async def test_build_payload_uses_audio_label_for_max_audio_caption():
    bridge_id = uuid.uuid4()
    repo = FakeRepository()
    worker = make_worker()

    payload = await worker._build_payload(
        repo,
        make_normalized(
            Platform.MAX,
            chat_id="100",
            message_id="mid-audio",
            text="listen",
            payload=make_supported_media_payload(
                "audio",
                source_platform="max",
            ),
        ),
        bridge_id,
    )

    assert payload["text_plain"] == "🔊 Alice\nlisten"
    assert payload["fallback_text"] == "🔊 Alice\nlisten"
    assert "post_send_text_plain" not in payload
    assert payload["media_kind"] == "audio"


@pytest.mark.asyncio
async def test_build_payload_includes_formatted_html_for_text_messages():
    bridge_id = uuid.uuid4()
    repo = FakeRepository()
    worker = make_worker()

    payload = await worker._build_payload(
        repo,
        make_normalized(
            Platform.TELEGRAM,
            chat_id="-100",
            message_id="202",
            text="hello",
            formatted_html="<i>hello</i>",
        ),
        bridge_id,
    )

    assert payload["text_plain"] == "Alice: hello"
    assert payload["text_html"] == "Alice: <i>hello</i>"
    assert payload["fallback_text"] == "Alice: hello"


@pytest.mark.asyncio
async def test_build_payload_includes_formatted_html_for_media_captions():
    bridge_id = uuid.uuid4()
    repo = FakeRepository()
    worker = make_worker()

    payload = await worker._build_payload(
        repo,
        make_normalized(
            Platform.TELEGRAM,
            chat_id="-100",
            message_id="203",
            text="caption",
            formatted_html="<b>caption</b>",
            payload={
                "media": {
                    "supported": True,
                    "kind": "image",
                    "text_hint": "[photo]",
                    "payload": {
                        "source_platform": "telegram",
                        "kind": "image",
                        "placeholder": "[photo]",
                        "filename": "photo.jpg",
                        "identity": "telegram:image:id:file-id",
                        "source": {"file_id": "file-id"},
                    },
                }
            },
        ),
        bridge_id,
    )

    assert payload["text_plain"] == "Alice: caption"
    assert payload["text_html"] == "Alice: <b>caption</b>"
    assert payload["fallback_text"] == "Alice: caption"


@pytest.mark.asyncio
async def test_build_payload_repeated_forward_of_mirrored_text_does_not_duplicate_alias():
    bridge_id = uuid.uuid4()
    repo = FakeRepository()
    worker = make_worker()
    normalized = normalize_update(
        Platform.MAX,
        {
            "update_type": "message_created",
            "message": {
                "recipient": {"chat_id": 100},
                "sender": {"user_id": 42, "first_name": "Alice"},
                "link": {
                    "type": "forward",
                    "sender": {
                        "user_id": 700,
                        "first_name": "Bridge",
                        "username": "bridge_bot",
                        "is_bot": True,
                    },
                    "message": {
                        "mid": "mid-source",
                        "seq": 5,
                        "text": "Alice: hello",
                    },
                },
                "body": {"mid": "mid-child", "seq": 8, "text": ""},
            },
        },
    )

    assert normalized is not None
    payload = await worker._build_payload(repo, normalized, bridge_id)

    assert payload["text_plain"] == "[forwarded]\nAlice: hello"
    assert "Alice: Alice:" not in str(payload["text_plain"])


@pytest.mark.asyncio
async def test_build_payload_uses_explicit_event_version():
    bridge_id = uuid.uuid4()
    repo = FakeRepository()
    worker = make_worker()

    payload = await worker._build_payload(
        repo,
        make_normalized(
            Platform.MAX,
            chat_id="100",
            message_id="mid-1",
            event_version=77,
        ),
        bridge_id,
    )

    assert payload["version"] == 77


@pytest.mark.asyncio
async def test_process_row_enqueues_max_edit_to_telegram():
    repo = ProcessingRepository(
        source_mapping=make_mapping(
            src_message_id="mid-1",
            dst_message_id="tg-msg-1",
        )
    )
    worker = make_worker()
    raw = {
        "update_type": "message_edited",
        "timestamp": 1_700_000_500_000,
        "message": {
            "recipient": {"chat_id": 100},
            "sender": {"user_id": 42, "first_name": "Alice"},
            "body": {"mid": "mid-1", "seq": 12, "text": "updated"},
        },
    }

    await worker._process_row(
        repo,
        Platform.MAX,
        raw,
        uuid.uuid4(),
    )

    assert repo.pending_mutations == []
    assert len(repo.enqueued) == 1
    queued = repo.enqueued[0]
    assert queued["dst_platform"] == Platform.TELEGRAM
    assert queued["action"].value == "edit"
    task = cast(dict[str, Any], queued["task"])
    assert task["dst_message_id"] == "tg-msg-1"
    assert task["version"] == stable_json_hash(raw)


@pytest.mark.asyncio
async def test_process_row_enqueues_distinct_outbox_edits_for_distinct_max_edits(
):
    repo = ProcessingRepository(
        source_mapping=make_mapping(
            src_message_id="mid-1",
            dst_message_id="tg-msg-1",
        )
    )
    worker = make_worker()
    first_raw = {
        "update_type": "message_edited",
        "timestamp": 1_700_000_500_000,
        "message": {
            "recipient": {"chat_id": 100},
            "sender": {"user_id": 42, "first_name": "Alice"},
            "body": {"mid": "mid-1", "seq": 12, "text": "Test2"},
        },
    }
    second_raw = {
        "update_type": "message_edited",
        "timestamp": 1_700_000_500_000,
        "message": {
            "recipient": {"chat_id": 100},
            "sender": {"user_id": 42, "first_name": "Alice"},
            "body": {"mid": "mid-1", "seq": 12, "text": "Test3"},
        },
    }

    await worker._process_row(repo, Platform.MAX, first_raw, uuid.uuid4())
    await worker._process_row(repo, Platform.MAX, second_raw, uuid.uuid4())

    assert len(repo.enqueued) == 2
    assert repo.enqueued[0]["dedup_key"] != repo.enqueued[1]["dedup_key"]


@pytest.mark.asyncio
async def test_process_row_stores_pending_edit_when_mapping_is_missing():
    repo = ProcessingRepository(dst_platform=Platform.MAX, dst_chat_id="200")
    worker = make_worker()

    await worker._process_row(
        repo,
        Platform.TELEGRAM,
        {
            "update_id": 10,
            "edited_message": {
                "message_id": 20,
                "date": 1_700_000_000,
                "edit_date": 1_700_000_050,
                "chat": {"id": -100},
                "from": {"id": 42, "first_name": "Alice", "is_bot": False},
                "text": "updated",
            },
        },
        uuid.uuid4(),
    )

    assert repo.enqueued == []
    assert len(repo.pending_mutations) == 1
    pending = repo.pending_mutations[0]
    assert pending["mutation_type"] == "edit"
    payload = cast(dict[str, Any], pending["payload"])
    assert payload["dst"] == {"platform": "max", "chat_id": "200"}
    assert payload["version"] == 1_700_000_050


@pytest.mark.asyncio
async def test_process_row_uses_telegram_from_user_for_identity_and_alias():
    repo = ProcessingRepository(dst_platform=Platform.MAX, dst_chat_id="200")
    worker = make_worker()

    await worker._process_row(
        repo,
        Platform.TELEGRAM,
        {
            "update_id": 110,
            "message": {
                "message_id": 210,
                "date": 1_700_000_000,
                "chat": {"id": -100},
                "from_user": {
                    "id": 42,
                    "first_name": "Alice",
                    "last_name": "Bob",
                    "is_bot": False,
                },
                "text": "hello",
            },
        },
        uuid.uuid4(),
    )

    assert len(repo.identities) == 1
    assert repo.identities[0]["user_id"] == "42"
    assert len(repo.enqueued) == 1
    task = cast(dict[str, Any], repo.enqueued[0]["task"])
    assert task["src"]["user_id"] == "42"
    assert task["text_plain"] == "Alice Bob: hello"


@pytest.mark.asyncio
async def test_flush_ready_telegram_media_group_enqueues_one_grouped_task():
    repo = GroupedProcessingRepository(dst_platform=Platform.MAX, dst_chat_id="200")
    worker = make_worker()
    received_at = datetime.now(UTC)

    await worker._process_row(
        repo,
        Platform.TELEGRAM,
        {
            "update_id": 200,
            "message": {
                "message_id": 300,
                "date": 1_700_000_000,
                "chat": {"id": -100},
                "from": {"id": 42, "first_name": "Alice", "is_bot": False},
                "media_group_id": "grp-1",
                "caption": "album",
                "photo": [{"file_id": "photo-1", "file_unique_id": "photo-1"}],
            },
        },
        uuid.uuid4(),
        received_at,
    )
    await worker._process_row(
        repo,
        Platform.TELEGRAM,
        {
            "update_id": 201,
            "message": {
                "message_id": 301,
                "date": 1_700_000_001,
                "chat": {"id": -100},
                "from": {"id": 42, "first_name": "Alice", "is_bot": False},
                "media_group_id": "grp-1",
                "video": {
                    "file_id": "video-1",
                    "file_unique_id": "video-1",
                    "file_size": 2048,
                },
            },
        },
        uuid.uuid4(),
        received_at,
    )

    flushed = await worker._flush_ready_telegram_media_groups(repo)

    assert flushed == 1
    assert len(repo.enqueued) == 1
    queued = repo.enqueued[0]
    task = cast(dict[str, Any], queued["task"])
    assert task["dst"] == {"platform": "max", "chat_id": "200"}
    assert task["group_kind"] == "photo_video_chunk"
    assert task["group_key"] == "telegram:-100:grp-1"
    assert task["source_member_message_ids"] == ["300", "301"]
    assert [item["kind"] for item in cast(list[dict[str, Any]], task["media_items"])] == [
        "image",
        "video",
    ]
