from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from maxogram.domain import LocalMediaFile, MediaKind, MediaPresentation
from maxogram.platforms.base import PlatformDeliveryError
from maxogram.platforms.telegram import TelegramClient


class FakeTelegramMessage:
    def __init__(self, message_id: int, kind: str) -> None:
        self.message_id = message_id
        self.kind = kind

    def model_dump(
        self,
        *,
        mode: str,
        by_alias: bool,
        exclude_none: bool,
    ) -> dict[str, object]:
        _ = mode, by_alias, exclude_none
        return {"kind": self.kind}


class FakeBot:
    def __init__(self) -> None:
        self.get_file_calls: list[str] = []
        self.get_updates_calls: list[dict[str, object]] = []
        self.download_file_calls: list[dict[str, object]] = []
        self.download_calls: list[dict[str, object]] = []
        self.send_message_calls: list[dict[str, object]] = []
        self.send_animation_calls: list[dict[str, object]] = []
        self.send_photo_calls: list[dict[str, object]] = []
        self.send_audio_calls: list[dict[str, object]] = []
        self.send_voice_calls: list[dict[str, object]] = []
        self.edit_message_caption_calls: list[dict[str, object]] = []
        self.edit_message_text_calls: list[dict[str, object]] = []
        self.edit_message_media_calls: list[dict[str, object]] = []
        self.updates: list[object] = []

    async def get_updates(self, **kwargs: object) -> list[object]:
        self.get_updates_calls.append(dict(kwargs))
        return self.updates

    async def get_file(self, file_id: str) -> object:
        self.get_file_calls.append(file_id)
        return SimpleNamespace(file_path="photos/file_1.jpg")

    async def download_file(self, file_path: str, *, destination: Path) -> None:
        self.download_file_calls.append(
            {
                "file_path": file_path,
                "destination": destination,
            }
        )
        await asyncio.to_thread(destination.write_bytes, b"telegram-media")

    async def download(self, file: object, *, destination: Path) -> None:
        self.download_calls.append(
            {
                "file": file,
                "destination": destination,
            }
        )

    async def send_animation(self, **kwargs: object) -> FakeTelegramMessage:
        self.send_animation_calls.append(dict(kwargs))
        return FakeTelegramMessage(101, "animation")

    async def send_message(self, **kwargs: object) -> FakeTelegramMessage:
        self.send_message_calls.append(dict(kwargs))
        return FakeTelegramMessage(100, "message")

    async def send_photo(self, **kwargs: object) -> FakeTelegramMessage:
        self.send_photo_calls.append(dict(kwargs))
        return FakeTelegramMessage(102, "photo")

    async def send_audio(self, **kwargs: object) -> FakeTelegramMessage:
        self.send_audio_calls.append(dict(kwargs))
        return FakeTelegramMessage(103, "audio")

    async def send_voice(self, **kwargs: object) -> FakeTelegramMessage:
        self.send_voice_calls.append(dict(kwargs))
        return FakeTelegramMessage(104, "voice")

    async def edit_message_caption(self, **kwargs: object) -> FakeTelegramMessage:
        self.edit_message_caption_calls.append(dict(kwargs))
        return FakeTelegramMessage(201, "caption")

    async def edit_message_text(self, **kwargs: object) -> FakeTelegramMessage:
        self.edit_message_text_calls.append(dict(kwargs))
        return FakeTelegramMessage(202, "text")

    async def edit_message_media(self, **kwargs: object) -> FakeTelegramMessage:
        self.edit_message_media_calls.append(dict(kwargs))
        return FakeTelegramMessage(203, "media")


def make_client(bot: FakeBot) -> TelegramClient:
    client = object.__new__(TelegramClient)
    client.bot = cast(Any, bot)
    return client


@pytest.mark.asyncio
async def test_download_media_uses_download_file_for_telegram_path(
    tmp_path: Path,
) -> None:
    bot = FakeBot()
    client = make_client(bot)

    media: dict[str, object] = {
        "kind": "image",
        "filename": "family-photo.jpg",
        "mime_type": "image/jpeg",
        "source": {"file_id": "photo-file-id"},
    }

    local_file = await client.download_media(media, tmp_path)

    assert bot.get_file_calls == ["photo-file-id"]
    assert len(bot.download_file_calls) == 1
    assert bot.download_file_calls[0]["file_path"] == "photos/file_1.jpg"
    assert bot.download_calls == []
    assert local_file is not None
    assert local_file.kind == MediaKind.IMAGE
    assert local_file.filename == "family-photo.jpg"
    assert local_file.mime_type == "image/jpeg"
    assert local_file.path.exists()
    assert local_file.path.read_bytes() == b"telegram-media"


@pytest.mark.asyncio
async def test_download_media_preserves_document_metadata(tmp_path: Path) -> None:
    bot = FakeBot()
    client = make_client(bot)

    media: dict[str, object] = {
        "kind": "document",
        "filename": "report.pdf",
        "mime_type": "application/pdf",
        "source": {"file_id": "document-file-id"},
    }

    local_file = await client.download_media(media, tmp_path)

    assert bot.get_file_calls == ["document-file-id"]
    assert len(bot.download_file_calls) == 1
    assert local_file is not None
    assert bot.download_file_calls[0]["destination"] == local_file.path
    assert local_file.kind == MediaKind.DOCUMENT
    assert local_file.filename == "report.pdf"
    assert local_file.mime_type == "application/pdf"


@pytest.mark.asyncio
async def test_send_message_uses_send_animation_for_animation_presentation(
    tmp_path: Path,
) -> None:
    bot = FakeBot()
    client = make_client(bot)
    file_path = tmp_path / "party.gif"
    file_path.write_bytes(b"gif89a")
    media = LocalMediaFile(
        kind=MediaKind.IMAGE,
        path=file_path,
        filename="party.gif",
        mime_type="image/gif",
        presentation=MediaPresentation.ANIMATION,
    )

    result = await client.send_message("-100", "Alice:", media=media)

    assert len(bot.send_animation_calls) == 1
    assert bot.send_photo_calls == []
    assert bot.send_animation_calls[0]["caption"] == "Alice:"
    assert result.message_id == "101"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "filename", "mime_type", "expected_call", "expected_message_id"),
    [
        (MediaKind.AUDIO, "track.mp3", "audio/mpeg", "send_audio_calls", "103"),
        (MediaKind.VOICE, "voice.ogg", "audio/ogg", "send_voice_calls", "104"),
    ],
)
async def test_send_message_uses_caption_for_audio_media(
    tmp_path: Path,
    kind: MediaKind,
    filename: str,
    mime_type: str,
    expected_call: str,
    expected_message_id: str,
) -> None:
    bot = FakeBot()
    client = make_client(bot)
    file_path = tmp_path / filename
    file_path.write_bytes(b"audio")
    media = LocalMediaFile(
        kind=kind,
        path=file_path,
        filename=filename,
        mime_type=mime_type,
    )

    result = await client.send_message("-100", "🔊 Alice", media=media)

    call_log = cast(list[dict[str, object]], getattr(bot, expected_call))
    assert len(call_log) == 1
    assert call_log[0]["caption"] == "🔊 Alice"
    assert result.message_id == expected_message_id


@pytest.mark.asyncio
async def test_send_message_uses_html_parse_mode_for_formatted_text() -> None:
    bot = FakeBot()
    client = make_client(bot)

    result = await client.send_message(
        "-100",
        "Alice: plain",
        text_html="Alice: <b>plain</b>",
    )

    assert result.message_id == "100"
    assert len(bot.send_message_calls) == 1
    assert bot.send_message_calls[0]["text"] == "Alice: <b>plain</b>"
    assert bot.send_message_calls[0]["parse_mode"] == "HTML"


@pytest.mark.asyncio
async def test_send_message_succeeds_when_result_serialization_fails(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    bot = FakeBot()
    client = make_client(bot)

    def fail_serialize(message: object) -> object:
        _ = message
        raise TypeError("boom")

    monkeypatch.setattr(
        "maxogram.platforms.telegram.deserialize_telegram_object_to_python",
        fail_serialize,
    )

    with caplog.at_level(logging.WARNING):
        result = await client.send_message("-100", "Alice: plain")

    assert result.message_id == "100"
    assert result.raw == {"message_id": 100}
    assert len(bot.send_message_calls) == 1
    assert "Falling back to minimal Telegram send result serialization" in caplog.text
    assert "chat_id=-100" in caplog.text
    assert "message_id=100" in caplog.text


@pytest.mark.asyncio
async def test_send_media_succeeds_when_result_serialization_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    bot = FakeBot()
    client = make_client(bot)
    file_path = tmp_path / "party.jpg"
    file_path.write_bytes(b"jpg")
    media = LocalMediaFile(
        kind=MediaKind.IMAGE,
        path=file_path,
        filename="party.jpg",
        mime_type="image/jpeg",
    )

    def fail_serialize(message: object) -> object:
        _ = message
        raise TypeError("boom")

    monkeypatch.setattr(
        "maxogram.platforms.telegram.deserialize_telegram_object_to_python",
        fail_serialize,
    )

    result = await client.send_message("-100", "Alice:", media=media)

    assert result.message_id == "102"
    assert result.raw == {"message_id": 102}
    assert len(bot.send_photo_calls) == 1


@pytest.mark.asyncio
async def test_edit_message_uses_caption_edit_for_existing_media() -> None:
    bot = FakeBot()
    client = make_client(bot)

    await client.edit_message("-100", "55", "Alice: updated", has_media=True)

    assert len(bot.edit_message_caption_calls) == 1
    assert bot.edit_message_caption_calls[0]["caption"] == "Alice: updated"
    assert bot.edit_message_media_calls == []
    assert bot.edit_message_text_calls == []


@pytest.mark.asyncio
async def test_edit_message_uses_html_parse_mode_for_text_edit() -> None:
    bot = FakeBot()
    client = make_client(bot)

    await client.edit_message(
        "-100",
        "55",
        "Alice: updated",
        text_html="Alice: <i>updated</i>",
    )

    assert len(bot.edit_message_text_calls) == 1
    assert bot.edit_message_text_calls[0]["text"] == "Alice: <i>updated</i>"
    assert bot.edit_message_text_calls[0]["parse_mode"] == "HTML"
    assert bot.edit_message_caption_calls == []


@pytest.mark.asyncio
async def test_edit_message_uses_html_parse_mode_for_caption_edit() -> None:
    bot = FakeBot()
    client = make_client(bot)

    await client.edit_message(
        "-100",
        "55",
        "Alice: updated",
        text_html="Alice: <i>updated</i>",
        has_media=True,
    )

    assert len(bot.edit_message_caption_calls) == 1
    assert bot.edit_message_caption_calls[0]["caption"] == "Alice: <i>updated</i>"
    assert bot.edit_message_caption_calls[0]["parse_mode"] == "HTML"


@pytest.mark.asyncio
async def test_edit_message_replaces_media_via_edit_message_media(
    tmp_path: Path,
) -> None:
    bot = FakeBot()
    client = make_client(bot)
    file_path = tmp_path / "replacement.jpg"
    file_path.write_bytes(b"jpg")
    media = LocalMediaFile(
        kind=MediaKind.IMAGE,
        path=file_path,
        filename="replacement.jpg",
        mime_type="image/jpeg",
    )

    await client.edit_message(
        "-100",
        "55",
        "Alice: updated",
        has_media=True,
        replacement_media=media,
    )

    assert len(bot.edit_message_media_calls) == 1
    input_media = cast(Any, bot.edit_message_media_calls[0]["media"])
    assert input_media.__class__.__name__ == "InputMediaPhoto"
    assert input_media.caption == "Alice: updated"
    assert bot.edit_message_caption_calls == []


@pytest.mark.asyncio
async def test_edit_message_replaces_media_with_html_caption(
    tmp_path: Path,
) -> None:
    bot = FakeBot()
    client = make_client(bot)
    file_path = tmp_path / "replacement.jpg"
    file_path.write_bytes(b"jpg")
    media = LocalMediaFile(
        kind=MediaKind.IMAGE,
        path=file_path,
        filename="replacement.jpg",
        mime_type="image/jpeg",
    )

    await client.edit_message(
        "-100",
        "55",
        "Alice: updated",
        text_html="Alice: <b>updated</b>",
        has_media=True,
        replacement_media=media,
    )

    input_media = cast(Any, bot.edit_message_media_calls[0]["media"])
    assert input_media.caption == "Alice: <b>updated</b>"
    assert input_media.parse_mode == "HTML"


@pytest.mark.asyncio
async def test_edit_message_rejects_voice_replacement_media(tmp_path: Path) -> None:
    bot = FakeBot()
    client = make_client(bot)
    file_path = tmp_path / "replacement.ogg"
    file_path.write_bytes(b"ogg")
    media = LocalMediaFile(
        kind=MediaKind.VOICE,
        path=file_path,
        filename="replacement.ogg",
        mime_type="audio/ogg",
    )

    with pytest.raises(PlatformDeliveryError) as exc_info:
        await client.edit_message(
            "-100",
            "55",
            "Alice: updated",
            has_media=True,
            replacement_media=media,
        )

    assert exc_info.value.code == "unsupported_voice_media_edit"
    assert bot.edit_message_media_calls == []


@pytest.mark.asyncio
async def test_poll_updates_skips_serialization_errors_and_advances_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bot = FakeBot()
    client = make_client(bot)
    bad_update = SimpleNamespace(update_id=1001)
    good_update = SimpleNamespace(update_id=1002)
    bot.updates = [bad_update, good_update]

    def fake_deserialize(update: object) -> object:
        if update is bad_update:
            raise TypeError("boom")
        return {"update_id": 1002, "message": {"message_id": 1}}

    monkeypatch.setattr(
        "maxogram.platforms.telegram.deserialize_telegram_object_to_python",
        fake_deserialize,
    )

    batch = await client.poll_updates(1000, limit=10, poll_timeout=30)

    assert len(batch.updates) == 1
    assert batch.updates[0].update_key == "1002"
    assert batch.next_cursor == 1003
