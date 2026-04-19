from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from maxapi.enums.parse_mode import TextFormat

from maxogram.domain import LocalMediaFile, MediaKind, MediaPresentation
from maxogram.platforms.max import DownloadedMediaInfo, MaxClient, _max_error


def make_client() -> MaxClient:
    return object.__new__(MaxClient)


class NoopRateLimiter:
    async def wait(self) -> None:
        return None


class FakeEditBot:
    def __init__(self) -> None:
        self.request_calls: list[dict[str, Any]] = []
        self.edit_message_calls: list[dict[str, Any]] = []
        self.send_message_calls: list[dict[str, Any]] = []

    async def request(self, **kwargs: Any) -> None:
        self.request_calls.append(dict(kwargs))

    async def edit_message(self, **kwargs: Any) -> None:
        self.edit_message_calls.append(dict(kwargs))

    async def send_message(self, **kwargs: Any) -> Any:
        self.send_message_calls.append(dict(kwargs))
        return FakeSendResult("mid-200")


class FakeSendResult:
    def __init__(self, mid: str) -> None:
        self.message = cast(Any, SimpleNamespace(body=SimpleNamespace(mid=mid)))

    def model_dump(
        self,
        *,
        mode: str,
        by_alias: bool,
        exclude_none: bool,
    ) -> dict[str, object]:
        _ = mode, by_alias, exclude_none
        return {"message": {"body": {"mid": self.message.body.mid}}}


class FakeMaxApiError(Exception):
    def __init__(self, *, code: int, raw: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.raw = raw


@pytest.mark.asyncio
async def test_download_media_upgrades_opaque_image_to_gif_from_content_type(
    tmp_path: Path,
) -> None:
    client = make_client()

    async def fake_download_to_path(url: str, destination: Path) -> DownloadedMediaInfo:
        assert url == "https://i.oneme.ru/i?r=opaque"
        await asyncio.to_thread(destination.write_bytes, b"GIF89a-content")
        return DownloadedMediaInfo(content_type="image/gif", is_gif=False)

    client._download_to_path = fake_download_to_path  # type: ignore[method-assign]
    media: dict[str, object] = {
        "kind": "image",
        "source": {"url": "https://i.oneme.ru/i?r=opaque"},
    }

    local_file = await client.download_media(media, tmp_path)

    assert local_file is not None
    assert local_file.kind == MediaKind.IMAGE
    assert local_file.presentation == MediaPresentation.ANIMATION
    assert local_file.mime_type == "image/gif"
    assert local_file.filename.endswith(".gif")
    assert local_file.path.exists()


@pytest.mark.asyncio
async def test_download_media_upgrades_opaque_image_to_gif_from_magic_bytes(
    tmp_path: Path,
) -> None:
    client = make_client()

    async def fake_download_to_path(url: str, destination: Path) -> DownloadedMediaInfo:
        assert url == "https://i.oneme.ru/i?r=opaque"
        await asyncio.to_thread(destination.write_bytes, b"GIF89a-content")
        return DownloadedMediaInfo(content_type=None, is_gif=True)

    client._download_to_path = fake_download_to_path  # type: ignore[method-assign]
    media: dict[str, object] = {
        "kind": "image",
        "source": {"url": "https://i.oneme.ru/i?r=opaque"},
    }

    local_file = await client.download_media(media, tmp_path)

    assert local_file is not None
    assert local_file.presentation == MediaPresentation.ANIMATION
    assert local_file.mime_type == "image/gif"
    assert local_file.filename.endswith(".gif")


@pytest.mark.asyncio
async def test_download_media_keeps_regular_image_without_animation_presentation(
    tmp_path: Path,
) -> None:
    client = make_client()

    async def fake_download_to_path(url: str, destination: Path) -> DownloadedMediaInfo:
        assert url == "https://i.oneme.ru/i?r=opaque"
        await asyncio.to_thread(destination.write_bytes, b"\x89PNG\r\n\x1a\ncontent")
        return DownloadedMediaInfo(content_type="image/jpeg", is_gif=False)

    client._download_to_path = fake_download_to_path  # type: ignore[method-assign]
    media: dict[str, object] = {
        "kind": "image",
        "source": {"url": "https://i.oneme.ru/i?r=opaque"},
    }

    local_file = await client.download_media(media, tmp_path)

    assert local_file is not None
    assert local_file.presentation is None
    assert local_file.mime_type == "image/jpeg"
    assert local_file.filename == "i"


@pytest.mark.asyncio
async def test_edit_message_keeps_existing_attachments_for_caption_only_edit() -> None:
    client = make_client()
    client.bot = FakeEditBot()  # type: ignore[attr-defined]
    client.rate_limiter = cast(Any, NoopRateLimiter())  # type: ignore[attr-defined]

    await client.edit_message("200", "mid-1", "updated", has_media=True)

    assert len(client.bot.request_calls) == 1  # type: ignore[attr-defined]
    request_call = client.bot.request_calls[0]  # type: ignore[attr-defined]
    assert request_call["json"] == {"text": "updated"}
    assert client.bot.edit_message_calls == []  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_send_message_uses_html_format_when_formatted_text_is_available() -> None:
    client = make_client()
    client.bot = FakeEditBot()  # type: ignore[attr-defined]
    client.rate_limiter = cast(Any, NoopRateLimiter())  # type: ignore[attr-defined]

    result = await client.send_message(
        "200",
        "plain text",
        text_html="<b>plain</b> text",
    )

    assert result.message_id == "mid-200"
    assert len(client.bot.send_message_calls) == 1  # type: ignore[attr-defined]
    send_call = client.bot.send_message_calls[0]  # type: ignore[attr-defined]
    assert send_call["text"] == "<b>plain</b> text"
    assert send_call["format"] == TextFormat.HTML


@pytest.mark.asyncio
async def test_send_message_with_media_keeps_text_and_attachment_in_one_request(
    tmp_path: Path,
) -> None:
    client = make_client()
    client.bot = FakeEditBot()  # type: ignore[attr-defined]
    client.rate_limiter = cast(Any, NoopRateLimiter())  # type: ignore[attr-defined]
    file_path = tmp_path / "track.mp3"
    file_path.write_bytes(b"audio")
    media = LocalMediaFile(
        kind=MediaKind.AUDIO,
        path=file_path,
        filename="track.mp3",
        mime_type="audio/mpeg",
    )

    result = await client.send_message("200", "🔊 Alice", media=media)

    assert result.message_id == "mid-200"
    assert len(client.bot.send_message_calls) == 1  # type: ignore[attr-defined]
    send_call = client.bot.send_message_calls[0]  # type: ignore[attr-defined]
    assert send_call["text"] == "🔊 Alice"
    assert "sleep_after_input_media" not in send_call
    assert len(send_call["attachments"]) == 1
    attachment = send_call["attachments"][0]
    assert attachment.__class__.__name__ == "InputMedia"


@pytest.mark.asyncio
async def test_send_message_with_media_uses_attachment_only_when_text_is_empty(
    tmp_path: Path,
) -> None:
    client = make_client()
    client.bot = FakeEditBot()  # type: ignore[attr-defined]
    client.rate_limiter = cast(Any, NoopRateLimiter())  # type: ignore[attr-defined]
    file_path = tmp_path / "voice.ogg"
    file_path.write_bytes(b"voice")
    media = LocalMediaFile(
        kind=MediaKind.VOICE,
        path=file_path,
        filename="voice.ogg",
        mime_type="audio/ogg",
    )

    await client.send_message("200", "", media=media)

    assert len(client.bot.send_message_calls) == 1  # type: ignore[attr-defined]
    send_call = client.bot.send_message_calls[0]  # type: ignore[attr-defined]
    assert send_call["text"] is None
    assert "sleep_after_input_media" not in send_call


@pytest.mark.asyncio
async def test_send_message_with_media_group_uses_multiple_attachments(
    tmp_path: Path,
) -> None:
    client = make_client()
    client.bot = FakeEditBot()  # type: ignore[attr-defined]
    client.rate_limiter = cast(Any, NoopRateLimiter())  # type: ignore[attr-defined]
    photo_path = tmp_path / "one.jpg"
    video_path = tmp_path / "two.mp4"
    photo_path.write_bytes(b"jpg")
    video_path.write_bytes(b"mp4")
    media_items = [
        LocalMediaFile(
            kind=MediaKind.IMAGE,
            path=photo_path,
            filename="one.jpg",
            mime_type="image/jpeg",
        ),
        LocalMediaFile(
            kind=MediaKind.VIDEO,
            path=video_path,
            filename="two.mp4",
            mime_type="video/mp4",
        ),
    ]

    result = await client.send_message("200", "album", media=media_items)

    assert result.message_id == "mid-200"
    send_call = client.bot.send_message_calls[0]  # type: ignore[attr-defined]
    assert len(send_call["attachments"]) == 2


@pytest.mark.asyncio
async def test_max_error_marks_entity_too_large_as_non_retryable() -> None:
    error = _max_error(
        cast(
            Any,
            FakeMaxApiError(
                code=413,
                raw="Request Entity Too Large",
                message="too large",
            ),
        )
    )

    assert error.code == "entity_too_large"
    assert error.retryable is False
    assert error.http_status == 413


@pytest.mark.asyncio
async def test_edit_message_caption_only_uses_html_format_when_available() -> None:
    client = make_client()
    client.bot = FakeEditBot()  # type: ignore[attr-defined]
    client.rate_limiter = cast(Any, NoopRateLimiter())  # type: ignore[attr-defined]

    await client.edit_message(
        "200",
        "mid-1",
        "updated",
        text_html="<i>updated</i>",
        has_media=True,
    )

    assert len(client.bot.request_calls) == 1  # type: ignore[attr-defined]
    request_call = client.bot.request_calls[0]  # type: ignore[attr-defined]
    assert request_call["json"] == {
        "text": "<i>updated</i>",
        "format": TextFormat.HTML.value,
    }


@pytest.mark.asyncio
async def test_edit_message_replaces_media_via_max_sdk_edit(tmp_path: Path) -> None:
    client = make_client()
    client.bot = FakeEditBot()  # type: ignore[attr-defined]
    client.rate_limiter = cast(Any, NoopRateLimiter())  # type: ignore[attr-defined]
    file_path = tmp_path / "replacement.jpg"
    file_path.write_bytes(b"jpg")
    media = LocalMediaFile(
        kind=MediaKind.IMAGE,
        path=file_path,
        filename="replacement.jpg",
        mime_type="image/jpeg",
    )

    await client.edit_message(
        "200",
        "mid-1",
        "updated",
        has_media=True,
        replacement_media=media,
    )

    assert client.bot.request_calls == []  # type: ignore[attr-defined]
    assert len(client.bot.edit_message_calls) == 1  # type: ignore[attr-defined]
    edit_call = client.bot.edit_message_calls[0]  # type: ignore[attr-defined]
    assert edit_call["message_id"] == "mid-1"
    assert edit_call["text"] == "updated"
    assert "sleep_after_input_media" not in edit_call
    assert len(edit_call["attachments"]) == 1


@pytest.mark.asyncio
async def test_edit_message_replaces_media_with_html_format_when_available(
    tmp_path: Path,
) -> None:
    client = make_client()
    client.bot = FakeEditBot()  # type: ignore[attr-defined]
    client.rate_limiter = cast(Any, NoopRateLimiter())  # type: ignore[attr-defined]
    file_path = tmp_path / "replacement.jpg"
    file_path.write_bytes(b"jpg")
    media = LocalMediaFile(
        kind=MediaKind.IMAGE,
        path=file_path,
        filename="replacement.jpg",
        mime_type="image/jpeg",
    )

    await client.edit_message(
        "200",
        "mid-1",
        "updated",
        text_html="<u>updated</u>",
        has_media=True,
        replacement_media=media,
    )

    assert len(client.bot.edit_message_calls) == 1  # type: ignore[attr-defined]
    edit_call = client.bot.edit_message_calls[0]  # type: ignore[attr-defined]
    assert edit_call["text"] == "<u>updated</u>"
    assert edit_call["format"] == TextFormat.HTML
    assert "sleep_after_input_media" not in edit_call


@pytest.mark.asyncio
async def test_edit_message_replaces_media_group_with_multiple_attachments(
    tmp_path: Path,
) -> None:
    client = make_client()
    client.bot = FakeEditBot()  # type: ignore[attr-defined]
    client.rate_limiter = cast(Any, NoopRateLimiter())  # type: ignore[attr-defined]
    photo_path = tmp_path / "one.jpg"
    video_path = tmp_path / "two.mp4"
    photo_path.write_bytes(b"jpg")
    video_path.write_bytes(b"mp4")
    media_items = [
        LocalMediaFile(
            kind=MediaKind.IMAGE,
            path=photo_path,
            filename="one.jpg",
            mime_type="image/jpeg",
        ),
        LocalMediaFile(
            kind=MediaKind.VIDEO,
            path=video_path,
            filename="two.mp4",
            mime_type="video/mp4",
        ),
    ]

    await client.edit_message(
        "200",
        "mid-1",
        "updated",
        has_media=True,
        replacement_media=media_items,
    )

    edit_call = client.bot.edit_message_calls[0]  # type: ignore[attr-defined]
    assert len(edit_call["attachments"]) == 2
