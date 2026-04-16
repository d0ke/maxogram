from __future__ import annotations

import pytest

from maxogram.domain import Platform, UserIdentity
from maxogram.services.rendering import (
    default_alias,
    render_audio_caption,
    render_audio_caption_html,
    render_media_caption,
    render_media_caption_html,
    render_mirror_html,
    render_mirror_text,
    sanitize_alias,
)


def test_sanitize_alias_collapses_space_and_removes_control_chars():
    assert sanitize_alias(" Alice\u200f \n  Bob ") == "Alice Bob"


def test_sanitize_alias_rejects_empty_alias():
    with pytest.raises(ValueError):
        sanitize_alias("\u200f\n")


def test_default_alias_uses_identity_name():
    identity = UserIdentity(
        platform=Platform.TELEGRAM,
        user_id="42",
        first_name="Alice",
        last_name="Bob",
    )
    assert default_alias(identity, "42") == "Alice Bob"


def test_render_mirror_text_includes_forward_and_reply_hints():
    rendered = render_mirror_text(
        "Alice",
        "hello",
        forwarded=True,
        reply_hint="123",
    )
    assert rendered == "[forwarded]\n[reply to 123]\nAlice: hello"


def test_render_media_caption_uses_alias_only_for_empty_media_text():
    rendered = render_media_caption(
        "Alice",
        None,
        forwarded=True,
        reply_hint="123",
    )
    assert rendered == "[forwarded]\n[reply to 123]\nAlice:"


def test_render_audio_caption_uses_audio_label_and_preserves_prefixes():
    rendered = render_audio_caption(
        "Alice",
        None,
        forwarded=True,
        reply_hint="123",
    )
    assert rendered == "[forwarded]\n[reply to 123]\n🔊 Alice"


def test_render_audio_caption_appends_text_on_next_line():
    rendered = render_audio_caption("Alice", "listen")
    assert rendered == "🔊 Alice\nlisten"


def test_render_mirror_html_keeps_prefixes_plain_and_body_formatted():
    rendered = render_mirror_html(
        "Alice <Admin>",
        "hello",
        "<i>hello</i>",
        forwarded=True,
        reply_hint="123",
    )
    assert rendered == "[forwarded]\n[reply to 123]\nAlice &lt;Admin&gt;: <i>hello</i>"


def test_render_media_caption_html_returns_none_without_formatted_body():
    assert render_media_caption_html("Alice", "caption", None) is None


def test_render_audio_caption_html_keeps_prefixes_plain_and_body_formatted():
    rendered = render_audio_caption_html(
        "Alice <Admin>",
        "listen",
        "<i>listen</i>",
        forwarded=True,
        reply_hint="123",
    )
    assert rendered == "[forwarded]\n[reply to 123]\n🔊 Alice &lt;Admin&gt;\n<i>listen</i>"
