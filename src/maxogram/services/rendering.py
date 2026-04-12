from __future__ import annotations

import re
import unicodedata

from maxogram.domain import UserIdentity
from maxogram.services.text_formatting import escape_html

MAX_ALIAS_CHARS = 32
CONTROL_OR_BIDI = {
    "Cc",
    "Cf",
}


def sanitize_alias(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value)
    cleaned = "".join(
        char for char in normalized if unicodedata.category(char) not in CONTROL_OR_BIDI
    )
    collapsed = re.sub(r"\s+", " ", cleaned).strip()
    if not collapsed:
        raise ValueError("Alias cannot be empty")
    return collapsed[:MAX_ALIAS_CHARS]


def default_alias(identity: UserIdentity | None, fallback_user_id: str | None) -> str:
    if identity is None:
        return f"user {fallback_user_id}" if fallback_user_id else "unknown"
    if identity.first_name:
        base = identity.first_name
        if identity.last_name:
            base = f"{base} {identity.last_name}"
        return sanitize_alias(base)
    if identity.username:
        return sanitize_alias(identity.username.lstrip("@"))
    return f"user {identity.user_id}"


def render_mirror_text(
    alias: str,
    text: str | None,
    *,
    forwarded: bool = False,
    reply_hint: str | None = None,
    media_hint: str | None = None,
) -> str:
    safe_alias = sanitize_alias(alias)
    chunks = _render_prefix_lines(forwarded=forwarded, reply_hint=reply_hint)
    body = text or media_hint or "[unsupported message]"
    chunks.append(f"{safe_alias}: {body}")
    return "\n".join(chunks)


def render_media_caption(
    alias: str,
    text: str | None,
    *,
    forwarded: bool = False,
    reply_hint: str | None = None,
) -> str:
    safe_alias = sanitize_alias(alias)
    chunks = _render_prefix_lines(forwarded=forwarded, reply_hint=reply_hint)
    if text:
        chunks.append(f"{safe_alias}: {text}")
    else:
        chunks.append(f"{safe_alias}:")
    return "\n".join(chunks)


def render_mirror_html(
    alias: str,
    text: str | None,
    formatted_body_html: str | None,
    *,
    forwarded: bool = False,
    reply_hint: str | None = None,
    media_hint: str | None = None,
) -> str | None:
    if formatted_body_html is None:
        return None
    safe_alias = escape_html(sanitize_alias(alias))
    chunks = _render_prefix_lines_html(forwarded=forwarded, reply_hint=reply_hint)
    _ = text, media_hint
    chunks.append(f"{safe_alias}: {formatted_body_html}")
    return "\n".join(chunks)


def render_media_caption_html(
    alias: str,
    text: str | None,
    formatted_body_html: str | None,
    *,
    forwarded: bool = False,
    reply_hint: str | None = None,
) -> str | None:
    _ = text
    if formatted_body_html is None:
        return None
    safe_alias = escape_html(sanitize_alias(alias))
    chunks = _render_prefix_lines_html(forwarded=forwarded, reply_hint=reply_hint)
    chunks.append(f"{safe_alias}: {formatted_body_html}")
    return "\n".join(chunks)


def _render_prefix_lines(
    *,
    forwarded: bool,
    reply_hint: str | None,
) -> list[str]:
    chunks: list[str] = []
    if forwarded:
        chunks.append("[forwarded]")
    if reply_hint:
        chunks.append(f"[reply to {reply_hint}]")
    return chunks


def _render_prefix_lines_html(
    *,
    forwarded: bool,
    reply_hint: str | None,
) -> list[str]:
    chunks: list[str] = []
    if forwarded:
        chunks.append(escape_html("[forwarded]"))
    if reply_hint:
        chunks.append(escape_html(f"[reply to {reply_hint}]"))
    return chunks
