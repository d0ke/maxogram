from __future__ import annotations

from dataclasses import dataclass
from typing import Any

HTML_ESCAPES = {
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
}


@dataclass(frozen=True, slots=True)
class _RichSpan:
    kind: str
    start_utf16: int
    end_utf16: int
    url: str | None = None


_SPAN_ORDER = {
    "bold": 1,
    "italic": 2,
    "underline": 3,
    "strikethrough": 4,
    "code": 5,
    "pre": 6,
    "link": 7,
}


def escape_html(text: str) -> str:
    escaped = text
    for source, replacement in HTML_ESCAPES.items():
        escaped = escaped.replace(source, replacement)
    return escaped


def telegram_entities_to_html(
    text: str | None,
    entities: list[dict[str, Any]] | None,
) -> str | None:
    if text is None or not entities:
        return None
    spans: list[_RichSpan] = []
    for entity in entities:
        span = _telegram_entity_to_span(text, entity)
        if span is not None:
            spans.append(span)
    return _render_supported_html(text, spans)


def max_markup_to_html(
    text: str | None,
    markup: list[dict[str, Any]] | None,
) -> str | None:
    if text is None or not markup:
        return None
    spans: list[_RichSpan] = []
    for item in markup:
        span = _max_markup_to_span(item)
        if span is not None:
            spans.append(span)
    return _render_supported_html(text, spans)


def _telegram_entity_to_span(
    text: str,
    entity: dict[str, Any],
) -> _RichSpan | None:
    entity_type = _string_value(entity.get("type"))
    offset = _int_value(entity.get("offset"))
    length = _int_value(entity.get("length"))
    if entity_type is None or offset is None or length is None or length <= 0:
        return None
    if entity_type == "bold":
        return _RichSpan("bold", offset, offset + length)
    if entity_type == "italic":
        return _RichSpan("italic", offset, offset + length)
    if entity_type == "underline":
        return _RichSpan("underline", offset, offset + length)
    if entity_type == "strikethrough":
        return _RichSpan("strikethrough", offset, offset + length)
    if entity_type == "code":
        return _RichSpan("code", offset, offset + length)
    if entity_type == "pre":
        return _RichSpan("pre", offset, offset + length)
    if entity_type == "text_link":
        url = _string_value(entity.get("url"))
        return (
            _RichSpan("link", offset, offset + length, url=url)
            if url is not None
            else None
        )
    if entity_type == "url":
        url = _slice_utf16(text, offset, offset + length)
        return _RichSpan("link", offset, offset + length, url=url)
    return None


def _max_markup_to_span(item: dict[str, Any]) -> _RichSpan | None:
    markup_type = _string_value(item.get("type"))
    start = _int_value(item.get("from"))
    length = _int_value(item.get("length"))
    if markup_type is None or start is None or length is None or length <= 0:
        return None
    if markup_type == "strong":
        return _RichSpan("bold", start, start + length)
    if markup_type == "emphasized":
        return _RichSpan("italic", start, start + length)
    if markup_type == "underline":
        return _RichSpan("underline", start, start + length)
    if markup_type == "strikethrough":
        return _RichSpan("strikethrough", start, start + length)
    if markup_type == "monospaced":
        return _RichSpan("code", start, start + length)
    if markup_type == "link":
        url = _string_value(item.get("url"))
        return (
            _RichSpan("link", start, start + length, url=url)
            if url is not None
            else None
        )
    return None


def _render_supported_html(text: str, spans: list[_RichSpan]) -> str | None:
    if not spans or not text:
        return None
    utf16_offsets = _utf16_offsets(text)
    if not utf16_offsets:
        return None
    char_styles: list[list[_RichSpan]] = []
    for index, utf16_pos in enumerate(utf16_offsets):
        _ = index
        active = [
            span
            for span in spans
            if span.start_utf16 <= utf16_pos < span.end_utf16
        ]
        active.sort(key=lambda span: (_SPAN_ORDER.get(span.kind, 99), span.start_utf16))
        unique_active: list[_RichSpan] = []
        for span in active:
            if span not in unique_active:
                unique_active.append(span)
        char_styles.append(unique_active)
    if not any(char_styles):
        return None
    parts: list[str] = []
    current_chunk = text[0]
    current_spans = char_styles[0]
    for index, active_spans in enumerate(char_styles[1:], start=1):
        if active_spans == current_spans:
            current_chunk += text[index]
            continue
        parts.append(_wrap_chunk(current_chunk, current_spans))
        current_chunk = text[index]
        current_spans = active_spans
    parts.append(_wrap_chunk(current_chunk, current_spans))
    return "".join(parts)


def _wrap_chunk(chunk: str, spans: list[_RichSpan]) -> str:
    html = escape_html(chunk)
    for span in reversed(spans):
        if span.kind == "bold":
            html = f"<b>{html}</b>"
        elif span.kind == "italic":
            html = f"<i>{html}</i>"
        elif span.kind == "underline":
            html = f"<u>{html}</u>"
        elif span.kind == "strikethrough":
            html = f"<s>{html}</s>"
        elif span.kind == "code":
            html = f"<code>{html}</code>"
        elif span.kind == "pre":
            html = f"<pre>{html}</pre>"
        elif span.kind == "link" and span.url is not None:
            html = f'<a href="{escape_html(span.url)}">{html}</a>'
    return html


def _utf16_offsets(text: str) -> list[int]:
    offsets: list[int] = []
    total = 0
    for char in text:
        offsets.append(total)
        total += 2 if ord(char) > 0xFFFF else 1
    return offsets


def _utf16_to_py_index(text: str, utf16_pos: int) -> int:
    if utf16_pos <= 0:
        return 0
    for index, offset in enumerate(_utf16_offsets(text)):
        if offset >= utf16_pos:
            return index
    return len(text)


def _slice_utf16(text: str, start_utf16: int, end_utf16: int) -> str:
    start = _utf16_to_py_index(text, start_utf16)
    end = _utf16_to_py_index(text, end_utf16)
    return text[start:end]


def _string_value(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _int_value(value: object) -> int | None:
    try:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            return int(value)
    except (TypeError, ValueError):
        return None
    return None
