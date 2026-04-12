from __future__ import annotations

from typing import Any

from maxogram.domain import EventType, Platform
from maxogram.services.dedup import stable_json_hash
from maxogram.services.media import TELEGRAM_DOWNLOAD_LIMIT_BYTES
from maxogram.services.normalization import normalize_update


def test_normalize_telegram_text_message():
    normalized = normalize_update(
        Platform.TELEGRAM,
        {
            "update_id": 1,
            "message": {
                "message_id": 2,
                "date": 1_700_000_000,
                "chat": {"id": -100},
                "from": {"id": 42, "first_name": "Alice", "is_bot": False},
                "text": "hello",
            },
        },
    )

    assert normalized is not None
    assert normalized.platform == Platform.TELEGRAM
    assert normalized.event_type == EventType.MESSAGE_CREATED
    assert normalized.chat_id == "-100"
    assert normalized.message_id == "2"
    assert normalized.event_version is None
    assert normalized.text == "hello"
    assert normalized.formatted_html is None


def test_normalize_telegram_text_message_accepts_from_user():
    normalized = normalize_update(
        Platform.TELEGRAM,
        {
            "update_id": 101,
            "message": {
                "message_id": 202,
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
    )

    assert normalized is not None
    assert normalized.user_id == "42"
    assert normalized.identity is not None
    assert normalized.identity.first_name == "Alice"
    assert normalized.identity.last_name == "Bob"
    assert normalized.is_bot_message is False


def test_normalize_telegram_entities_build_formatted_html():
    normalized = normalize_update(
        Platform.TELEGRAM,
        {
            "update_id": 3,
            "message": {
                "message_id": 4,
                "date": 1_700_000_000,
                "chat": {"id": -100},
                "from": {"id": 42, "first_name": "Alice", "is_bot": False},
                "text": "hello world",
                "entities": [
                    {"type": "bold", "offset": 0, "length": 5},
                    {
                        "type": "text_link",
                        "offset": 6,
                        "length": 5,
                        "url": "https://example.test",
                    },
                ],
            },
        },
    )

    assert normalized is not None
    assert normalized.formatted_html == (
        '<b>hello</b> <a href="https://example.test">world</a>'
    )


def test_normalize_telegram_caption_entities_build_formatted_html():
    normalized = normalize_update(
        Platform.TELEGRAM,
        {
            "update_id": 4,
            "message": {
                "message_id": 5,
                "date": 1_700_000_000,
                "chat": {"id": -100},
                "from": {"id": 42, "first_name": "Alice", "is_bot": False},
                "caption": "photo https://example.test",
                "caption_entities": [
                    {"type": "italic", "offset": 0, "length": 5},
                    {"type": "url", "offset": 6, "length": 20},
                ],
                "photo": [{"file_id": "p1", "file_size": 1}],
            },
        },
    )

    assert normalized is not None
    assert normalized.formatted_html == (
        '<i>photo</i> <a href="https://example.test">https://example.test</a>'
    )


def test_normalize_telegram_unsupported_entities_degrade_without_html():
    normalized = normalize_update(
        Platform.TELEGRAM,
        {
            "update_id": 5,
            "message": {
                "message_id": 6,
                "date": 1_700_000_000,
                "chat": {"id": -100},
                "from": {"id": 42, "first_name": "Alice", "is_bot": False},
                "text": "hidden",
                "entities": [{"type": "spoiler", "offset": 0, "length": 6}],
            },
        },
    )

    assert normalized is not None
    assert normalized.text == "hidden"
    assert normalized.formatted_html is None


def test_normalize_telegram_edited_message_uses_edit_date_as_version():
    normalized = normalize_update(
        Platform.TELEGRAM,
        {
            "update_id": 2,
            "edited_message": {
                "message_id": 3,
                "date": 1_700_000_000,
                "edit_date": 1_700_000_099,
                "chat": {"id": -100},
                "from": {"id": 42, "first_name": "Alice", "is_bot": False},
                "text": "updated",
            },
        },
    )

    assert normalized is not None
    assert normalized.event_type == EventType.MESSAGE_EDITED
    assert normalized.event_version == 1_700_000_099


def test_normalize_telegram_edited_message_accepts_from_user():
    normalized = normalize_update(
        Platform.TELEGRAM,
        {
            "update_id": 102,
            "edited_message": {
                "message_id": 303,
                "date": 1_700_000_000,
                "edit_date": 1_700_000_111,
                "chat": {"id": -100},
                "from_user": {"id": 42, "first_name": "Alice", "is_bot": False},
                "text": "updated",
            },
        },
    )

    assert normalized is not None
    assert normalized.user_id == "42"
    assert normalized.identity is not None
    assert normalized.identity.first_name == "Alice"
    assert normalized.event_version == 1_700_000_111


def test_normalize_telegram_reply_to_message_accepts_from_user():
    normalized = normalize_update(
        Platform.TELEGRAM,
        {
            "update_id": 103,
            "message": {
                "message_id": 304,
                "date": 1_700_000_000,
                "chat": {"id": -100},
                "from_user": {"id": 42, "first_name": "Alice", "is_bot": False},
                "text": "reply",
                "reply_to_message": {
                    "message_id": 200,
                    "from_user": {"id": 99, "first_name": "Bob", "is_bot": False},
                },
            },
        },
    )

    assert normalized is not None
    assert normalized.reply_to_message_id == "200"
    assert normalized.reply_to_user_id == "99"


def test_normalize_telegram_forwarded_bot_bridge_text_unwraps_and_keeps_entities():
    normalized = normalize_update(
        Platform.TELEGRAM,
        {
            "update_id": 104,
            "message": {
                "message_id": 305,
                "date": 1_700_000_000,
                "chat": {"id": -100},
                "from_user": {"id": 42, "first_name": "Alice", "is_bot": False},
                "text": "[forwarded]\nAn😁: hello",
                "entities": [{"type": "bold", "offset": 18, "length": 5}],
                "forward_from": {"id": 700, "first_name": "Bridge", "is_bot": True},
                "forward_origin": {
                    "type": "user",
                    "sender_user": {
                        "id": 700,
                        "first_name": "Bridge",
                        "is_bot": True,
                    },
                },
            },
        },
    )

    assert normalized is not None
    assert normalized.text == "hello"
    assert normalized.formatted_html == "<b>hello</b>"
    assert normalized.forwarded is True


def test_normalize_telegram_forwarded_bot_bridge_text_strips_old_prefixes():
    normalized = normalize_update(
        Platform.TELEGRAM,
        {
            "update_id": 105,
            "message": {
                "message_id": 306,
                "date": 1_700_000_000,
                "chat": {"id": -100},
                "from_user": {"id": 42, "first_name": "Alice", "is_bot": False},
                "text": "[forwarded]\n[reply to 123]\nAlice: hello",
                "forward_from": {"id": 700, "first_name": "Bridge", "is_bot": True},
            },
        },
    )

    assert normalized is not None
    assert normalized.text == "hello"
    assert normalized.forwarded is True


def test_normalize_telegram_forwarded_user_message_keeps_alias_like_text():
    normalized = normalize_update(
        Platform.TELEGRAM,
        {
            "update_id": 106,
            "message": {
                "message_id": 307,
                "date": 1_700_000_000,
                "chat": {"id": -100},
                "from_user": {"id": 42, "first_name": "Alice", "is_bot": False},
                "text": "Bob: hello",
                "forward_from": {"id": 777, "first_name": "Bob", "is_bot": False},
                "forward_origin": {
                    "type": "user",
                    "sender_user": {"id": 777, "first_name": "Bob", "is_bot": False},
                },
            },
        },
    )

    assert normalized is not None
    assert normalized.text == "Bob: hello"
    assert normalized.forwarded is True


def test_normalize_max_text_message():
    normalized = normalize_update(
        Platform.MAX,
        {
            "update_type": "message_created",
            "message": {
                "recipient": {"chat_id": 100},
                "sender": {"user_id": 42, "first_name": "Alice"},
                "body": {"mid": "mid1", "seq": 7, "text": "hello"},
            },
        },
    )

    assert normalized is not None
    assert normalized.platform == Platform.MAX
    assert normalized.event_type == EventType.MESSAGE_CREATED
    assert normalized.chat_id == "100"
    assert normalized.message_id == "mid1"
    assert normalized.formatted_html is None


def test_normalize_max_markup_builds_formatted_html():
    normalized = normalize_update(
        Platform.MAX,
        {
            "update_type": "message_created",
            "message": {
                "recipient": {"chat_id": 100},
                "sender": {"user_id": 42, "first_name": "Alice"},
                "body": {
                    "mid": "mid-markup",
                    "seq": 9,
                    "text": "hello link",
                    "markup": [
                        {"type": "strong", "from": 0, "length": 5},
                        {
                            "type": "link",
                            "from": 6,
                            "length": 4,
                            "url": "https://example.test",
                        },
                    ],
                },
            },
        },
    )

    assert normalized is not None
    assert normalized.formatted_html == (
        '<b>hello</b> <a href="https://example.test">link</a>'
    )


def test_normalize_max_reply_uses_linked_message_mid():
    normalized = normalize_update(
        Platform.MAX,
        {
            "update_type": "message_created",
            "message": {
                "recipient": {"chat_id": 100},
                "sender": {"user_id": 42, "first_name": "Alice"},
                "link": {
                    "type": "reply",
                    "message": {"mid": "mid-parent"},
                },
                "body": {"mid": "mid-child", "seq": 8, "text": "hello"},
            },
        },
    )

    assert normalized is not None
    assert normalized.reply_to_message_id == "mid-parent"


def test_normalize_max_forward_uses_linked_message_text_and_unwraps_bridge_wrapper():
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
                        "text": "[forwarded]\nAn😁: hello",
                        "markup": [{"type": "strong", "from": 18, "length": 5}],
                    },
                },
                "body": {"mid": "mid-child", "seq": 8, "text": ""},
            },
        },
    )

    assert normalized is not None
    assert normalized.message_id == "mid-child"
    assert normalized.text == "hello"
    assert normalized.formatted_html == "<b>hello</b>"
    assert normalized.forwarded is True


def test_normalize_max_forward_uses_linked_message_attachments_when_outer_body_is_empty():
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
                        "text": "[forwarded]\nAlice:",
                        "attachments": [
                            {
                                "type": "image",
                                "payload": {
                                    "photo_id": 123,
                                    "url": "https://example.test/photo.jpg",
                                    "token": "photo-token",
                                },
                            }
                        ],
                    },
                },
                "body": {"mid": "mid-child", "seq": 8, "text": ""},
            },
        },
    )

    assert normalized is not None
    assert normalized.text is None
    media = _media_payload(normalized.payload)
    assert media["supported"] is True
    assert media["kind"] == "image"
    assert media["payload"]["identity"] == "max:image:id:photo_id:123"


def test_normalize_max_edited_message_uses_raw_fingerprint_as_version():
    raw = {
        "update_type": "message_edited",
        "timestamp": 1_700_000_500_000,
        "message": {
            "recipient": {"chat_id": 100},
            "sender": {"user_id": 42, "first_name": "Alice"},
            "body": {"mid": "mid-child", "seq": 11, "text": "updated"},
        },
    }
    normalized = normalize_update(
        Platform.MAX,
        raw,
    )

    assert normalized is not None
    assert normalized.event_type == EventType.MESSAGE_EDITED
    assert normalized.message_id == "mid-child"
    assert normalized.event_version == stable_json_hash(raw)


def test_normalize_max_edits_produce_distinct_versions_for_distinct_raw_updates():
    first_raw = {
        "update_type": "message_edited",
        "timestamp": 1_700_000_500_000,
        "message": {
            "recipient": {"chat_id": 100},
            "sender": {"user_id": 42, "first_name": "Alice"},
            "body": {"mid": "mid-child", "seq": 11, "text": "Test2"},
        },
    }
    second_raw = {
        "update_type": "message_edited",
        "timestamp": 1_700_000_500_000,
        "message": {
            "recipient": {"chat_id": 100},
            "sender": {"user_id": 42, "first_name": "Alice"},
            "body": {"mid": "mid-child", "seq": 11, "text": "Test3"},
        },
    }

    first = normalize_update(Platform.MAX, first_raw)
    second = normalize_update(Platform.MAX, second_raw)

    assert first is not None
    assert second is not None
    assert first.event_version != second.event_version
    assert first.dedup_key != second.dedup_key


def test_normalize_max_removed_message_uses_top_level_fields():
    normalized = normalize_update(
        Platform.MAX,
        {
            "update_type": "message_removed",
            "chat_id": 100,
            "user_id": 42,
            "message_id": "mid-removed",
            "timestamp": 1_700_000_600_000,
        },
    )

    assert normalized is not None
    assert normalized.event_type == EventType.MESSAGE_DELETED
    assert normalized.chat_id == "100"
    assert normalized.message_id == "mid-removed"
    assert normalized.event_version == 1_700_000_600_000


def test_normalize_telegram_photo_keeps_largest_file_id():
    normalized = normalize_update(
        Platform.TELEGRAM,
        {
            "update_id": 10,
            "message": {
                "message_id": 20,
                "date": 1_700_000_000,
                "chat": {"id": -100},
                "from": {"id": 42, "first_name": "Alice", "is_bot": False},
                "photo": [
                    {"file_id": "small", "width": 50, "height": 50, "file_size": 10},
                    {"file_id": "large", "width": 100, "height": 100, "file_size": 20},
                ],
            },
        },
    )

    assert normalized is not None
    media = _media_payload(normalized.payload)
    assert media["supported"] is True
    assert media["kind"] == "image"
    assert media["text_hint"] == "[photo]"
    assert media["payload"]["source"]["file_id"] == "large"
    assert media["payload"]["identity"] == "telegram:image:id:large"


def test_normalize_telegram_document_keeps_download_metadata():
    normalized = normalize_update(
        Platform.TELEGRAM,
        {
            "update_id": 11,
            "message": {
                "message_id": 21,
                "date": 1_700_000_000,
                "chat": {"id": -100},
                "from": {"id": 42, "first_name": "Alice", "is_bot": False},
                "document": {
                    "file_id": "doc-id",
                    "file_name": "file.pdf",
                    "mime_type": "application/pdf",
                    "file_size": 1024,
                },
            },
        },
    )

    assert normalized is not None
    media = _media_payload(normalized.payload)
    assert media["supported"] is True
    assert media["kind"] == "document"
    assert media["payload"]["filename"] == "file.pdf"
    assert media["payload"]["mime_type"] == "application/pdf"
    assert media["payload"]["identity"] == "telegram:document:id:doc-id"


def test_normalize_telegram_prefers_file_unique_id_for_media_identity():
    normalized = normalize_update(
        Platform.TELEGRAM,
        {
            "update_id": 14,
            "message": {
                "message_id": 24,
                "date": 1_700_000_000,
                "chat": {"id": -100},
                "from": {"id": 42, "first_name": "Alice", "is_bot": False},
                "document": {
                    "file_id": "doc-id-1",
                    "file_unique_id": "doc-unique-id",
                    "file_name": "file.pdf",
                    "mime_type": "application/pdf",
                    "file_size": 1024,
                },
            },
        },
    )

    assert normalized is not None
    media = _media_payload(normalized.payload)
    assert media["payload"]["identity"] == "telegram:document:id:doc-unique-id"


def test_normalize_telegram_media_replacement_changes_identity():
    first = normalize_update(
        Platform.TELEGRAM,
        {
            "update_id": 15,
            "message": {
                "message_id": 25,
                "date": 1_700_000_000,
                "chat": {"id": -100},
                "from": {"id": 42, "first_name": "Alice", "is_bot": False},
                "photo": [
                    {
                        "file_id": "photo-file-1",
                        "file_unique_id": "photo-unique-1",
                        "file_size": 100,
                    }
                ],
            },
        },
    )
    second = normalize_update(
        Platform.TELEGRAM,
        {
            "update_id": 16,
            "edited_message": {
                "message_id": 25,
                "date": 1_700_000_000,
                "edit_date": 1_700_000_050,
                "chat": {"id": -100},
                "from": {"id": 42, "first_name": "Alice", "is_bot": False},
                "photo": [
                    {
                        "file_id": "photo-file-2",
                        "file_unique_id": "photo-unique-2",
                        "file_size": 100,
                    }
                ],
            },
        },
    )

    assert first is not None
    assert second is not None
    first_media = _media_payload(first.payload)
    second_media = _media_payload(second.payload)
    assert first_media["payload"]["identity"] != second_media["payload"]["identity"]


def test_normalize_telegram_gif_animation_prefers_animation_over_document():
    normalized = normalize_update(
        Platform.TELEGRAM,
        {
            "update_id": 111,
            "message": {
                "message_id": 211,
                "date": 1_700_000_000,
                "chat": {"id": -100},
                "from": {"id": 42, "first_name": "Alice", "is_bot": False},
                "animation": {
                    "file_id": "gif-id",
                    "file_name": "party.gif",
                    "mime_type": "image/gif",
                    "file_size": 1024,
                },
                "document": {
                    "file_id": "doc-id",
                    "file_name": "party.mp4",
                    "mime_type": "video/mp4",
                    "file_size": 2048,
                },
            },
        },
    )

    assert normalized is not None
    media = _media_payload(normalized.payload)
    assert media["supported"] is True
    assert media["kind"] == "image"
    assert media["text_hint"] == "[gif]"
    assert media["payload"]["presentation"] == "animation"
    assert media["payload"]["filename"] == "party.gif"
    assert media["payload"]["source"]["file_id"] == "gif-id"
    assert media["payload"]["identity"] == "telegram:image:id:gif-id"


def test_normalize_telegram_mp4_animation_uses_video_kind():
    normalized = normalize_update(
        Platform.TELEGRAM,
        {
            "update_id": 112,
            "message": {
                "message_id": 212,
                "date": 1_700_000_000,
                "chat": {"id": -100},
                "from": {"id": 42, "first_name": "Alice", "is_bot": False},
                "animation": {
                    "file_id": "anim-id",
                    "file_name": "loop.mp4",
                    "mime_type": "video/mp4",
                    "file_size": 2048,
                },
            },
        },
    )

    assert normalized is not None
    media = _media_payload(normalized.payload)
    assert media["supported"] is True
    assert media["kind"] == "video"
    assert media["text_hint"] == "[animation]"
    assert media["payload"]["presentation"] == "animation"
    assert media["payload"]["filename"] == "loop.mp4"
    assert media["payload"]["source"]["file_id"] == "anim-id"
    assert media["payload"]["identity"] == "telegram:video:id:anim-id"


def test_normalize_telegram_oversized_video_uses_explicit_limit_hint():
    normalized = normalize_update(
        Platform.TELEGRAM,
        {
            "update_id": 12,
            "message": {
                "message_id": 22,
                "date": 1_700_000_000,
                "chat": {"id": -100},
                "from": {"id": 42, "first_name": "Alice", "is_bot": False},
                "video": {
                    "file_id": "video-id",
                    "file_size": TELEGRAM_DOWNLOAD_LIMIT_BYTES + 1,
                },
            },
        },
    )

    assert normalized is not None
    media = _media_payload(normalized.payload)
    assert media["supported"] is False
    assert (
        media["text_hint"]
        == "[video unavailable: exceeds Telegram bot 20 MB download limit]"
    )


def test_normalize_telegram_oversized_document_uses_explicit_limit_hint():
    normalized = normalize_update(
        Platform.TELEGRAM,
        {
            "update_id": 13,
            "message": {
                "message_id": 23,
                "date": 1_700_000_000,
                "chat": {"id": -100},
                "from": {"id": 42, "first_name": "Alice", "is_bot": False},
                "document": {
                    "file_id": "document-id",
                    "file_name": "archive.zip",
                    "file_size": TELEGRAM_DOWNLOAD_LIMIT_BYTES + 5,
                },
            },
        },
    )

    assert normalized is not None
    media = _media_payload(normalized.payload)
    assert media["supported"] is False
    assert (
        media["text_hint"]
        == "[document unavailable: exceeds Telegram bot 20 MB download limit]"
    )


def test_normalize_telegram_video_sticker_is_relayable_video():
    normalized = normalize_update(
        Platform.TELEGRAM,
        {
            "update_id": 12,
            "message": {
                "message_id": 22,
                "date": 1_700_000_000,
                "chat": {"id": -100},
                "from": {"id": 42, "first_name": "Alice", "is_bot": False},
                "sticker": {
                    "file_id": "sticker-id",
                    "is_video": True,
                    "is_animated": False,
                    "file_size": 4096,
                },
            },
        },
    )

    assert normalized is not None
    media = _media_payload(normalized.payload)
    assert media["supported"] is True
    assert media["kind"] == "video"
    assert media["payload"]["sticker_variant"] == "video"
    assert media["payload"]["identity"] == "telegram:video:id:sticker-id"


def test_normalize_max_image_attachment_keeps_direct_url():
    normalized = normalize_update(
        Platform.MAX,
        {
            "update_type": "message_created",
            "message": {
                "recipient": {"chat_id": 100},
                "sender": {"user_id": 42, "first_name": "Alice"},
                "body": {
                    "mid": "mid1",
                    "seq": 7,
                    "attachments": [
                        {
                            "type": "image",
                            "payload": {
                                "url": "https://example.test/image.jpg",
                                "token": "img-token",
                            },
                        }
                    ],
                },
            },
        },
    )

    assert normalized is not None
    media = _media_payload(normalized.payload)
    assert media["supported"] is True
    assert media["kind"] == "image"
    assert media["payload"]["source"]["url"] == "https://example.test/image.jpg"
    assert media["payload"]["identity"].startswith("max:image:path:")


def test_normalize_max_photo_edit_keeps_identity_when_only_token_changes():
    first = normalize_update(
        Platform.MAX,
        {
            "update_type": "message_created",
            "message": {
                "recipient": {"chat_id": 100},
                "sender": {"user_id": 42, "first_name": "Alice"},
                "body": {
                    "mid": "mid-photo",
                    "seq": 7,
                    "attachments": [
                        {
                            "type": "image",
                            "payload": {
                                "photo_id": 123,
                                "url": "https://i.oneme.ru/i?r=old",
                                "token": "old-token",
                            },
                        }
                    ],
                },
            },
        },
    )
    second = normalize_update(
        Platform.MAX,
        {
            "update_type": "message_edited",
            "message": {
                "recipient": {"chat_id": 100},
                "sender": {"user_id": 42, "first_name": "Alice"},
                "body": {
                    "mid": "mid-photo",
                    "seq": 7,
                    "text": "updated",
                    "attachments": [
                        {
                            "type": "image",
                            "payload": {
                                "photo_id": 123,
                                "url": "https://i.oneme.ru/i?r=new",
                                "token": "new-token",
                            },
                        }
                    ],
                },
            },
        },
    )

    assert first is not None
    assert second is not None
    first_media = _media_payload(first.payload)
    second_media = _media_payload(second.payload)
    assert first_media["payload"]["identity"] == "max:image:id:photo_id:123"
    assert second_media["payload"]["identity"] == "max:image:id:photo_id:123"


def test_normalize_max_attachment_identity_ignores_query_token_churn_for_same_file():
    first = normalize_update(
        Platform.MAX,
        {
            "update_type": "message_created",
            "message": {
                "recipient": {"chat_id": 100},
                "sender": {"user_id": 42, "first_name": "Alice"},
                "body": {
                    "mid": "mid-file",
                    "seq": 7,
                    "attachments": [
                        {
                            "type": "file",
                            "filename": "report.pdf",
                            "mime_type": "application/pdf",
                            "payload": {"url": "https://example.test/report.pdf?token=old"},
                        }
                    ],
                },
            },
        },
    )
    second = normalize_update(
        Platform.MAX,
        {
            "update_type": "message_edited",
            "message": {
                "recipient": {"chat_id": 100},
                "sender": {"user_id": 42, "first_name": "Alice"},
                "body": {
                    "mid": "mid-file",
                    "seq": 7,
                    "attachments": [
                        {
                            "type": "file",
                            "filename": "report.pdf",
                            "mime_type": "application/pdf",
                            "payload": {"url": "https://example.test/report.pdf?token=new"},
                        }
                    ],
                },
            },
        },
    )

    assert first is not None
    assert second is not None
    first_media = _media_payload(first.payload)
    second_media = _media_payload(second.payload)
    assert first_media["payload"]["identity"] == second_media["payload"]["identity"]


def test_normalize_max_photo_replacement_changes_identity_when_photo_id_changes():
    first = normalize_update(
        Platform.MAX,
        {
            "update_type": "message_created",
            "message": {
                "recipient": {"chat_id": 100},
                "sender": {"user_id": 42, "first_name": "Alice"},
                "body": {
                    "mid": "mid-photo",
                    "seq": 7,
                    "attachments": [
                        {
                            "type": "image",
                            "payload": {
                                "photo_id": 123,
                                "url": "https://i.oneme.ru/i?r=old",
                                "token": "old-token",
                            },
                        }
                    ],
                },
            },
        },
    )
    second = normalize_update(
        Platform.MAX,
        {
            "update_type": "message_edited",
            "message": {
                "recipient": {"chat_id": 100},
                "sender": {"user_id": 42, "first_name": "Alice"},
                "body": {
                    "mid": "mid-photo",
                    "seq": 7,
                    "attachments": [
                        {
                            "type": "image",
                            "payload": {
                                "photo_id": 456,
                                "url": "https://i.oneme.ru/i?r=new",
                                "token": "new-token",
                            },
                        }
                    ],
                },
            },
        },
    )

    assert first is not None
    assert second is not None
    first_media = _media_payload(first.payload)
    second_media = _media_payload(second.payload)
    assert first_media["payload"]["identity"] == "max:image:id:photo_id:123"
    assert second_media["payload"]["identity"] == "max:image:id:photo_id:456"


def test_normalize_max_gif_image_attachment_marks_animation_presentation():
    normalized = normalize_update(
        Platform.MAX,
        {
            "update_type": "message_created",
            "message": {
                "recipient": {"chat_id": 100},
                "sender": {"user_id": 42, "first_name": "Alice"},
                "body": {
                    "mid": "mid-gif",
                    "seq": 8,
                    "attachments": [
                        {
                            "type": "image",
                            "filename": "loop.gif",
                            "mime_type": "image/gif",
                            "payload": {
                                "url": "https://example.test/loop.gif",
                                "token": "gif-token",
                            },
                        }
                    ],
                },
            },
        },
    )

    assert normalized is not None
    media = _media_payload(normalized.payload)
    assert media["supported"] is True
    assert media["kind"] == "image"
    assert media["text_hint"] == "[gif]"
    assert media["payload"]["presentation"] == "animation"
    assert media["payload"]["source"]["url"] == "https://example.test/loop.gif"
    assert media["payload"]["identity"].startswith("max:image:path:")


def test_normalize_max_video_attachment_uses_best_video_url():
    normalized = normalize_update(
        Platform.MAX,
        {
            "update_type": "message_created",
            "message": {
                "recipient": {"chat_id": 100},
                "sender": {"user_id": 42, "first_name": "Alice"},
                "body": {
                    "mid": "mid2",
                    "seq": 8,
                    "attachments": [
                        {
                            "type": "video",
                            "token": "video-token",
                            "urls": {
                                "mp4_360": "https://example.test/video-360.mp4",
                                "mp4_720": "https://example.test/video-720.mp4",
                            },
                        }
                    ],
                },
            },
        },
    )

    assert normalized is not None
    media = _media_payload(normalized.payload)
    assert media["supported"] is True
    assert media["kind"] == "video"
    assert media["payload"]["source"]["url"] == "https://example.test/video-720.mp4"
    assert media["payload"]["identity"].startswith("max:video:path:")


def test_normalize_max_file_and_sticker_attachments():
    file_update = normalize_update(
        Platform.MAX,
        {
            "update_type": "message_created",
            "message": {
                "recipient": {"chat_id": 100},
                "sender": {"user_id": 42, "first_name": "Alice"},
                "body": {
                    "mid": "mid3",
                    "seq": 9,
                    "attachments": [
                        {
                            "type": "file",
                            "filename": "report.pdf",
                            "payload": {"url": "https://example.test/report.pdf"},
                        }
                    ],
                },
            },
        },
    )
    sticker_update = normalize_update(
        Platform.MAX,
        {
            "update_type": "message_created",
            "message": {
                "recipient": {"chat_id": 100},
                "sender": {"user_id": 42, "first_name": "Alice"},
                "body": {
                    "mid": "mid4",
                    "seq": 10,
                    "attachments": [
                        {
                            "type": "sticker",
                            "payload": {
                                "url": "https://example.test/sticker.webp",
                                "code": "smile",
                            },
                        }
                    ],
                },
            },
        },
    )

    assert file_update is not None
    file_media = _media_payload(file_update.payload)
    assert file_media["kind"] == "document"
    assert file_media["payload"]["filename"] == "report.pdf"
    assert file_media["payload"]["identity"].startswith("max:file:path:")
    assert sticker_update is not None
    sticker_media = _media_payload(sticker_update.payload)
    assert sticker_media["kind"] == "image"
    assert sticker_media["payload"]["sticker_variant"] == "static"
    assert sticker_media["payload"]["identity"] == "max:sticker:id:code:smile"


def _media_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    assert payload is not None
    media = payload.get("media")
    assert isinstance(media, dict)
    return media
