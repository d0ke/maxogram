from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

poller_lag_seconds = Gauge(
    "maxogram_poller_lag_seconds",
    "Seconds since newest processed inbox update",
    ["platform"],
)
inbox_queue_depth = Gauge("maxogram_inbox_queue_depth", "Inbox rows waiting")
outbox_queue_depth = Gauge("maxogram_outbox_queue_depth", "Outbox rows waiting")
delivery_latency = Histogram(
    "maxogram_delivery_latency_seconds",
    "Delivery latency by destination platform",
    ["platform"],
)
retry_total = Counter("maxogram_retry_total", "Retry decisions", ["platform"])
dlq_total = Counter("maxogram_dlq_total", "Dead-lettered tasks", ["platform"])
duplicate_update_total = Counter(
    "maxogram_duplicate_update_total",
    "Duplicate inbox updates",
    ["platform"],
)
mutation_event_total = Counter(
    "maxogram_mutation_event_total",
    "Normalized edit/delete events by source platform",
    ["platform", "event_type"],
)
attachment_not_ready_total = Counter(
    "maxogram_attachment_not_ready_total",
    "MAX attachment.not.ready retry signals",
)
telegram_media_oversize_total = Counter(
    "maxogram_telegram_media_oversize_total",
    "Telegram media above the bot download limit",
    ["kind"],
)
telegram_skipped_update_total = Counter(
    "maxogram_telegram_skipped_update_total",
    "Telegram updates skipped during polling after serialization failures",
    ["reason"],
)
rate_limit_total = Counter(
    "maxogram_rate_limit_429_total",
    "Rate limit responses",
    ["platform"],
)
