from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from maxogram.db.models import Bridge
from maxogram.db.repositories import Repository
from maxogram.domain import BridgeStatus, CommandContext, CommandReply
from maxogram.services.rendering import sanitize_alias

AdminChecker = Callable[[CommandContext], Awaitable[bool]]


@dataclass(frozen=True, slots=True)
class ParsedCommand:
    root: str
    action: str | None
    args: str


def parse_command(text: str) -> ParsedCommand | None:
    if not text.startswith("/"):
        return None
    first, *rest = text.strip().split(maxsplit=1)
    root = first.split("@", 1)[0].lower()
    args_text = rest[0] if rest else ""
    if root in {"/bridge", "/nick"}:
        action, _, args = args_text.partition(" ")
        return ParsedCommand(
            root=root,
            action=action.lower() or None,
            args=args.strip(),
        )
    return ParsedCommand(root=root, action=None, args=args_text.strip())


class CommandProcessor:
    async def process(
        self,
        session: AsyncSession,
        context: CommandContext,
        *,
        is_admin: AdminChecker,
    ) -> CommandReply | None:
        parsed = parse_command(context.text)
        if parsed is None:
            return None
        repo = Repository(session)
        bridge = await repo.find_bridge_by_chat(
            context.platform, context.chat_id, include_paused=True
        )
        await repo.log_command(
            platform=context.platform,
            chat_id=context.chat_id,
            message_id=context.message_id,
            user_id=context.user_id,
            bridge_id=bridge.bridge_id if bridge else None,
            command=parsed.root,
            args=f"{parsed.action or ''} {parsed.args}".strip() or None,
        )

        if parsed.root == "/bridge":
            text = await self._bridge(repo, context, parsed, bridge, is_admin)
        elif parsed.root == "/nick":
            text = await self._nick(repo, context, parsed, bridge, is_admin)
        elif parsed.root == "/whois":
            text = self._whois(context)
        elif parsed.root == "/dlq":
            text = await self._admin_only(context, bridge, is_admin, "DLQ is recorded.")
        else:
            return None
        return CommandReply(context.platform, context.chat_id, text)

    async def _bridge(
        self,
        repo: Repository,
        context: CommandContext,
        parsed: ParsedCommand,
        bridge: Bridge | None,
        is_admin: AdminChecker,
    ) -> str:
        action = parsed.action or "help"
        if action == "help":
            return (
                "/bridge link, /bridge confirm <code>, /bridge status, "
                "/bridge pause, /bridge resume, /bridge unlink"
            )
        if action == "link":
            code = await repo.create_link_code(
                context.platform, context.chat_id, context.user_id
            )
            return f"Link code: {code}. It expires in 3 minutes."
        if action == "confirm":
            if not await is_admin(context):
                return "Only a platform admin can confirm a bridge."
            if not parsed.args:
                return "Usage: /bridge confirm <code>"
            bridge_id = await repo.consume_link_code(
                parsed.args.strip(), context.platform, context.chat_id, context.user_id
            )
            if bridge_id is None:
                return "Link code is invalid, expired, or points to this chat."
            return "Bridge active."
        if bridge is None:
            return "This chat is not linked yet."
        bridge_id = bridge.bridge_id
        if action == "status":
            return f"Bridge status: {bridge.status.value}."
        if action in {"pause", "resume", "unlink"}:
            if not await is_admin(context):
                return "Only a platform admin can change bridge status."
            status = {
                "pause": BridgeStatus.PAUSED,
                "resume": BridgeStatus.ACTIVE,
                "unlink": BridgeStatus.DELETED,
            }[action]
            await repo.set_bridge_status(bridge_id, status)
            return f"Bridge status changed to {status.value}."
        return "Unknown /bridge command. Use /bridge help."

    async def _nick(
        self,
        repo: Repository,
        context: CommandContext,
        parsed: ParsedCommand,
        bridge: Bridge | None,
        is_admin: AdminChecker,
    ) -> str:
        if bridge is None:
            return "This chat is not linked yet."
        bridge_id: uuid.UUID = bridge.bridge_id
        action = parsed.action or "show"
        target_user_id = context.reply_to_user_id or context.user_id
        is_override = target_user_id != context.user_id
        if is_override and not await is_admin(context):
            return "Only a platform admin can set another member alias."
        if action == "set":
            if not parsed.args:
                return "Usage: /nick set <alias>"
            alias = sanitize_alias(parsed.args)
            await repo.set_alias(
                bridge_id,
                context.platform,
                target_user_id,
                alias,
                context.user_id,
                is_admin_override=is_override,
            )
            return f"Alias set to {alias}."
        if action == "remove":
            await repo.remove_alias(
                bridge_id, context.platform, target_user_id, context.user_id
            )
            return "Alias removed."
        if action == "show":
            current_alias = await repo.get_alias(
                bridge_id, context.platform, target_user_id
            )
            return f"Alias: {current_alias or '(default)'}."
        if action == "list":
            aliases = await repo.list_aliases(bridge_id)
            if not aliases:
                return "No aliases set."
            return "\n".join(
                f"{item.platform.value}:{item.user_id} = {item.alias}"
                for item in aliases[:50]
            )
        return "Unknown /nick command."

    async def _admin_only(
        self,
        context: CommandContext,
        bridge: Bridge | None,
        is_admin: AdminChecker,
        ok_text: str,
    ) -> str:
        if bridge is None:
            return "This chat is not linked yet."
        if not await is_admin(context):
            return "Only a platform admin can use this command."
        return ok_text

    def _whois(self, context: CommandContext) -> str:
        user_id = context.reply_to_user_id or context.user_id
        message_id = context.reply_to_message_id or context.message_id or "unknown"
        return (
            f"platform={context.platform.value} "
            f"user_id={user_id} message_id={message_id}"
        )
