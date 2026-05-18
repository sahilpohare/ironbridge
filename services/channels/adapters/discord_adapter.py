"""
Discord channel adapter.

Outbound: on_message → send text to the Discord channel the message came from.
Inbound:  discord.py bot listens for messages, routes to thread via receive().

Each Discord channel (or DM) maps to one Ironbridge thread. The mapping is
stored in Channel.config["threads"] as {discord_channel_id: thread_id}.
The bot token is read from Channel.config["bot_token"].

/new   — start a fresh thread for this Discord channel
/reset — alias for /new

Run the bot:
    python -m services.channels.adapters.discord_adapter \\
        --tenant tenant-a --channel <ironbridge_channel_id>

Registration:
    register_adapter(DiscordAdapter()) in main.py to enable outbound delivery.
    The bot process is separate — run it per Channel record.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import discord
import httpx
from cuid2 import cuid_wrapper

from ironbridge.platform.channels.context import ChannelContext
from ironbridge.platform.channels.message import (
    ChannelMessage,
    EventPart,
    ResponseRequestPart,
    TextPart,
)
from ironbridge.shared.db import tenant_session
from ironbridge.shared.derive.repository import SqlAlchemyRepository
from services.channels.adapters.base import BaseChannelAdapter

logger = logging.getLogger(__name__)
_cuid = cuid_wrapper()


class DiscordAdapter(BaseChannelAdapter):
    channel_type = "discord"

    # ── Outbound ──────────────────────────────────────────────────────────────

    def on_message(self, message: ChannelMessage, config: dict, ctx: ChannelContext) -> None:
        """Send assistant/system messages back to the originating Discord channel."""
        if message.role not in ("ASSISTANT", "SYSTEM"):
            return

        # Find the Discord channel_id that maps to this thread
        threads: dict[str, str] = config.get("threads", {})
        discord_channel_id = next(
            (dc_id for dc_id, tid in threads.items() if tid == message.thread_id),
            None,
        )
        if not discord_channel_id:
            return

        text = " ".join(p.text for p in message.parts if isinstance(p, TextPart) and p.text)
        if not text:
            return

        bot_token = config.get("bot_token") or os.environ.get("DISCORD_BOT_TOKEN")
        if not bot_token:
            logger.warning("discord adapter: no bot_token in config")
            return

        try:
            httpx.post(
                f"https://discord.com/api/v10/channels/{discord_channel_id}/messages",
                headers={"Authorization": f"Bot {bot_token}"},
                json={"content": text},
                timeout=10,
            )
        except Exception:
            logger.exception(
                "discord adapter: failed to send message to channel %s", discord_channel_id
            )

    # ── Thread mapping helpers ────────────────────────────────────────────────

    def _get_thread_for_discord_channel(
        self, tenant_id: str, channel_id: str, discord_channel_id: str
    ) -> str | None:
        """Return the active thread_id for a Discord channel, or None."""
        from ironbridge.platform.channels.channel import Channel

        with tenant_session(tenant_id) as db:
            repo = SqlAlchemyRepository(db, Channel)
            ch = repo.find_by_id(channel_id)
            if not ch:
                return None
            return (ch.config or {}).get("threads", {}).get(discord_channel_id)

    def _set_thread_for_discord_channel(
        self, tenant_id: str, channel_id: str, discord_channel_id: str, thread_id: str
    ) -> None:
        """Persist discord_channel_id → thread_id in Channel.config."""
        from ironbridge.platform.channels.channel import Channel

        with tenant_session(tenant_id) as db:
            repo = SqlAlchemyRepository(db, Channel)
            ch = repo.find_by_id(channel_id)
            if not ch:
                return
            config = dict(ch.config or {})
            threads = dict(config.get("threads", {}))
            threads[discord_channel_id] = thread_id
            config["threads"] = threads
            ch.config = config
            repo.save(ch)
            db.commit()

    # ── Bot runner ────────────────────────────────────────────────────────────

    def run_bot(self, tenant_id: str, channel_id: str, bot_token: str) -> None:
        """
        Start the discord.py bot. Blocks until stopped.
        Each Discord text channel / DM gets its own Ironbridge thread on first message.
        """
        intents = discord.Intents.default()
        intents.message_content = True
        client = discord.Client(intents=intents)

        @client.event
        async def on_ready() -> None:
            logger.info("discord adapter: bot ready as %s", client.user)

        @client.event
        async def on_message(msg: discord.Message) -> None:
            if msg.author.bot:
                return

            discord_channel_id = str(msg.channel.id)
            text = msg.content.strip()

            # /new or /reset — start a fresh thread
            if text.lower() in ("/new", "/reset"):
                thread_id = self.new_thread(tenant_id, channel_id)
                self._set_thread_for_discord_channel(
                    tenant_id, channel_id, discord_channel_id, thread_id
                )
                await msg.channel.send(f"New conversation started. (thread `{thread_id}`)")
                return

            # Find or create thread for this Discord channel
            thread_id = self._get_thread_for_discord_channel(
                tenant_id, channel_id, discord_channel_id
            )
            if not thread_id:
                thread_id = self.new_thread(tenant_id, channel_id)
                self._set_thread_for_discord_channel(
                    tenant_id, channel_id, discord_channel_id, thread_id
                )

            participant_id = str(msg.author.id)
            ikey = f"discord-{discord_channel_id}-{msg.id}"
            self.receive(
                content={"version": 1, "parts": [{"type": "text", "text": text}]},
                thread_id=thread_id,
                tenant_id=tenant_id,
                participant_id=participant_id,
                idempotency_key=ikey,
            )

        client.run(bot_token)


# Self-register for outbound delivery
from ironbridge.platform.channels.registry import register_adapter  # noqa: E402

register_adapter(DiscordAdapter())


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Ironbridge Discord bot")
    p.add_argument("--tenant", default=os.environ.get("TENANT_ID", "tenant-a"))
    p.add_argument("--channel", required=True, help="Ironbridge channel_id (from DB)")
    p.add_argument("--token", default=os.environ.get("DISCORD_BOT_TOKEN"))
    args = p.parse_args()

    if not args.token:
        raise SystemExit("DISCORD_BOT_TOKEN required")

    adapter = DiscordAdapter()
    adapter.run_bot(args.tenant, args.channel, args.token)
