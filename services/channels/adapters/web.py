"""
Web channel adapter — delivers thread messages to browser clients via Pusher.

Outbound: on_message → Pusher trigger → browser receives via JS SDK.
Inbound:  browser posts to /api/{tenant_id}/channels/web/bind and /send

Pusher channel per thread: "thread-{thread_id}"
Pusher event: "message_added"
"""

from __future__ import annotations

import hashlib
import logging
import os

import pusher
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ironbridge.platform.channels.context import ChannelContext
from ironbridge.platform.channels.message import ChannelMessage
from ironbridge.platform.channels.registry import register_adapter
from services.channels.adapters.base import BaseChannelAdapter

logger = logging.getLogger(__name__)

_client: pusher.Pusher | None = None


def _get_client() -> pusher.Pusher:
    global _client
    if _client is None:
        _client = pusher.Pusher(
            app_id=os.environ["PUSHER_APP_ID"],
            key=os.environ["PUSHER_KEY"],
            secret=os.environ["PUSHER_SECRET"],
            cluster=os.environ.get("PUSHER_CLUSTER", "eu"),
            ssl=True,
        )
    return _client


class _CreateThreadRequest(BaseModel):
    channel_id: str


class _SendMessageRequest(BaseModel):
    content: dict
    participant_id: str = ""
    agent_id: str = "stub"


class WebAdapter(BaseChannelAdapter):
    channel_type = "web"

    def on_message(self, message: ChannelMessage, config: dict, ctx: ChannelContext) -> None:
        if not message.thread_id:
            return
        payload = {
            "thread_id": message.thread_id,
            "participant_id": message.participant_id,
            "participant_type": message.participant_type,
            "role": message.role,
            "content": {"version": 1, "parts": [p.model_dump() for p in message.parts]},
            "position": message.position,
        }
        try:
            pusher_channel = f"thread-{message.thread_id}"
            _get_client().trigger(pusher_channel, "message_added", payload)
        except Exception:
            logger.exception("web adapter: pusher publish failed thread=%s", message.thread_id)

    def get_router(self) -> APIRouter:
        router = APIRouter(prefix="/api")

        @router.post("/{tenant_id}/channels/web/bind")
        async def bind(tenant_id: str, body: _BindRequest, request: Request) -> JSONResponse:
            """
            Ensure a 'web' channel exists for this tenant and bind it to the thread.
            Idempotent — safe to call on every openThread().
            """
            header_tenant = request.headers.get("X-Tenant-Id", "").strip()
            if not header_tenant or header_tenant != tenant_id:
                raise HTTPException(status_code=403, detail="Tenant mismatch")

            self.bind_thread(tenant_id, body.thread_id, body.channel_id)
            return JSONResponse({"ok": True})

        @router.post("/{tenant_id}/channels/web/send")
        async def send(
            tenant_id: str,
            body: _SendRequest,
            request: Request,
            background_tasks: BackgroundTasks,
        ) -> JSONResponse:
            """Inbound message from browser — forward to Restate thread."""
            header_tenant = request.headers.get("X-Tenant-Id", "").strip()
            header_user = request.headers.get("X-User-Name", "").strip()
            if not header_tenant or header_tenant != tenant_id:
                raise HTTPException(status_code=403, detail="Tenant mismatch")
            if not header_user:
                raise HTTPException(status_code=401, detail="X-User-Name header required")

            ikey = hashlib.sha256(
                f"{body.thread_id}:{body.participant_id or header_user}:{body.text[:128]}".encode()
            ).hexdigest()[:16]
            background_tasks.add_task(
                self.receive,
                content={"version": 1, "parts": [{"type": "text", "text": body.text}]},
                thread_id=body.thread_id,
                tenant_id=tenant_id,
                participant_id=body.participant_id or header_user,
                agent_id=body.agent_id,
                idempotency_key=ikey,
            )
            return JSONResponse({"ok": True})

        return router


# Self-register
register_adapter(WebAdapter())
