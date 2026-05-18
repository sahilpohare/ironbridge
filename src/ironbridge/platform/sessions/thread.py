import json
from datetime import UTC, datetime

from cuid2 import cuid_wrapper
from pydantic import BaseModel
from sqlalchemy import DateTime, String, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ironbridge.platform.channels.channel import resolve_agent_for_channel
from ironbridge.platform.channels.channel_binding import resolve_channels_for_thread
from ironbridge.platform.sessions.message import Message, MessageRole, ParticipantType
from ironbridge.shared.db import tenant_session
from ironbridge.shared.framework import ActionContext, ActionKind, Resource, action


class AddMessageRequest(BaseModel):
    participant_id: str
    participant_type: str
    role: str
    content: dict
    idempotency_key: str
    tenant_id: str
    user_name: str = ""
    agent_id: str | None = None

_cuid = cuid_wrapper()
_utcnow = lambda: datetime.now(UTC)  # noqa: E731


class Thread(Resource):
    class Meta:
        tenant_scoped = True
        restate_object = True

    __tablename__ = "threads"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_cuid)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow
    )
    messages: Mapped[list[Message]] = relationship(
        "Message",
        cascade="all, delete-orphan",
        order_by="Message.position",
        lazy="selectin",
        foreign_keys="Message.thread_id",
    )

    @action(kind=ActionKind.CREATE)
    def create(self) -> "Thread":
        if not self.id:
            self.id = _cuid()
        return self

    @action(kind=ActionKind.ACTION)
    def add_message(
        self,
        action_ctx: ActionContext,
        participant_id: str,
        participant_type: str,
        role: str,
        content: dict,
        idempotency_key: str,
        raw_response: dict | None = None,
        agent_id: str | None = None,
    ) -> Message:
        msg = Message(
            id=_cuid(),
            thread_id=self.id,
            participant_id=participant_id,
            participant_type=ParticipantType(participant_type),
            role=MessageRole(role),
            content=content,
            raw_response=raw_response,
            idempotency_key=idempotency_key,
            position=-1,  # assigned by derive/restate.py via ctx position counter
        )

        parts = content.get("parts", []) if isinstance(content, dict) else []

        # HITL: response_reply → resolve the named promise on the AgentRun workflow
        for part in parts:
            if part.get("type") == "response_reply":
                request_id = part.get("request_id")
                run_id = _find_run_id_for_request(self.id, request_id, self.tenant_id)
                if run_id:
                    action_ctx.send_workflow(
                        service="AgentRun",
                        key=run_id,
                        handler="resolve_hitl",
                        arg={
                            "request_id": request_id,
                            "thread_id": self.id,
                            "tenant_id": self.tenant_id,
                            "selected": part.get("selected", []),
                            "submitted_by": participant_id,
                        },
                    )

        is_response_reply = any(p.get("type") == "response_reply" for p in parts)

        # Trigger agent for inbound human messages
        if ParticipantType(participant_type) == ParticipantType.HUMAN and not is_response_reply:
            run_id = _cuid()
            if not agent_id:
                channel_ids = resolve_channels_for_thread(self.id, self.tenant_id)
                agent_id = resolve_agent_for_channel(channel_ids[0], self.tenant_id) if channel_ids else "stub"
            action_ctx.send_workflow(
                service="AgentRun",
                key=run_id,
                arg={
                    "run_id": run_id,
                    "agent_id": agent_id,
                    "thread_id": self.id,
                    "tenant_id": self.tenant_id,
                },
            )

        # Fanout to all channels bound to this thread.
        # send_after defers arg construction until after position is assigned.
        _thread_id = self.id
        _tenant_id = self.tenant_id
        _participant_id = participant_id
        _participant_type = participant_type
        _role = role
        _content = content
        for _channel_id in resolve_channels_for_thread(self.id, self.tenant_id):
            def _deliver_arg(result: dict, cid: str = _channel_id) -> dict:
                return {
                    "thread_id": _thread_id,
                    "channel_id": cid,
                    "tenant_id": _tenant_id,
                    "message": {
                        "participant_id": _participant_id,
                        "participant_type": _participant_type,
                        "role": _role,
                        "content": _content,
                        "position": result.get("position"),
                    },
                }

            action_ctx.send_after(
                service="ChannelDelivery",
                handler="deliver",
                key=None,
                factory=_deliver_arg,
            )

        return msg

    @action(kind=ActionKind.READ)
    def get(self) -> dict:
        return {
            "id": self.id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "messages": [
                {
                    "id": m.id,
                    "participant_id": m.participant_id,
                    "participant_type": m.participant_type.value if hasattr(m.participant_type, "value") else m.participant_type,
                    "role": m.role.value if hasattr(m.role, "value") else m.role,
                    "content": m.content,
                    "position": m.position,
                }
                for m in (self.messages or [])
            ],
        }

    @action(kind=ActionKind.STREAM)
    def observe(self) -> "Thread":
        return self


# ── Domain helpers — pure DB reads, no Restate ────────────────────────────────

def _find_run_id_for_request(thread_id: str, request_id: str | None, tenant_id: str | None) -> str | None:
    if not request_id or not tenant_id:
        return None
    with tenant_session(tenant_id) as db:
        rows = db.execute(
            text("SELECT content, participant_id FROM messages WHERE thread_id = :tid ORDER BY position"),
            {"tid": thread_id},
        ).fetchall()
    for content_raw, participant_id in rows:
        content = content_raw if isinstance(content_raw, dict) else json.loads(content_raw or "{}")
        for part in content.get("parts", []):
            if part.get("type") == "response_request" and part.get("request_id") == request_id:
                if participant_id and participant_id.startswith("agent-run-"):
                    return participant_id[len("agent-run-"):]
    return None
