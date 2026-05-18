"""
ChannelBinding — maps a thread to the channel it arrived from.

Written on the first inbound message from a channel. Used by add_message
to route ASSISTANT responses back to the originating channel via Restate.

No restate_object — pure DB record, written directly in channel inbound handler.
"""

from datetime import UTC, datetime

from cuid2 import cuid_wrapper
from sqlalchemy import DateTime, String, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column

from ironbridge.shared.db import tenant_session
from ironbridge.shared.framework import Resource

_cuid = cuid_wrapper()
_utcnow = lambda: datetime.now(UTC)  # noqa: E731


class ChannelBinding(Resource):
    class Meta:
        tenant_scoped = True
        restate_object = False

    __tablename__ = "channel_bindings"
    __table_args__ = (
        UniqueConstraint("thread_id", "channel_id", name="uq_channel_bindings_thread_channel"),
    )

    id         : Mapped[str]      = mapped_column(String, primary_key=True, default=_cuid)
    thread_id  : Mapped[str]      = mapped_column(String, nullable=False, index=True)
    channel_id : Mapped[str]      = mapped_column(String, nullable=False, index=True)
    created_at : Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


def resolve_channels_for_thread(thread_id: str, tenant_id: str | None) -> list[str]:
    """Return all channel_ids bound to this thread."""
    if not tenant_id:
        return []
    with tenant_session(tenant_id) as db:
        rows = db.execute(
            text("SELECT channel_id FROM channel_bindings WHERE thread_id = :tid"),
            {"tid": thread_id},
        ).fetchall()
    return [row[0] for row in rows]
