"""channel_bindings: drop unique(thread_id), add unique(thread_id, channel_id)

Revision ID: c2d3e4f5a6b7
Revises: b1c2d3e4f5a6
Create Date: 2026-05-18
"""

from alembic import op
from sqlalchemy import inspect

revision = "c2d3e4f5a6b7"
down_revision = "b1c2d3e4f5a6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    constraints = {c["name"] for c in inspect(conn).get_unique_constraints("channel_bindings")}
    if "channel_bindings_thread_id_key" in constraints:
        op.drop_constraint("channel_bindings_thread_id_key", "channel_bindings", type_="unique")
    indexes = {i["name"] for i in inspect(conn).get_indexes("channel_bindings")}
    if "ix_channel_bindings_thread_id" in indexes:
        op.drop_index("ix_channel_bindings_thread_id", table_name="channel_bindings")
    op.create_index("ix_channel_bindings_thread_id", "channel_bindings", ["thread_id"])
    if "uq_channel_bindings_thread_channel" not in constraints:
        op.create_unique_constraint(
            "uq_channel_bindings_thread_channel",
            "channel_bindings",
            ["thread_id", "channel_id"],
        )


def downgrade() -> None:
    op.drop_constraint("uq_channel_bindings_thread_channel", "channel_bindings", type_="unique")
    op.drop_index("ix_channel_bindings_thread_id", table_name="channel_bindings")
    op.create_index("ix_channel_bindings_thread_id", "channel_bindings", ["thread_id"], unique=True)
    op.create_unique_constraint("channel_bindings_thread_id_key", "channel_bindings", ["thread_id"])
