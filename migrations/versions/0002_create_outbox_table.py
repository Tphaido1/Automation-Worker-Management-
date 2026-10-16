"""create outbox table

Revision ID: 0002_create_outbox_table
Revises: 0001_create_jobs_and_executions
Create Date: 2026-10-16 10:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0002_create_outbox_table"
down_revision: str | None = "0001_create_jobs_and_executions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "outbox",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "job_id",
            sa.Uuid(),
            sa.ForeignKey("jobs.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        sa.Column("dedup_key", sa.String(length=255), nullable=False, unique=True),
        sa.Column("kind", sa.String(length=100), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("dispatched_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("outbox_pending_idx", "outbox", ["created_at"])


def downgrade() -> None:
    op.drop_index("outbox_pending_idx", table_name="outbox")
    op.drop_table("outbox")
