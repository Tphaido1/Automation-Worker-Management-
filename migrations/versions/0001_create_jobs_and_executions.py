"""create jobs and executions tables

Revision ID: 0001_create_jobs_and_executions
Revises:
Create Date: 2026-10-14 10:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0001_create_jobs_and_executions"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 1. Create jobs table
    op.create_table(
        "jobs",
        sa.Column("id", sa.Uuid(), nullable=False, primary_key=True),
        sa.Column("type", sa.String(length=255), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=50), nullable=False, server_default="PENDING"),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "run_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("idempotency_key", sa.String(length=255), nullable=True),
        sa.Column("max_retries", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("timeout_seconds", sa.Integer(), nullable=False, server_default="60"),
        sa.Column("attempt", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("fencing_token", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("lease_owner", sa.String(length=255), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("execution_deadline", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result", sa.JSON(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("type", "idempotency_key", name="uq_jobs_type_idempotency_key"),
    )

    op.create_index("jobs_claim_idx", "jobs", ["priority", "run_at"])
    op.create_index("jobs_lease_idx", "jobs", ["lease_expires_at"])
    op.create_index("jobs_retry_idx", "jobs", ["run_at"])
    op.create_index("ix_jobs_status", "jobs", ["status"])

    # 2. Create executions table
    op.create_table(
        "executions",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "job_id", sa.Uuid(), sa.ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.String(length=255), nullable=False),
        sa.Column("fencing_token", sa.BigInteger(), nullable=False),
        sa.Column(
            "started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("outcome", sa.String(length=50), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
    )
    op.create_index("ix_executions_job_id", "executions", ["job_id"])


def downgrade() -> None:
    op.drop_table("executions")
    op.drop_table("jobs")
