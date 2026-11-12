"""跨流发布队列：pq_* 表（双流入库、台账、outbox、检查点、租约、审计）。

Revision ID: 0002_publish_queue
"""
from __future__ import annotations

from alembic import op

from src.config.settings import settings
from src.models.publish_queue import (
    PqAuditEvent,
    PqCheckpoint,
    PqCustomer,
    PqCustomerInbox,
    PqLease,
    PqOrder,
    PqOrderInbox,
    PqOutbox,
    PqPumpRun,
    PqRuntime,
)

revision = "0002_publish_queue"
down_revision = "0001_initial"
branch_labels = None
depends_on = None

PQ_TABLES = (
    PqCustomerInbox,
    PqOrderInbox,
    PqCustomer,
    PqOrder,
    PqOutbox,
    PqCheckpoint,
    PqPumpRun,
    PqAuditEvent,
    PqRuntime,
    PqLease,
)


def upgrade() -> None:
    bind = op.get_bind()
    op.execute(f"CREATE SCHEMA IF NOT EXISTS {settings.cdc_schema}")
    for model in PQ_TABLES:
        model.__table__.create(bind=bind, checkfirst=True)


def downgrade() -> None:
    bind = op.get_bind()
    for model in reversed(PQ_TABLES):
        model.__table__.drop(bind=bind, checkfirst=True)
