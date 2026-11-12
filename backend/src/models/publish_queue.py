"""跨流发布队列持久化模型（cdc schema，表名统一 pq_ 前缀）。

设计要点：
- 两条流各自一张 append-only inbox（pq_customer_inbox / pq_order_inbox），
  生产中对应 Kafka topic；event_uid 唯一，承担第一层去重。
- pq_checkpoints 按流记录已消费 seq，与业务状态更新在同一事务里推进，
  故障重启后从下一个 seq 续跑（at-least-once 消费 + 幂等处理）。
- pq_orders 是订单台账（waiting/published/blocked 状态机），pq_outbox
  以 order_id 唯一约束作为第二层、也是最终的发布幂等防线。
- pq_audit_events 记录每次状态迁移，页面时间线与「暂存/放行/阻断」解释都来自它。
类型做了方言适配：PostgreSQL 走 JSONB/BIGINT IDENTITY，SQLite 走 JSON/INTEGER，
同一套引擎代码可以在本地无 PG 环境下单测断点续跑。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Integer,
    JSON,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.config.settings import settings
from src.models.base import Base, utcnow

# PG 上 BIGINT 自增；SQLite 只有 INTEGER PRIMARY KEY 能 rowid 自增
BigIntPK = BigInteger().with_variant(Integer, "sqlite")
# PG 上用 JSONB，其它方言（SQLite 测试库）回落到通用 JSON
JsonType = JSON().with_variant(JSONB(), "postgresql")
JsonColumn = JsonType


class PqCustomerInbox(Base):
    """客户资料流 inbox：append-only，event_uid 唯一。"""

    __tablename__ = "pq_customer_inbox"
    __table_args__ = {"schema": settings.cdc_schema}

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    event_uid: Mapped[str] = mapped_column(String(80), nullable=False, unique=True, index=True)
    customer_id: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    op: Mapped[str] = mapped_column(String(16), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class PqOrderInbox(Base):
    """订单流 inbox：append-only，event_uid 唯一；同 order_id 可重复投递。"""

    __tablename__ = "pq_order_inbox"
    __table_args__ = {"schema": settings.cdc_schema}

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    event_uid: Mapped[str] = mapped_column(String(80), nullable=False, unique=True, index=True)
    order_id: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    customer_id: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    op: Mapped[str] = mapped_column(String(16), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class PqCustomer(Base):
    """客户物化状态：当前是否有效（active/deregistered）由客户流折叠而来。"""

    __tablename__ = "pq_customers"
    __table_args__ = {"schema": settings.cdc_schema}

    customer_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    profile: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    last_event_uid: Mapped[str | None] = mapped_column(String(80), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False
    )


class PqOrder(Base):
    """订单台账：跨流发布队列的核心状态机。"""

    __tablename__ = "pq_orders"
    __table_args__ = {"schema": settings.cdc_schema}

    order_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    customer_id: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)
    first_event_uid: Mapped[str] = mapped_column(String(80), nullable=False)
    last_event_uid: Mapped[str | None] = mapped_column(String(80), nullable=True)
    delivery_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    blocked_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    staged_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False
    )


class PqOutbox(Base):
    """发布 outbox：模拟仓库发布账本。order_id 唯一 = 最终幂等防线。"""

    __tablename__ = "pq_outbox"
    __table_args__ = {"schema": settings.cdc_schema}

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    order_id: Mapped[str] = mapped_column(String(80), nullable=False, unique=True, index=True)
    customer_id: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)
    event_uid: Mapped[str] = mapped_column(String(80), nullable=False)
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class PqCheckpoint(Base):
    """每条流的消费位点（已处理 inbox.id）；与业务变更同事务提交。"""

    __tablename__ = "pq_checkpoints"
    __table_args__ = {"schema": settings.cdc_schema}

    stream: Mapped[str] = mapped_column(String(32), primary_key=True)
    last_seq: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False
    )


class PqPumpRun(Base):
    """pump 运行记录：成功/失败都留痕，失败原因与处理计数可在页面查看。"""

    __tablename__ = "pq_pump_runs"
    __table_args__ = {"schema": settings.cdc_schema}

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    trigger: Mapped[str] = mapped_column(String(32), nullable=False, default="manual")
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    stats: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class PqAuditEvent(Base):
    """状态迁移审计：页面时间线与各状态记录的解释来源。"""

    __tablename__ = "pq_audit_events"
    __table_args__ = {"schema": settings.cdc_schema}

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    action: Mapped[str] = mapped_column(String(48), nullable=False, index=True)
    stream: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    entity_type: Mapped[str] = mapped_column(String(24), nullable=False)
    entity_id: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    detail: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False, index=True)


class PqRuntime(Base):
    """运行时键值配置：故障注入开关、演示批次等。"""

    __tablename__ = "pq_runtime"
    __table_args__ = {"schema": settings.cdc_schema}

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False
    )


class PqLease(Base):
    """pump 单行租约：同一时刻只允许一个 pump 提交，beat 与手动触发互不踩踏。"""

    __tablename__ = "pq_lease"
    __table_args__ = {"schema": settings.cdc_schema}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    holder: Mapped[str | None] = mapped_column(String(80), nullable=True)
    leased_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    stale_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
