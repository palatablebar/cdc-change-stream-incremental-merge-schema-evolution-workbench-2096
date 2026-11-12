"""跨流发布队列 · 持久化存取与演示数据。

只负责「写 inbox / 读配置 / 看板查询」这类确定性 DB 操作；
状态机与 pump 主循环在 pq_engine.py，便于单测复用同一份代码。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.constants import pq_enums as pq
from src.constants.pq_enums import (
    AUDIT_CUSTOMER_DEREGISTERED,
    AUDIT_CUSTOMER_REGISTERED,
    AUDIT_CUSTOMER_UPDATED,
    AUDIT_ORDER_DUPLICATE,
    AUDIT_ORDER_STAGED,
    CFG_CRASH_ONCE,
    CFG_CRASH_ON_ORDER_UID,
    CFG_DEMO_BATCH,
    OP_UPSERT,
    STREAM_CUSTOMER,
    STREAM_ORDER,
)
from src.models.base import utcnow
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

# ---------------------------------------------------------------------------
# inbox 写入（第一层去重：event_uid 唯一）
# ---------------------------------------------------------------------------


def _dialect_name(db: Session) -> str:
    return db.bind.dialect.name if db.bind is not None else ""


def insert_customer_event(
    db: Session, event_uid: str, customer_id: str, op: str, payload: dict[str, Any]
) -> bool:
    """写入客户流事件。返回 True 表示新写入，False 表示 event_uid 已存在（幂等忽略）。"""
    if _dialect_name(db) == "postgresql":
        stmt = (
            pg_insert(PqCustomerInbox)
            .values(event_uid=event_uid, customer_id=customer_id, op=op, payload=payload)
            .on_conflict_do_nothing(index_elements=["event_uid"])
            .returning(PqCustomerInbox.id)
        )
        inserted = db.execute(stmt).scalar_one_or_none()
        return inserted is not None
    # SAVEPOINT 吸收唯一冲突，避免回滚外层事务里同批次的其它写入
    try:
        with db.begin_nested():
            db.add(
                PqCustomerInbox(event_uid=event_uid, customer_id=customer_id, op=op, payload=payload)
            )
        return True
    except IntegrityError:
        return False


def insert_order_event(
    db: Session, event_uid: str, order_id: str, customer_id: str, op: str, payload: dict[str, Any]
) -> bool:
    """写入订单流事件。返回 True 表示新写入，False 表示重复 event_uid。"""
    if _dialect_name(db) == "postgresql":
        stmt = (
            pg_insert(PqOrderInbox)
            .values(
                event_uid=event_uid,
                order_id=order_id,
                customer_id=customer_id,
                op=op,
                payload=payload,
            )
            .on_conflict_do_nothing(index_elements=["event_uid"])
            .returning(PqOrderInbox.id)
        )
        inserted = db.execute(stmt).scalar_one_or_none()
        return inserted is not None
    try:
        with db.begin_nested():
            db.add(
                PqOrderInbox(
                    event_uid=event_uid,
                    order_id=order_id,
                    customer_id=customer_id,
                    op=op,
                    payload=payload,
                )
            )
        return True
    except IntegrityError:
        return False


# ---------------------------------------------------------------------------
# runtime 键值配置（故障注入 / 演示批次）
# ---------------------------------------------------------------------------


def get_config(db: Session, key: str) -> str | None:
    row = db.get(PqRuntime, key)
    return row.value if row else None


def set_config(db: Session, key: str, value: str | None) -> None:
    if _dialect_name(db) == "postgresql":
        stmt = (
            pg_insert(PqRuntime)
            .values(key=key, value=value, updated_at=utcnow())
            .on_conflict_do_update(index_elements=["key"], set_={"value": value, "updated_at": utcnow()})
        )
        db.execute(stmt)
        return
    row = db.get(PqRuntime, key)
    if row is None:
        db.add(PqRuntime(key=key, value=value))
    else:
        row.value = value
        row.updated_at = utcnow()


def configure_crash(db: Session, event_uid: str | None, once: bool = True) -> None:
    """配置/清除订单流故障注入点。"""
    if event_uid:
        set_config(db, CFG_CRASH_ON_ORDER_UID, event_uid)
        set_config(db, CFG_CRASH_ONCE, "1" if once else "0")
    else:
        set_config(db, CFG_CRASH_ON_ORDER_UID, None)
        set_config(db, CFG_CRASH_ONCE, None)


# ---------------------------------------------------------------------------
# 重置（演示用，单事务清空全部队列状态）
# ---------------------------------------------------------------------------


def reset_all(db: Session) -> None:
    for model in (
        PqAuditEvent,
        PqOutbox,
        PqOrder,
        PqCustomer,
        PqCheckpoint,
        PqPumpRun,
        PqRuntime,
        PqLease,
        PqOrderInbox,
        PqCustomerInbox,
    ):
        db.query(model).delete()
    db.flush()


# ---------------------------------------------------------------------------
# 看板查询
# ---------------------------------------------------------------------------


def _stream_progress(db: Session, stream: str, inbox_model: type) -> dict[str, Any]:
    total = db.execute(select(func.count()).select_from(inbox_model)).scalar_one()
    cp = db.get(PqCheckpoint, stream)
    processed = int(cp.last_seq) if cp else 0
    return {
        "stream": stream,
        "stream_label": pq.PQ_STREAM_LABELS[stream],
        "total": int(total or 0),
        "processed": processed,
        "backlog": max(0, int(total or 0) - processed),
        "last_seq": processed,
    }


def _order_counts(db: Session) -> dict[str, int]:
    rows = db.execute(select(PqOrder.status, func.count()).group_by(PqOrder.status)).all()
    counts = {pq.ORDER_WAITING: 0, pq.ORDER_PUBLISHED: 0, pq.ORDER_BLOCKED: 0}
    for status, count in rows:
        counts[status] = int(count)
    counts["outbox"] = int(db.execute(select(func.count()).select_from(PqOutbox)).scalar_one() or 0)
    return counts


def _missing_customers(db: Session) -> list[dict[str, Any]]:
    """仍缺依赖：waiting 订单引用、但客户流里从未建档（或已不存在）的客户。"""
    rows = db.execute(
        select(PqOrder.customer_id, func.count().label("n"))
        .where(PqOrder.status == pq.ORDER_WAITING)
        .group_by(PqOrder.customer_id)
        .order_by(PqOrder.customer_id)
    ).all()
    result: list[dict[str, Any]] = []
    for customer_id, n in rows:
        customer = db.get(PqCustomer, customer_id)
        if customer is None:
            result.append(
                {
                    "customer_id": customer_id,
                    "waiting_orders": int(n),
                    "reason": "客户事件尚未到达",
                }
            )
    return result


def _serialize_order(row: PqOrder, customer: PqCustomer | None) -> dict[str, Any]:
    return {
        "order_id": row.order_id,
        "customer_id": row.customer_id,
        "status": row.status,
        "status_label": pq.PQ_ORDER_STATUS_LABELS[row.status],
        "delivery_count": row.delivery_count,
        "attempts": row.attempts,
        "blocked_reason": row.blocked_reason,
        "customer_status": customer.status if customer else "missing",
        "payload": row.payload,
        "staged_at": row.staged_at.isoformat() if row.staged_at else None,
        "published_at": row.published_at.isoformat() if row.published_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def list_orders(db: Session, status: str | None, limit: int) -> list[dict[str, Any]]:
    query = select(PqOrder).order_by(PqOrder.updated_at.desc(), PqOrder.order_id.asc()).limit(limit)
    if status:
        query = select(PqOrder).where(PqOrder.status == status).order_by(
            PqOrder.updated_at.desc(), PqOrder.order_id.asc()
        ).limit(limit)
    items = []
    for row in db.execute(query).scalars():
        items.append(_serialize_order(row, db.get(PqCustomer, row.customer_id)))
    return items


def list_audit(db: Session, limit: int) -> list[dict[str, Any]]:
    rows = (
        db.execute(select(PqAuditEvent).order_by(PqAuditEvent.id.desc()).limit(limit))
        .scalars()
        .all()
    )
    return [
        {
            "id": row.id,
            "action": row.action,
            "action_label": pq.PQ_AUDIT_LABELS.get(row.action, row.action),
            "stream": row.stream,
            "entity_type": row.entity_type,
            "entity_id": row.entity_id,
            "detail": row.detail,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        for row in rows
    ]


def list_pump_runs(db: Session, limit: int) -> list[dict[str, Any]]:
    rows = (
        db.execute(select(PqPumpRun).order_by(PqPumpRun.id.desc()).limit(limit))
        .scalars()
        .all()
    )
    return [
        {
            "id": row.id,
            "trigger": row.trigger,
            "status": row.status,
            "stats": row.stats,
            "error": row.error,
            "started_at": row.started_at.isoformat() if row.started_at else None,
            "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        }
        for row in rows
    ]


def dashboard(db: Session, order_limit: int = 60, audit_limit: int = 40) -> dict[str, Any]:
    crash_uid = get_config(db, CFG_CRASH_ON_ORDER_UID)
    return {
        "streams": [
            _stream_progress(db, STREAM_CUSTOMER, PqCustomerInbox),
            _stream_progress(db, STREAM_ORDER, PqOrderInbox),
        ],
        "counts": _order_counts(db),
        "missing_customers": _missing_customers(db),
        "orders": list_orders(db, None, order_limit),
        "audit": list_audit(db, audit_limit),
        "pump_runs": list_pump_runs(db, 10),
        "crash_injection": {
            "enabled": bool(crash_uid),
            "order_event_uid": crash_uid,
            "once": get_config(db, CFG_CRASH_ONCE) != "0",
        },
        "demo_batch": get_config(db, CFG_DEMO_BATCH) or "0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# 演示剧本：分批制造「订单先到 / 客户后到 / 注销 / 重复投递 / 故障」
# ---------------------------------------------------------------------------

# 每个批次：(customer_events, order_events)，event_uid 全局稳定，保证 seed 幂等
DEMO_BATCHES: list[dict[str, list[dict[str, Any]]]] = [
    # 批次 1：5 个订单先到，只有 C-001/C-002 已建档 → 2 放行、3 暂存
    {
        "customers": [
            {"event_uid": "demo-c-001", "customer_id": "C-001", "op": OP_UPSERT,
             "payload": {"name": "上海 · 林岚", "tier": "gold", "city": "上海"}},
            {"event_uid": "demo-c-002", "customer_id": "C-002", "op": OP_UPSERT,
             "payload": {"name": "杭州 · 周牧", "tier": "silver", "city": "杭州"}},
        ],
        "orders": [
            {"event_uid": "demo-o-001", "order_id": "ORD-1001", "customer_id": "C-001",
             "payload": {"amount": 329.0, "qty": 1, "channel": "app"}},
            {"event_uid": "demo-o-002", "order_id": "ORD-1002", "customer_id": "C-002",
             "payload": {"amount": 118.5, "qty": 2, "channel": "mini"}},
            {"event_uid": "demo-o-003", "order_id": "ORD-1003", "customer_id": "C-003",
             "payload": {"amount": 89.9, "qty": 1, "channel": "app"}},
            {"event_uid": "demo-o-004", "order_id": "ORD-1004", "customer_id": "C-003",
             "payload": {"amount": 256.0, "qty": 3, "channel": "web"}},
            {"event_uid": "demo-o-005", "order_id": "ORD-1005", "customer_id": "C-004",
             "payload": {"amount": 1299.0, "qty": 1, "channel": "app"}},
        ],
    },
    # 批次 2：缺失客户到齐 + 两个订单重复投递（同 event_uid 与不同 event_uid 各一）
    {
        "customers": [
            {"event_uid": "demo-c-003", "customer_id": "C-003", "op": OP_UPSERT,
             "payload": {"name": "苏州 · 韩朔", "tier": "bronze", "city": "苏州"}},
        ],
        "orders": [
            # 完全相同的 event_uid：inbox 层直接拒收
            {"event_uid": "demo-o-003", "order_id": "ORD-1003", "customer_id": "C-003",
             "payload": {"amount": 89.9, "qty": 1, "channel": "app"}},
            # 新 event_uid、同 order_id：台账层识别为重复投递，不重复发布
            {"event_uid": "demo-o-003b", "order_id": "ORD-1003", "customer_id": "C-003",
             "payload": {"amount": 89.9, "qty": 1, "channel": "app-redeliver"}},
        ],
    },
    # 批次 3：C-004 建档（放行 ORD-1005），随后 C-002 注销（关联订单阻断在注销时刻）
    {
        "customers": [
            {"event_uid": "demo-c-004", "customer_id": "C-004", "op": OP_UPSERT,
             "payload": {"name": "宁波 · 沈砚", "tier": "gold", "city": "宁波"}},
            {"event_uid": "demo-c-005", "customer_id": "C-002", "op": "delete",
             "payload": {"reason": "账户注销"}},
        ],
        "orders": [],
    },
    # 批次 4：注销客户的新订单与重复投递，一律阻断、绝不放行
    {
        "customers": [],
        "orders": [
            {"event_uid": "demo-o-006", "order_id": "ORD-1006", "customer_id": "C-002",
             "payload": {"amount": 66.0, "qty": 1, "channel": "app"}},
            {"event_uid": "demo-o-006b", "order_id": "ORD-1006", "customer_id": "C-002",
             "payload": {"amount": 66.0, "qty": 1, "channel": "app"}},
        ],
    },
    # 批次 5：故障注入演示——该事件首次处理必然失败，清除注入后从原处续跑成功
    {
        "customers": [
            {"event_uid": "demo-c-006", "customer_id": "C-005", "op": OP_UPSERT,
             "payload": {"name": "南京 · 顾潮", "tier": "silver", "city": "南京"}},
        ],
        "orders": [
            {"event_uid": "demo-o-007", "order_id": "ORD-1007", "customer_id": "C-005",
             "payload": {"amount": 499.0, "qty": 1, "channel": "web"}},
        ],
    },
]


def seed_next_batch(db: Session) -> dict[str, Any]:
    """灌入下一个演示批次（幂等：event_uid 冲突自动跳过），返回批次内容与序号。"""
    batch_index = int(get_config(db, CFG_DEMO_BATCH) or "0")
    if batch_index >= len(DEMO_BATCHES):
        return {"batch": batch_index, "finished": True, "customers": 0, "orders": 0}
    batch = DEMO_BATCHES[batch_index]
    customers_added = 0
    orders_added = 0
    for event in batch["customers"]:
        if insert_customer_event(db, event["event_uid"], event["customer_id"], event["op"], event["payload"]):
            customers_added += 1
    for event in batch["orders"]:
        if insert_order_event(
            db,
            event["event_uid"],
            event["order_id"],
            event["customer_id"],
            OP_UPSERT,
            event["payload"],
        ):
            orders_added += 1
    set_config(db, CFG_DEMO_BATCH, str(batch_index + 1))
    return {
        "batch": batch_index + 1,
        "finished": False,
        "customers": customers_added,
        "orders": orders_added,
        "total_batches": len(DEMO_BATCHES),
    }


# 供 seed_demo / 测试引用的审计动作集合
DEMO_AUDIT_ACTIONS = (
    AUDIT_ORDER_STAGED,
    AUDIT_ORDER_DUPLICATE,
    AUDIT_CUSTOMER_REGISTERED,
    AUDIT_CUSTOMER_UPDATED,
    AUDIT_CUSTOMER_DEREGISTERED,
)
