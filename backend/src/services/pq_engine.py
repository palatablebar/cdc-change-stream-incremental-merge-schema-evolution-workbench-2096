"""跨流发布队列引擎：可恢复的 pump 主循环与状态机。

可恢复性（失败后从原处续跑）：
  每条 inbox 事件独立事务，checkpoint 与业务状态在同一事务提交；
  处理事件 N 时崩溃 → 事务整体回滚，checkpoint 仍停在 N-1，
  下次 pump 从 N 重新投递。重复投递由台账与 outbox 唯一约束吸收，
  因此「至少一次消费 + 幂等处理」对外呈现为恰好一次发布。

状态机：
  订单先到、客户缺失      → waiting（暂存，计入「仍缺依赖」）
  客户 upsert 到达        → 有效客户；waiting 订单在客户事件事务内放行（回补）
  订单到达且客户有效      → 直接放行
  客户 delete（注销）     → 关联 waiting 订单转 blocked，已发布订单不动；
                            之后任何同客户订单（含重复投递）一律阻断
  同 order_id 重复投递   → 不新增发布，只累加 delivery_count 并记审计
"""
from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.constants.pq_enums import (
    AUDIT_CUSTOMER_DEREGISTERED,
    AUDIT_CUSTOMER_REGISTERED,
    AUDIT_CUSTOMER_UPDATED,
    AUDIT_ORDER_BLOCKED,
    AUDIT_ORDER_DUPLICATE,
    AUDIT_ORDER_REDELIVERED_BLOCKED,
    AUDIT_ORDER_RELEASED,
    AUDIT_ORDER_STAGED,
    AUDIT_PUMP_FAILED,
    AUDIT_PUMP_STARTED,
    CFG_CRASH_ON_ORDER_UID,
    OP_DELETE,
    ORDER_BLOCKED,
    ORDER_PUBLISHED,
    ORDER_WAITING,
    PUMP_FAILED,
    PUMP_RUNNING,
    PUMP_SUCCEEDED,
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
)
from src.services import pq_service

# 租约超过该时长视为僵死，允许接管
LEASE_TTL = timedelta(minutes=2)


class InjectedFailure(RuntimeError):
    """演示用故障注入：模拟事件处理途中进程崩溃（事务回滚、checkpoint 不前进）。"""


class PumpBusy(RuntimeError):
    """已有 pump 在跑（租约未释放且未僵死）。"""


# ---------------------------------------------------------------------------
# 小工具
# ---------------------------------------------------------------------------


def _audit(
    db: Session,
    action: str,
    entity_type: str,
    entity_id: str,
    stream: str | None = None,
    detail: dict[str, Any] | None = None,
) -> None:
    db.add(
        PqAuditEvent(
            action=action,
            stream=stream,
            entity_type=entity_type,
            entity_id=entity_id,
            detail=detail or {},
        )
    )


def _get_checkpoint(db: Session, stream: str) -> PqCheckpoint:
    cp = db.get(PqCheckpoint, stream)
    if cp is None:
        cp = PqCheckpoint(stream=stream, last_seq=0)
        db.add(cp)
        db.flush()
    return cp


def _maybe_crash(db: Session, event_uid: str) -> None:
    target = pq_service.get_config(db, CFG_CRASH_ON_ORDER_UID)
    if target and target == event_uid:
        raise InjectedFailure(f"故障注入：订单事件 {event_uid} 处理途中失败")


def _new_stats() -> dict[str, int]:
    return {
        "customer_events": 0,
        "order_events": 0,
        "staged": 0,
        "released": 0,
        "blocked": 0,
        "duplicate": 0,
        "redelivered_blocked": 0,
        "customer_registered": 0,
        "customer_updated": 0,
        "customer_deregistered": 0,
    }


# ---------------------------------------------------------------------------
# 单事件处理（每个事件一个事务，由 pump 统一 commit/rollback）
# ---------------------------------------------------------------------------


def _publish_order(db: Session, order: PqOrder, event_uid: str) -> bool:
    """向 outbox（模拟仓库发布账本）写入；order_id 唯一约束兜底防重。

    返回 True 表示本次真的发布，False 表示 outbox 已存在（重复发布被吸收）。
    用 SAVEPOINT 捕获唯一冲突，避免回滚掉整个事件事务。
    """
    try:
        with db.begin_nested():
            db.add(
                PqOutbox(
                    order_id=order.order_id,
                    customer_id=order.customer_id,
                    payload=order.payload,
                    event_uid=event_uid,
                )
            )
        order.status = ORDER_PUBLISHED
        order.attempts = order.attempts + 1
        order.published_at = utcnow()
        return True
    except IntegrityError:
        existing = db.get(PqOrder, order.order_id)
        if existing is not None and existing.status != ORDER_PUBLISHED:
            existing.status = ORDER_PUBLISHED
            existing.published_at = existing.published_at or utcnow()
        return False


def _handle_order_event(db: Session, event: PqOrderInbox) -> tuple[str, dict[str, int]]:
    """处理一条订单事件，返回 (审计动作, 该事件的增量统计)。"""
    _maybe_crash(db, event.event_uid)
    delta = {"released": 0, "staged": 0, "blocked": 0, "duplicate": 0, "redelivered_blocked": 0}

    existing = db.get(PqOrder, event.order_id)
    if existing is not None:
        # 同一订单的重复投递：台账层去重，绝不重复发布
        existing.delivery_count = existing.delivery_count + 1
        existing.last_event_uid = event.event_uid
        if existing.status == ORDER_BLOCKED:
            _audit(
                db,
                AUDIT_ORDER_REDELIVERED_BLOCKED,
                "order",
                event.order_id,
                STREAM_ORDER,
                {"event_uid": event.event_uid, "reason": existing.blocked_reason},
            )
            delta["redelivered_blocked"] = 1
            return AUDIT_ORDER_REDELIVERED_BLOCKED, delta
        _audit(
            db,
            AUDIT_ORDER_DUPLICATE,
            "order",
            event.order_id,
            STREAM_ORDER,
            {"event_uid": event.event_uid, "delivery_count": existing.delivery_count,
             "status": existing.status},
        )
        delta["duplicate"] = 1
        return AUDIT_ORDER_DUPLICATE, delta

    order = PqOrder(
        order_id=event.order_id,
        customer_id=event.customer_id,
        status=ORDER_WAITING,
        payload=event.payload,
        first_event_uid=event.event_uid,
        delivery_count=1,
        attempts=0,
    )
    db.add(order)
    db.flush()

    customer = db.get(PqCustomer, event.customer_id)
    if customer is not None and customer.status == "active":
        _publish_order(db, order, event.event_uid)
        _audit(
            db,
            AUDIT_ORDER_RELEASED,
            "order",
            event.order_id,
            STREAM_ORDER,
            {"event_uid": event.event_uid, "customer_id": event.customer_id, "path": "direct"},
        )
        delta["released"] = 1
        return AUDIT_ORDER_RELEASED, delta

    if customer is not None and customer.status == "deregistered":
        order.status = ORDER_BLOCKED
        order.blocked_reason = f"客户 {event.customer_id} 已注销，关联订单禁止放行"
        _audit(
            db,
            AUDIT_ORDER_BLOCKED,
            "order",
            event.order_id,
            STREAM_ORDER,
            {"event_uid": event.event_uid, "customer_id": event.customer_id,
             "path": "arrive_after_deregister"},
        )
        delta["blocked"] = 1
        return AUDIT_ORDER_BLOCKED, delta

    # 客户缺失 → 暂存
    _audit(
        db,
        AUDIT_ORDER_STAGED,
        "order",
        event.order_id,
        STREAM_ORDER,
        {"event_uid": event.event_uid, "customer_id": event.customer_id, "missing": True},
    )
    delta["staged"] = 1
    return AUDIT_ORDER_STAGED, delta


def _release_waiting_orders(db: Session, customer_id: str, event_uid: str) -> int:
    """客户到齐后回补：把该客户全部 waiting 订单放行。"""
    rows = db.execute(
        select(PqOrder).where(PqOrder.customer_id == customer_id, PqOrder.status == ORDER_WAITING)
    ).scalars().all()
    released = 0
    for order in rows:
        _publish_order(db, order, event_uid)
        released += 1
        _audit(
            db,
            AUDIT_ORDER_RELEASED,
            "order",
            order.order_id,
            STREAM_ORDER,
            {"event_uid": event_uid, "customer_id": customer_id,
             "path": "customer_arrived_backfill"},
        )
    return released


def _block_waiting_orders(db: Session, customer_id: str, event_uid: str) -> int:
    """客户注销：关联暂存订单全部阻断，此后不再放行。"""
    rows = db.execute(
        select(PqOrder).where(PqOrder.customer_id == customer_id, PqOrder.status == ORDER_WAITING)
    ).scalars().all()
    for order in rows:
        order.status = ORDER_BLOCKED
        order.blocked_reason = f"客户 {customer_id} 已注销，关联订单禁止放行"
        _audit(
            db,
            AUDIT_ORDER_BLOCKED,
            "order",
            order.order_id,
            STREAM_CUSTOMER,
            {"event_uid": event_uid, "customer_id": customer_id, "path": "customer_deregistered"},
        )
    return len(rows)


def _handle_customer_event(db: Session, event: PqCustomerInbox) -> tuple[str, dict[str, int]]:
    delta = {"released": 0, "blocked": 0}
    customer = db.get(PqCustomer, event.customer_id)
    if event.op == OP_DELETE:
        if customer is None:
            customer = PqCustomer(
                customer_id=event.customer_id,
                status="deregistered",
                profile=event.payload,
                version=1,
                last_event_uid=event.event_uid,
            )
            db.add(customer)
        else:
            customer.status = "deregistered"
            customer.profile = event.payload
            customer.version = customer.version + 1
            customer.last_event_uid = event.event_uid
        db.flush()
        delta["blocked"] = _block_waiting_orders(db, event.customer_id, event.event_uid)
        _audit(
            db,
            AUDIT_CUSTOMER_DEREGISTERED,
            "customer",
            event.customer_id,
            STREAM_CUSTOMER,
            {"event_uid": event.event_uid, "blocked_orders": delta["blocked"]},
        )
        return AUDIT_CUSTOMER_DEREGISTERED, delta

    # upsert：建档或更新。注销后重新建档不会自动解禁 blocked 订单（阻断是终态，需人工介入）
    if customer is None:
        customer = PqCustomer(
            customer_id=event.customer_id,
            status="active",
            profile=event.payload,
            version=1,
            last_event_uid=event.event_uid,
        )
        db.add(customer)
        action = AUDIT_CUSTOMER_REGISTERED
    else:
        customer.status = "active"
        customer.profile = event.payload
        customer.version = customer.version + 1
        customer.last_event_uid = event.event_uid
        action = AUDIT_CUSTOMER_UPDATED
    db.flush()
    delta["released"] = _release_waiting_orders(db, event.customer_id, event.event_uid)
    _audit(
        db,
        action,
        "customer",
        event.customer_id,
        STREAM_CUSTOMER,
        {"event_uid": event.event_uid, "released_orders": delta["released"],
         "payload_keys": sorted(event.payload.keys())},
    )
    return action, delta


# ---------------------------------------------------------------------------
# 租约（方言中立：普通行 + 僵死接管，不依赖 PG 专有语法）
# ---------------------------------------------------------------------------


def _acquire_lease(db: Session, holder: str) -> PqLease:
    # PG 上行锁串行化租约竞争；SQLite 方言会忽略 FOR UPDATE
    lease = db.get(PqLease, 1, with_for_update=True)
    now = utcnow()
    if lease is None:
        lease = PqLease(id=1, holder=holder, leased_at=now, stale_after=now + LEASE_TTL)
        db.add(lease)
        db.flush()
        return lease
    # SQLite 不保留时区，读出的时间是 naive；按 UTC 归一化后再比较
    stale_after = lease.stale_after
    if stale_after is not None and stale_after.tzinfo is None:
        from datetime import timezone

        stale_after = stale_after.replace(tzinfo=timezone.utc)
    if lease.holder is not None and stale_after is not None and stale_after > now:
        raise PumpBusy(f"pump 租约被 {lease.holder} 持有")
    lease.holder = holder
    lease.leased_at = now
    lease.stale_after = now + LEASE_TTL
    db.flush()
    return lease


def _release_lease(db: Session, holder: str) -> None:
    lease = db.get(PqLease, 1)
    if lease is not None and lease.holder == holder:
        lease.holder = None
        lease.leased_at = None
        lease.stale_after = None
        db.flush()


# ---------------------------------------------------------------------------
# pump 主循环
# ---------------------------------------------------------------------------


def _next_event(db: Session, inbox_model: type, after_seq: int):
    return db.execute(
        select(inbox_model).where(inbox_model.id > after_seq).order_by(inbox_model.id.asc()).limit(1)
    ).scalar_one_or_none()


def _record_failure(db: Session, run_id: int, holder: str, exc: Exception, stats: dict[str, int]) -> None:
    """新事务记录失败并释放租约：失败也留痕、租约不泄漏，下一次 pump 可照常接管。"""
    db.rollback()
    run = db.get(PqPumpRun, run_id)
    if run is not None:
        run.status = PUMP_FAILED
        run.error = f"{type(exc).__name__}: {exc}"[:600]
        run.stats = stats
        run.finished_at = utcnow()
    _release_lease(db, holder)
    _audit(db, AUDIT_PUMP_FAILED, "pump", holder, detail={"error": str(exc)[:300]})
    db.commit()


def _is_caught_up(db: Session) -> bool:
    """两条流都没有未消费事件。beat 空转时用它提前返回，避免堆积无意义运行记录。"""
    cp_c = db.get(PqCheckpoint, STREAM_CUSTOMER)
    cp_o = db.get(PqCheckpoint, STREAM_ORDER)
    c_seq = cp_c.last_seq if cp_c else 0
    o_seq = cp_o.last_seq if cp_o else 0
    return (
        _next_event(db, PqCustomerInbox, c_seq) is None
        and _next_event(db, PqOrderInbox, o_seq) is None
    )


def run_pump(db: Session, trigger: str = "manual", max_events: int = 500) -> dict[str, Any]:
    """消费两条流直到追平或达到上限。失败抛异常，同时落 failed 运行记录。"""
    # 已追平：beat 每 5s 的空轮询直接返回，不写 pump 运行记录、不碰租约
    if _is_caught_up(db):
        return {"status": PUMP_SUCCEEDED, "stats": _new_stats(), "processed": 0, "holder": None, "noop": True}

    holder = f"{trigger}-{uuid.uuid4().hex[:8]}"
    run = PqPumpRun(trigger=trigger, status=PUMP_RUNNING, stats={}, started_at=utcnow())
    db.add(run)
    try:
        _acquire_lease(db, holder)
    except PumpBusy as exc:
        run.status = PUMP_FAILED
        run.error = str(exc)
        run.finished_at = utcnow()
        run.stats = {"skipped": 1}
        db.commit()
        raise

    stats = _new_stats()
    _audit(db, AUDIT_PUMP_STARTED, "pump", holder, detail={"trigger": trigger})
    db.commit()

    processed = 0
    try:
        while processed < max_events:
            cp_c = _get_checkpoint(db, STREAM_CUSTOMER)
            cp_o = _get_checkpoint(db, STREAM_ORDER)
            customer_event = _next_event(db, PqCustomerInbox, cp_c.last_seq)
            order_event = _next_event(db, PqOrderInbox, cp_o.last_seq)
            if customer_event is None and order_event is None:
                break

            # 每轮两条流都推进：客户流优先（先建档/注销，再决定订单放行还是阻断），
            # 两条流各自独立提交检查点，客户后到也能在下一轮回补暂存订单
            if customer_event is not None:
                action, delta = _handle_customer_event(db, customer_event)
                cp_c.last_seq = customer_event.id
                stats["customer_events"] += 1
                stats["released"] += delta.get("released", 0)
                stats["blocked"] += delta.get("blocked", 0)
                if action == AUDIT_CUSTOMER_REGISTERED:
                    stats["customer_registered"] += 1
                elif action == AUDIT_CUSTOMER_DEREGISTERED:
                    stats["customer_deregistered"] += 1
                else:
                    stats["customer_updated"] += 1
                db.commit()
                processed += 1

            if order_event is not None:
                action, delta = _handle_order_event(db, order_event)
                cp_o.last_seq = order_event.id
                stats["order_events"] += 1
                for key in ("released", "staged", "blocked", "duplicate", "redelivered_blocked"):
                    stats[key] += delta.get(key, 0)
                # 每条事件一个事务：checkpoint 与状态原子落盘，崩溃只回滚当前事件
                db.commit()
                processed += 1

        run = db.get(PqPumpRun, run.id)
        run.status = PUMP_SUCCEEDED
        run.stats = stats
        run.finished_at = utcnow()
        _release_lease(db, holder)
        db.commit()
        return {"status": PUMP_SUCCEEDED, "stats": stats, "processed": processed, "holder": holder}
    except Exception as exc:
        _record_failure(db, run.id, holder, exc, stats)
        raise
