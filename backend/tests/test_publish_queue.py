"""跨流发布队列引擎测试：暂存/回补/去重/注销阻断/故障续跑，全在 SQLite 上验证。"""
from __future__ import annotations

import pytest

from src.constants import pq_enums as pq
from src.models.publish_queue import (
    PqCheckpoint,
    PqCustomer,
    PqOrder,
    PqOutbox,
)
from src.services import pq_engine, pq_service


def _order_event(uid: str, order_id: str, customer_id: str, amount: float = 100.0):
    return {
        "event_uid": uid,
        "order_id": order_id,
        "customer_id": customer_id,
        "payload": {"amount": amount, "qty": 1},
    }


def _customer_event(uid: str, customer_id: str, op: str = "upsert", payload=None):
    return {
        "event_uid": uid,
        "customer_id": customer_id,
        "op": op,
        "payload": payload or {"name": customer_id},
    }


def _seed_orders(db, *events):
    for event in events:
        assert pq_service.insert_order_event(
            db, event["event_uid"], event["order_id"], event["customer_id"], "upsert", event["payload"]
        )
    db.commit()


def _seed_customers(db, *events):
    for event in events:
        assert pq_service.insert_customer_event(
            db, event["event_uid"], event["customer_id"], event["op"], event["payload"]
        )
    db.commit()


# --------------------------------------------------------------------------
# 1. 订单先到 → 暂存；客户后到 → 自动回补放行
# --------------------------------------------------------------------------


def test_order_arrives_before_customer_is_staged_then_released(db):
    _seed_orders(db, _order_event("o1", "ORD-1", "C-1"), _order_event("o2", "ORD-2", "C-1"))
    result = pq_engine.run_pump(db)

    assert result["stats"]["staged"] == 2
    assert db.get(PqOrder, "ORD-1").status == pq.ORDER_WAITING
    cp = db.get(PqCheckpoint, pq.STREAM_ORDER)
    assert cp.last_seq == 2  # 订单流全部消费完，位点已推进

    # 客户到齐
    _seed_customers(db, _customer_event("c1", "C-1"))
    result = pq_engine.run_pump(db)

    assert result["stats"]["released"] == 2
    assert db.get(PqOrder, "ORD-1").status == pq.ORDER_PUBLISHED
    assert db.get(PqOrder, "ORD-2").status == pq.ORDER_PUBLISHED
    assert db.query(PqOutbox).count() == 2
    # 客户流位点推进，订单流无新事件不回退
    assert db.get(PqCheckpoint, pq.STREAM_CUSTOMER).last_seq == 1


def test_order_with_active_customer_publishes_directly(db):
    _seed_customers(db, _customer_event("c1", "C-1"))
    _seed_orders(db, _order_event("o1", "ORD-1", "C-1"))
    result = pq_engine.run_pump(db)

    assert result["stats"]["released"] == 1
    assert result["stats"]["staged"] == 0
    assert db.get(PqOrder, "ORD-1").status == pq.ORDER_PUBLISHED


# --------------------------------------------------------------------------
# 2. 重复投递：同 event_uid 与同 order_id 新事件都不重复发布
# --------------------------------------------------------------------------


def test_duplicate_event_uid_is_ignored_at_inbox(db):
    event = _order_event("o1", "ORD-1", "C-1")
    _seed_orders(db, event)
    # 同 event_uid 再写一次 → inbox 拒收
    assert pq_service.insert_order_event(db, "o1", "ORD-1", "C-1", "upsert", event["payload"]) is False
    db.commit()
    pq_engine.run_pump(db)
    assert db.query(PqOutbox).count() == 0  # 客户缺失，暂存而非发布


def test_redelivered_order_is_not_published_twice(db):
    _seed_customers(db, _customer_event("c1", "C-1"))
    _seed_orders(
        db,
        _order_event("o1", "ORD-1", "C-1", amount=100.0),
        _order_event("o1-redeliver", "ORD-1", "C-1", amount=100.0),
        _order_event("o2", "ORD-2", "C-1", amount=50.0),
    )
    result = pq_engine.run_pump(db)

    assert result["stats"]["released"] == 2  # 只有两张不同订单被发布
    assert result["stats"]["duplicate"] == 1
    order = db.get(PqOrder, "ORD-1")
    assert order.delivery_count == 2
    # outbox 唯一约束兜底：ORD-1 只有一行
    assert db.query(PqOutbox).filter(PqOutbox.order_id == "ORD-1").count() == 1
    assert db.query(PqOutbox).count() == 2


def test_duplicate_after_release_still_single_outbox_row(db):
    _seed_orders(db, _order_event("o1", "ORD-1", "C-9"))
    pq_engine.run_pump(db)  # 暂存
    _seed_customers(db, _customer_event("c9", "C-9"))
    pq_engine.run_pump(db)  # 回补发布
    _seed_orders(db, _order_event("o1-again", "ORD-1", "C-9"))
    result = pq_engine.run_pump(db)  # 重复投递

    assert result["stats"]["duplicate"] == 1
    assert db.query(PqOutbox).filter(PqOutbox.order_id == "ORD-1").count() == 1
    assert db.get(PqOrder, "ORD-1").delivery_count == 2


# --------------------------------------------------------------------------
# 3. 客户注销：关联订单阻断，之后永不放行
# --------------------------------------------------------------------------


def test_deregistration_blocks_waiting_orders(db):
    _seed_orders(db, _order_event("o1", "ORD-1", "C-1"), _order_event("o2", "ORD-2", "C-1"))
    pq_engine.run_pump(db)  # 全部暂存
    _seed_customers(db, _customer_event("c1-del", "C-1", op="delete", payload={"reason": "注销"}))
    result = pq_engine.run_pump(db)

    assert result["stats"]["blocked"] == 2
    for order_id in ("ORD-1", "ORD-2"):
        order = db.get(PqOrder, order_id)
        assert order.status == pq.ORDER_BLOCKED
        assert "注销" in order.blocked_reason
    assert db.query(PqOutbox).count() == 0


def test_orders_after_deregistration_are_blocked(db):
    _seed_customers(db, _customer_event("c1-del", "C-1", op="delete"))
    _seed_orders(
        db,
        _order_event("o1", "ORD-1", "C-1"),
        _order_event("o1b", "ORD-1", "C-1"),
    )
    result = pq_engine.run_pump(db)

    assert result["stats"]["blocked"] == 1
    assert result["stats"]["redelivered_blocked"] == 1
    assert db.query(PqOutbox).count() == 0
    assert db.get(PqOrder, "ORD-1").delivery_count == 2


def test_re_registration_does_not_unblock_blocked_orders(db):
    _seed_customers(db, _customer_event("c1-del", "C-1", op="delete"))
    _seed_orders(db, _order_event("o1", "ORD-1", "C-1"))
    pq_engine.run_pump(db)  # 阻断
    _seed_customers(db, _customer_event("c1-back", "C-1", op="upsert"))
    result = pq_engine.run_pump(db)

    assert result["stats"]["released"] == 0
    assert db.get(PqOrder, "ORD-1").status == pq.ORDER_BLOCKED
    assert db.query(PqOutbox).count() == 0


def test_published_orders_survive_deregistration(db):
    _seed_customers(db, _customer_event("c1", "C-1"))
    _seed_orders(db, _order_event("o1", "ORD-1", "C-1"))
    pq_engine.run_pump(db)  # 已发布
    _seed_customers(db, _customer_event("c1-del", "C-1", op="delete"))
    pq_engine.run_pump(db)

    assert db.get(PqOrder, "ORD-1").status == pq.ORDER_PUBLISHED
    assert db.get(PqCustomer, "C-1").status == "deregistered"
    assert db.query(PqOutbox).count() == 1


# --------------------------------------------------------------------------
# 4. 故障注入：崩溃后 checkpoint 不前进，清除注入后从原处续跑
# --------------------------------------------------------------------------


def test_crash_resumes_from_same_event(db):
    _seed_customers(db, _customer_event("c1", "C-1"))
    _seed_orders(
        db,
        _order_event("o1", "ORD-1", "C-1"),
        _order_event("o2-crash", "ORD-2", "C-1"),
        _order_event("o3", "ORD-3", "C-1"),
    )
    pq_service.configure_crash(db, "o2-crash", once=True)
    db.commit()

    # 第一次 pump：o1 成功，o2 崩溃，o3 未触及
    with pytest.raises(pq_engine.InjectedFailure):
        pq_engine.run_pump(db, trigger="manual")

    cp = db.get(PqCheckpoint, pq.STREAM_ORDER)
    assert cp.last_seq == 1  # 只推进到 o1
    assert db.get(PqOrder, "ORD-1").status == pq.ORDER_PUBLISHED
    assert db.get(PqOrder, "ORD-2") is None  # o2 事务整体回滚，台账无残留
    assert db.get(PqOrder, "ORD-3") is None
    assert db.query(PqOutbox).count() == 1

    # 不清除注入再跑：仍在原处失败，位点不变
    with pytest.raises(pq_engine.InjectedFailure):
        pq_engine.run_pump(db, trigger="retry-still-broken")
    assert db.get(PqCheckpoint, pq.STREAM_ORDER).last_seq == 1
    assert db.query(PqOutbox).count() == 1  # o1 没有被重复发布

    # 清除注入后续跑：o2、o3 一次追平
    pq_service.configure_crash(db, None)
    db.commit()
    result = pq_engine.run_pump(db, trigger="retry-ok")
    assert result["processed"] == 2
    assert db.get(PqCheckpoint, pq.STREAM_ORDER).last_seq == 3
    assert db.query(PqOutbox).count() == 3  # 精确一次：o1 仍只有一行


def test_repeated_pump_is_idempotent_when_caught_up(db):
    _seed_customers(db, _customer_event("c1", "C-1"))
    _seed_orders(db, _order_event("o1", "ORD-1", "C-1"))
    pq_engine.run_pump(db)
    again = pq_engine.run_pump(db)
    assert again["processed"] == 0
    assert db.query(PqOutbox).count() == 1


# --------------------------------------------------------------------------
# 5. 租约：并发 pump 互斥，僵死租约可接管
# --------------------------------------------------------------------------


def test_lease_prevents_concurrent_pumps(db):
    from src.models.publish_queue import PqLease
    from src.models.base import utcnow
    from datetime import timedelta

    _seed_orders(db, _order_event("o1", "ORD-1", "C-1"))
    db.add(PqLease(id=1, holder="other", leased_at=utcnow(), stale_after=utcnow() + timedelta(minutes=1)))
    db.commit()

    with pytest.raises(pq_engine.PumpBusy):
        pq_engine.run_pump(db)
    # 忙等时不消费任何事件
    assert db.get(PqCheckpoint, pq.STREAM_ORDER) is None or db.get(PqCheckpoint, pq.STREAM_ORDER).last_seq == 0


def test_stale_lease_is_taken_over(db):
    from src.models.publish_queue import PqLease
    from src.models.base import utcnow
    from datetime import timedelta

    _seed_orders(db, _order_event("o1", "ORD-1", "C-1"))
    db.add(PqLease(id=1, holder="dead-worker", leased_at=utcnow() - timedelta(minutes=10),
                   stale_after=utcnow() - timedelta(minutes=8)))
    db.commit()

    result = pq_engine.run_pump(db)
    assert result["status"] == pq.PUMP_SUCCEEDED
    assert db.query(PqOutbox).count() == 0  # 客户缺失 → 暂存
    assert db.get(PqOrder, "ORD-1").status == pq.ORDER_WAITING


# --------------------------------------------------------------------------
# 6. 看板：缺依赖清单与计数口径
# --------------------------------------------------------------------------


def test_dashboard_missing_customers_and_counts(db):
    _seed_orders(
        db,
        _order_event("o1", "ORD-1", "C-1"),
        _order_event("o2", "ORD-2", "C-1"),
        _order_event("o3", "ORD-3", "C-2"),
    )
    pq_engine.run_pump(db)
    board = pq_service.dashboard(db)

    assert board["counts"]["waiting"] == 3
    missing = {item["customer_id"]: item["waiting_orders"] for item in board["missing_customers"]}
    assert missing == {"C-1": 2, "C-2": 1}
    progress = {s["stream"]: s for s in board["streams"]}
    assert progress[pq.STREAM_ORDER]["processed"] == 3
    assert progress[pq.STREAM_ORDER]["backlog"] == 0
    assert progress[pq.STREAM_CUSTOMER]["total"] == 0
