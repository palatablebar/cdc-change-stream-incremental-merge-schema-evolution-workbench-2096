"""演示剧本端到端：五个批次 + 故障注入，验证页面操作链与最终精确一次发布。"""
from __future__ import annotations

from src.constants import pq_enums as pq
from src.models.publish_queue import PqOutbox
from src.services import pq_engine, pq_service


def test_full_demo_scenario_with_crash_and_resume(db):
    # 批次 1：2 客户 + 5 订单 → 2 放行、3 暂存
    batch = pq_service.seed_next_batch(db)
    db.commit()
    assert batch["batch"] == 1
    pq_engine.run_pump(db)
    board = pq_service.dashboard(db)
    assert board["counts"]["published"] == 2
    assert board["counts"]["waiting"] == 3
    assert len(board["missing_customers"]) == 2  # C-003、C-004

    # 批次 2：C-003 到齐（回补 2 张）；同 event_uid 重投被 inbox 拒收，
    # 新 event_uid 的同单重投进引擎被台账去重，均不产生新发布
    pq_service.seed_next_batch(db)
    db.commit()
    result = pq_engine.run_pump(db)
    assert result["stats"]["duplicate"] == 1
    board = pq_service.dashboard(db)
    assert board["counts"]["published"] == 4
    assert board["counts"]["waiting"] == 1

    # 批次 3：C-004 建档放行 ORD-1005；C-002 注销（无 waiting，不新增阻断）
    pq_service.seed_next_batch(db)
    db.commit()
    pq_engine.run_pump(db)
    board = pq_service.dashboard(db)
    assert board["counts"]["published"] == 5
    assert board["counts"]["waiting"] == 0
    assert board["counts"]["blocked"] == 0

    # 批次 4：注销客户的新订单 + 重复投递 → 阻断，不进 outbox
    pq_service.seed_next_batch(db)
    db.commit()
    result = pq_engine.run_pump(db)
    assert result["stats"]["blocked"] == 1
    assert result["stats"]["redelivered_blocked"] == 1
    board = pq_service.dashboard(db)
    assert board["counts"]["blocked"] == 1
    assert board["counts"]["outbox"] == 5

    # 批次 5：在 demo-o-007 上注入故障，pump 必失败
    pq_service.seed_next_batch(db)
    db.commit()
    pq_service.configure_crash(db, "demo-o-007", once=True)
    db.commit()
    failed = False
    try:
        pq_engine.run_pump(db)
    except pq_engine.InjectedFailure:
        failed = True
    assert failed

    # 故障记录已留痕，租约已释放，可重新接管
    board = pq_service.dashboard(db)
    assert any(run["status"] == pq.PUMP_FAILED for run in board["pump_runs"])

    # 清除注入后从原处续跑：ORD-1007 放行，outbox 精确增加 1 行
    pq_service.configure_crash(db, None)
    db.commit()
    pq_engine.run_pump(db)
    board = pq_service.dashboard(db)
    assert board["counts"]["outbox"] == 6
    assert board["counts"]["published"] == 6
    assert db.query(PqOutbox).count() == 6
    # 每个 order_id 在 outbox 中唯一
    rows = db.query(PqOutbox.order_id).all()
    assert len(rows) == len({row[0] for row in rows})

    # 批次耗尽后再推进是幂等 no-op
    done = pq_service.seed_next_batch(db)
    db.commit()
    assert done["finished"] is True


def test_reset_clears_everything(db):
    pq_service.seed_next_batch(db)
    db.commit()
    pq_engine.run_pump(db)
    pq_service.reset_all(db)
    db.commit()
    board = pq_service.dashboard(db)
    assert board["counts"] == {
        pq.ORDER_WAITING: 0,
        pq.ORDER_PUBLISHED: 0,
        pq.ORDER_BLOCKED: 0,
        "outbox": 0,
    }
    assert board["streams"][0]["total"] == 0
    assert board["demo_batch"] == "0"
