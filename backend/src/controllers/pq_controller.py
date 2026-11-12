"""跨流发布队列控制器：看板查询、事件注入、pump 控制、演示剧本。"""
from __future__ import annotations

from fastapi import Request
from sqlalchemy.orm import Session

from src.models.user import User
from src.schemas.publish_queue import CrashConfigIn, CustomerEventIn, OrderEventIn
from src.services import audit_service, pq_engine, pq_service


def get_dashboard(db: Session) -> dict:
    return pq_service.dashboard(db)


def list_orders(db: Session, status: str | None, limit: int) -> dict:
    return {"items": pq_service.list_orders(db, status, limit)}


def list_audit(db: Session, limit: int) -> dict:
    return {"items": pq_service.list_audit(db, limit)}


def ingest_customer(request: Request, payload: CustomerEventIn, user: User, db: Session) -> dict:
    accepted = pq_service.insert_customer_event(
        db, payload.event_uid, payload.customer_id, payload.op, payload.payload
    )
    audit_service.record(
        db,
        action="pq.ingest_customer",
        entity_type="PqCustomerInbox",
        entity_id=payload.event_uid,
        actor_id=str(user.id),
        payload={"accepted": accepted, "customer_id": payload.customer_id, "op": payload.op},
        ip=getattr(request.state, "client_ip", None),
        trace_id=getattr(request.state, "trace_id", None),
    )
    return {
        "accepted": accepted,
        "event_uid": payload.event_uid,
        "note": "已进入客户资料流" if accepted else "event_uid 已存在，重复投递已忽略",
    }


def ingest_order(request: Request, payload: OrderEventIn, user: User, db: Session) -> dict:
    accepted = pq_service.insert_order_event(
        db, payload.event_uid, payload.order_id, payload.customer_id, "upsert", payload.payload
    )
    audit_service.record(
        db,
        action="pq.ingest_order",
        entity_type="PqOrderInbox",
        entity_id=payload.event_uid,
        actor_id=str(user.id),
        payload={"accepted": accepted, "order_id": payload.order_id, "customer_id": payload.customer_id},
        ip=getattr(request.state, "client_ip", None),
        trace_id=getattr(request.state, "trace_id", None),
    )
    return {
        "accepted": accepted,
        "event_uid": payload.event_uid,
        "note": "已进入订单流" if accepted else "event_uid 已存在，重复投递已忽略",
    }


def trigger_pump(request: Request, user: User, db: Session) -> dict:
    try:
        result = pq_engine.run_pump(db, trigger="manual")
    except pq_engine.PumpBusy as exc:
        return {"status": "busy", "message": str(exc)}
    except pq_engine.InjectedFailure as exc:
        # 事务已回滚、位点未前进；返回失败态，下一次触发从同一事件续跑
        return {
            "status": "failed_injected",
            "message": str(exc),
            "resumable": True,
            "hint": "检查点未前进，清除故障注入后再次触发即可从原处续跑",
        }
    audit_service.record(
        db,
        action="pq.pump",
        entity_type="PqPumpRun",
        entity_id=result["holder"],
        actor_id=str(user.id),
        payload=result,
        ip=getattr(request.state, "client_ip", None),
        trace_id=getattr(request.state, "trace_id", None),
    )
    return result


def configure_crash(request: Request, payload: CrashConfigIn, user: User, db: Session) -> dict:
    pq_service.configure_crash(db, payload.order_event_uid, once=payload.once)
    audit_service.record(
        db,
        action="pq.crash_config",
        entity_type="PqRuntime",
        entity_id=payload.order_event_uid or "",
        actor_id=str(user.id),
        payload={"order_event_uid": payload.order_event_uid, "once": payload.once},
        ip=getattr(request.state, "client_ip", None),
        trace_id=getattr(request.state, "trace_id", None),
    )
    return {
        "enabled": bool(payload.order_event_uid),
        "order_event_uid": payload.order_event_uid,
        "once": payload.once,
    }


def reset_demo(request: Request, user: User, db: Session) -> dict:
    pq_service.reset_all(db)
    audit_service.record(
        db,
        action="pq.reset",
        entity_type="PqRuntime",
        entity_id="*",
        actor_id=str(user.id),
        payload={},
        ip=getattr(request.state, "client_ip", None),
        trace_id=getattr(request.state, "trace_id", None),
    )
    return {"reset": True}


def advance_demo(request: Request, user: User, db: Session) -> dict:
    result = pq_service.seed_next_batch(db)
    audit_service.record(
        db,
        action="pq.demo_batch",
        entity_type="PqRuntime",
        entity_id=str(result.get("batch")),
        actor_id=str(user.id),
        payload=result,
        ip=getattr(request.state, "client_ip", None),
        trace_id=getattr(request.state, "trace_id", None),
    )
    return result
