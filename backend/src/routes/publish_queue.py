"""跨流发布队列路由：/publish-queue/*。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from src.config.database import get_db
from src.constants.limits import MAX_PAGE_SIZE
from src.controllers import pq_controller
from src.middlewares.rbac import require_permission
from src.models.user import User
from src.schemas.publish_queue import CrashConfigIn, CustomerEventIn, OrderEventIn

router = APIRouter(prefix="/publish-queue", tags=["publish-queue"])


@router.get("/status")
def queue_status(
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("publish_queue:read")),
) -> dict:
    return pq_controller.get_dashboard(db)


@router.get("/orders")
def queue_orders(
    status: str | None = None,
    limit: int = 100,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("publish_queue:read")),
) -> dict:
    return pq_controller.list_orders(db, status, min(limit, MAX_PAGE_SIZE))


@router.get("/audit")
def queue_audit(
    limit: int = 100,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("publish_queue:read")),
) -> dict:
    return pq_controller.list_audit(db, min(limit, MAX_PAGE_SIZE))


@router.post("/events/customers")
def ingest_customer(
    request: Request,
    payload: CustomerEventIn,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("publish_queue:write")),
) -> dict:
    return pq_controller.ingest_customer(request, payload, user, db)


@router.post("/events/orders")
def ingest_order(
    request: Request,
    payload: OrderEventIn,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("publish_queue:write")),
) -> dict:
    return pq_controller.ingest_order(request, payload, user, db)


@router.post("/pump")
def pump(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("publish_queue:write")),
) -> dict:
    return pq_controller.trigger_pump(request, user, db)


@router.post("/crash-injection")
def crash_injection(
    request: Request,
    payload: CrashConfigIn,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("publish_queue:admin")),
) -> dict:
    return pq_controller.configure_crash(request, payload, user, db)


@router.post("/demo/reset")
def demo_reset(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("publish_queue:admin")),
) -> dict:
    return pq_controller.reset_demo(request, user, db)


@router.post("/demo/advance")
def demo_advance(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("publish_queue:write")),
) -> dict:
    return pq_controller.advance_demo(request, user, db)
