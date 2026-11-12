"""API 层冒烟：TestClient + SQLite 覆盖 get_db，验证鉴权、RBAC 与完整操作链。"""
from __future__ import annotations

import sys
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

BACKEND = Path(__file__).resolve().parents[2]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.config.database import get_db  # noqa: E402
from src.main import app  # noqa: E402
from src.middlewares.auth import get_current_user  # noqa: E402
from src.models import Base  # noqa: E402

ADMIN_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
VIEWER_ID = uuid.UUID("00000000-0000-0000-0000-000000000002")


@pytest.fixture()
def client():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _attach(conn, _):
        conn.execute("ATTACH DATABASE ':memory:' AS cdc")

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)

    def _override_db():
        db = Session()
        try:
            yield db
        finally:
            db.close()

    def _make_user(role):
        return SimpleNamespace(id=ADMIN_ID if role == "admin" else VIEWER_ID, role=role, is_active=True,
                               email=f"{role}@cdc.local", display_name=role)

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_current_user] = lambda: _make_user("admin")
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()
    engine.dispose()


def test_full_queue_lifecycle_over_http(client):
    # 初始看板
    board = client.get("/api/publish-queue/status").json()
    assert board["counts"]["outbox"] == 0
    assert len(board["streams"]) == 2

    # 订单先到（客户缺失）
    for i in range(2):
        resp = client.post(
            "/api/publish-queue/events/orders",
            json={"event_uid": f"o{i}", "order_id": f"ORD-{i}", "customer_id": "C-1",
                  "payload": {"amount": 10 + i}},
        )
        assert resp.status_code == 200 and resp.json()["accepted"] is True

    # 同 event_uid 重投：inbox 拒收
    dup = client.post(
        "/api/publish-queue/events/orders",
        json={"event_uid": "o0", "order_id": "ORD-0", "customer_id": "C-1", "payload": {}},
    ).json()
    assert dup["accepted"] is False

    pump1 = client.post("/api/publish-queue/pump").json()
    assert pump1["stats"]["staged"] == 2

    # 客户后到 → 自动回补
    client.post(
        "/api/publish-queue/events/customers",
        json={"event_uid": "c1", "customer_id": "C-1", "op": "upsert", "payload": {"name": "客户一"}},
    )
    pump2 = client.post("/api/publish-queue/pump").json()
    assert pump2["stats"]["released"] == 2

    board = client.get("/api/publish-queue/status").json()
    assert board["counts"]["published"] == 2
    assert board["counts"]["outbox"] == 2
    assert board["missing_customers"] == []

    # 订单列表与审计可查
    orders = client.get("/api/publish-queue/orders?status=published").json()["items"]
    assert len(orders) == 2
    audit = client.get("/api/publish-queue/audit").json()["items"]
    actions = {row["action"] for row in audit}
    assert "order.released" in actions and "order.staged" in actions


def test_viewer_cannot_write(client):
    # 把当前用户换成 viewer，写操作应被 RBAC 拒绝
    from src.middlewares.auth import get_current_user as gcu
    app.dependency_overrides[gcu] = lambda: SimpleNamespace(
        id=VIEWER_ID, role="viewer", is_active=True,
        email="v@cdc.local", display_name="v")
    resp = client.post(
        "/api/publish-queue/events/orders",
        json={"event_uid": "x", "order_id": "O", "customer_id": "C", "payload": {}},
    )
    assert resp.status_code == 403
    # 读操作允许
    assert client.get("/api/publish-queue/status").status_code == 200


def test_demo_advance_and_crash_flow(client):
    # 先推进 5 个批次的事件并逐批 pump（无注入，全部正常处理）
    for _ in range(5):
        r = client.post("/api/publish-queue/demo/advance").json()
        client.post("/api/publish-queue/pump")
        if r.get("finished"):
            break
    board = client.get("/api/publish-queue/status").json()
    assert board["streams"][1]["backlog"] == 0
    assert board["counts"]["blocked"] >= 1

    # 重置演示；reset 会清空 runtime 配置，因此注入要在 reset 之后开启
    client.post("/api/publish-queue/demo/reset")
    cfg = client.post("/api/publish-queue/crash-injection",
                      json={"order_event_uid": "demo-o-007", "once": True}).json()
    assert cfg["enabled"] is True

    for _ in range(5):
        client.post("/api/publish-queue/demo/advance")
        client.post("/api/publish-queue/pump")
    board = client.get("/api/publish-queue/status").json()
    # 批次5 的订单事件命中注入 → 该事件未处理，存在积压与失败记录
    assert any(run["status"] == "failed" for run in board["pump_runs"])
    assert board["streams"][1]["backlog"] >= 1

    # 清除注入，再 pump 续跑追平
    client.post("/api/publish-queue/crash-injection", json={"order_event_uid": None})
    client.post("/api/publish-queue/pump")
    board = client.get("/api/publish-queue/status").json()
    assert board["streams"][1]["backlog"] == 0
