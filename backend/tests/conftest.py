"""pytest 共享夹具：内存 SQLite，ATTACH 出 cdc schema 以兼容 PG 的 schema 限定名。

生产用 PostgreSQL（cdc. 前缀），引擎代码方言中立；SQLite 通过 ATTACH 同名
schema 让 ORM 原样建表/查询，从而用同一套模型验证状态机、检查点与故障恢复。
PG 专有类型（UUID/JSONB）在 SQLite 上编译为 CHAR/JSON，并注册全局 bind 钩子
把 uuid.UUID 转成字符串。
"""
from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.pool import StaticPool
from sqlalchemy.orm import sessionmaker

BACKEND = Path(__file__).resolve().parents[2]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.models import Base  # noqa: E402
from src.models import publish_queue as pq_models  # noqa: E402


@compiles(UUID, "sqlite")
def _sqlite_uuid(type_, compiler, **kw):
    return "CHAR(36)"


@compiles(JSONB, "sqlite")
def _sqlite_jsonb(type_, compiler, **kw):
    return "JSON"


# PG 的 UUID.bind_processor 假定输入是 uuid.UUID 并取 .hex；SQLite 上重写为
# "UUID/字符串都接受，统一落库为字符串"，取回时不强制还原（测试里字符串即可）。
def _sqlite_uuid_bind_processor(self, dialect):
    def process(value):
        if value is None:
            return None
        return str(value)

    return process


UUID.bind_processor = _sqlite_uuid_bind_processor  # type: ignore[method-assign]


def _make_engine():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _attach(dbapi_conn, _record):
        dbapi_conn.execute("ATTACH DATABASE ':memory:' AS cdc")

    return engine


@pytest.fixture()
def db():
    engine = _make_engine()
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


@pytest.fixture()
def models():
    return pq_models
