"""跨流发布队列请求/响应 schema。"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


class CustomerEventIn(BaseModel):
    event_uid: str = Field(min_length=1, max_length=80)
    customer_id: str = Field(min_length=1, max_length=80)
    op: Literal["upsert", "delete"]
    payload: dict[str, Any] = Field(default_factory=dict)


class OrderEventIn(BaseModel):
    event_uid: str = Field(min_length=1, max_length=80)
    order_id: str = Field(min_length=1, max_length=80)
    customer_id: str = Field(min_length=1, max_length=80)
    payload: dict[str, Any] = Field(default_factory=dict)


class IngestResult(BaseModel):
    accepted: bool
    event_uid: str
    note: str


class CrashConfigIn(BaseModel):
    order_event_uid: str | None = Field(default=None, max_length=80)
    once: bool = True

    @field_validator("order_event_uid")
    @classmethod
    def blank_to_none(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            return None
        return value
