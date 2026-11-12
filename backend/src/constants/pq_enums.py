"""跨流发布队列（publish-queue）专属枚举与中文文案。

两条变更流：
  customer_stream 客户资料流（upsert=建档/更新，delete=注销）
  order_stream    订单流（只有 upsert，结账时可能比客户先到几秒）

订单台账状态机：
  waiting   客户缺失，暂存中（仍缺依赖）
  published 客户已到齐且发布成功（放行）
  blocked   客户已注销，关联订单永久阻断，不再放行

发布 outbox 是最终幂等账本：order_id 唯一，重复投递最多产生一行。
"""
from __future__ import annotations

STREAM_CUSTOMER = "customer_stream"
STREAM_ORDER = "order_stream"
PQ_STREAMS: tuple[str, str] = (STREAM_CUSTOMER, STREAM_ORDER)

OP_UPSERT = "upsert"
OP_DELETE = "delete"

# pq_orders.status
ORDER_WAITING = "waiting"
ORDER_PUBLISHED = "published"
ORDER_BLOCKED = "blocked"
PQ_ORDER_STATUSES: tuple[str, str, str] = (ORDER_WAITING, ORDER_PUBLISHED, ORDER_BLOCKED)

# pq_pump_runs.status
PUMP_RUNNING = "running"
PUMP_SUCCEEDED = "succeeded"
PUMP_FAILED = "failed"

# pq_audit_events.action
AUDIT_ORDER_STAGED = "order.staged"
AUDIT_ORDER_DUPLICATE = "order.duplicate"
AUDIT_ORDER_RELEASED = "order.released"
AUDIT_ORDER_BLOCKED = "order.blocked"
AUDIT_ORDER_REDELIVERED_BLOCKED = "order.redelivered_blocked"
AUDIT_CUSTOMER_REGISTERED = "customer.registered"
AUDIT_CUSTOMER_UPDATED = "customer.updated"
AUDIT_CUSTOMER_DEREGISTERED = "customer.deregistered"
AUDIT_PUMP_STARTED = "pump.started"
AUDIT_PUMP_FAILED = "pump.failed"

# 运行时配置键
CFG_CRASH_ON_ORDER_UID = "crash_on_order_uid"   # 命中该事件 UID 时注入故障（演示断点续跑）
CFG_CRASH_ONCE = "crash_once"                    # 故障是否只触发一次
CFG_DEMO_BATCH = "demo_batch"

PQ_ORDER_STATUS_LABELS: dict[str, str] = {
    ORDER_WAITING: "暂存中",
    ORDER_PUBLISHED: "已放行",
    ORDER_BLOCKED: "已阻断",
}

PQ_STREAM_LABELS: dict[str, str] = {
    STREAM_CUSTOMER: "客户资料流",
    STREAM_ORDER: "订单流",
}

PQ_AUDIT_LABELS: dict[str, str] = {
    AUDIT_ORDER_STAGED: "订单暂存",
    AUDIT_ORDER_DUPLICATE: "重复投递（忽略）",
    AUDIT_ORDER_RELEASED: "订单放行发布",
    AUDIT_ORDER_BLOCKED: "注销阻断",
    AUDIT_ORDER_REDELIVERED_BLOCKED: "阻断后重复投递（拒绝）",
    AUDIT_CUSTOMER_REGISTERED: "客户建档",
    AUDIT_CUSTOMER_UPDATED: "客户更新",
    AUDIT_CUSTOMER_DEREGISTERED: "客户注销",
    AUDIT_PUMP_STARTED: "队列处理启动",
    AUDIT_PUMP_FAILED: "队列处理失败",
}
