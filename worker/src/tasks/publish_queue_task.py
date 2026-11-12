"""跨流发布队列定时 pump：beat 周期触发，故障后下个周期自动从原处续跑。"""
from __future__ import annotations

from typing import Any

from src.config.database import session_scope
from src.services.pq_engine import InjectedFailure, PumpBusy, run_pump
from src.utils.logger import get_logger

from worker_src.config.celery_app import celery_app

logger = get_logger("cdcmerge.worker.publish_queue")

TASK_NAME = "cdcmerge.publish_queue_pump"


@celery_app.task(name=TASK_NAME, bind=True, max_retries=0)
def publish_queue_pump_task(self) -> dict[str, Any]:
    try:
        with session_scope() as db:
            result = run_pump(db, trigger="scheduled")
        if result["processed"]:
            logger.info("publish-queue pump processed=%s stats=%s", result["processed"], result["stats"])
        return result
    except PumpBusy as exc:
        # 另一个 pump（通常是手动触发）正在跑，跳过本周期即可
        logger.info("publish-queue pump skipped: %s", exc)
        return {"status": "busy", "message": str(exc)}
    except InjectedFailure as exc:
        # 演示故障：事务已回滚、位点未前进，下周期从同一事件续跑
        logger.warning("publish-queue pump injected failure (will resume next tick): %s", exc)
        return {"status": "failed_injected", "error": str(exc)}
    except Exception as exc:  # noqa: BLE001 - beat 任务不能抛出打断心跳
        logger.error("publish-queue pump failed (will resume next tick): %s", exc, exc_info=True)
        return {"status": "failed", "error": str(exc)}
