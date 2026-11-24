"""任务队列服务：排队顺序管理、资源额度占用与派发回收。

维护待执行任务的排队顺序、优先级和资源额度占用情况，
派发失败时回收入队。支持优先级排序和等待秒数计算。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.base import BaseModuleService


class TaskQueueService(BaseModuleService):
    MODULE = "task_queue"
    ENTITY = "排队任务"
    OPERATION_TERM = "关联"
    LIST_FIELDS = [
        "队列编号", "关联模板", "优先级", "期望开始", "重试次数",
        "占用节点", "等待秒数", "入队时间",
    ]
    REQUIRED_FIELDS = ["队列编号", "关联模板", "优先级"]
    STATUS_ORDER = ["待派发", "已派发", "已消费", "已丢弃"]
    ACTION_RULES = {"派发任务": "已派发", "回收任务": "已消费", "丢弃任务": "已丢弃"}
    NEGATIVE_ACTIONS = []

    _PRIORITY_WEIGHTS = {"high": 3, "medium": 2, "low": 1}

    def get_pending_by_priority(self) -> list[dict[str, Any]]:
        """按优先级排序返回待派发任务。"""
        items, _ = self.list_entries(size=10000)
        pending = [it for it in items if it.get("status") == "待派发"]
        pending.sort(
            key=lambda t: self._PRIORITY_WEIGHTS.get(
                str(t.get("优先级", "low")).lower(), 1
            ),
            reverse=True,
        )
        return pending

    def split_dispatch_recovery(self) -> dict[str, list[dict[str, Any]]]:
        """按派发是否已由节点接手分类已派发任务。

        已接手（登记了占用节点）：归属原节点，缺失的首条采集轨迹由发布
        恢复补写，不在此回收；未接手（无占用节点）：交由队列回收任务。
        """
        items, _ = self.list_entries(size=10000)
        dispatched = [it for it in items if it.get("status") == "已派发"]
        taken_over = [it for it in dispatched if str(it.get("占用节点") or "").strip()]
        unclaimed = [it for it in dispatched if not str(it.get("占用节点") or "").strip()]
        return {"taken_over": taken_over, "unclaimed": unclaimed}

    def recycle_failed_dispatches(self) -> dict[str, Any]:
        """回收已派发但无人接手的任务；节点已接手的任务仍归属原节点，不回收。"""
        unclaimed = self.split_dispatch_recovery()["unclaimed"]
        for it in unclaimed:
            self.run_action(it["id"], "回收任务")
        return {"ok": True, "recycled_count": len(unclaimed)}

    # 常见近义命名，统一到同一份回收实现
    reclaim_failed_dispatches = recycle_failed_dispatches
    recover_failed_dispatches = recycle_failed_dispatches
    reclaim_unclaimed_tasks = recycle_failed_dispatches

    def dispatch_to_node(self, task_id: int, node_code: str) -> dict[str, Any]:
        """将任务派发到指定节点。"""
        entry = self.get_entry(task_id)
        if entry is None:
            return {"ok": False, "reason": "任务不存在"}
        if entry.get("status") != "待派发":
            return {"ok": False, "reason": "任务状态不是待派发"}
        entry["占用节点"] = node_code
        self.run_action(task_id, "派发任务")
        return {"ok": True, "task_id": task_id, "node": node_code}