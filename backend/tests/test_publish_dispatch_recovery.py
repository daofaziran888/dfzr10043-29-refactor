"""流程模板发布采集调度的资料准备与中断恢复测试。

覆盖重构要求：
- 排程输入、队列数据、执行记录、首轨迹的独立资料准备；
- 过期拒绝（不派发）、无节点待派、正常派发三类既有输入输出；
- 首轨迹写入中断后：队列回收区分「无人接手→回收」与「节点已接手→保留」；
- 恢复只补 A 缺失的首轨迹（一份执行、一份首轨迹、幂等），
  B 的同模板第二次发布独立可查并可继续原节点采集。
"""
from __future__ import annotations

import pytest

from app.pipeline import (
    prepare_execution_record,
    prepare_first_trace,
    prepare_queue_task,
    prepare_schedule_input,
    publish_flow_and_schedule,
    recover_publish_dispatch,
)
from app.services import service_for


@pytest.fixture(autouse=True)
def _clear_inflight_context():
    from app.pipeline import _inflight_publish_context
    _inflight_publish_context.clear()
    yield
    _inflight_publish_context.clear()


class TestPublishPreparation:
    """排程输入 / 队列数据 / 执行记录 / 首轨迹资料准备相互独立。"""

    def test_preparation_inputs_are_independent(self):
        template = {"模板编号": "FLOW-X", "起始地址": "https://example-x/data"}
        plan_values = prepare_schedule_input(9, template, {"计划编号": "SCHE-X"})
        queue_values = prepare_queue_task(9, template, {})
        run_values = prepare_execution_record(9, template, {"节点编号": "ROBO-9"})
        trace_values = prepare_first_trace(template, {"id": 5, "执行编号": "RUN-P9"})

        assert plan_values["计划编号"] == "SCHE-X"
        assert plan_values["关联模板"] == "FLOW-X"
        assert queue_values["队列编号"] == "TASK-P9"
        assert queue_values["占用节点"] == ""
        assert run_values["执行编号"] == "RUN-P9"
        assert run_values["执行节点"] == "ROBO-9"
        assert trace_values["轨迹编号"] == "STEP-R5"
        assert trace_values["关联执行"] == "RUN-P9"
        assert "https://example-x/data" in trace_values["输入快照"]


class TestPublishGuardrails:
    """既有三类输入输出保持不变。"""

    def test_expired_schedule_rejects_without_dispatch(self):
        tpl_svc = service_for("flow_template")
        tpl_id = tpl_svc.list_entries()[0][0]["id"]
        template_code = tpl_svc.get_entry(tpl_id)["模板编号"]
        queue_svc = service_for("task_queue")
        run_svc = service_for("flow_run")
        queue_before = len(queue_svc.list_entries(size=10000)[0])

        result = publish_flow_and_schedule(tpl_id, {
            "计划编号": "SCHE-EXP",
            "生效止日": "2020-01-01",
        })

        assert result["status"] == "rejected"
        assert "过期" in result["reason"]
        assert "task_id" not in result
        assert len(queue_svc.list_entries(size=10000)[0]) == queue_before
        assert run_svc.get_runs_by_template(template_code) == []

    def test_offline_nodes_leave_task_pending_only(self):
        node_svc = service_for("robot_node")
        for node in node_svc.list_entries(size=1000)[0]:
            node["status"] = "离线"
        tpl_svc = service_for("flow_template")
        tpl_id = tpl_svc.list_entries()[0][0]["id"]
        template_code = tpl_svc.get_entry(tpl_id)["模板编号"]

        result = publish_flow_and_schedule(tpl_id, {"计划编号": "SCHE-OFF"})

        assert result["status"] == "pending"
        assert "无在线节点" in result["reason"]
        queue_svc = service_for("task_queue")
        task = queue_svc.get_entry(result["task_id"])
        assert task["status"] == "待派发"
        run_svc = service_for("flow_run")
        assert run_svc.get_runs_by_template(template_code) == []

        # 恢复不会把未派发数据补进采集执行
        recovered = recover_publish_dispatch(tpl_id)
        assert recovered["status"] == "nothing_to_recover"
        assert run_svc.get_runs_by_template(template_code) == []


class TestQueueRecoveryClassification:
    def test_classifies_taken_over_vs_unclaimed(self):
        queue_svc = service_for("task_queue")
        taken, missing = queue_svc.create_entry({
            "队列编号": "TASK-TAKEN", "关联模板": "FLOW-0001", "优先级": "high",
        })
        assert missing == []
        queue_svc.dispatch_to_node(taken["id"], "ROBO-0002")
        orphan, _ = queue_svc.create_entry({
            "队列编号": "TASK-UNCLAIMED", "关联模板": "FLOW-0001", "优先级": "low",
        })
        queue_svc.run_action(orphan["id"], "派发任务")

        groups = queue_svc.split_dispatch_recovery()
        assert taken["id"] in {t["id"] for t in groups["taken_over"]}
        assert orphan["id"] in {t["id"] for t in groups["unclaimed"]}

        result = queue_svc.recycle_failed_dispatches()
        assert result["ok"] is True
        assert result["recycled_count"] >= 1
        assert queue_svc.get_entry(orphan["id"])["status"] == "已消费"
        assert queue_svc.get_entry(taken["id"])["status"] == "已派发"
        assert queue_svc.get_entry(taken["id"])["占用节点"] == "ROBO-0002"


class TestInterruptedFirstTraceRecovery:
    """阻断首轨迹 → B 上线另发 → 队列回收 → 恢复 A 的完整场景。"""

    def test_full_interrupt_reclaim_recover_scenario(self, monkeypatch):
        tpl_svc = service_for("flow_template")
        tpl_id = tpl_svc.list_entries()[0][0]["id"]
        template_code = tpl_svc.get_entry(tpl_id)["模板编号"]
        queue_svc = service_for("task_queue")
        run_svc = service_for("flow_run")
        trace_svc = service_for("step_trace")
        node_svc = service_for("robot_node")

        # 种子数据中仅 ROBO-0002 在线，即节点 A
        node_a = next(n for n in node_svc.list_entries(size=1000)[0]
                      if n["节点编号"] == "ROBO-0002")
        assert node_a["status"] == "在线"

        # 公开发布正常派给 A，但阻断首条采集轨迹写入
        original_create_trace = trace_svc.create_entry

        def block_first_trace(values):
            raise RuntimeError("首条采集轨迹写入中断")

        monkeypatch.setattr(trace_svc, "create_entry", block_first_trace)
        with pytest.raises(RuntimeError, match="中断"):
            publish_flow_and_schedule(tpl_id, {"计划编号": "SCHE-REC-1"})
        monkeypatch.setattr(trace_svc, "create_entry", original_create_trace)

        # 中断现场：任务已派发且归属 A，执行已建成，首轨迹缺失
        task_a = next(t for t in queue_svc.list_entries(size=10000)[0]
                      if t.get("关联模板") == template_code and t.get("status") == "已派发")
        task_a_id = task_a["id"]
        assert task_a["占用节点"] == "ROBO-0002"
        run_a = next(r for r in run_svc.get_runs_by_template(template_code)
                     if r.get("执行节点") == "ROBO-0002")
        run_a_id = run_a["id"]
        assert not any(t.get("轨迹编号") == f"STEP-R{run_a_id}"
                       for t in trace_svc.list_entries(size=10000)[0])

        # 另有一条已派发但实际无人接手的队列条目
        orphan, missing = queue_svc.create_entry({
            "队列编号": "TASK-ORPHAN-REC",
            "关联模板": template_code,
            "优先级": "low",
        })
        assert missing == []
        queue_svc.run_action(orphan["id"], "派发任务")
        assert not queue_svc.get_entry(orphan["id"])["占用节点"]

        # 节点 B（ROBO-0001）上线
        node_b = next(n for n in node_svc.list_entries(size=1000)[0]
                      if n["节点编号"] == "ROBO-0001")
        node_svc.run_action(node_b["id"], "注册上线")
        assert node_svc.get_entry(node_b["id"])["status"] == "在线"

        # 另发同模板排程：发布入口先执行队列回收，再正常派给 B
        runs_before_second_publish = len(run_svc.list_entries(size=10000)[0])
        result_b = publish_flow_and_schedule(tpl_id, {"计划编号": "SCHE-REC-2"})
        assert result_b["status"] == "dispatched"
        assert result_b["node"] == "ROBO-0001"

        # 实际无人接手的条目仍可回收
        assert queue_svc.get_entry(orphan["id"])["status"] == "已消费"
        # A 的条目仍属于 A：首轨迹未写成也不能回收
        task_a_after = queue_svc.get_entry(task_a_id)
        assert task_a_after["status"] == "已派发"
        assert task_a_after["占用节点"] == "ROBO-0002"
        assert not any(t.get("轨迹编号") == f"STEP-R{run_a_id}"
                       for t in trace_svc.list_entries(size=10000)[0])
        # 第二次发布新建了 B 的执行，没有复用 A 的执行
        assert len(run_svc.list_entries(size=10000)[0]) == runs_before_second_publish + 1

        # 恢复未完成的发布：返回沿用原派发结构
        runs_before_recover = len(run_svc.list_entries(size=10000)[0])
        traces_before_recover = len(trace_svc.list_entries(size=10000)[0])
        recovered = recover_publish_dispatch(tpl_id)
        assert recovered["status"] == "dispatched"
        assert set(recovered.keys()) == {
            "status", "template_id", "plan_id", "task_id",
            "node", "run_id", "trace_id",
        }
        assert recovered["template_id"] == tpl_id
        assert recovered["task_id"] == task_a_id
        assert recovered["node"] == "ROBO-0002"
        assert recovered["run_id"] == run_a_id
        assert recovered["trace_id"] > 0

        # A 只产生缺失的首轨迹，不新建执行
        assert len(run_svc.list_entries(size=10000)[0]) == runs_before_recover
        assert len(trace_svc.list_entries(size=10000)[0]) == traces_before_recover + 1
        trace_a = trace_svc.get_entry(recovered["trace_id"])
        assert trace_a["轨迹编号"] == f"STEP-R{run_a_id}"
        assert trace_a["关联执行"] == run_a["执行编号"]
        assert trace_a["退出码"] == "0"

        # 已由节点接手的范围只对应一份执行与首轨迹：重复恢复幂等
        again = recover_publish_dispatch(tpl_id)
        assert again["status"] == "dispatched"
        assert again["trace_id"] == recovered["trace_id"]
        assert len(run_svc.list_entries(size=10000)[0]) == runs_before_recover
        assert len(trace_svc.list_entries(size=10000)[0]) == traces_before_recover + 1
        assert sum(1 for r in run_svc.get_runs_by_template(template_code)
                   if r.get("执行节点") == "ROBO-0002") == 1
        assert sum(1 for t in trace_svc.list_entries(size=10000)[0]
                   if t.get("轨迹编号") == f"STEP-R{run_a_id}") == 1

        # B 的执行可独立查询，并继续归属原节点 B 采集
        run_b = run_svc.get_entry(result_b["run_id"])
        assert run_b is not None
        assert run_b["执行节点"] == "ROBO-0001"
        trace_b = next(t for t in trace_svc.list_entries(size=10000)[0]
                       if t.get("轨迹编号") == f"STEP-R{run_b['id']}")
        assert trace_b["关联执行"] == run_b["执行编号"]
        assert queue_svc.get_entry(result_b["task_id"])["占用节点"] == "ROBO-0001"

        # 两次发布的执行编号已消歧，按执行编号可分别独立查询各自的首轨迹
        assert [t["轨迹编号"] for t in trace_svc.get_traces_by_run(run_b["执行编号"])] \
            == [f"STEP-R{run_b['id']}"]
        assert [t["轨迹编号"] for t in trace_svc.get_traces_by_run(run_a["执行编号"])] \
            == [f"STEP-R{run_a_id}"]

        # B 的执行可在原节点继续后续采集：追加步骤轨迹后仍只归属 B 的执行
        next_trace, missing = trace_svc.create_entry({
            "轨迹编号": f"STEP-R{run_b['id']}-2",
            "关联执行": run_b["执行编号"],
            "步骤序号": "2",
            "步骤名称": "采集步骤-翻页",
            "退出码": "0",
        })
        assert missing == []
        assert len(trace_svc.get_traces_by_run(run_b["执行编号"])) == 2
        assert len(trace_svc.get_traces_by_run(run_a["执行编号"])) == 1
        run_svc.complete_run(run_b["id"], 30)
        assert run_svc.get_entry(run_b["id"])["status"] == "已完成"
