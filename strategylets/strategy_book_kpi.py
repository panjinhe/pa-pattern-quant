from __future__ import annotations

from typing import Any


def _infer_bars_per_day(timeframe: str) -> float | None:
    normalized = (timeframe or "").strip().lower()
    if len(normalized) < 2:
        return None
    unit = normalized[-1]
    raw_value = normalized[:-1]
    if not raw_value.isdigit():
        return None
    value = int(raw_value)
    if value <= 0:
        return None
    if unit == "m":
        return (24 * 60) / value
    if unit == "h":
        return 24 / value
    if unit == "d":
        return 1 / value
    return None


def _display_number(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "待补充"
    return f"{value:.{digits}f}"


def _metric(
    metric_id: str,
    label: str,
    status: str,
    display: str,
    pass_rule: str,
    watch_rule: str | None = None,
    comment: str | None = None,
) -> dict[str, Any]:
    payload = {
        "metric_id": metric_id,
        "label": label,
        "status": status,
        "display": display,
        "pass_rule": pass_rule,
    }
    if watch_rule:
        payload["watch_rule"] = watch_rule
    if comment:
        payload["comment"] = comment
    return payload


def build_strategy_book_assessment(
    *,
    timeframe: str,
    sample_rows: int,
    signal_count: int,
    trade_count: int,
    avg_r: float | None,
    profit_factor: float | None,
    max_drawdown: float | None,
    realtime_safe: bool,
    uses_future_bars: bool,
    supports_visualization: bool,
    manual_review_status: str = "pending",
) -> dict[str, Any]:
    bars_per_day = _infer_bars_per_day(timeframe)
    sample_days = (sample_rows / bars_per_day) if bars_per_day else None
    opportunities_per_day = (signal_count / sample_days) if sample_days and sample_days > 0 else None

    if opportunities_per_day is None:
        frequency_status = "待补充"
    elif opportunities_per_day >= 1.0:
        frequency_status = "通过"
    elif opportunities_per_day >= 0.4:
        frequency_status = "观察"
    else:
        frequency_status = "不通过"

    if avg_r is None or profit_factor is None or max_drawdown is None:
        baseline_status = "待补充"
    elif avg_r >= 0.0 and profit_factor >= 1.0 and max_drawdown >= -0.25:
        baseline_status = "通过"
    elif avg_r >= -0.05 and profit_factor >= 0.95 and max_drawdown >= -0.35:
        baseline_status = "观察"
    else:
        baseline_status = "不通过"

    if trade_count >= 120:
        sample_status = "通过"
    elif trade_count >= 60:
        sample_status = "观察"
    else:
        sample_status = "不通过"

    execution_status = (
        "通过"
        if realtime_safe and (not uses_future_bars) and supports_visualization
        else "不通过"
    )

    manual_status_map = {
        "pending": "待人工复核",
        "pass": "通过",
        "fail": "不通过",
    }
    manual_status = manual_status_map.get(manual_review_status, "待人工复核")

    auto_statuses = [frequency_status, baseline_status, sample_status, execution_status]
    failing_metrics = [
        label
        for label, status in [
            ("无过滤机会频率", frequency_status),
            ("基线表现", baseline_status),
            ("样本充足性", sample_status),
            ("实盘化前提", execution_status),
        ]
        if status == "不通过"
    ]
    watching_metrics = [
        label
        for label, status in [
            ("无过滤机会频率", frequency_status),
            ("基线表现", baseline_status),
            ("样本充足性", sample_status),
            ("实盘化前提", execution_status),
        ]
        if status == "观察"
    ]

    if failing_metrics:
        auto_stage = "暂不纳入"
        final_status = "暂不纳入"
        recommendation = f"至少有 1 项硬门槛未达标：{', '.join(failing_metrics)}。先修正后再谈入书。"
    elif all(status == "通过" for status in auto_statuses):
        auto_stage = "可进入人工复核"
        if manual_status == "通过":
            final_status = "可纳入策略书"
            recommendation = "自动门槛与人工复核均通过，可以纳入策略书。"
        elif manual_status == "不通过":
            final_status = "暂不纳入"
            recommendation = "自动门槛通过，但人工图表复核未通过，暂不纳入策略书。"
        else:
            final_status = "待人工复核"
            recommendation = "自动门槛通过，下一步做样本图表人工复核。"
    else:
        auto_stage = "观察名单"
        if manual_status == "通过":
            final_status = "观察名单"
            recommendation = "人工复核通过，但自动 KPI 仍有观察项，建议先放观察名单。"
        elif manual_status == "不通过":
            final_status = "暂不纳入"
            recommendation = "自动 KPI 尚未稳定，且人工复核未通过，暂不纳入策略书。"
        else:
            final_status = "继续观察"
            recommendation = (
                "自动 KPI 未完全通过，先放观察名单。优先补齐："
                + ", ".join(watching_metrics)
                + "。"
            )

    metrics = [
        _metric(
            "opportunity_frequency",
            "无过滤机会频率",
            frequency_status,
            (
                f"{_display_number(opportunities_per_day)} 次/天"
                + (f"（signal_count={signal_count}）" if opportunities_per_day is not None else "")
            ),
            ">= 1.00 次/天",
            ">= 0.40 次/天",
            "这里用 signal_count 近似 unfilter 机会数，便于先统一口径。",
        ),
        _metric(
            "baseline_performance",
            "基线表现",
            baseline_status,
            (
                f"avg_r={_display_number(avg_r, 4)} / "
                f"profit_factor={_display_number(profit_factor, 3)} / "
                f"max_drawdown={_display_number(max_drawdown, 3)}"
            ),
            "avg_r >= 0 且 PF >= 1.0 且最大回撤 >= -0.25",
            "avg_r >= -0.05 且 PF >= 0.95 且最大回撤 >= -0.35",
            "leader 提到的“至少 break even、不要亏太惨”先落成这组组合门槛。",
        ),
        _metric(
            "sample_sufficiency",
            "样本充足性",
            sample_status,
            f"{trade_count} 笔 baseline 交易",
            ">= 120 笔",
            ">= 60 笔",
            "样本太少时，回测盈亏很容易被偶然波动主导。",
        ),
        _metric(
            "execution_readiness",
            "实盘化前提",
            execution_status,
            (
                f"realtime_safe={realtime_safe}, "
                f"uses_future_bars={uses_future_bars}, "
                f"supports_visualization={supports_visualization}"
            ),
            "实时安全 + 不依赖未来 K 线 + 可视化可复核",
            comment="这是策略能否进入策略书的硬前提，不满足时不进入人工复核。",
        ),
        _metric(
            "manual_visual_review",
            "人工图表复核",
            manual_status,
            "待人工打分",
            "Top20 高分样本 + Random20 时间分散样本中，>= 70% 被判定为结构合理",
            comment="建议额外记录“明显误触发/像未来函数/真人不会做”的样本数，超过 3 张直接不通过。",
        ),
    ]

    return {
        "version": "v1",
        "timeframe": timeframe,
        "sample_rows": sample_rows,
        "sample_days": round(sample_days, 2) if sample_days is not None else None,
        "signal_count": signal_count,
        "trade_count": trade_count,
        "auto_stage": auto_stage,
        "final_status": final_status,
        "manual_review_required": True,
        "manual_review_status": manual_status,
        "metrics": metrics,
        "blocking_metrics": failing_metrics,
        "watch_metrics": watching_metrics,
        "recommendation": recommendation,
    }


def render_strategy_book_assessment(assessment: dict[str, Any]) -> str:
    sample_days = assessment.get("sample_days")
    sample_days_display = f"{sample_days:.2f} 天" if sample_days is not None else "待补充"
    lines = [
        "## 策略书 KPI 与准入",
        "",
        f"- 自动分级：`{assessment['auto_stage']}`",
        f"- 当前结论：`{assessment['final_status']}`",
        f"- 样本覆盖：`{sample_days_display}`",
        f"- 建议动作：{assessment['recommendation']}",
    ]
    for metric in assessment.get("metrics", []):
        lines.append(
            f"- {metric['label']}：`{metric['display']}`，状态 `{metric['status']}`；"
            f"通过线 `{metric['pass_rule']}`"
            + (
                f"；观察线 `{metric['watch_rule']}`"
                if metric.get("watch_rule")
                else ""
            )
            + (f"；说明：{metric['comment']}" if metric.get("comment") else "")
        )
    return "\n".join(lines)
