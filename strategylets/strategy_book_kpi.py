from __future__ import annotations

from datetime import datetime
from typing import Any


FREQUENCY_PASS_THRESHOLD = 0.75
FREQUENCY_WATCH_THRESHOLD = 0.10
SAMPLE_PASS_THRESHOLD = 100
SAMPLE_WATCH_THRESHOLD = 40


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


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed.replace(tzinfo=None)


def _infer_sample_days_from_window(
    timeframe: str,
    sample_start: Any | None,
    sample_end: Any | None,
) -> float | None:
    start_dt = _parse_datetime(sample_start)
    end_dt = _parse_datetime(sample_end)
    bars_per_day = _infer_bars_per_day(timeframe)
    if start_dt is None or end_dt is None or bars_per_day is None or end_dt < start_dt:
        return None
    bar_days = 1.0 / bars_per_day
    return ((end_dt - start_dt).total_seconds() / 86_400.0) + bar_days


def _display_number(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "待补充"
    return f"{value:.{digits}f}"


def _display_datetime(value: Any | None) -> str:
    parsed = _parse_datetime(value)
    if parsed is None:
        return "待补充"
    return parsed.isoformat(sep=" ", timespec="seconds")


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
    symbol: str | None = None,
    sample_start: Any | None = None,
    sample_end: Any | None = None,
) -> dict[str, Any]:
    bars_per_day = _infer_bars_per_day(timeframe)
    sample_days = _infer_sample_days_from_window(timeframe, sample_start, sample_end)
    if sample_days is None and bars_per_day:
        sample_days = sample_rows / bars_per_day
    opportunities_per_day = (signal_count / sample_days) if sample_days and sample_days > 0 else None

    if opportunities_per_day is None:
        frequency_status = "待补充"
    elif opportunities_per_day >= FREQUENCY_PASS_THRESHOLD:
        frequency_status = "通过"
    elif opportunities_per_day >= FREQUENCY_WATCH_THRESHOLD:
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

    if trade_count >= SAMPLE_PASS_THRESHOLD:
        sample_status = "通过"
    elif trade_count >= SAMPLE_WATCH_THRESHOLD:
        sample_status = "观察"
    else:
        sample_status = "不通过"

    execution_status = (
        "通过"
        if realtime_safe and (not uses_future_bars) and supports_visualization
        else "不通过"
    )

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
        auto_stage = "不通过"
        final_status = "不通过"
        recommendation = (
            f"{symbol or '当前品种'} 至少有 1 项硬门槛未达标："
            f"{', '.join(failing_metrics)}。先修正后再讨论纳入策略集。"
        )
    elif all(status == "通过" for status in auto_statuses):
        auto_stage = "通过"
        final_status = "通过"
        recommendation = (
            f"{symbol or '当前品种'} 已达到单品种正式通过线，"
            "若通用门禁与人工复核同步满足，可进入通用策略集。"
        )
    else:
        auto_stage = "观察"
        final_status = "观察"
        recommendation = (
            f"{symbol or '当前品种'} 暂未跌到硬失败区，但仍有观察项："
            f"{', '.join(watching_metrics) if watching_metrics else '待补充'}。"
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
            ">= 0.75 次/天",
            ">= 0.10 次/天",
            "v2.1 适度放宽频率门槛，降低早筛误杀率；这里继续用 signal_count 近似 unfilter 机会数，方便 BTC / ETH / 合并统一口径。",
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
            "对应 leader 提的“至少 break even、不要亏太惨”。",
        ),
        _metric(
            "sample_sufficiency",
            "样本充足性",
            sample_status,
            f"{trade_count} 笔 baseline 交易",
            ">= 100 笔",
            ">= 40 笔",
            "v2.1 适度放宽样本门槛，优先减少早筛阶段对中低频策略的误杀。",
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
            comment=(
                "若策略依赖指标做机会判断或强弱判断，这些指标必须能画到主图/副图，"
                "让人直接看到触发依据。"
            ),
        ),
    ]

    return {
        "version": "v2",
        "evaluation_scope": "symbol",
        "symbol": symbol,
        "timeframe": timeframe,
        "sample_rows": sample_rows,
        "sample_start": _display_datetime(sample_start) if sample_start is not None else None,
        "sample_end": _display_datetime(sample_end) if sample_end is not None else None,
        "sample_days": round(sample_days, 2) if sample_days is not None else None,
        "signal_count": signal_count,
        "trade_count": trade_count,
        "auto_stage": auto_stage,
        "final_status": final_status,
        "metrics": metrics,
        "blocking_metrics": failing_metrics,
        "watch_metrics": watching_metrics,
        "recommendation": recommendation,
    }


def build_manual_review_assessment(
    *,
    status: str = "pending",
    review_pool_counts: dict[str, int] | None = None,
    top_count: int = 20,
    random_count: int = 20,
    min_per_symbol: int = 10,
    pass_rate: float | None = None,
    suspicious_sample_count: int | None = None,
) -> dict[str, Any]:
    counts = {
        str(symbol): int(count)
        for symbol, count in (review_pool_counts or {}).items()
    }
    insufficient_symbols = sorted(
        symbol
        for symbol, count in counts.items()
        if count < min_per_symbol
    )

    status_map = {
        "pending": "待人工复核",
        "pass": "通过",
        "fail": "不通过",
        "needs_samples": "待补样复核",
    }
    resolved_status = status_map.get(status, "待人工复核")
    if resolved_status != "不通过" and counts and insufficient_symbols:
        resolved_status = "待补样复核"

    if resolved_status == "通过":
        recommendation = "人工复核已通过，可以按自动门禁结果决定进入通用策略集或品种特化池。"
    elif resolved_status == "不通过":
        recommendation = "人工图表复核未通过，暂不纳入策略集。"
    elif resolved_status == "待补样复核":
        recommendation = (
            "合并抽样对每个品种至少需要 10 张样本；"
            f"当前不足的品种：{', '.join(insufficient_symbols)}。"
        )
    else:
        recommendation = "自动门禁满足后，下一步做 BTC + ETH 合并抽样复核。"

    return {
        "status": resolved_status,
        "top_count": top_count,
        "random_count": random_count,
        "min_per_symbol": min_per_symbol,
        "review_pool_counts": counts,
        "insufficient_symbols": insufficient_symbols,
        "pass_rate": pass_rate,
        "suspicious_sample_count": suspicious_sample_count,
        "pass_rule": ">= 70% 样本被判定为结构合理、可解释、像真人会做",
        "suspicious_rule": "明显误触发 / 明显未来函数嫌疑样本 <= 3 张",
        "recommendation": recommendation,
    }


def build_universal_strategy_book_assessment(
    *,
    timeframe: str,
    symbol_assessments: dict[str, dict[str, Any]],
    combined_assessment: dict[str, Any],
    manual_review: dict[str, Any] | None = None,
    required_symbols: list[str] | None = None,
) -> dict[str, Any]:
    required = list(required_symbols or ["BTCUSDT", "ETHUSDT"])
    manual_payload = manual_review or build_manual_review_assessment()
    missing_required_symbols = [symbol for symbol in required if symbol not in symbol_assessments]
    required_results = {
        symbol: symbol_assessments.get(symbol)
        for symbol in required
        if symbol in symbol_assessments
    }
    required_floor_pass = (
        not missing_required_symbols
        and all(item["auto_stage"] != "不通过" for item in required_results.values())
    )
    combined_stage = combined_assessment["auto_stage"]
    applicable_symbols = sorted(
        symbol
        for symbol, assessment in symbol_assessments.items()
        if assessment["auto_stage"] == "通过"
    )
    weak_but_not_failed_symbols = sorted(
        symbol
        for symbol, assessment in symbol_assessments.items()
        if assessment["auto_stage"] == "观察"
    )

    if manual_payload["status"] == "不通过":
        auto_stage = "暂不纳入"
        final_bucket = "暂不纳入"
        recommendation = "人工复核明确不通过，暂不纳入通用策略集，也不进入品种特化池。"
    elif required_floor_pass and combined_stage == "通过":
        auto_stage = "可进入人工复核"
        if manual_payload["status"] == "通过":
            final_bucket = "可纳入通用策略集"
            recommendation = "BTC、ETH 单品种都没掉到硬失败区，且合并结果通过，已满足通用策略集门禁。"
        else:
            final_bucket = "待人工复核"
            recommendation = "自动门禁已满足通用标准，下一步做 BTC + ETH 合并抽样人工复核。"
    elif required_floor_pass and combined_stage == "观察":
        auto_stage = "观察名单"
        final_bucket = "观察名单"
        recommendation = "BTC、ETH 单品种都不差，但 BTC+ETH 合并结果还没到正式通过线，先放观察名单。"
    elif applicable_symbols and manual_payload["status"] == "通过":
        auto_stage = "品种特化候选"
        final_bucket = "品种特化策略池"
        recommendation = (
            "未满足通用门禁，但至少有 1 个品种达到正式通过线且人审通过，"
            f"建议进入品种特化策略池：{', '.join(applicable_symbols)}。"
        )
    elif applicable_symbols:
        auto_stage = "品种特化候选"
        final_bucket = "暂不纳入"
        recommendation = (
            f"已有可作为品种特化候选的品种：{', '.join(applicable_symbols)}，"
            "但人工复核尚未通过前，不进入特化池。"
        )
    else:
        auto_stage = "暂不纳入"
        final_bucket = "暂不纳入"
        if missing_required_symbols:
            recommendation = (
                "通用策略集要求同时评估 BTC 和 ETH；"
                f"当前缺失：{', '.join(missing_required_symbols)}。"
            )
        else:
            recommendation = "至少有 1 个必需品种跌到观察线以下，或合并结果与实盘化前提未达标，暂不纳入。"

    return {
        "version": "v2",
        "evaluation_scope": "universal",
        "timeframe": timeframe,
        "required_symbols": required,
        "present_symbols": sorted(symbol_assessments.keys()),
        "missing_required_symbols": missing_required_symbols,
        "applicable_symbols": applicable_symbols,
        "weak_but_not_failed_symbols": weak_but_not_failed_symbols,
        "symbols": symbol_assessments,
        "combined": combined_assessment,
        "manual_review": manual_payload,
        "auto_stage": auto_stage,
        "final_status": final_bucket,
        "final_bucket": final_bucket,
        "recommendation": recommendation,
    }


def _render_metric_lines(metrics: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for metric in metrics:
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
    return lines


def render_strategy_book_assessment(assessment: dict[str, Any]) -> str:
    version = assessment.get("version")
    if version != "v2":
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
        lines.extend(_render_metric_lines(assessment.get("metrics", [])))
        return "\n".join(lines)

    if assessment.get("evaluation_scope") == "symbol":
        sample_days = assessment.get("sample_days")
        sample_days_display = f"{sample_days:.2f} 天" if sample_days is not None else "待补充"
        title = assessment.get("symbol") or "当前品种"
        lines = [
            "## 策略书 KPI 与准入（单品种）",
            "",
            f"- 评估对象：`{title}`",
            f"- 自动结论：`{assessment['auto_stage']}`",
            f"- 样本覆盖：`{sample_days_display}`",
            f"- 样本窗口：`{assessment.get('sample_start') or '待补充'} -> {assessment.get('sample_end') or '待补充'}`",
            f"- 建议动作：{assessment['recommendation']}",
        ]
        lines.extend(_render_metric_lines(assessment.get("metrics", [])))
        return "\n".join(lines)

    combined = assessment["combined"]
    combined_sample_days = combined.get("sample_days")
    combined_sample_days_display = (
        f"{combined_sample_days:.2f} 天"
        if combined_sample_days is not None
        else "待补充"
    )
    manual_review = assessment["manual_review"]
    lines = [
        "## 策略书 KPI 与准入（通用策略集 v2）",
        "",
        f"- 通用自动分级：`{assessment['auto_stage']}`",
        f"- 最终归类：`{assessment['final_bucket']}`",
        f"- 必需评估品种：`{', '.join(assessment['required_symbols'])}`",
        f"- 已评估品种：`{', '.join(assessment['present_symbols']) if assessment['present_symbols'] else '待补充'}`",
        f"- 缺失品种：`{', '.join(assessment['missing_required_symbols']) if assessment['missing_required_symbols'] else '无'}`",
        f"- 适用品种：`{', '.join(assessment['applicable_symbols']) if assessment['applicable_symbols'] else '暂无'}`",
        f"- 人工复核：`{manual_review['status']}`",
        f"- 建议动作：{assessment['recommendation']}",
        "",
        "### 人工复核要求",
        "",
        f"- 合并抽样：`Top{manual_review['top_count']} + Random{manual_review['random_count']}`",
        f"- 单品种最少覆盖：`{manual_review['min_per_symbol']} 张`",
        f"- 当前复核池：`{manual_review['review_pool_counts']}`",
        f"- 复核说明：{manual_review['recommendation']}",
        "",
    ]
    for symbol, payload in assessment["symbols"].items():
        sample_days = payload.get("sample_days")
        sample_days_display = f"{sample_days:.2f} 天" if sample_days is not None else "待补充"
        lines.extend(
            [
                f"### 单品种：{symbol}",
                "",
                f"- 自动结论：`{payload['auto_stage']}`",
                f"- 样本覆盖：`{sample_days_display}`",
                f"- 样本窗口：`{payload.get('sample_start') or '待补充'} -> {payload.get('sample_end') or '待补充'}`",
                f"- 建议动作：{payload['recommendation']}",
            ]
        )
        lines.extend(_render_metric_lines(payload.get("metrics", [])))
        lines.append("")

    lines.extend(
        [
            "### BTC + ETH 合并",
            "",
            f"- 自动结论：`{combined['auto_stage']}`",
            f"- 样本覆盖：`{combined_sample_days_display}`",
            f"- 样本窗口：`{combined.get('sample_start') or '待补充'} -> {combined.get('sample_end') or '待补充'}`",
            f"- 建议动作：{combined['recommendation']}",
        ]
    )
    lines.extend(_render_metric_lines(combined.get("metrics", [])))
    return "\n".join(lines)
