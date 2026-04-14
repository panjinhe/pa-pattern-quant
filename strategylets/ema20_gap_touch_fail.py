from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from strategylets.trend_strategy_common import (
    BacktestConfig,
    add_common_features,
    attach_scored_columns,
    candidate_parquet_rows,
    clip_score,
    coerce_backtest_config,
    dataclass_to_dict,
    index_rows,
    iso,
    label_outcomes as common_label_outcomes,
    make_key_point,
    make_segment_overlay,
    make_zone_overlay,
    plot_outputs,
    prepare_ohlcv,
    read_ohlcv,
    route,
    run_backtest,
    score_band,
    stable_id,
    write_json,
    write_jsonl,
)
from quant_interface import (
    DetectorBinding,
    OpportunityCandidate,
    QuantEvent,
    QuantRun,
    QuantSpec,
    RunSummary,
    TradePlan,
)


SPEC_ID = "ema20_gap_touch_fail-v1-btc-5m"
CONCEPT_ID = "ema20_gap_touch_fail"
TITLE_ZH = "20 均线缺口"
DEFAULT_STRATEGY_SEQ = "004"
DEFAULT_STRATEGY_CODE = "PA_taifei_004"
DEFAULT_STRATEGY_NAME_STD = f"{DEFAULT_STRATEGY_CODE}_{TITLE_ZH}"


@dataclass(frozen=True)
class EmaGapConfig:
    atr_window: int = 20
    breakout_window: int = 20
    gap_min_bars: int = 20
    gap_soft_max_bars: int = 30
    gap_hard_max_bars: int = 40
    min_ema_gap_atr: float = 0.10
    touch_wick_ratio_min: float = 0.25
    signal_body_ratio_min: float = 0.35
    signal_close_pos_min: float = 0.58
    signal_close_pos_max: float = 0.42
    target_r_min: float = 1.00
    label_lookahead: int = 32


def _coerce_config(params: EmaGapConfig | dict[str, Any] | None) -> EmaGapConfig:
    if params is None:
        return EmaGapConfig()
    if isinstance(params, EmaGapConfig):
        return params
    return EmaGapConfig(**params)


def _strategy_meta(registry_entry: dict[str, Any] | None = None) -> dict[str, str]:
    return {
        "strategy_seq": str((registry_entry or {}).get("strategy_seq", DEFAULT_STRATEGY_SEQ)),
        "strategy_code": str((registry_entry or {}).get("strategy_code", DEFAULT_STRATEGY_CODE)),
        "strategy_name_std": str(
            (registry_entry or {}).get("strategy_name_std", DEFAULT_STRATEGY_NAME_STD)
        ),
        "strategy_name_zh": str((registry_entry or {}).get("strategy_name_zh", TITLE_ZH)),
    }


def add_features(
    df: pl.DataFrame,
    params: EmaGapConfig | dict[str, Any] | None = None,
) -> pl.DataFrame:
    config = _coerce_config(params)
    return (
        add_common_features(
            df,
            atr_window=config.atr_window,
            breakout_window=config.breakout_window,
        )
        .with_columns(
            (
                (pl.col("ema20") > pl.col("ema50"))
                & (pl.col("ema20_slope_atr") > 0.01)
                & (pl.col("ema_gap_atr") >= config.min_ema_gap_atr)
            ).alias("trend_context_long"),
            (
                (pl.col("ema20") < pl.col("ema50"))
                & (pl.col("ema20_slope_atr") < -0.01)
                & (pl.col("ema_gap_atr") >= config.min_ema_gap_atr)
            ).alias("trend_context_short"),
            ((pl.col("low") <= pl.col("ema20")) & (pl.col("high") >= pl.col("ema20"))).alias("touch_ema20"),
        )
    )


def _scan_gap_touch_signals(
    df: pl.DataFrame,
    config: EmaGapConfig,
) -> dict[str, list[object]]:
    rows = df.to_dicts()
    n = len(rows)
    columns: dict[str, list[object]] = {
        "signal_bar": [False] * n,
        "signal_side": [None] * n,
        "touch_idx": [None] * n,
        "origin_idx": [None] * n,
        "trend_extreme_idx": [None] * n,
        "gap_bars": [None] * n,
    }

    bull_gap_run = [0] * n
    bear_gap_run = [0] * n
    bull_gap_extreme_idx: list[int | None] = [None] * n
    bear_gap_extreme_idx: list[int | None] = [None] * n

    for idx, row in enumerate(rows):
        ema20 = row["ema20"]
        if ema20 is None:
            continue
        ema20_value = float(ema20)

        if float(row["low"]) > ema20_value:
            prev_count = bull_gap_run[idx - 1] if idx > 0 else 0
            bull_gap_run[idx] = prev_count + 1
            prev_extreme = bull_gap_extreme_idx[idx - 1] if idx > 0 and prev_count > 0 else None
            if prev_extreme is None or float(row["high"]) >= float(rows[prev_extreme]["high"]):
                bull_gap_extreme_idx[idx] = idx
            else:
                bull_gap_extreme_idx[idx] = prev_extreme

        if float(row["high"]) < ema20_value:
            prev_count = bear_gap_run[idx - 1] if idx > 0 else 0
            bear_gap_run[idx] = prev_count + 1
            prev_extreme = bear_gap_extreme_idx[idx - 1] if idx > 0 and prev_count > 0 else None
            if prev_extreme is None or float(row["low"]) <= float(rows[prev_extreme]["low"]):
                bear_gap_extreme_idx[idx] = idx
            else:
                bear_gap_extreme_idx[idx] = prev_extreme

    for idx, row in enumerate(rows):
        ema20 = row["ema20"]
        if idx == 0 or ema20 is None or str(row["snapshot_status"]) != "complete":
            continue

        ema20_value = float(ema20)
        close = float(row["close"])
        prev = rows[idx - 1]

        if (
            bool(row["trend_context_long"])
            and bool(row["touch_ema20"])
            and close >= ema20_value
            and (bool(row["bull_bar"]) or float(row["lower_wick_ratio"] or 0.0) >= config.touch_wick_ratio_min)
        ):
            gap_bars = bull_gap_run[idx - 1]
            extreme_idx = bull_gap_extreme_idx[idx - 1]
            if (
                config.gap_min_bars <= gap_bars <= config.gap_hard_max_bars
                and extreme_idx is not None
            ):
                columns["signal_bar"][idx] = True
                columns["signal_side"][idx] = "long"
                columns["touch_idx"][idx] = idx
                columns["origin_idx"][idx] = max(0, idx - gap_bars)
                columns["trend_extreme_idx"][idx] = extreme_idx
                columns["gap_bars"][idx] = gap_bars
                continue

        if (
            bool(row["trend_context_long"])
            and bool(prev["touch_ema20"])
            and close > float(prev["high"])
            and close >= ema20_value
            and float(row["close_pos"] or 0.0) >= config.signal_close_pos_min
            and float(row["body_ratio"] or 0.0) >= config.signal_body_ratio_min
        ):
            gap_bars = bull_gap_run[idx - 2] if idx >= 2 else 0
            extreme_idx = bull_gap_extreme_idx[idx - 2] if idx >= 2 else None
            if (
                config.gap_min_bars <= gap_bars <= config.gap_hard_max_bars
                and extreme_idx is not None
            ):
                columns["signal_bar"][idx] = True
                columns["signal_side"][idx] = "long"
                columns["touch_idx"][idx] = idx - 1
                columns["origin_idx"][idx] = max(0, idx - 1 - gap_bars)
                columns["trend_extreme_idx"][idx] = extreme_idx
                columns["gap_bars"][idx] = gap_bars
                continue

        if (
            bool(row["trend_context_short"])
            and bool(row["touch_ema20"])
            and close <= ema20_value
            and (bool(row["bear_bar"]) or float(row["upper_wick_ratio"] or 0.0) >= config.touch_wick_ratio_min)
        ):
            gap_bars = bear_gap_run[idx - 1]
            extreme_idx = bear_gap_extreme_idx[idx - 1]
            if (
                config.gap_min_bars <= gap_bars <= config.gap_hard_max_bars
                and extreme_idx is not None
            ):
                columns["signal_bar"][idx] = True
                columns["signal_side"][idx] = "short"
                columns["touch_idx"][idx] = idx
                columns["origin_idx"][idx] = max(0, idx - gap_bars)
                columns["trend_extreme_idx"][idx] = extreme_idx
                columns["gap_bars"][idx] = gap_bars
                continue

        if (
            bool(row["trend_context_short"])
            and bool(prev["touch_ema20"])
            and close < float(prev["low"])
            and close <= ema20_value
            and float(row["close_pos"] or 1.0) <= config.signal_close_pos_max
            and float(row["body_ratio"] or 0.0) >= config.signal_body_ratio_min
        ):
            gap_bars = bear_gap_run[idx - 2] if idx >= 2 else 0
            extreme_idx = bear_gap_extreme_idx[idx - 2] if idx >= 2 else None
            if (
                config.gap_min_bars <= gap_bars <= config.gap_hard_max_bars
                and extreme_idx is not None
            ):
                columns["signal_bar"][idx] = True
                columns["signal_side"][idx] = "short"
                columns["touch_idx"][idx] = idx - 1
                columns["origin_idx"][idx] = max(0, idx - 1 - gap_bars)
                columns["trend_extreme_idx"][idx] = extreme_idx
                columns["gap_bars"][idx] = gap_bars

    return columns


def detect(
    df: pl.DataFrame,
    params: EmaGapConfig | dict[str, Any] | None = None,
) -> pl.DataFrame:
    config = _coerce_config(params)
    featured = add_features(df, params=config)
    scan_cols = _scan_gap_touch_signals(featured, config)
    return featured.with_columns(
        *[
            pl.Series(name=column_name, values=values)
            for column_name, values in scan_cols.items()
        ]
    )


def score(
    df: pl.DataFrame,
    params: EmaGapConfig | dict[str, Any] | None = None,
    backtest_params: BacktestConfig | dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    config = _coerce_config(params)
    backtest_config = coerce_backtest_config(backtest_params)
    rows_by_idx = index_rows(df)
    scored: list[dict[str, Any]] = []

    for signal_row in df.filter(pl.col("signal_bar")).to_dicts():
        signal_idx = int(signal_row["idx"])
        touch_idx = int(signal_row["touch_idx"])
        origin_idx = int(signal_row["origin_idx"])
        trend_extreme_idx = int(signal_row["trend_extreme_idx"])
        side = str(signal_row["signal_side"])
        touch_row = rows_by_idx[touch_idx]
        origin_row = rows_by_idx[origin_idx]
        trend_extreme_row = rows_by_idx[trend_extreme_idx]
        entry_price = float(signal_row["close"])
        gap_bars = int(signal_row["gap_bars"] or 0)
        atr = max(float(signal_row["atr"] or 0.0), 1e-9)

        if side == "long":
            stop_price = min(
                float(touch_row["low"]),
                float(signal_row["low"]),
                float(origin_row["low"]),
            ) - backtest_config.stop_buffer_atr * atr
            stop_price = min(stop_price, entry_price - backtest_config.min_stop_atr * atr)
            target_primary = max(float(trend_extreme_row["high"]), entry_price + atr)
            target_secondary = target_primary + 0.5 * max(target_primary - entry_price, atr)
            space_to_target_r = (target_primary - entry_price) / max(entry_price - stop_price, 1e-9)
            same_side_close = float(signal_row["close"]) >= float(signal_row["ema20"])
            touch_rejection = float(touch_row["lower_wick_ratio"] or 0.0) >= config.touch_wick_ratio_min
            weak_signal = float(signal_row["upper_wick_ratio"] or 0.0) >= 0.25
            counter_pressure = int(signal_row["recent_bear_pressure_3"] or 0)
        else:
            stop_price = max(
                float(touch_row["high"]),
                float(signal_row["high"]),
                float(origin_row["high"]),
            ) + backtest_config.stop_buffer_atr * atr
            stop_price = max(stop_price, entry_price + backtest_config.min_stop_atr * atr)
            target_primary = min(float(trend_extreme_row["low"]), entry_price - atr)
            target_secondary = target_primary - 0.5 * max(entry_price - target_primary, atr)
            space_to_target_r = (entry_price - target_primary) / max(stop_price - entry_price, 1e-9)
            same_side_close = float(signal_row["close"]) <= float(signal_row["ema20"])
            touch_rejection = float(touch_row["upper_wick_ratio"] or 0.0) >= config.touch_wick_ratio_min
            weak_signal = float(signal_row["lower_wick_ratio"] or 0.0) >= 0.25
            counter_pressure = int(signal_row["recent_bull_pressure_3"] or 0)

        hard_gate_checks: list[tuple[str, bool]] = [
            ("snapshot 完整", str(signal_row["snapshot_status"]) == "complete"),
            (
                "价格已连续 20 根以上远离 EMA20",
                gap_bars >= config.gap_min_bars,
            ),
            (
                "趋势环境仍健康，EMA20 与 EMA50 同向",
                bool(signal_row["trend_context_long"] if side == "long" else signal_row["trend_context_short"]),
            ),
            ("首次回碰 EMA20 后仍收在趋势侧", same_side_close),
            ("触均线后出现立即拒绝或确认延续", touch_rejection or signal_idx > touch_idx),
            ("入场到止损风险为正", (entry_price - stop_price) > 0 if side == "long" else (stop_price - entry_price) > 0),
        ]
        hard_gates = [label for label, passed in hard_gate_checks if passed]
        hard_gate_pass = all(passed for _, passed in hard_gate_checks)

        strengtheners: list[str] = []
        if config.gap_min_bars <= gap_bars <= config.gap_soft_max_bars:
            strengtheners.append("缺口根数处于 20 到 30 根的理想区间")
        if touch_rejection:
            strengtheners.append("触均线位置出现明显拒绝尾巴")
        if counter_pressure == 0:
            strengtheners.append("最近 3 根几乎没有额外反向强压力")
        if space_to_target_r >= config.target_r_min:
            strengtheners.append("回测原趋势极值至少有 1R 空间")
        if abs(float(signal_row["distance_ema20_atr"] or 0.0)) <= 1.2:
            strengtheners.append("触均线后的回归距离仍然紧凑")

        cautions: list[str] = []
        if gap_bars > config.gap_soft_max_bars:
            cautions.append("缺口根数偏晚，趋势可能已进入尾段")
        if weak_signal:
            cautions.append("确认 K 留下较长反向影线")
        if counter_pressure >= 1:
            cautions.append("最近 3 根出现了额外反向强压力")
        if float(signal_row["ema_gap_atr"] or 0.0) >= 1.5:
            cautions.append("EMA 偏离过大，容易从窄通道过渡到宽通道")
        if signal_idx > touch_idx and not touch_rejection:
            cautions.append("触均线后并非立即反应，存在迟滞")

        score_value = clip_score(70 + 5 * len(strengtheners) - 5 * len(cautions))
        route_value = route(score_value)
        score_band_value = score_band(score_value)

        scored.append(
            {
                "event_id": f"evt_{stable_id(SPEC_ID, signal_row['timestamp'], side)}",
                "candidate_id": f"cand_{stable_id(SPEC_ID, signal_row['timestamp'], side)}",
                "signal_idx": signal_idx,
                "touch_idx": touch_idx,
                "origin_idx": origin_idx,
                "trend_extreme_idx": trend_extreme_idx,
                "side": side,
                "snapshot_status": str(signal_row["snapshot_status"]),
                "hard_gate_pass": hard_gate_pass,
                "hard_gates": hard_gates,
                "strengtheners": strengtheners,
                "cautions": cautions,
                "score": score_value,
                "score_band": score_band_value,
                "route": route_value,
                "entry_price": entry_price,
                "entry_zone_low": entry_price - 0.04 * atr,
                "entry_zone_high": entry_price + 0.04 * atr,
                "stop_price": stop_price,
                "take_profit_prices": [target_primary, target_secondary],
                "invalidation_price": stop_price,
                "gap_bars": gap_bars,
                "space_to_target_r": space_to_target_r,
                "signal_bar_time": iso(signal_row["timestamp"]),
                "analysis_summary": (
                    f"{TITLE_ZH}：{('多头' if side == 'long' else '空头')}连续偏离 EMA20 {gap_bars} 根后首次回碰，"
                    f"当前给出延续确认，评分 {score_value}，分档 {score_band_value}，路由 {route_value}。"
                ),
                "tags": [
                    CONCEPT_ID,
                    DEFAULT_STRATEGY_CODE,
                    "ema20_gap",
                    "first_touch",
                    side,
                    route_value,
                    score_band_value.lower(),
                ],
            }
        )

    return scored


def _build_points_and_overlays(
    rows_by_idx: dict[int, dict[str, Any]],
    record: dict[str, Any],
) -> tuple[list[Any], list[Any]]:
    signal_row = rows_by_idx[record["signal_idx"]]
    touch_row = rows_by_idx[record["touch_idx"]]
    trend_extreme_row = rows_by_idx[record["trend_extreme_idx"]]
    side = record["side"]
    ema_price = float(touch_row["ema20"])

    key_points = [
        make_key_point(
            f"{record['event_id']}-touch",
            "touch",
            touch_row["timestamp"],
            ema_price,
            "EMA20",
        ),
        make_key_point(
            f"{record['event_id']}-signal",
            "signal",
            signal_row["timestamp"],
            float(record["entry_price"]),
            "确认",
        ),
        make_key_point(
            f"{record['event_id']}-extreme",
            "trend_extreme",
            trend_extreme_row["timestamp"],
            float(trend_extreme_row["high"] if side == "long" else trend_extreme_row["low"]),
            "极值",
        ),
        make_key_point(
            f"{record['event_id']}-target",
            "target",
            signal_row["timestamp"],
            float(record["take_profit_prices"][0]),
            "目标1",
        ),
    ]

    ema_start = make_key_point(
        f"{record['event_id']}-ema-start",
        "ema_start",
        touch_row["timestamp"],
        ema_price,
    )
    ema_end = make_key_point(
        f"{record['event_id']}-ema-end",
        "ema_end",
        signal_row["timestamp"],
        ema_price,
    )
    stop_start = make_key_point(
        f"{record['event_id']}-stop-start",
        "stop_start",
        signal_row["timestamp"],
        float(record["stop_price"]),
    )
    stop_end = make_key_point(
        f"{record['event_id']}-stop-end",
        "stop_end",
        signal_row["timestamp"],
        float(record["stop_price"]),
    )

    overlays = [
        make_segment_overlay(
            f"{record['event_id']}-ema-line",
            "ema20_line",
            ema_start,
            ema_end,
            "#7f8c8d",
        ),
        make_segment_overlay(
            f"{record['event_id']}-stop-line",
            "stop_line",
            stop_start,
            stop_end,
            "#cb4335",
            dash="dash",
        ),
        make_zone_overlay(
            f"{record['event_id']}-entry-zone",
            "entry_zone",
            signal_row["timestamp"],
            signal_row["timestamp"],
            float(record["entry_zone_low"]),
            float(record["entry_zone_high"]),
            "#f5b041",
        ),
    ]
    return key_points, overlays


def build_candidates(
    df: pl.DataFrame,
    run_ctx: dict[str, Any],
) -> tuple[list[OpportunityCandidate], list[QuantEvent]]:
    rows_by_idx = index_rows(df)
    scored_records: list[dict[str, Any]] = run_ctx["scored_records"]
    strategy_meta: dict[str, str] = run_ctx.get("strategy_meta", _strategy_meta())
    candidates: list[OpportunityCandidate] = []
    events: list[QuantEvent] = []
    dedupe_keys: set[tuple[str, str, str, str, str]] = set()

    for record in scored_records:
        key_points, overlays = _build_points_and_overlays(rows_by_idx, record)
        trade_plan = TradePlan(
            direction="bullish" if record["side"] == "long" else "bearish",
            entry_trigger="20 根以上均线缺口后首次回碰 EMA20 并失败，确认后参与",
            entry_price=float(record["entry_price"]),
            stop_price=float(record["stop_price"]),
            target_prices=[float(price) for price in record["take_profit_prices"]],
            timeout_bars=run_ctx["backtest_config"].max_holding_bars,
            invalidation_rule="重新跌破/涨破触均线失败结构之外",
            tags=["ema20_gap", "first_touch_fail"],
        )

        event = QuantEvent(
            event_id=record["event_id"],
            run_id=run_ctx["run_id"],
            concept_id=CONCEPT_ID,
            spec_id=SPEC_ID,
            symbol=run_ctx["symbol"],
            timeframe=run_ctx["timeframe"],
            direction="bullish" if record["side"] == "long" else "bearish",
            stage="confirmed",
            detected_at=record["signal_bar_time"],
            start_time=iso(rows_by_idx[record["origin_idx"]]["timestamp"]),
            end_time=record["signal_bar_time"],
            confidence=round(record["score"] / 100.0, 4),
            score=float(record["score"]),
            family="ema20_gap_touch_fail",
            features={
                "hard_gate_pass": record["hard_gate_pass"],
                "hard_gates": record["hard_gates"],
                "strengtheners": record["strengtheners"],
                "cautions": record["cautions"],
                "route": record["route"],
                "gap_bars": record["gap_bars"],
                "space_to_target_r": round(record["space_to_target_r"], 4),
            },
            key_points=key_points,
            overlays=overlays,
            trade_plan=trade_plan,
            outcome=None,
            tags=record["tags"],
            meta={
                "candidate_id": record["candidate_id"],
                "strategy_code": strategy_meta["strategy_code"],
                "strategy_name_std": strategy_meta["strategy_name_std"],
                "score_band": record["score_band"],
                "snapshot_status": record["snapshot_status"],
                "signal_idx": record["signal_idx"],
                "touch_idx": record["touch_idx"],
                "origin_idx": record["origin_idx"],
                "entry_zone_low": record["entry_zone_low"],
                "entry_zone_high": record["entry_zone_high"],
            },
        )
        events.append(event)

        if not record["hard_gate_pass"] or record["snapshot_status"] != "complete" or record["route"] == "drop":
            continue

        dedupe_key = (
            SPEC_ID,
            run_ctx["symbol"],
            run_ctx["timeframe"],
            record["signal_bar_time"],
            record["side"],
        )
        if dedupe_key in dedupe_keys:
            continue
        dedupe_keys.add(dedupe_key)

        candidates.append(
            OpportunityCandidate(
                candidate_id=record["candidate_id"],
                run_id=run_ctx["run_id"],
                spec_id=SPEC_ID,
                concept_id=CONCEPT_ID,
                symbol=run_ctx["symbol"],
                timeframe=run_ctx["timeframe"],
                signal_bar_time=record["signal_bar_time"],
                side=record["side"],
                setup_type="ema20_gap_first_touch_fail",
                entry_type="confirm_close",
                entry_zone_low=float(record["entry_zone_low"]),
                entry_zone_high=float(record["entry_zone_high"]),
                stop_price=float(record["stop_price"]),
                take_profit_prices=[float(price) for price in record["take_profit_prices"]],
                invalidation_price=float(record["invalidation_price"]),
                score=int(record["score"]),
                score_band=record["score_band"],
                candidate_tier=record["route"],
                hard_gates=record["hard_gates"],
                strengtheners=record["strengtheners"],
                cautions=record["cautions"],
                analysis_summary=record["analysis_summary"],
                key_points=key_points,
                overlays=overlays,
                snapshot_status="complete",
                realtime_safe=True,
                tags=record["tags"],
            )
        )

    return candidates, events


def label_outcomes(
    df: pl.DataFrame,
    events: list[QuantEvent],
    params: EmaGapConfig | dict[str, Any] | None = None,
    backtest_params: BacktestConfig | dict[str, Any] | None = None,
) -> list[QuantEvent]:
    config = _coerce_config(params)
    return common_label_outcomes(
        df,
        events,
        lookahead_bars=config.label_lookahead,
        backtest_params=backtest_params,
    )


def build_quant_spec(
    registry_entry: dict[str, Any] | None = None,
) -> QuantSpec:
    strategy_meta = _strategy_meta(registry_entry)
    return QuantSpec(
        spec_id=SPEC_ID,
        concept_id=CONCEPT_ID,
        version="v1",
        title=strategy_meta["strategy_name_std"],
        input_columns=["timestamp", "open", "high", "low", "close", "volume"],
        feature_columns=[
            "atr",
            "ema20",
            "ema50",
            "ema_gap_atr",
            "trend_context_long",
            "trend_context_short",
            "touch_ema20",
            "gap_bars",
        ],
        event_columns=[
            "signal_bar",
            "signal_side",
            "touch_idx",
            "origin_idx",
            "trend_extreme_idx",
            "score",
            "score_band",
            "route",
        ],
        required_context_ids=["spike", "ema20_gap"],
        required_primitive_ids=["first_touch_ema20", "failure_to_reverse", "trend_resume"],
        params={
            **asdict(EmaGapConfig()),
            "backtest": asdict(BacktestConfig()),
            "strategy_meta": strategy_meta,
            "registry_entry": registry_entry or {},
        },
        detector=DetectorBinding(
            module_path="strategylets.ema20_gap_touch_fail",
            detect_function="detect",
            label_function="label_outcomes",
            strategy_function="run_backtest",
            plot_function="plot_outputs",
        ),
        realtime_safe=True,
        uses_future_bars=False,
        supports_backtest=True,
        supports_visualization=True,
    )


def _build_readme(
    run: QuantRun,
    strategy_meta: dict[str, str],
    backtest_summary: dict[str, Any],
    candidates: list[OpportunityCandidate],
    scored_records: list[dict[str, Any]],
    plot_summary: dict[str, Any],
    input_path: Path,
) -> str:
    primary_count = sum(1 for candidate in candidates if candidate.candidate_tier == "primary")
    secondary_count = sum(1 for candidate in candidates if candidate.candidate_tier == "secondary")
    drop_count = sum(1 for item in scored_records if item["route"] == "drop")

    return f"""# {strategy_meta['strategy_name_std']}

- 策略代码：`{strategy_meta['strategy_code']}`
- 中文名：`{strategy_meta['strategy_name_zh']}`

## 策略是什么

这是一个“20 根以上偏离 EMA20 后，首次回碰均线失败”的顺势模板。价格已经长期运行在均线同侧，说明趋势一度很强；当它第一次回碰 EMA20，如果无法顺利完成反转而是再次回到趋势侧，我们把它解释成首次回调失败，目标先看原趋势极值的回测。

## 成立条件

- snapshot 必须完整，`snapshot_status == complete`
- 价格已连续 20 根以上远离 EMA20
- EMA20 与 EMA50 同向，且趋势环境仍健康
- 首次回碰 EMA20 后仍收在趋势侧
- 触均线后出现立即拒绝，或下一根快速确认延续
- 只在确认 K 收线后产出，不做盘中预告

## 加强条件

- 缺口根数处于 20 到 30 根的理想区间
- 触均线位置出现明显拒绝尾巴
- 最近 3 根几乎没有额外反向强压力
- 回测原趋势极值至少有 1R 空间
- 触均线后的回归距离仍然紧凑

## 谨慎信号

- 缺口根数偏晚，趋势可能已进入尾段
- 确认 K 留下较长反向影线
- 最近 3 根出现了额外反向强压力
- EMA 偏离过大，容易从窄通道过渡到宽通道
- 触均线后并非立即反应，存在迟滞

## 买卖点与风控

- 方向：多空双向
- 入场：触均线失败后的确认 K 收线参与，直播输出为 `entry_zone_low ~ entry_zone_high`
- 止损：触均线失败结构外，再加 `0.15 ATR`
- 目标位：
  - 第一目标：原趋势极值
  - 第二目标：极值之后再推半段延伸
- 失效：价格重新跌破/涨破触均线失败结构之外

## 本次运行

- `spec_id`: `{run.spec_id}`
- `run_id`: `{run.run_id}`
- 数据文件：`{input_path.resolve()}`
- 样本区间：`{run.sample_start}` 至 `{run.sample_end}`
- QuantEvent 数：`{len(run.events)}`
- OpportunityCandidate 数：`{len(candidates)}`
- `primary / secondary / drop`：`{primary_count} / {secondary_count} / {drop_count}`

## 回测摘要

- 手续费口径：`双边合计 2bp`
- 交易笔数：`{backtest_summary['trade_count']}`
- 胜率：`{backtest_summary['win_rate']:.4f}%`
- 平均 R：`{backtest_summary['avg_r']}`
- 总 R：`{backtest_summary['total_r']}`
- 总净盈亏：`{backtest_summary['total_net_pnl']}`
- 最大回撤：`{backtest_summary['max_drawdown']}`
- Profit Factor：`{backtest_summary['profit_factor']}`

## 文件清单

- `strategy_spec.json`
- `quant_run.json`
- `candidates.jsonl`
- `candidates.parquet`
- `signals.parquet`
- `backtest-summary.json`
- `trades.csv`
- `plots/`

## 图表产物

- 案例图数量：`{plot_summary.get('example_count', 0)}`
- 净值图：`{plot_summary.get('equity_curve_png', '未生成')}`
"""


def run_strategylet(
    input_path: Path,
    output_root: Path,
    symbol: str,
    timeframe: str,
    registry_entry: dict[str, Any] | None = None,
    example_count: int = 6,
) -> dict[str, Any]:
    input_path = Path(input_path)
    output_root = Path(output_root)
    strategy_meta = _strategy_meta(registry_entry)
    started_at = datetime.now().replace(microsecond=0)
    run_id = f"{started_at.strftime('%Y%m%dT%H%M%S')}-{stable_id(SPEC_ID, symbol, timeframe, started_at.date())}"
    output_dir = output_root / SPEC_ID / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    config = EmaGapConfig()
    backtest_config = BacktestConfig()
    raw_df = read_ohlcv(input_path)
    base_df = prepare_ohlcv(raw_df)
    signal_df = detect(base_df, params=config)
    scored_records = score(signal_df, params=config, backtest_params=backtest_config)
    signal_df = attach_scored_columns(signal_df, scored_records)

    run_ctx = {
        "run_id": run_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "backtest_config": backtest_config,
        "scored_records": scored_records,
        "strategy_meta": strategy_meta,
    }
    candidates, events = build_candidates(signal_df, run_ctx=run_ctx)
    labeled_events = label_outcomes(
        signal_df,
        events=events,
        params=config,
        backtest_params=backtest_config,
    )
    trades_df, backtest_summary, equity_df = run_backtest(
        labeled_events,
        backtest_params=backtest_config,
    )
    plot_summary = plot_outputs(
        signal_df,
        events=labeled_events,
        equity_df=equity_df,
        output_dir=output_dir,
        example_count=example_count,
    )

    quant_spec = build_quant_spec(registry_entry=registry_entry)
    sample_info = base_df.select(
        pl.len().alias("rows"),
        pl.col("timestamp").min().alias("sample_start"),
        pl.col("timestamp").max().alias("sample_end"),
    ).to_dicts()[0]

    run_summary = RunSummary(
        rows=int(sample_info["rows"]),
        event_count=len(labeled_events),
        signal_count=len(scored_records),
        trade_count=backtest_summary["trade_count"],
        win_rate=backtest_summary["win_rate"],
        avg_r=backtest_summary["avg_r"],
        total_r=backtest_summary["total_r"],
        total_net_pnl=backtest_summary["total_net_pnl"],
        max_drawdown=backtest_summary["max_drawdown"],
        profit_factor=backtest_summary["profit_factor"],
        extra={
            "candidate_count": len(candidates),
            "primary_count": sum(1 for item in candidates if item.candidate_tier == "primary"),
            "secondary_count": sum(1 for item in candidates if item.candidate_tier == "secondary"),
            "drop_count": sum(1 for item in scored_records if item["route"] == "drop"),
        },
    )

    strategy_spec_path = output_dir / "strategy_spec.json"
    quant_run_path = output_dir / "quant_run.json"
    candidates_jsonl_path = output_dir / "candidates.jsonl"
    candidates_parquet_path = output_dir / "candidates.parquet"
    signals_path = output_dir / "signals.parquet"
    backtest_summary_path = output_dir / "backtest-summary.json"
    trades_path = output_dir / "trades.csv"
    readme_path = output_dir / "README.md"
    artifacts = {
        "strategy_spec": str(strategy_spec_path.resolve()),
        "quant_run": str(quant_run_path.resolve()),
        "candidates_jsonl": str(candidates_jsonl_path.resolve()),
        "candidates_parquet": str(candidates_parquet_path.resolve()),
        "signals": str(signals_path.resolve()),
        "backtest_summary": str(backtest_summary_path.resolve()),
        "trades": str(trades_path.resolve()),
        "readme": str(readme_path.resolve()),
    }

    quant_run = QuantRun(
        run_id=run_id,
        spec_id=SPEC_ID,
        concept_id=CONCEPT_ID,
        symbol=symbol,
        timeframe=timeframe,
        data_source=str(input_path.resolve()),
        started_at=started_at.isoformat(),
        ended_at=datetime.now().replace(microsecond=0).isoformat(),
        sample_start=iso(sample_info["sample_start"]),
        sample_end=iso(sample_info["sample_end"]),
        params={
            "detect": asdict(config),
            "backtest": asdict(backtest_config),
            "strategy_meta": strategy_meta,
        },
        summary=run_summary,
        events=labeled_events,
        artifacts=artifacts,
    )

    candidate_rows = [dataclass_to_dict(item) for item in candidates]
    write_json(strategy_spec_path, dataclass_to_dict(quant_spec))
    write_jsonl(candidates_jsonl_path, candidate_rows)
    if candidate_rows:
        pl.DataFrame(candidate_parquet_rows(candidates)).write_parquet(candidates_parquet_path)
    else:
        pl.DataFrame(schema={"candidate_id": pl.String}).write_parquet(candidates_parquet_path)
    signal_df.write_parquet(signals_path)
    trades_df.write_csv(trades_path)
    write_json(
        backtest_summary_path,
        {
            "run_id": run_id,
            "strategy_code": strategy_meta["strategy_code"],
            "strategy_name_std": strategy_meta["strategy_name_std"],
            "spec_id": SPEC_ID,
            "concept_id": CONCEPT_ID,
            "symbol": symbol,
            "timeframe": timeframe,
            "signal_count": len(scored_records),
            "event_count": len(labeled_events),
            "candidate_count": len(candidates),
            "backtest": backtest_summary,
            "plots": plot_summary,
            "assumptions": {
                "fee_bps_round_trip": backtest_config.fee_bps_round_trip,
                "notional_usdt": backtest_config.notional_usdt,
                "label_lookahead": config.label_lookahead,
            },
        },
    )
    write_json(quant_run_path, quant_run.to_dict())
    readme_path.write_text(
        _build_readme(
            run=quant_run,
            strategy_meta=strategy_meta,
            backtest_summary=backtest_summary,
            candidates=candidates,
            scored_records=scored_records,
            plot_summary=plot_summary,
            input_path=input_path,
        ),
        encoding="utf-8",
    )

    return {
        "strategy_code": strategy_meta["strategy_code"],
        "strategy_name_std": strategy_meta["strategy_name_std"],
        "spec_id": SPEC_ID,
        "run_id": run_id,
        "output_dir": str(output_dir.resolve()),
        "signal_count": len(scored_records),
        "event_count": len(labeled_events),
        "candidate_count": len(candidates),
        "trade_count": backtest_summary["trade_count"],
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=f"运行标准策略：{TITLE_ZH}")
    parser.add_argument("--input", type=Path, required=True, help="OHLCV 文件路径")
    parser.add_argument("--output-root", type=Path, required=True, help="标准策略输出根目录")
    parser.add_argument("--symbol", default="ETHUSDT", help="symbol 标签")
    parser.add_argument("--timeframe", default="5m", help="timeframe 标签")
    parser.add_argument("--example-count", type=int, default=6, help="案例图数量")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    result = run_strategylet(
        input_path=args.input,
        output_root=args.output_root,
        symbol=args.symbol,
        timeframe=args.timeframe,
        registry_entry=None,
        example_count=args.example_count,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
