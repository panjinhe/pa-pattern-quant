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
from strategylets.strategy_book_kpi import (
    build_strategy_book_assessment,
    render_strategy_book_assessment,
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


SPEC_ID = "inverse1_fail_forward1_success-v1-btc-5m"
CONCEPT_ID = "inverse1_fail_forward1_success"
TITLE_ZH = "逆一失败，顺一成功"
DEFAULT_STRATEGY_SEQ = "003"
DEFAULT_STRATEGY_CODE = "PA_taifei_003"
DEFAULT_STRATEGY_NAME_STD = f"{DEFAULT_STRATEGY_CODE}_{TITLE_ZH}"


@dataclass(frozen=True)
class InverseSuccessConfig:
    atr_window: int = 20
    breakout_window: int = 20
    breakout_buffer_atr: float = 0.08
    min_breakout_distance_atr: float = 0.90
    max_pullback_retrace_ratio: float = 0.55
    max_pullback_into_breakout_atr: float = 0.20
    signal_close_pos_min: float = 0.58
    signal_close_pos_max: float = 0.42
    signal_body_ratio_min: float = 0.38
    min_ema_gap_atr: float = 0.12
    target_r_min: float = 1.00
    label_lookahead: int = 28


def _coerce_config(
    params: InverseSuccessConfig | dict[str, Any] | None,
) -> InverseSuccessConfig:
    if params is None:
        return InverseSuccessConfig()
    if isinstance(params, InverseSuccessConfig):
        return params
    return InverseSuccessConfig(**params)


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
    params: InverseSuccessConfig | dict[str, Any] | None = None,
) -> pl.DataFrame:
    config = _coerce_config(params)
    prev_high = pl.col("high").shift(1)
    prev_low = pl.col("low").shift(1)
    return (
        add_common_features(
            df,
            atr_window=config.atr_window,
            breakout_window=config.breakout_window,
        )
        .with_columns(
            (
                (pl.col("ema20") > pl.col("ema50"))
                & (pl.col("ema20_slope_atr") > 0.02)
                & pl.col("close_above_ema20")
                & (pl.col("ema_gap_atr") >= config.min_ema_gap_atr)
            ).alias("trend_context_long"),
            (
                (pl.col("ema20") < pl.col("ema50"))
                & (pl.col("ema20_slope_atr") < -0.02)
                & pl.col("close_below_ema20")
                & (pl.col("ema_gap_atr") >= config.min_ema_gap_atr)
            ).alias("trend_context_short"),
            (
                pl.col("bull_trend_bar")
                & (pl.col("close_vs_recent_high_atr") >= config.breakout_buffer_atr)
            ).alias("bull_breakout_bar"),
            (
                pl.col("bear_trend_bar")
                & (pl.col("close_vs_recent_low_atr") >= config.breakout_buffer_atr)
            ).alias("bear_breakout_bar"),
            ((pl.col("high") <= prev_high) & (pl.col("low") >= prev_low)).alias("inside_bar"),
        )
    )


def _scan_inverse_success_signals(
    df: pl.DataFrame,
    config: InverseSuccessConfig,
) -> dict[str, list[object]]:
    rows = df.to_dicts()
    n = len(rows)
    columns: dict[str, list[object]] = {
        "signal_bar": [False] * n,
        "signal_side": [None] * n,
        "breakout_idx": [None] * n,
        "pullback_idx": [None] * n,
        "origin_idx": [None] * n,
        "breakout_level": [None] * n,
        "impulse_extreme": [None] * n,
        "pullback_retrace_ratio": [None] * n,
    }

    for signal_idx in range(config.breakout_window + 2, n):
        current = rows[signal_idx]
        pullback = rows[signal_idx - 1]
        pre_break = rows[signal_idx - 2]
        atr = max(float(current["atr"] or 0.0), 1e-9)
        if str(current["snapshot_status"]) != "complete":
            continue

        if (
            bool(current["trend_context_long"])
            and bool(pre_break["bull_breakout_bar"])
            and bool(pullback["bear_bar"] or pullback["inside_bar"])
            and float(current["high"]) > float(pullback["high"])
            and float(current["close"]) > float(pullback["high"])
            and float(current["close_pos"] or 0.0) >= config.signal_close_pos_min
            and float(current["body_ratio"] or 0.0) >= config.signal_body_ratio_min
        ):
            breakout_level = pre_break["recent_high_pre"]
            if breakout_level is not None:
                impulse_extreme = max(float(pre_break["high"]), float(pullback["high"]))
                impulse_size = max(impulse_extreme - float(breakout_level), atr)
                pullback_depth = impulse_extreme - float(pullback["low"])
                retrace_ratio = pullback_depth / max(impulse_size, 1e-9)
                if (
                    retrace_ratio <= config.max_pullback_retrace_ratio
                    and float(pullback["low"]) > float(breakout_level) - config.max_pullback_into_breakout_atr * atr
                ):
                    start_idx = max(0, signal_idx - config.breakout_window)
                    origin_idx = min(range(start_idx, signal_idx - 1), key=lambda idx: float(rows[idx]["low"]))
                    columns["signal_bar"][signal_idx] = True
                    columns["signal_side"][signal_idx] = "long"
                    columns["breakout_idx"][signal_idx] = signal_idx - 2
                    columns["pullback_idx"][signal_idx] = signal_idx - 1
                    columns["origin_idx"][signal_idx] = origin_idx
                    columns["breakout_level"][signal_idx] = float(breakout_level)
                    columns["impulse_extreme"][signal_idx] = impulse_extreme
                    columns["pullback_retrace_ratio"][signal_idx] = retrace_ratio
                    continue

        if (
            bool(current["trend_context_short"])
            and bool(pre_break["bear_breakout_bar"])
            and bool(pullback["bull_bar"] or pullback["inside_bar"])
            and float(current["low"]) < float(pullback["low"])
            and float(current["close"]) < float(pullback["low"])
            and float(current["close_pos"] or 1.0) <= config.signal_close_pos_max
            and float(current["body_ratio"] or 0.0) >= config.signal_body_ratio_min
        ):
            breakout_level = pre_break["recent_low_pre"]
            if breakout_level is not None:
                impulse_extreme = min(float(pre_break["low"]), float(pullback["low"]))
                impulse_size = max(float(breakout_level) - impulse_extreme, atr)
                pullback_depth = float(pullback["high"]) - impulse_extreme
                retrace_ratio = pullback_depth / max(impulse_size, 1e-9)
                if (
                    retrace_ratio <= config.max_pullback_retrace_ratio
                    and float(pullback["high"]) < float(breakout_level) + config.max_pullback_into_breakout_atr * atr
                ):
                    start_idx = max(0, signal_idx - config.breakout_window)
                    origin_idx = max(range(start_idx, signal_idx - 1), key=lambda idx: float(rows[idx]["high"]))
                    columns["signal_bar"][signal_idx] = True
                    columns["signal_side"][signal_idx] = "short"
                    columns["breakout_idx"][signal_idx] = signal_idx - 2
                    columns["pullback_idx"][signal_idx] = signal_idx - 1
                    columns["origin_idx"][signal_idx] = origin_idx
                    columns["breakout_level"][signal_idx] = float(breakout_level)
                    columns["impulse_extreme"][signal_idx] = impulse_extreme
                    columns["pullback_retrace_ratio"][signal_idx] = retrace_ratio

    return columns


def detect(
    df: pl.DataFrame,
    params: InverseSuccessConfig | dict[str, Any] | None = None,
) -> pl.DataFrame:
    config = _coerce_config(params)
    featured = add_features(df, params=config)
    scan_cols = _scan_inverse_success_signals(featured, config)
    return featured.with_columns(
        *[
            pl.Series(name=column_name, values=values)
            for column_name, values in scan_cols.items()
        ]
    )


def score(
    df: pl.DataFrame,
    params: InverseSuccessConfig | dict[str, Any] | None = None,
    backtest_params: BacktestConfig | dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    config = _coerce_config(params)
    backtest_config = coerce_backtest_config(backtest_params)
    rows_by_idx = index_rows(df)
    scored: list[dict[str, Any]] = []

    for signal_row in df.filter(pl.col("signal_bar")).to_dicts():
        signal_idx = int(signal_row["idx"])
        breakout_idx = int(signal_row["breakout_idx"])
        pullback_idx = int(signal_row["pullback_idx"])
        origin_idx = int(signal_row["origin_idx"])
        side = str(signal_row["signal_side"])
        breakout_row = rows_by_idx[breakout_idx]
        pullback_row = rows_by_idx[pullback_idx]
        origin_row = rows_by_idx[origin_idx]
        entry_price = float(signal_row["close"])
        breakout_level = float(signal_row["breakout_level"])
        impulse_extreme = float(signal_row["impulse_extreme"])
        retrace_ratio = float(signal_row["pullback_retrace_ratio"] or 0.0)
        atr = max(float(signal_row["atr"] or 0.0), 1e-9)

        if side == "long":
            stop_price = min(
                float(origin_row["low"]),
                float(pullback_row["low"]),
                float(signal_row["low"]),
            ) - backtest_config.stop_buffer_atr * atr
            stop_price = min(stop_price, entry_price - backtest_config.min_stop_atr * atr)
            target_primary = float(pullback_row["low"]) + (impulse_extreme - breakout_level)
            target_secondary = target_primary + 0.5 * max(impulse_extreme - breakout_level, atr)
            space_to_target_r = (target_primary - entry_price) / max(entry_price - stop_price, 1e-9)
            signal_strength_ok = (
                float(signal_row["close_pos"] or 0.0) >= config.signal_close_pos_min
                and float(signal_row["body_ratio"] or 0.0) >= config.signal_body_ratio_min
            )
            counter_pressure = int(signal_row["recent_bear_pressure_3"] or 0)
            weak_wick = float(signal_row["upper_wick_ratio"] or 0.0) >= 0.25
            magnet_price = signal_row.get("recent_high_96")
            magnet_hits = (
                magnet_price is not None
                and float(magnet_price) > entry_price
                and float(magnet_price) < target_primary
            )
        else:
            stop_price = max(
                float(origin_row["high"]),
                float(pullback_row["high"]),
                float(signal_row["high"]),
            ) + backtest_config.stop_buffer_atr * atr
            stop_price = max(stop_price, entry_price + backtest_config.min_stop_atr * atr)
            target_primary = float(pullback_row["high"]) - (breakout_level - impulse_extreme)
            target_secondary = target_primary - 0.5 * max(breakout_level - impulse_extreme, atr)
            space_to_target_r = (entry_price - target_primary) / max(stop_price - entry_price, 1e-9)
            signal_strength_ok = (
                float(signal_row["close_pos"] or 1.0) <= config.signal_close_pos_max
                and float(signal_row["body_ratio"] or 0.0) >= config.signal_body_ratio_min
            )
            counter_pressure = int(signal_row["recent_bull_pressure_3"] or 0)
            weak_wick = float(signal_row["lower_wick_ratio"] or 0.0) >= 0.25
            magnet_price = signal_row.get("recent_low_96")
            magnet_hits = (
                magnet_price is not None
                and float(magnet_price) < entry_price
                and float(magnet_price) > target_primary
            )

        hard_gate_checks: list[tuple[str, bool]] = [
            ("snapshot 完整", str(signal_row["snapshot_status"]) == "complete"),
            (
                "强突破后出现首个逆势一推",
                bool(breakout_row["bull_breakout_bar"] if side == "long" else breakout_row["bear_breakout_bar"]),
            ),
            ("逆势一推回调深度受控", retrace_ratio <= config.max_pullback_retrace_ratio),
            ("顺势一推成功并强收线确认", signal_strength_ok),
            (
                "趋势环境仍健康",
                bool(signal_row["trend_context_long"] if side == "long" else signal_row["trend_context_short"]),
            ),
            ("入场到止损风险为正", (entry_price - stop_price) > 0 if side == "long" else (stop_price - entry_price) > 0),
        ]
        hard_gates = [label for label, passed in hard_gate_checks if passed]
        hard_gate_pass = all(passed for _, passed in hard_gate_checks)

        strengtheners: list[str] = []
        if retrace_ratio <= 0.35:
            strengtheners.append("逆一很浅，测试明显受控")
        if counter_pressure == 0:
            strengtheners.append("最近 3 根几乎没有额外反向强压力")
        if abs(float(signal_row["distance_ema20_atr"] or 0.0)) <= 2.6:
            strengtheners.append("价格与 EMA20 的关系仍属健康急速/窄通道")
        if space_to_target_r >= config.target_r_min:
            strengtheners.append("AB=CD 到目标至少有 1R 空间")
        if not weak_wick:
            strengtheners.append("顺一确认 K 反向尾巴不重")

        cautions: list[str] = []
        if retrace_ratio >= 0.45:
            cautions.append("逆一回调偏深，恢复难度上升")
        if counter_pressure >= 1:
            cautions.append("最近 3 根出现了额外反向强压力")
        if weak_wick:
            cautions.append("顺一确认 K 留下较长反向影线")
        if magnet_hits:
            cautions.append("利润路径上存在左侧已知磁体")
        if float(signal_row["ema_gap_atr"] or 0.0) >= 1.5:
            cautions.append("EMA 偏离过大，容易由窄通道走向宽通道")

        score_value = clip_score(70 + 5 * len(strengtheners) - 5 * len(cautions))
        route_value = route(score_value)
        score_band_value = score_band(score_value)

        scored.append(
            {
                "event_id": f"evt_{stable_id(SPEC_ID, signal_row['timestamp'], side)}",
                "candidate_id": f"cand_{stable_id(SPEC_ID, signal_row['timestamp'], side)}",
                "signal_idx": signal_idx,
                "breakout_idx": breakout_idx,
                "pullback_idx": pullback_idx,
                "origin_idx": origin_idx,
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
                "pullback_retrace_ratio": retrace_ratio,
                "space_to_target_r": space_to_target_r,
                "breakout_level": breakout_level,
                "impulse_extreme": impulse_extreme,
                "signal_bar_time": iso(signal_row["timestamp"]),
                "analysis_summary": (
                    f"{TITLE_ZH}：{('多头' if side == 'long' else '空头')}强突破后，"
                    f"首个逆势一推的回调比例约 {retrace_ratio:.2f}，顺势确认成功，"
                    f"评分 {score_value}，分档 {score_band_value}，路由 {route_value}。"
                ),
                "tags": [
                    CONCEPT_ID,
                    DEFAULT_STRATEGY_CODE,
                    "spike",
                    "first_pullback",
                    "ab_equal_cd",
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
    breakout_row = rows_by_idx[record["breakout_idx"]]
    pullback_row = rows_by_idx[record["pullback_idx"]]
    origin_row = rows_by_idx[record["origin_idx"]]
    side = record["side"]

    key_points = [
        make_key_point(
            f"{record['event_id']}-a",
            "point_a",
            breakout_row["timestamp"],
            float(record["breakout_level"]),
            "A",
        ),
        make_key_point(
            f"{record['event_id']}-b",
            "point_b",
            pullback_row["timestamp"],
            float(record["impulse_extreme"]),
            "B",
        ),
        make_key_point(
            f"{record['event_id']}-c",
            "point_c",
            pullback_row["timestamp"],
            float(pullback_row["low"] if side == "long" else pullback_row["high"]),
            "C",
        ),
        make_key_point(
            f"{record['event_id']}-s",
            "signal",
            signal_row["timestamp"],
            float(record["entry_price"]),
            "顺一",
        ),
        make_key_point(
            f"{record['event_id']}-t",
            "target1",
            signal_row["timestamp"],
            float(record["take_profit_prices"][0]),
            "D",
        ),
    ]

    start_point = make_key_point(
        f"{record['event_id']}-start",
        "start",
        origin_row["timestamp"],
        float(origin_row["low"] if side == "long" else origin_row["high"]),
    )
    breakout_point = make_key_point(
        f"{record['event_id']}-breakout",
        "breakout",
        breakout_row["timestamp"],
        float(record["breakout_level"]),
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
    target_start = make_key_point(
        f"{record['event_id']}-target-start",
        "target_start",
        signal_row["timestamp"],
        float(record["take_profit_prices"][0]),
    )
    target_end = make_key_point(
        f"{record['event_id']}-target-end",
        "target_end",
        signal_row["timestamp"],
        float(record["take_profit_prices"][0]),
    )

    overlays = [
        make_segment_overlay(
            f"{record['event_id']}-ab",
            "ab_measure",
            breakout_point,
            key_points[1],
            "#b9770e",
        ),
        make_segment_overlay(
            f"{record['event_id']}-cd",
            "cd_projection",
            key_points[2],
            key_points[4],
            "#1d8348",
            dash="dash",
        ),
        make_segment_overlay(
            f"{record['event_id']}-stop",
            "stop_line",
            stop_start,
            stop_end,
            "#cb4335",
            dash="dash",
        ),
        make_segment_overlay(
            f"{record['event_id']}-origin-link",
            "origin_link",
            start_point,
            breakout_point,
            "#7f8c8d",
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
            entry_trigger="首个逆势一推失败，顺势一推收线确认后参与",
            entry_price=float(record["entry_price"]),
            stop_price=float(record["stop_price"]),
            target_prices=[float(price) for price in record["take_profit_prices"]],
            timeout_bars=run_ctx["backtest_config"].max_holding_bars,
            invalidation_rule="跌破/涨破本次 pullback 结构止损之外",
            tags=["first_pullback", "ab_equal_cd"],
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
            family="inverse_fail_forward_success",
            features={
                "hard_gate_pass": record["hard_gate_pass"],
                "hard_gates": record["hard_gates"],
                "strengtheners": record["strengtheners"],
                "cautions": record["cautions"],
                "route": record["route"],
                "pullback_retrace_ratio": round(record["pullback_retrace_ratio"], 4),
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
                "breakout_idx": record["breakout_idx"],
                "pullback_idx": record["pullback_idx"],
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
                setup_type="first_pullback_resumption",
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
    params: InverseSuccessConfig | dict[str, Any] | None = None,
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
            "bull_breakout_bar",
            "bear_breakout_bar",
            "inside_bar",
            "pullback_retrace_ratio",
        ],
        event_columns=[
            "signal_bar",
            "signal_side",
            "breakout_idx",
            "pullback_idx",
            "origin_idx",
            "score",
            "score_band",
            "route",
        ],
        required_context_ids=["spike", "narrow_channel"],
        required_primitive_ids=["first_pullback", "high1_low1", "ab_equal_cd"],
        params={
            **asdict(InverseSuccessConfig()),
            "backtest": asdict(BacktestConfig()),
            "strategy_meta": strategy_meta,
            "registry_entry": registry_entry or {},
        },
        detector=DetectorBinding(
            module_path="strategylets.inverse1_fail_forward1_success",
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
    strategy_book_assessment: dict[str, Any],
    input_path: Path,
) -> str:
    primary_count = sum(1 for candidate in candidates if candidate.candidate_tier == "primary")
    secondary_count = sum(1 for candidate in candidates if candidate.candidate_tier == "secondary")
    drop_count = sum(1 for item in scored_records if item["route"] == "drop")

    return f"""# {strategy_meta['strategy_name_std']}

- 策略代码：`{strategy_meta['strategy_code']}`
- 中文名：`{strategy_meta['strategy_name_zh']}`

## 策略是什么

这是一个更偏右侧的首次回调延续模板。先要求市场刚完成一段强突破，再等首个逆势一推出现；如果这次逆势尝试很浅、很快被顺势一推重新压回去，就把它视作“逆一失败，顺一成功”。

## 成立条件

- snapshot 必须完整，`snapshot_status == complete`
- 前面刚出现强突破，且环境仍是急速/窄通道
- 紧接着出现首个逆势一推，而不是多轮深回调
- 逆势一推回调深度受控
- 顺势一推重新突破 pullback 结构并强收线
- 只在确认 K 收线后产出，不做盘中预告

## 加强条件

- 逆一很浅，测试明显受控
- 最近 3 根几乎没有额外反向强压力
- 价格与 EMA20 的关系仍属健康急速/窄通道
- AB=CD 到目标至少有 1R 空间
- 顺一确认 K 反向尾巴不重

## 谨慎信号

- 逆一回调偏深，恢复难度上升
- 最近 3 根出现了额外反向强压力
- 顺一确认 K 留下较长反向影线
- 利润路径上存在左侧已知磁体
- EMA 偏离过大，容易由窄通道走向宽通道

## 买卖点与风控

- 方向：多空双向
- 入场：顺一确认 K 收线后参与，直播输出为 `entry_zone_low ~ entry_zone_high`
- 止损：逆一极值 / 急速起点结构外，再加 `0.15 ATR`
- 目标位：
  - 第一目标：`AB = CD`
  - 第二目标：第一目标后再加半段延伸
- 失效：价格重新跌破/涨破本次 pullback 结构止损之外

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

{render_strategy_book_assessment(strategy_book_assessment)}

## 文件清单

- `strategy_spec.json`
- `quant_run.json`
- `candidates.jsonl`
- `candidates.parquet`
- `signals.parquet`
- `backtest-summary.json`
- `strategy-book-assessment.json`
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

    config = InverseSuccessConfig()
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
    strategy_book_assessment = build_strategy_book_assessment(
        timeframe=timeframe,
        sample_rows=int(sample_info["rows"]),
        signal_count=len(scored_records),
        trade_count=backtest_summary["trade_count"],
        avg_r=backtest_summary["avg_r"],
        profit_factor=backtest_summary["profit_factor"],
        max_drawdown=backtest_summary["max_drawdown"],
        realtime_safe=quant_spec.realtime_safe,
        uses_future_bars=quant_spec.uses_future_bars,
        supports_visualization=quant_spec.supports_visualization,
    )

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
            "strategy_book_assessment": strategy_book_assessment,
        },
    )

    strategy_spec_path = output_dir / "strategy_spec.json"
    quant_run_path = output_dir / "quant_run.json"
    candidates_jsonl_path = output_dir / "candidates.jsonl"
    candidates_parquet_path = output_dir / "candidates.parquet"
    signals_path = output_dir / "signals.parquet"
    backtest_summary_path = output_dir / "backtest-summary.json"
    strategy_book_assessment_path = output_dir / "strategy-book-assessment.json"
    trades_path = output_dir / "trades.csv"
    readme_path = output_dir / "README.md"
    artifacts = {
        "strategy_spec": str(strategy_spec_path.resolve()),
        "quant_run": str(quant_run_path.resolve()),
        "candidates_jsonl": str(candidates_jsonl_path.resolve()),
        "candidates_parquet": str(candidates_parquet_path.resolve()),
        "signals": str(signals_path.resolve()),
        "backtest_summary": str(backtest_summary_path.resolve()),
        "strategy_book_assessment": str(strategy_book_assessment_path.resolve()),
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
    write_json(strategy_book_assessment_path, strategy_book_assessment)
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
            "strategy_book_assessment": strategy_book_assessment,
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
            strategy_book_assessment=strategy_book_assessment,
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
        "strategy_book_auto_stage": strategy_book_assessment["auto_stage"],
        "strategy_book_final_status": strategy_book_assessment["final_status"],
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
