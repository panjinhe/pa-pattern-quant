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


SPEC_ID = "close_chase_entry-v1-btc-5m"
CONCEPT_ID = "close_chase_entry"
TITLE_ZH = "收线市价追进"
DEFAULT_STRATEGY_SEQ = "002"
DEFAULT_STRATEGY_CODE = "PA_taifei_002"
DEFAULT_STRATEGY_NAME_STD = f"{DEFAULT_STRATEGY_CODE}_{TITLE_ZH}"


@dataclass(frozen=True)
class CloseChaseConfig:
    atr_window: int = 20
    breakout_window: int = 20
    breakout_buffer_atr: float = 0.10
    min_breakout_distance_atr: float = 1.00
    signal_close_pos_min: float = 0.68
    signal_close_pos_max: float = 0.32
    signal_body_ratio_min: float = 0.52
    min_ema_gap_atr: float = 0.15
    max_ema_stretch_atr: float = 3.80
    max_pullback_into_breakout_atr: float = 0.20
    target_r_min: float = 1.00
    label_lookahead: int = 24


def _coerce_config(params: CloseChaseConfig | dict[str, Any] | None) -> CloseChaseConfig:
    if params is None:
        return CloseChaseConfig()
    if isinstance(params, CloseChaseConfig):
        return params
    return CloseChaseConfig(**params)


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
    params: CloseChaseConfig | dict[str, Any] | None = None,
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
        )
        .with_columns(
            (
                pl.col("trend_context_long")
                & pl.col("bull_trend_bar")
                & (pl.col("close_vs_recent_high_atr") >= config.breakout_buffer_atr)
            ).alias("bull_breakout_bar"),
            (
                pl.col("trend_context_short")
                & pl.col("bear_trend_bar")
                & (pl.col("close_vs_recent_low_atr") >= config.breakout_buffer_atr)
            ).alias("bear_breakout_bar"),
        )
    )


def _scan_close_chase_signals(
    df: pl.DataFrame,
    config: CloseChaseConfig,
) -> dict[str, list[object]]:
    data = {
        "snapshot_status": df["snapshot_status"].to_list(),
        "trend_context_long": df["trend_context_long"].to_list(),
        "trend_context_short": df["trend_context_short"].to_list(),
        "bull_breakout_bar": df["bull_breakout_bar"].to_list(),
        "bear_breakout_bar": df["bear_breakout_bar"].to_list(),
        "bull_trend_bar": df["bull_trend_bar"].to_list(),
        "bear_trend_bar": df["bear_trend_bar"].to_list(),
        "close": [float(v) for v in df["close"].to_list()],
        "high": [float(v) for v in df["high"].to_list()],
        "low": [float(v) for v in df["low"].to_list()],
        "atr": [float(v) if v is not None else 0.0 for v in df["atr"].to_list()],
        "recent_high_pre": [float(v) if v is not None else None for v in df["recent_high_pre"].to_list()],
        "recent_low_pre": [float(v) if v is not None else None for v in df["recent_low_pre"].to_list()],
    }
    n = len(df)
    columns: dict[str, list[object]] = {
        "signal_bar": [False] * n,
        "signal_side": [None] * n,
        "breakout_idx": [None] * n,
        "origin_idx": [None] * n,
        "breakout_level": [None] * n,
        "distance_from_breakout_atr": [None] * n,
    }

    for signal_idx in range(config.breakout_window + 1, n):
        prev_idx = signal_idx - 1
        atr = max(data["atr"][signal_idx], 1e-9)
        if data["snapshot_status"][signal_idx] != "complete":
            continue

        if (
            data["trend_context_long"][signal_idx]
            and data["bull_breakout_bar"][prev_idx]
            and data["bull_trend_bar"][signal_idx]
            and data["close"][signal_idx] > data["close"][prev_idx]
        ):
            breakout_level = data["recent_high_pre"][prev_idx]
            if breakout_level is not None:
                distance_atr = (data["close"][signal_idx] - breakout_level) / atr
                if (
                    distance_atr >= config.min_breakout_distance_atr
                    and data["low"][signal_idx] > breakout_level - config.max_pullback_into_breakout_atr * atr
                ):
                    start_idx = max(0, prev_idx - config.breakout_window)
                    origin_idx = min(
                        range(start_idx, prev_idx + 1),
                        key=lambda idx: data["low"][idx],
                    )
                    columns["signal_bar"][signal_idx] = True
                    columns["signal_side"][signal_idx] = "long"
                    columns["breakout_idx"][signal_idx] = prev_idx
                    columns["origin_idx"][signal_idx] = origin_idx
                    columns["breakout_level"][signal_idx] = breakout_level
                    columns["distance_from_breakout_atr"][signal_idx] = distance_atr
                    continue

        if (
            data["trend_context_short"][signal_idx]
            and data["bear_breakout_bar"][prev_idx]
            and data["bear_trend_bar"][signal_idx]
            and data["close"][signal_idx] < data["close"][prev_idx]
        ):
            breakout_level = data["recent_low_pre"][prev_idx]
            if breakout_level is not None:
                distance_atr = (breakout_level - data["close"][signal_idx]) / atr
                if (
                    distance_atr >= config.min_breakout_distance_atr
                    and data["high"][signal_idx] < breakout_level + config.max_pullback_into_breakout_atr * atr
                ):
                    start_idx = max(0, prev_idx - config.breakout_window)
                    origin_idx = max(
                        range(start_idx, prev_idx + 1),
                        key=lambda idx: data["high"][idx],
                    )
                    columns["signal_bar"][signal_idx] = True
                    columns["signal_side"][signal_idx] = "short"
                    columns["breakout_idx"][signal_idx] = prev_idx
                    columns["origin_idx"][signal_idx] = origin_idx
                    columns["breakout_level"][signal_idx] = breakout_level
                    columns["distance_from_breakout_atr"][signal_idx] = distance_atr

    return columns


def detect(
    df: pl.DataFrame,
    params: CloseChaseConfig | dict[str, Any] | None = None,
) -> pl.DataFrame:
    """只做实时可用的规则检测，不使用未来 K 线。"""

    config = _coerce_config(params)
    featured = add_features(df, params=config)
    scan_cols = _scan_close_chase_signals(featured, config)
    return featured.with_columns(
        *[
            pl.Series(name=column_name, values=values)
            for column_name, values in scan_cols.items()
        ]
    )


def score(
    df: pl.DataFrame,
    params: CloseChaseConfig | dict[str, Any] | None = None,
    backtest_params: BacktestConfig | dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """把成立条件、加强条件、谨慎信号组织成结构化评分。"""

    config = _coerce_config(params)
    backtest_config = coerce_backtest_config(backtest_params)
    rows_by_idx = index_rows(df)
    scored: list[dict[str, Any]] = []

    for signal_row in df.filter(pl.col("signal_bar")).to_dicts():
        signal_idx = int(signal_row["idx"])
        breakout_idx = int(signal_row["breakout_idx"])
        origin_idx = int(signal_row["origin_idx"])
        side = str(signal_row["signal_side"])
        breakout_row = rows_by_idx[breakout_idx]
        origin_row = rows_by_idx[origin_idx]
        entry_price = float(signal_row["close"])
        breakout_level = float(signal_row["breakout_level"])
        atr = max(float(signal_row["atr"] or 0.0), 1e-9)
        distance_atr = float(signal_row["distance_from_breakout_atr"] or 0.0)

        if side == "long":
            structural_stop = min(
                float(origin_row["low"]),
                float(breakout_row["low"]),
                float(signal_row["low"]),
                breakout_level,
            ) - backtest_config.stop_buffer_atr * atr
            min_stop = entry_price - backtest_config.min_stop_atr * atr
            stop_price = min(structural_stop, min_stop)
            projection = max(entry_price - breakout_level, atr)
            target_primary = entry_price + projection
            target_secondary = entry_price + 1.5 * projection
            signal_strength_ok = (
                float(signal_row["close_pos"] or 0.0) >= config.signal_close_pos_min
                and float(signal_row["body_ratio"] or 0.0) >= config.signal_body_ratio_min
            )
            counter_pressure = int(signal_row["recent_bear_pressure_3"] or 0)
            wick_caution = float(signal_row["upper_wick_ratio"] or 0.0) >= 0.25
            magnet_price = signal_row.get("recent_high_96")
            magnet_hits = (
                magnet_price is not None
                and float(magnet_price) > entry_price
                and float(magnet_price) < target_primary
            )
            space_to_target_r = (target_primary - entry_price) / max(entry_price - stop_price, 1e-9)
        else:
            structural_stop = max(
                float(origin_row["high"]),
                float(breakout_row["high"]),
                float(signal_row["high"]),
                breakout_level,
            ) + backtest_config.stop_buffer_atr * atr
            min_stop = entry_price + backtest_config.min_stop_atr * atr
            stop_price = max(structural_stop, min_stop)
            projection = max(breakout_level - entry_price, atr)
            target_primary = entry_price - projection
            target_secondary = entry_price - 1.5 * projection
            signal_strength_ok = (
                float(signal_row["close_pos"] or 1.0) <= config.signal_close_pos_max
                and float(signal_row["body_ratio"] or 0.0) >= config.signal_body_ratio_min
            )
            counter_pressure = int(signal_row["recent_bull_pressure_3"] or 0)
            wick_caution = float(signal_row["lower_wick_ratio"] or 0.0) >= 0.25
            magnet_price = signal_row.get("recent_low_96")
            magnet_hits = (
                magnet_price is not None
                and float(magnet_price) < entry_price
                and float(magnet_price) > target_primary
            )
            space_to_target_r = (entry_price - target_primary) / max(stop_price - entry_price, 1e-9)

        hard_gate_checks: list[tuple[str, bool]] = [
            ("snapshot 完整", str(signal_row["snapshot_status"]) == "complete"),
            ("突破后有跟进且当前仍为强趋势 K", bool(signal_strength_ok)),
            (
                "趋势环境健康：EMA20/EMA50 同向且价格站在趋势侧",
                bool(
                    signal_row["trend_context_long"] if side == "long" else signal_row["trend_context_short"]
                ),
            ),
            (
                "距离突破点足够远",
                distance_atr >= config.min_breakout_distance_atr,
            ),
            ("未明显跌回/涨回突破点内部", bool(signal_row["signal_bar"])),
            ("入场到止损风险为正", (entry_price - stop_price) > 0 if side == "long" else (stop_price - entry_price) > 0),
        ]
        hard_gates = [label for label, passed in hard_gate_checks if passed]
        hard_gate_pass = all(passed for _, passed in hard_gate_checks)

        strengtheners: list[str] = []
        if distance_atr >= 1.8:
            strengtheners.append("突破到当前收线距离较远，动能充足")
        if (
            bool(breakout_row["bull_trend_bar"]) and bool(signal_row["bull_trend_bar"])
            if side == "long"
            else bool(breakout_row["bear_trend_bar"]) and bool(signal_row["bear_trend_bar"])
        ):
            strengtheners.append("连续两根强趋势 K 完成突破与跟进")
        if counter_pressure == 0:
            strengtheners.append("最近 3 根几乎没有反向强压力")
        if space_to_target_r >= config.target_r_min:
            strengtheners.append("等距投影到目标至少有 1R 空间")
        if abs(float(signal_row["distance_ema20_atr"] or 0.0)) <= config.max_ema_stretch_atr * 0.55:
            strengtheners.append("与 EMA20 的乖离尚未过度失真")

        cautions: list[str] = []
        if abs(float(signal_row["distance_ema20_atr"] or 0.0)) >= config.max_ema_stretch_atr * 0.8:
            cautions.append("价格已经明显远离 EMA20，追进成本偏高")
        if counter_pressure >= 1:
            cautions.append("最近 3 根出现了反方向强压力")
        if wick_caution:
            cautions.append("信号 K 留下较长反向影线")
        if magnet_hits:
            cautions.append("利润路径上存在左侧已知磁体")
        if float(signal_row["ema_gap_atr"] or 0.0) >= 1.4:
            cautions.append("EMA 偏离过大，容易从急速过渡为宽通道")

        score_value = clip_score(70 + 5 * len(strengtheners) - 5 * len(cautions))
        route_value = route(score_value)
        score_band_value = score_band(score_value)

        summary = (
            f"{TITLE_ZH}：{('多头' if side == 'long' else '空头')}在突破后获得了收线跟进，"
            f"当前距突破点约 {distance_atr:.2f} ATR，评分 {score_value}，分档 {score_band_value}，"
            f"路由 {route_value}。"
        )

        scored.append(
            {
                "event_id": f"evt_{stable_id(SPEC_ID, signal_row['timestamp'], side)}",
                "candidate_id": f"cand_{stable_id(SPEC_ID, signal_row['timestamp'], side)}",
                "signal_idx": signal_idx,
                "breakout_idx": breakout_idx,
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
                "entry_zone_low": entry_price - 0.05 * atr,
                "entry_zone_high": entry_price + 0.05 * atr,
                "stop_price": stop_price,
                "take_profit_prices": [target_primary, target_secondary],
                "invalidation_price": stop_price,
                "distance_from_breakout_atr": distance_atr,
                "space_to_target_r": space_to_target_r,
                "breakout_level": breakout_level,
                "signal_bar_time": iso(signal_row["timestamp"]),
                "analysis_summary": summary,
                "tags": [
                    CONCEPT_ID,
                    DEFAULT_STRATEGY_CODE,
                    "momentum",
                    "breakout_follow_through",
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
    origin_row = rows_by_idx[record["origin_idx"]]
    side = record["side"]

    direction_label = "多" if side == "long" else "空"
    key_points = [
        make_key_point(
            f"{record['event_id']}-origin",
            "origin",
            origin_row["timestamp"],
            float(origin_row["low"] if side == "long" else origin_row["high"]),
            "起点",
        ),
        make_key_point(
            f"{record['event_id']}-breakout",
            "breakout",
            breakout_row["timestamp"],
            float(record["breakout_level"]),
            "破位",
        ),
        make_key_point(
            f"{record['event_id']}-signal",
            "signal",
            signal_row["timestamp"],
            float(record["entry_price"]),
            f"{direction_label}追",
        ),
        make_key_point(
            f"{record['event_id']}-target1",
            "target1",
            signal_row["timestamp"],
            float(record["take_profit_prices"][0]),
            "目标1",
        ),
    ]

    breakout_start = make_key_point(
        f"{record['event_id']}-bk-start",
        "breakout_start",
        origin_row["timestamp"],
        float(record["breakout_level"]),
    )
    breakout_end = make_key_point(
        f"{record['event_id']}-bk-end",
        "breakout_end",
        signal_row["timestamp"],
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
            f"{record['event_id']}-breakout-line",
            "breakout_level",
            breakout_start,
            breakout_end,
            "#b9770e",
        ),
        make_segment_overlay(
            f"{record['event_id']}-stop-line",
            "stop_line",
            stop_start,
            stop_end,
            "#cb4335",
            dash="dash",
        ),
        make_segment_overlay(
            f"{record['event_id']}-target-line",
            "target_line",
            target_start,
            target_end,
            "#1d8348",
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
    """输出直播侧 OpportunityCandidate。"""

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
            entry_trigger="突破后收线跟进，按当前收线或小回抽追进",
            entry_price=float(record["entry_price"]),
            stop_price=float(record["stop_price"]),
            target_prices=[float(price) for price in record["take_profit_prices"]],
            timeout_bars=run_ctx["backtest_config"].max_holding_bars,
            invalidation_rule="重新跌回/涨回本次趋势追进的结构止损之外",
            tags=["momentum", "close_chase_entry"],
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
            family="close_chase_entry",
            features={
                "hard_gate_pass": record["hard_gate_pass"],
                "hard_gates": record["hard_gates"],
                "strengtheners": record["strengtheners"],
                "cautions": record["cautions"],
                "route": record["route"],
                "distance_from_breakout_atr": round(record["distance_from_breakout_atr"], 4),
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
                "origin_idx": record["origin_idx"],
                "entry_zone_low": record["entry_zone_low"],
                "entry_zone_high": record["entry_zone_high"],
            },
        )
        events.append(event)

        if not record["hard_gate_pass"]:
            continue
        if record["snapshot_status"] != "complete":
            continue
        if record["route"] == "drop":
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
                setup_type="close_chase_follow_through",
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
    params: CloseChaseConfig | dict[str, Any] | None = None,
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
            "distance_ema20_atr",
            "recent_high_pre",
            "recent_low_pre",
            "trend_context_long",
            "trend_context_short",
            "bull_breakout_bar",
            "bear_breakout_bar",
            "distance_from_breakout_atr",
        ],
        event_columns=[
            "signal_bar",
            "signal_side",
            "breakout_idx",
            "origin_idx",
            "score",
            "score_band",
            "route",
        ],
        required_context_ids=["spike", "follow_through"],
        required_primitive_ids=["strong_breakout", "trend_bar", "close_chase"],
        params={
            **asdict(CloseChaseConfig()),
            "backtest": asdict(BacktestConfig()),
            "strategy_meta": strategy_meta,
            "registry_entry": registry_entry or {},
        },
        detector=DetectorBinding(
            module_path="strategylets.close_chase_entry",
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

这是一个顺势追进模板。当前一根强突破 K 已经把价格明显带离突破点，并且下一根继续给出同向强收线时，我们把它解释为“突破有跟进、急速仍未结束”，从而在收线时给出追进候选。

## 成立条件

- snapshot 必须完整，`snapshot_status == complete`
- EMA20 与 EMA50 同向，且价格站在趋势侧
- 前一根是有效突破 K，当前根是同向跟进 K
- 当前收线距离突破点足够远
- 当前 K 没有明显跌回/涨回突破点内部
- 只在当前 K 收线后产出，不做盘中预告

## 加强条件

- 突破到当前收线距离较远，动能充足
- 连续两根强趋势 K 完成突破与跟进
- 最近 3 根几乎没有反向强压力
- 等距投影到目标至少有 1R 空间
- 与 EMA20 的乖离尚未过度失真

## 谨慎信号

- 价格已经明显远离 EMA20，追进成本偏高
- 最近 3 根出现了反方向强压力
- 信号 K 留下较长反向影线
- 利润路径上存在左侧已知磁体
- EMA 偏离过大，容易从急速过渡为宽通道

## 买卖点与风控

- 方向：多空双向
- 入场：当前跟进 K 收线后追进，直播输出为 `entry_zone_low ~ entry_zone_high`
- 止损：急速起点 / 突破 K / 信号 K 结构外，再加 `0.15 ATR`
- 目标位：
  - 第一目标：以“当前收线距突破点”的幅度做等距投影
  - 第二目标：第一目标的 1.5 倍扩展
- 失效：价格重新回到本次结构止损之外

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

    config = CloseChaseConfig()
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
