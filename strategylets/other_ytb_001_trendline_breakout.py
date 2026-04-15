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

TRENDLINE_SOURCE_DIR = ROOT / "产出" / "趋势线识别"
if str(TRENDLINE_SOURCE_DIR) not in sys.path:
    sys.path.insert(0, str(TRENDLINE_SOURCE_DIR))

CONTRACT_DIR = ROOT / "阿布课程术语体系" / "contracts"
if str(CONTRACT_DIR) not in sys.path:
    sys.path.insert(0, str(CONTRACT_DIR))

from detect_trendline import (  # noqa: E402
    TrendlineConfig as SourceTrendlineConfig,
    detect_trendline_breakouts,
    label_trendline_outcomes as source_label_outcomes,
    prepare_ohlcv as source_prepare_ohlcv,
    read_ohlcv as source_read_ohlcv,
    summarize_detection,
)
from quant_interface import (  # noqa: E402
    DetectorBinding,
    OpportunityCandidate,
    QuantEvent,
    QuantRun,
    QuantSpec,
    RunSummary,
    TradePlan,
)
from strategylets.trend_strategy_common import (  # noqa: E402
    attach_scored_columns,
    candidate_parquet_rows,
    clip_score,
    dataclass_to_dict,
    index_rows,
    iso,
    label_outcomes as common_label_outcomes,
    make_key_point,
    make_segment_overlay,
    make_zone_overlay,
    plot_outputs,
    route,
    run_backtest as common_run_backtest,
    score_band,
    stable_id,
    write_json,
    write_jsonl,
)
from strategylets.strategy_book_kpi import (  # noqa: E402
    build_strategy_book_assessment,
    render_strategy_book_assessment,
)


SPEC_ID = "trendline_breakout-v1-eth-5m"
CONCEPT_ID = "trendline_breakout"
TITLE_ZH = "趋势线突破"
DEFAULT_STRATEGY_SEQ = "001"
DEFAULT_STRATEGY_CODE = "other_ytb_001"
DEFAULT_STRATEGY_NAME_STD = f"{DEFAULT_STRATEGY_CODE}_{TITLE_ZH}"

CHANNEL_LABELS = {
    "ascending": "上升通道",
    "descending": "下降通道",
    "flat": "平行区间",
}


@dataclass(frozen=True)
class TrendlineScoringConfig:
    strong_breakout_distance_atr: float = 0.32
    ideal_channel_height_atr_low: float = 2.0
    ideal_channel_height_atr_high: float = 6.5
    weak_body_ratio: float = 0.52
    weak_close_pos_long: float = 0.66
    weak_close_pos_short: float = 0.34
    slope_against_threshold_atr: float = 0.06
    breakout_margin_factor: float = 1.15
    target_r_min: float = 1.0
    label_lookahead: int = 48


@dataclass(frozen=True)
class TrendlineBacktestConfig:
    stop_buffer_atr: float = 0.12
    min_stop_atr: float = 0.70
    target_height_multiplier: float = 0.90
    target_r_multiple: float = 1.80
    second_target_factor: float = 1.35
    max_holding_bars: int = 48
    cooldown_bars: int = 6
    fee_bps_round_trip: float = 2.0
    notional_usdt: float = 10_000.0


def _coerce_detect_config(
    params: SourceTrendlineConfig | dict[str, Any] | None,
) -> SourceTrendlineConfig:
    if params is None:
        return SourceTrendlineConfig()
    if isinstance(params, SourceTrendlineConfig):
        return params
    return SourceTrendlineConfig(**params)


def _coerce_score_config(
    params: TrendlineScoringConfig | dict[str, Any] | None,
) -> TrendlineScoringConfig:
    if params is None:
        return TrendlineScoringConfig()
    if isinstance(params, TrendlineScoringConfig):
        return params
    return TrendlineScoringConfig(**params)


def _coerce_backtest_config(
    params: TrendlineBacktestConfig | dict[str, Any] | None,
) -> TrendlineBacktestConfig:
    if params is None:
        return TrendlineBacktestConfig()
    if isinstance(params, TrendlineBacktestConfig):
        return params
    return TrendlineBacktestConfig(**params)


def _common_backtest_kwargs(config: TrendlineBacktestConfig) -> dict[str, Any]:
    return {
        "stop_buffer_atr": config.stop_buffer_atr,
        "min_stop_atr": config.min_stop_atr,
        "max_holding_bars": config.max_holding_bars,
        "cooldown_bars": config.cooldown_bars,
        "fee_bps_round_trip": config.fee_bps_round_trip,
        "notional_usdt": config.notional_usdt,
    }


def _strategy_meta(registry_entry: dict[str, Any] | None = None) -> dict[str, str]:
    return {
        "strategy_seq": str((registry_entry or {}).get("strategy_seq", DEFAULT_STRATEGY_SEQ)),
        "strategy_code": str((registry_entry or {}).get("strategy_code", DEFAULT_STRATEGY_CODE)),
        "strategy_name_std": str(
            (registry_entry or {}).get("strategy_name_std", DEFAULT_STRATEGY_NAME_STD)
        ),
        "strategy_name_zh": str((registry_entry or {}).get("strategy_name_zh", TITLE_ZH)),
    }


def read_ohlcv(path: Path) -> pl.DataFrame:
    return source_read_ohlcv(path)


def prepare_ohlcv(df: pl.DataFrame) -> pl.DataFrame:
    """统一 OHLCV 字段。实时安全，不使用未来 K 线。"""

    return source_prepare_ohlcv(df)


def detect(
    df: pl.DataFrame,
    params: SourceTrendlineConfig | dict[str, Any] | None = None,
) -> pl.DataFrame:
    """趋势线突破检测。实时安全，不使用未来 K 线。"""

    config = _coerce_detect_config(params)
    detected = detect_trendline_breakouts(df, config=config)
    complete_threshold = max(config.atr_window, config.line_window)
    return detected.with_columns(
        pl.when(
            (pl.col("idx") >= complete_threshold)
            & pl.col("atr").is_not_null()
            & pl.col("trend_window_start_idx").is_not_null()
        )
        .then(pl.lit("complete"))
        .otherwise(pl.lit("degraded"))
        .alias("snapshot_status"),
        (pl.col("bullish_breakout_signal") | pl.col("bearish_breakdown_signal")).alias("signal_bar"),
        pl.when(pl.col("bullish_breakout_signal"))
        .then(pl.lit("long"))
        .when(pl.col("bearish_breakdown_signal"))
        .then(pl.lit("short"))
        .otherwise(None)
        .alias("signal_side"),
        pl.when(pl.col("bullish_breakout_signal") & (pl.col("atr") > 0))
        .then((pl.col("close") - pl.col("resistance_line_current")) / pl.col("atr"))
        .when(pl.col("bearish_breakdown_signal") & (pl.col("atr") > 0))
        .then((pl.col("support_line_current") - pl.col("close")) / pl.col("atr"))
        .otherwise(None)
        .alias("breakout_distance_atr"),
    )


def _channel_label(channel_type: str | None) -> str:
    if channel_type is None:
        return "未知通道"
    return CHANNEL_LABELS.get(channel_type, str(channel_type))


def score(
    df: pl.DataFrame,
    params: TrendlineScoringConfig | dict[str, Any] | None = None,
    *,
    detect_params: SourceTrendlineConfig | dict[str, Any] | None = None,
    backtest_params: TrendlineBacktestConfig | dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    score_config = _coerce_score_config(params)
    detect_config = _coerce_detect_config(detect_params)
    backtest_config = _coerce_backtest_config(backtest_params)

    scored: list[dict[str, Any]] = []
    for signal_row in df.filter(pl.col("signal_bar")).to_dicts():
        signal_idx = int(signal_row["idx"])
        side = str(signal_row["signal_side"])
        atr = max(float(signal_row["atr"] or 0.0), 1e-9)
        entry_price = float(signal_row["close"])
        channel_type = str(signal_row["trend_channel_type"] or "flat")
        channel_height = max(float(signal_row["channel_height"] or 0.0), atr)
        channel_height_atr = float(signal_row["channel_height_atr"] or 0.0)
        trend_slope_atr = float(signal_row["trend_slope_atr"] or 0.0)
        body_ratio = float(signal_row["body_ratio"] or 0.0)
        close_pos = float(signal_row["close_pos"] or 0.5)
        breakout_distance_atr = float(signal_row["breakout_distance_atr"] or 0.0)
        support_touch_count = int(signal_row["support_touch_count"] or 0)
        resistance_touch_count = int(signal_row["resistance_touch_count"] or 0)

        if side == "long":
            boundary_price = float(signal_row["resistance_line_current"] or entry_price)
            structure_price = float(signal_row["support_line_current"] or entry_price)
            stop_price = min(
                structure_price - backtest_config.stop_buffer_atr * atr,
                entry_price - backtest_config.min_stop_atr * atr,
            )
            breakout_confirmed = entry_price > (
                boundary_price + detect_config.breakout_tolerance_atr * atr
            )
            strong_close = close_pos >= 0.60 and body_ratio >= 0.45
            slope_aligned = trend_slope_atr >= -score_config.slope_against_threshold_atr
            weak_close = close_pos < score_config.weak_close_pos_long
            slope_against = trend_slope_atr < -score_config.slope_against_threshold_atr
            target_distance = max(
                channel_height * backtest_config.target_height_multiplier,
                max(entry_price - stop_price, 1e-9) * backtest_config.target_r_multiple,
            )
            target_primary = entry_price + target_distance
            target_secondary = entry_price + target_distance * backtest_config.second_target_factor
            side_text = "向上突破"
        else:
            boundary_price = float(signal_row["support_line_current"] or entry_price)
            structure_price = float(signal_row["resistance_line_current"] or entry_price)
            stop_price = max(
                structure_price + backtest_config.stop_buffer_atr * atr,
                entry_price + backtest_config.min_stop_atr * atr,
            )
            breakout_confirmed = entry_price < (
                boundary_price - detect_config.breakout_tolerance_atr * atr
            )
            strong_close = close_pos <= 0.40 and body_ratio >= 0.45
            slope_aligned = trend_slope_atr <= score_config.slope_against_threshold_atr
            weak_close = close_pos > score_config.weak_close_pos_short
            slope_against = trend_slope_atr > score_config.slope_against_threshold_atr
            target_distance = max(
                channel_height * backtest_config.target_height_multiplier,
                max(stop_price - entry_price, 1e-9) * backtest_config.target_r_multiple,
            )
            target_primary = entry_price - target_distance
            target_secondary = entry_price - target_distance * backtest_config.second_target_factor
            side_text = "向下跌破"

        risk_per_unit = (
            entry_price - stop_price if side == "long" else stop_price - entry_price
        )
        space_to_target_r = abs(target_primary - entry_price) / max(risk_per_unit, 1e-9)

        hard_gate_checks: list[tuple[str, bool]] = [
            ("snapshot 完整", str(signal_row["snapshot_status"]) == "complete"),
            ("支撑与阻力边界同时有效", bool(signal_row["trend_channel_valid"])),
            (
                "支撑与阻力各至少 3 次分离触碰",
                support_touch_count >= detect_config.min_touches
                and resistance_touch_count >= detect_config.min_touches,
            ),
            (
                "通道高度处于有效 ATR 区间",
                detect_config.min_channel_height_atr
                <= channel_height_atr
                <= detect_config.max_channel_height_atr,
            ),
            ("突破 K 已有效越过趋势线边界", breakout_confirmed),
            ("突破 K 收盘位置与实体强度达标", strong_close),
            ("入场到止损风险为正", risk_per_unit > 0),
        ]
        hard_gates = [label for label, passed in hard_gate_checks if passed]
        hard_gate_pass = all(passed for _, passed in hard_gate_checks)

        strengtheners: list[str] = []
        if (
            support_touch_count >= detect_config.min_touches + 1
            and resistance_touch_count >= detect_config.min_touches + 1
        ):
            strengtheners.append("支撑与阻力都出现了 4 次及以上有效触碰")
        if breakout_distance_atr >= score_config.strong_breakout_distance_atr:
            strengtheners.append("收盘突破距离明显超过边界，确认力度较强")
        if slope_aligned:
            strengtheners.append("通道斜率与突破方向不冲突")
        if (
            score_config.ideal_channel_height_atr_low
            <= channel_height_atr
            <= score_config.ideal_channel_height_atr_high
        ):
            strengtheners.append("通道高度适中，既可见又不过宽")
        if space_to_target_r >= max(score_config.target_r_min, backtest_config.target_r_multiple):
            strengtheners.append("按测量目标投影至少保留 1.8R 空间")

        cautions: list[str] = []
        if slope_against:
            cautions.append("通道斜率与突破方向相反，存在衰竭而非延续的风险")
        if channel_height_atr > score_config.ideal_channel_height_atr_high + 1.5:
            cautions.append("通道已经偏宽，突破后更容易演变成大幅震荡")
        if body_ratio < score_config.weak_body_ratio:
            cautions.append("突破 K 实体不够饱满，推动力偏一般")
        if weak_close:
            cautions.append("突破 K 收盘位置一般，边界外站稳程度有限")
        if breakout_distance_atr < detect_config.breakout_tolerance_atr * score_config.breakout_margin_factor:
            cautions.append("收盘仅略微越线，假突破概率仍需提防")
        if space_to_target_r < score_config.target_r_min:
            cautions.append("目标位空间不足 1R，盈亏比不够理想")

        score_value = clip_score(70 + 5 * len(strengtheners) - 5 * len(cautions))
        route_value = route(score_value)
        score_band_value = score_band(score_value)
        channel_label = _channel_label(channel_type)

        scored.append(
            {
                "event_id": f"evt_{stable_id(SPEC_ID, signal_row['timestamp'], side)}",
                "candidate_id": f"cand_{stable_id(SPEC_ID, signal_row['timestamp'], side)}",
                "signal_idx": signal_idx,
                "window_start_idx": int(signal_row["trend_window_start_idx"]),
                "window_end_idx": int(signal_row["trend_window_end_idx"]),
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
                "channel_type": channel_type,
                "channel_label": channel_label,
                "channel_height_atr": channel_height_atr,
                "support_touch_count": support_touch_count,
                "resistance_touch_count": resistance_touch_count,
                "breakout_distance_atr": breakout_distance_atr,
                "space_to_target_r": space_to_target_r,
                "signal_bar_time": iso(signal_row["timestamp"]),
                "analysis_summary": (
                    f"{TITLE_ZH}：{channel_label}{side_text}，"
                    f"支撑触碰 {support_touch_count} 次、阻力触碰 {resistance_touch_count} 次，"
                    f"突破发生在 {iso(signal_row['timestamp'])} 收线。"
                    f"评分 {score_value}，分档 {score_band_value}，路由 {route_value}。"
                ),
                "tags": [
                    CONCEPT_ID,
                    DEFAULT_STRATEGY_CODE,
                    channel_type,
                    "trendline",
                    "breakout",
                    side,
                    route_value,
                    score_band_value.lower(),
                ],
            }
        )

    return scored


def _collect_touch_points(
    rows_by_idx: dict[int, dict[str, Any]],
    signal_row: dict[str, Any],
    *,
    prefix: str,
    event_id: str,
) -> list[Any]:
    points: list[Any] = []
    label_prefix = "S" if prefix == "support" else "R"
    price_key = "low" if prefix == "support" else "high"
    for slot in range(1, 4):
        idx_value = signal_row.get(f"{prefix}_touch_{slot}_idx")
        if idx_value is None:
            continue
        row = rows_by_idx.get(int(idx_value))
        if row is None:
            continue
        points.append(
            make_key_point(
                f"{event_id}-{prefix}-{slot}",
                f"{prefix}_touch_{slot}",
                row["timestamp"],
                float(row[price_key]),
                f"{label_prefix}{slot}",
            )
        )
    return points


def _last_touch_point(touch_points: list[Any]) -> Any | None:
    if not touch_points:
        return None
    return max(touch_points, key=lambda point: point.timestamp)


def _build_points_and_overlays(
    rows_by_idx: dict[int, dict[str, Any]],
    record: dict[str, Any],
) -> tuple[list[Any], list[Any]]:
    signal_row = rows_by_idx[record["signal_idx"]]
    window_start_row = rows_by_idx[record["window_start_idx"]]
    support_points = _collect_touch_points(
        rows_by_idx,
        signal_row,
        prefix="support",
        event_id=record["event_id"],
    )
    resistance_points = _collect_touch_points(
        rows_by_idx,
        signal_row,
        prefix="resistance",
        event_id=record["event_id"],
    )
    breakout_point = make_key_point(
        f"{record['event_id']}-signal",
        "signal",
        signal_row["timestamp"],
        float(record["entry_price"]),
        "突破",
    )
    key_points = support_points + resistance_points + [breakout_point]

    support_line_start = make_key_point(
        f"{record['event_id']}-support-start",
        "support_line_start",
        window_start_row["timestamp"],
        float(signal_row["support_line_start"]),
        "支撑起",
    )
    support_line_end = make_key_point(
        f"{record['event_id']}-support-end",
        "support_line_end",
        signal_row["timestamp"],
        float(signal_row["support_line_current"]),
        "支撑终",
    )
    resistance_line_start = make_key_point(
        f"{record['event_id']}-resistance-start",
        "resistance_line_start",
        window_start_row["timestamp"],
        float(signal_row["resistance_line_start"]),
        "阻力起",
    )
    resistance_line_end = make_key_point(
        f"{record['event_id']}-resistance-end",
        "resistance_line_end",
        signal_row["timestamp"],
        float(signal_row["resistance_line_current"]),
        "阻力终",
    )
    stop_line_start = make_key_point(
        f"{record['event_id']}-stop-start",
        "stop_start",
        window_start_row["timestamp"],
        float(record["stop_price"]),
        "止损",
    )
    stop_line_end = make_key_point(
        f"{record['event_id']}-stop-end",
        "stop_end",
        signal_row["timestamp"],
        float(record["stop_price"]),
        "止损",
    )
    target_line_start = make_key_point(
        f"{record['event_id']}-target-start",
        "target_start",
        window_start_row["timestamp"],
        float(record["take_profit_prices"][0]),
        "目标1",
    )
    target_line_end = make_key_point(
        f"{record['event_id']}-target-end",
        "target_end",
        signal_row["timestamp"],
        float(record["take_profit_prices"][0]),
        "目标1",
    )
    guide_touch = (
        _last_touch_point(resistance_points)
        if record["side"] == "long"
        else _last_touch_point(support_points)
    )

    overlays = [
        make_segment_overlay(
            f"{record['event_id']}-support-line",
            "support_line",
            support_line_start,
            support_line_end,
            "#2874a6",
        ),
        make_segment_overlay(
            f"{record['event_id']}-resistance-line",
            "resistance_line",
            resistance_line_start,
            resistance_line_end,
            "#b9770e",
        ),
        make_segment_overlay(
            f"{record['event_id']}-stop-line",
            "stop_line",
            stop_line_start,
            stop_line_end,
            "#cb4335",
            dash="dash",
        ),
        make_segment_overlay(
            f"{record['event_id']}-target-line",
            "target_line",
            target_line_start,
            target_line_end,
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
    if guide_touch is not None:
        overlays.append(
            make_segment_overlay(
                f"{record['event_id']}-breakout-guide",
                "breakout_guide",
                guide_touch,
                breakout_point,
                "#7f8c8d",
                dash="dash",
            )
        )
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
            entry_trigger="趋势线被多次测试后，突破 K 收线确认再参与",
            entry_price=float(record["entry_price"]),
            stop_price=float(record["stop_price"]),
            target_prices=[float(price) for price in record["take_profit_prices"]],
            timeout_bars=run_ctx["backtest_config"].max_holding_bars,
            invalidation_rule="价格重新回到本次趋势线结构止损之外",
            tags=["trendline", "breakout", record["channel_type"]],
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
            start_time=iso(rows_by_idx[record["window_start_idx"]]["timestamp"]),
            end_time=record["signal_bar_time"],
            confidence=round(record["score"] / 100.0, 4),
            score=float(record["score"]),
            family="trendline_breakout",
            features={
                "hard_gate_pass": record["hard_gate_pass"],
                "hard_gates": record["hard_gates"],
                "strengtheners": record["strengtheners"],
                "cautions": record["cautions"],
                "route": record["route"],
                "channel_type": record["channel_type"],
                "channel_height_atr": round(record["channel_height_atr"], 4),
                "support_touch_count": record["support_touch_count"],
                "resistance_touch_count": record["resistance_touch_count"],
                "breakout_distance_atr": round(record["breakout_distance_atr"], 4),
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
                "window_start_idx": record["window_start_idx"],
                "window_end_idx": record["window_end_idx"],
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
                setup_type="trendline_breakout",
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
    params: TrendlineScoringConfig | dict[str, Any] | None = None,
    backtest_params: TrendlineBacktestConfig | dict[str, Any] | None = None,
) -> list[QuantEvent]:
    score_config = _coerce_score_config(params)
    backtest_config = _coerce_backtest_config(backtest_params)
    return common_label_outcomes(
        df,
        events,
        lookahead_bars=score_config.label_lookahead,
        backtest_params=_common_backtest_kwargs(backtest_config),
    )


def build_quant_spec(
    registry_entry: dict[str, Any] | None = None,
) -> QuantSpec:
    strategy_meta = _strategy_meta(registry_entry)
    detect_config = SourceTrendlineConfig()
    score_config = TrendlineScoringConfig()
    backtest_config = TrendlineBacktestConfig()
    return QuantSpec(
        spec_id=SPEC_ID,
        concept_id=CONCEPT_ID,
        version="v1",
        title=strategy_meta["strategy_name_std"],
        input_columns=["timestamp", "open", "high", "low", "close", "volume"],
        feature_columns=[
            "atr",
            "close_pos",
            "body_ratio",
            "trend_window_start_idx",
            "trend_window_end_idx",
            "trend_slope",
            "trend_slope_atr",
            "trend_channel_type",
            "support_line_start",
            "support_line_current",
            "resistance_line_start",
            "resistance_line_current",
            "channel_height_atr",
            "support_touch_count",
            "resistance_touch_count",
            "breakout_distance_atr",
        ],
        event_columns=[
            "signal_bar",
            "signal_side",
            "bullish_breakout_signal",
            "bearish_breakdown_signal",
            "score",
            "score_band",
            "route",
        ],
        required_context_ids=["trendline", "breakout"],
        required_primitive_ids=["support_line", "resistance_line", "confirm_close"],
        params={
            "detect": asdict(detect_config),
            "score": asdict(score_config),
            "backtest": asdict(backtest_config),
            "strategy_meta": strategy_meta,
            "registry_entry": registry_entry or {},
        },
        detector=DetectorBinding(
            module_path="strategylets.other_ytb_001_trendline_breakout",
            detect_function="detect",
            label_function="label_outcomes",
            strategy_function="run_strategylet",
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
    detection_summary: dict[str, Any],
) -> str:
    primary_count = sum(1 for candidate in candidates if candidate.candidate_tier == "primary")
    secondary_count = sum(1 for candidate in candidates if candidate.candidate_tier == "secondary")
    drop_count = sum(1 for item in scored_records if item["route"] == "drop")

    return f"""# {strategy_meta['strategy_name_std']}

- 策略代码：`{strategy_meta['strategy_code']}`
- 中文名：`{strategy_meta['strategy_name_zh']}`

## 策略是什么

这是一个“多次测试后的趋势线确认突破”模板。它先在最近 `48` 根 K 中拟合趋势通道，再要求支撑与阻力都被市场反复触碰，最后只在突破 K 收线确认时产出直播候选。

## 成立条件

- snapshot 必须完整，`snapshot_status == complete`
- 支撑与阻力边界同时有效
- 支撑与阻力各至少 `3` 次分离触碰
- 通道高度位于有效 ATR 区间，不接受过窄或过宽结构
- 当前突破 K 收盘有效越过趋势线边界
- 当前突破 K 的收盘位置与实体比例达标
- 入场到止损风险为正

## 加强条件

- 支撑与阻力都出现了 `4` 次及以上有效触碰
- 收盘突破距离明显超过边界，确认力度较强
- 通道斜率与突破方向不冲突
- 通道高度适中，便于直播展示与执行
- 按测量目标投影至少保留 `1.8R` 空间

## 谨慎信号

- 通道斜率与突破方向相反，可能是衰竭突破
- 通道已经偏宽，突破后容易进入大幅震荡
- 突破 K 实体不够饱满
- 收盘位置一般，边界外站稳程度有限
- 收盘只略微越线，需提防假突破
- 目标位空间不足 `1R`

## 买卖点与风控

- 方向：多空双向
- 入场：突破 K 收线确认后参与，直播输出为 `entry_zone_low ~ entry_zone_high`
- 止损：多头放在当前支撑线下方，空头放在当前阻力线上方，并保证最小止损不低于 `0.7 ATR`
- 目标位：
  - 第一目标：`max(0.9 * 通道高度, 1.8R)`
  - 第二目标：第一目标再扩展 `1.35` 倍
- 失效：价格重新回到本次趋势线结构止损之外

## 本次运行

- `spec_id`: `{run.spec_id}`
- `run_id`: `{run.run_id}`
- 数据文件：`{input_path.resolve()}`
- 样本区间：`{run.sample_start}` 至 `{run.sample_end}`
- QuantEvent 数：`{len(run.events)}`
- OpportunityCandidate 数：`{len(candidates)}`
- `primary / secondary / drop`：`{primary_count} / {secondary_count} / {drop_count}`

## 结构统计

- 趋势通道有效窗口数：`{detection_summary.get('trend_channel_valid', 0)}`
- 上破信号数：`{detection_summary.get('bullish_breakout_signal', 0)}`
- 下破信号数：`{detection_summary.get('bearish_breakdown_signal', 0)}`

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
    run_id: str | None = None,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    input_path = Path(input_path)
    output_root = Path(output_root)
    strategy_meta = _strategy_meta(registry_entry)
    started_at = datetime.now().replace(microsecond=0)
    run_id = run_id or (
        f"{started_at.strftime('%Y%m%dT%H%M%S')}-"
        f"{stable_id(SPEC_ID, symbol, timeframe, started_at.date())}"
    )
    output_dir = Path(output_dir) if output_dir is not None else (output_root / SPEC_ID / run_id)
    output_dir.mkdir(parents=True, exist_ok=True)

    detect_config = SourceTrendlineConfig()
    score_config = TrendlineScoringConfig()
    backtest_config = TrendlineBacktestConfig()

    raw_df = read_ohlcv(input_path)
    base_df = prepare_ohlcv(raw_df)
    detected_df = detect(base_df, params=detect_config)
    scored_records = score(
        detected_df,
        params=score_config,
        detect_params=detect_config,
        backtest_params=backtest_config,
    )
    signal_df = attach_scored_columns(detected_df, scored_records)
    signal_df = source_label_outcomes(signal_df, config=detect_config)

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
        params=score_config,
        backtest_params=backtest_config,
    )
    trades_df, backtest_summary, equity_df = common_run_backtest(
        labeled_events,
        backtest_params=_common_backtest_kwargs(backtest_config),
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
    detection_summary = summarize_detection(signal_df)
    primary_count = sum(1 for item in candidates if item.candidate_tier == "primary")
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
        symbol=symbol,
        sample_start=sample_info["sample_start"],
        sample_end=sample_info["sample_end"],
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
            "primary_count": primary_count,
            "secondary_count": sum(1 for item in candidates if item.candidate_tier == "secondary"),
            "drop_count": sum(1 for item in scored_records if item["route"] == "drop"),
            "detection_summary": detection_summary,
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
            "detect": asdict(detect_config),
            "score": asdict(score_config),
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
            "detection_summary": detection_summary,
            "backtest": backtest_summary,
            "plots": plot_summary,
            "strategy_book_assessment": strategy_book_assessment,
            "assumptions": {
                "fee_bps_round_trip": backtest_config.fee_bps_round_trip,
                "notional_usdt": backtest_config.notional_usdt,
                "label_lookahead": score_config.label_lookahead,
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
            detection_summary=detection_summary,
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
