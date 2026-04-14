from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Any

import matplotlib
import polars as pl

from matplotlib.patches import Rectangle


matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


ROOT = Path(__file__).resolve().parent.parent
CONTRACT_DIR = ROOT / "阿布课程术语体系" / "contracts"
if str(CONTRACT_DIR) not in sys.path:
    sys.path.append(str(CONTRACT_DIR))

from quant_interface import (  # noqa: E402
    DetectorBinding,
    KeyPoint,
    OpportunityCandidate,
    OutcomeLabel,
    Overlay,
    QuantEvent,
    QuantRun,
    QuantSpec,
    RunSummary,
    TradePlan,
)


SPEC_ID = "range_edge_failed_breakout-v1-btc-5m"
CONCEPT_ID = "range_edge_failed_breakout"
TITLE_ZH = "看衰区间边线突破"
DEFAULT_STRATEGY_SEQ = "001"
DEFAULT_STRATEGY_CODE = "PA_taifei_001"
DEFAULT_STRATEGY_NAME_STD = f"{DEFAULT_STRATEGY_CODE}_{TITLE_ZH}"


@dataclass(frozen=True)
class FailedBreakoutConfig:
    atr_window: int = 20
    range_window: int = 24
    reentry_window: int = 4
    min_touches: int = 2
    touch_tolerance_atr: float = 0.18
    touch_separation_bars: int = 3
    breakout_buffer_atr: float = 0.08
    breakout_close_pos: float = 0.68
    breakout_body_ratio: float = 0.45
    reentry_buffer_atr: float = 0.06
    confirm_close_pos_max: float = 0.42
    confirm_body_ratio_min: float = 0.30
    range_height_atr_min: float = 1.00
    range_height_atr_max: float = 6.80
    range_overlap_min: float = 0.52
    range_ema_gap_atr_max: float = 0.95
    htf_magnet_window: int = 96
    htf_magnet_gap_ratio_max: float = 0.60
    max_breakout_depth_atr: float = 1.35
    target_mid_min_r: float = 1.00
    weak_confirm_close_pos_max: float = 0.48
    weak_confirm_body_ratio_max: float = 0.26
    caution_range_height_atr: float = 5.80
    caution_expansion_ratio: float = 1.18
    label_lookahead: int = 24


@dataclass(frozen=True)
class BacktestConfig:
    stop_buffer_atr: float = 0.15
    min_stop_atr: float = 0.70
    second_target_fraction: float = 0.25
    max_holding_bars: int = 24
    cooldown_bars: int = 1
    fee_bps_round_trip: float = 2.0
    notional_usdt: float = 10_000.0


def _coerce_config(params: FailedBreakoutConfig | dict[str, Any] | None) -> FailedBreakoutConfig:
    if params is None:
        return FailedBreakoutConfig()
    if isinstance(params, FailedBreakoutConfig):
        return params
    return FailedBreakoutConfig(**params)


def _coerce_backtest_config(
    params: BacktestConfig | dict[str, Any] | None,
) -> BacktestConfig:
    if params is None:
        return BacktestConfig()
    if isinstance(params, BacktestConfig):
        return params
    return BacktestConfig(**params)


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
    if path.suffix.lower() == ".parquet":
        return pl.read_parquet(path)
    return pl.read_csv(path)


def prepare_ohlcv(df: pl.DataFrame) -> pl.DataFrame:
    """统一 OHLCV 字段与时间列。实时安全，不使用未来 K 线。"""

    if "open_time" in df.columns and "timestamp" not in df.columns:
        df = df.rename({"open_time": "timestamp"})

    exprs: list[pl.Expr] = []
    if "timestamp" in df.columns:
        if df.schema["timestamp"] != pl.Datetime:
            exprs.append(pl.col("timestamp").cast(pl.Datetime("ms")))
        exprs.append(pl.col("timestamp").dt.replace_time_zone(None).alias("timestamp"))
    if "close_time" in df.columns:
        if df.schema["close_time"] != pl.Datetime:
            exprs.append(pl.col("close_time").cast(pl.Datetime("ms")))
        exprs.append(pl.col("close_time").dt.replace_time_zone(None).alias("close_time"))

    float_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "taker_buy_volume",
        "taker_buy_quote_volume",
    ]
    present_float_cols = [col for col in float_cols if col in df.columns]
    if present_float_cols:
        exprs.append(pl.col(present_float_cols).cast(pl.Float64))
    if "count" in df.columns:
        exprs.append(pl.col("count").cast(pl.Int64))

    out = df.with_columns(*exprs) if exprs else df
    if "ignore" in out.columns:
        out = out.drop("ignore")
    return out.sort("timestamp")


def _group_touch_indices(indices: list[int], separation: int) -> list[int]:
    grouped: list[int] = []
    for idx in indices:
        if not grouped or idx - grouped[-1] >= separation:
            grouped.append(idx)
    return grouped


def _compute_range_state(
    df: pl.DataFrame,
    config: FailedBreakoutConfig,
) -> dict[str, list[object]]:
    highs = [float(value) for value in df["high"].to_list()]
    lows = [float(value) for value in df["low"].to_list()]
    atrs = [float(value) if value is not None else 0.0 for value in df["atr"].to_list()]
    n = len(highs)

    columns: dict[str, list[object]] = {
        "range_window_start_idx": [None] * n,
        "range_window_end_idx": [None] * n,
        "range_high_pre": [None] * n,
        "range_low_pre": [None] * n,
        "range_mid_pre": [None] * n,
        "range_height_pre": [None] * n,
        "range_height_atr": [None] * n,
        "support_touch_count": [0] * n,
        "resistance_touch_count": [0] * n,
        "support_touch_1_idx": [None] * n,
        "support_touch_2_idx": [None] * n,
        "support_touch_3_idx": [None] * n,
        "resistance_touch_1_idx": [None] * n,
        "resistance_touch_2_idx": [None] * n,
        "resistance_touch_3_idx": [None] * n,
    }

    for current_idx in range(config.range_window, n):
        start_idx = current_idx - config.range_window
        end_idx = current_idx
        window_highs = highs[start_idx:end_idx]
        window_lows = lows[start_idx:end_idx]
        atr = max(atrs[current_idx], 1e-9)

        range_high = max(window_highs)
        range_low = min(window_lows)
        range_mid = (range_high + range_low) / 2.0
        range_height = range_high - range_low
        touch_tol = config.touch_tolerance_atr * atr

        support_candidates: list[int] = []
        resistance_candidates: list[int] = []
        for idx in range(start_idx, end_idx):
            if abs(lows[idx] - range_low) <= touch_tol:
                support_candidates.append(idx)
            if abs(highs[idx] - range_high) <= touch_tol:
                resistance_candidates.append(idx)

        support_touches = _group_touch_indices(support_candidates, config.touch_separation_bars)
        resistance_touches = _group_touch_indices(
            resistance_candidates,
            config.touch_separation_bars,
        )

        columns["range_window_start_idx"][current_idx] = start_idx
        columns["range_window_end_idx"][current_idx] = end_idx - 1
        columns["range_high_pre"][current_idx] = range_high
        columns["range_low_pre"][current_idx] = range_low
        columns["range_mid_pre"][current_idx] = range_mid
        columns["range_height_pre"][current_idx] = range_height
        columns["range_height_atr"][current_idx] = range_height / atr if atr > 0 else None
        columns["support_touch_count"][current_idx] = len(support_touches)
        columns["resistance_touch_count"][current_idx] = len(resistance_touches)

        for slot, value in enumerate(support_touches[-3:], start=1):
            columns[f"support_touch_{slot}_idx"][current_idx] = value
        for slot, value in enumerate(resistance_touches[-3:], start=1):
            columns[f"resistance_touch_{slot}_idx"][current_idx] = value

    return columns


def add_features(
    df: pl.DataFrame,
    params: FailedBreakoutConfig | dict[str, Any] | None = None,
) -> pl.DataFrame:
    config = _coerce_config(params)
    prev_close = pl.col("close").shift(1)
    prev_open = pl.col("open").shift(1)
    prev_high = pl.col("high").shift(1)
    prev_low = pl.col("low").shift(1)

    df = (
        df.with_row_index("idx")
        .with_columns(
            (pl.col("high") - pl.col("low")).alias("bar_range"),
            (pl.col("close") - pl.col("open")).abs().alias("body_size"),
            (pl.col("high") - pl.max_horizontal(pl.col("open"), pl.col("close"))).alias("upper_wick"),
            (pl.min_horizontal(pl.col("open"), pl.col("close")) - pl.col("low")).alias("lower_wick"),
            pl.max_horizontal(
                pl.col("high") - pl.col("low"),
                (pl.col("high") - prev_close).abs(),
                (pl.col("low") - prev_close).abs(),
            ).alias("true_range"),
            pl.col("close").ewm_mean(span=20, adjust=False).alias("ema20"),
            pl.col("close").ewm_mean(span=50, adjust=False).alias("ema50"),
            pl.col("close").ewm_mean(span=200, adjust=False).alias("ema200"),
        )
        .with_columns(
            pl.col("true_range").rolling_mean(window_size=config.atr_window).alias("atr"),
            pl.when(pl.col("bar_range") > 0)
            .then((pl.col("close") - pl.col("low")) / pl.col("bar_range"))
            .otherwise(0.5)
            .alias("close_pos"),
            pl.when(pl.col("bar_range") > 0)
            .then(pl.col("body_size") / pl.col("bar_range"))
            .otherwise(0.0)
            .alias("body_ratio"),
            (
                ((pl.col("low") <= prev_high) & (pl.col("high") >= prev_low))
                .cast(pl.Float64)
                .alias("overlap_with_prev")
            ),
            pl.when(pl.col("bar_range") > 0)
            .then(pl.col("upper_wick") / pl.col("bar_range"))
            .otherwise(0.0)
            .alias("upper_wick_ratio"),
            pl.when(pl.col("bar_range") > 0)
            .then(pl.col("lower_wick") / pl.col("bar_range"))
            .otherwise(0.0)
            .alias("lower_wick_ratio"),
        )
        .with_columns(
            pl.col("overlap_with_prev")
            .rolling_mean(window_size=config.range_window)
            .alias("overlap_ratio"),
            pl.when(pl.col("atr") > 0)
            .then((pl.col("ema20") - pl.col("ema50")).abs() / pl.col("atr"))
            .otherwise(None)
            .alias("ema_gap_atr"),
            pl.max_horizontal(pl.col("high") - pl.col("close"), pl.col("high") - pl.col("open"))
            .alias("rejection_from_high"),
            (
                (pl.col("close") < pl.col("open"))
                & (prev_close > prev_open)
                & (pl.col("open") >= prev_close)
                & (pl.col("close") <= prev_open)
            ).alias("bearish_engulf"),
            (
                (pl.col("close") > pl.col("open"))
                & (pl.col("close_pos") >= 0.65)
                & (pl.col("body_ratio") >= 0.45)
            )
            .cast(pl.Int64)
            .rolling_sum(window_size=3)
            .alias("recent_bull_pressure_3"),
            pl.col("high")
            .rolling_max(window_size=config.htf_magnet_window)
            .alias("htf_high"),
            pl.col("low")
            .rolling_min(window_size=config.htf_magnet_window)
            .alias("htf_low"),
        )
        .with_columns(
            ((pl.col("htf_high") + pl.col("htf_low")) / 2.0).alias("htf_mid"),
            pl.when(pl.col("atr") > 0)
            .then((pl.col("ema200") - pl.col("close")).abs() / pl.col("atr"))
            .otherwise(None)
            .alias("ema200_gap_atr"),
        )
    )

    state_cols = _compute_range_state(df, config=config)
    df = df.with_columns(
        *[
            pl.Series(name=column_name, values=values)
            for column_name, values in state_cols.items()
        ]
    )

    return (
        df.with_columns(
            (
                (pl.col("range_height_atr") >= config.range_height_atr_min)
                & (pl.col("range_height_atr") <= config.range_height_atr_max)
                & (pl.col("overlap_ratio") >= config.range_overlap_min)
                & (pl.col("ema_gap_atr") <= config.range_ema_gap_atr_max)
            ).alias("range_context"),
            pl.when(
                (pl.col("range_high_pre").is_not_null())
                & (pl.col("idx") >= max(config.range_window, config.htf_magnet_window))
            )
            .then(pl.lit("complete"))
            .otherwise(pl.lit("degraded"))
            .alias("snapshot_status"),
            pl.when(pl.col("range_height_pre").shift(4) > 0)
            .then(pl.col("range_height_pre") / pl.col("range_height_pre").shift(4))
            .otherwise(None)
            .alias("range_expansion_ratio"),
        )
        .with_columns(
            (
                (pl.col("range_high_pre").is_not_null())
                & (pl.col("resistance_touch_count") >= config.min_touches)
            ).alias("range_boundary_valid"),
            (
                (pl.col("close") < pl.col("open"))
                & (pl.col("close_pos") <= config.confirm_close_pos_max)
                & (pl.col("body_ratio") >= config.confirm_body_ratio_min)
            )
            .alias("bearish_confirm_core"),
        )
    )


def _scan_failed_breakouts(
    df: pl.DataFrame,
    params: FailedBreakoutConfig | dict[str, Any] | None = None,
) -> dict[str, list[object]]:
    config = _coerce_config(params)
    data = {
        "open": [float(value) for value in df["open"].to_list()],
        "high": [float(value) for value in df["high"].to_list()],
        "low": [float(value) for value in df["low"].to_list()],
        "close": [float(value) for value in df["close"].to_list()],
        "atr": [float(value) if value is not None else 0.0 for value in df["atr"].to_list()],
        "close_pos": [float(value) if value is not None else 0.5 for value in df["close_pos"].to_list()],
        "body_ratio": [float(value) if value is not None else 0.0 for value in df["body_ratio"].to_list()],
        "upper_wick_ratio": [
            float(value) if value is not None else 0.0
            for value in df["upper_wick_ratio"].to_list()
        ],
        "range_high_pre": [value for value in df["range_high_pre"].to_list()],
        "range_context": [bool(value) if value is not None else False for value in df["range_context"].to_list()],
        "range_boundary_valid": [
            bool(value) if value is not None else False
            for value in df["range_boundary_valid"].to_list()
        ],
        "snapshot_status": [str(value) for value in df["snapshot_status"].to_list()],
        "bearish_confirm_core": [
            bool(value) if value is not None else False
            for value in df["bearish_confirm_core"].to_list()
        ],
        "bearish_engulf": [
            bool(value) if value is not None else False
            for value in df["bearish_engulf"].to_list()
        ],
    }
    n = len(data["close"])

    columns: dict[str, list[object]] = {
        "upside_breakout_bar": [False] * n,
        "signal_bar": [False] * n,
        "breakout_idx": [None] * n,
        "reentry_idx": [None] * n,
        "signal_idx": [None] * n,
        "reentry_latency": [None] * n,
        "breakout_depth_atr": [None] * n,
        "confirm_on_reentry": [False] * n,
    }

    for breakout_idx in range(config.range_window, n):
        range_high = data["range_high_pre"][breakout_idx]
        if range_high is None:
            continue
        if not data["range_boundary_valid"][breakout_idx]:
            continue

        atr = max(data["atr"][breakout_idx], 1e-9)
        breakout_bar = (
            data["snapshot_status"][breakout_idx] == "complete"
            and data["range_context"][breakout_idx]
            and data["close"][breakout_idx] > float(range_high) + config.breakout_buffer_atr * atr
            and data["close_pos"][breakout_idx] >= config.breakout_close_pos
            and data["body_ratio"][breakout_idx] >= config.breakout_body_ratio
        )
        if not breakout_bar:
            continue

        columns["upside_breakout_bar"][breakout_idx] = True
        max_excess_high = data["high"][breakout_idx]

        for offset in range(1, config.reentry_window + 1):
            reentry_idx = breakout_idx + offset
            if reentry_idx >= n:
                break

            max_excess_high = max(max_excess_high, data["high"][reentry_idx])
            reentry_atr = max(data["atr"][reentry_idx], 1e-9)
            reentered = (
                data["close"][reentry_idx]
                <= float(range_high) - config.reentry_buffer_atr * reentry_atr
            )
            if not reentered:
                continue

            confirm_idx: int | None = None
            if data["bearish_confirm_core"][reentry_idx] or data["bearish_engulf"][reentry_idx]:
                confirm_idx = reentry_idx
            elif reentry_idx + 1 < n:
                if data["bearish_confirm_core"][reentry_idx + 1] or data["bearish_engulf"][reentry_idx + 1]:
                    confirm_idx = reentry_idx + 1

            if confirm_idx is None:
                continue

            columns["signal_bar"][confirm_idx] = True
            columns["breakout_idx"][confirm_idx] = breakout_idx
            columns["reentry_idx"][confirm_idx] = reentry_idx
            columns["signal_idx"][confirm_idx] = confirm_idx
            columns["reentry_latency"][confirm_idx] = reentry_idx - breakout_idx
            columns["breakout_depth_atr"][confirm_idx] = (
                (max_excess_high - float(range_high)) / atr if atr > 0 else None
            )
            columns["confirm_on_reentry"][confirm_idx] = confirm_idx == reentry_idx
            break

    return columns


def detect(
    df: pl.DataFrame,
    params: FailedBreakoutConfig | dict[str, Any] | None = None,
) -> pl.DataFrame:
    """只做实时可用的规则检测，不使用未来 K 线。"""

    config = _coerce_config(params)
    featured = add_features(df, params=config)
    scan_cols = _scan_failed_breakouts(featured, params=config)
    return featured.with_columns(
        *[
            pl.Series(name=column_name, values=values)
            for column_name, values in scan_cols.items()
        ]
    )


def _clip_score(value: int) -> int:
    return max(0, min(100, value))


def _score_band(score: int) -> str:
    if score >= 85:
        return "A"
    if score >= 75:
        return "B"
    return "C"


def _route(score: int) -> str:
    if score >= 75:
        return "primary"
    if score >= 60:
        return "secondary"
    return "drop"


def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _stable_id(*parts: object) -> str:
    digest = hashlib.sha1(
        "|".join("" if part is None else str(part) for part in parts).encode("utf-8")
    ).hexdigest()
    return digest[:20]


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _make_key_point(
    point_id: str,
    role: str,
    timestamp: Any,
    price: float,
    meta: dict[str, Any] | None = None,
) -> KeyPoint:
    return KeyPoint(
        point_id=point_id,
        role=role,
        timestamp=_iso(timestamp),
        price=float(price),
        meta=meta or {},
    )


def _point_display_label(role: str) -> str:
    mapping = {
        "resistance_touch_1": "R1",
        "resistance_touch_2": "R2",
        "resistance_touch_3": "R3",
        "breakout": "上破",
        "reentry": "回区",
        "signal": "确认",
        "target_mid": "中轴",
    }
    return mapping.get(role, role)


def _make_segment_overlay(
    overlay_id: str,
    role: str,
    start: KeyPoint,
    end: KeyPoint,
    color: str,
    dash: str | None = None,
) -> Overlay:
    style: dict[str, Any] = {"color": color, "width": 2}
    if dash:
        style["dash"] = dash
    return Overlay(
        overlay_id=overlay_id,
        kind="segment",
        role=role,
        points=[start, end],
        style=style,
        meta={},
    )


def _make_zone_overlay(
    overlay_id: str,
    role: str,
    start_time: Any,
    end_time: Any,
    low: float,
    high: float,
    color: str,
) -> Overlay:
    return Overlay(
        overlay_id=overlay_id,
        kind="zone",
        role=role,
        points=[
            _make_key_point(f"{overlay_id}-start", "zone_start", start_time, low),
            _make_key_point(f"{overlay_id}-end", "zone_end", end_time, high),
        ],
        style={"fillcolor": color, "opacity": 0.15},
        meta={},
    )


def _index_rows(df: pl.DataFrame) -> dict[int, dict[str, Any]]:
    return {
        int(row["idx"]): row
        for row in df.to_dicts()
    }


def score(
    df: pl.DataFrame,
    params: FailedBreakoutConfig | dict[str, Any] | None = None,
    backtest_params: BacktestConfig | dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """把成立条件、加强条件、谨慎信号组织成结构化评分。"""

    config = _coerce_config(params)
    backtest_config = _coerce_backtest_config(backtest_params)
    rows_by_idx = _index_rows(df)
    scored: list[dict[str, Any]] = []

    for signal_row in df.filter(pl.col("signal_bar")).to_dicts():
        signal_idx = int(signal_row["idx"])
        breakout_idx = int(signal_row["breakout_idx"])
        reentry_idx = int(signal_row["reentry_idx"])
        breakout_row = rows_by_idx[breakout_idx]
        reentry_row = rows_by_idx[reentry_idx]

        entry_price = float(signal_row["close"])
        range_high = float(breakout_row["range_high_pre"])
        range_low = float(breakout_row["range_low_pre"])
        range_mid = float(breakout_row["range_mid_pre"])
        range_height = float(breakout_row["range_height_pre"])
        signal_high = float(signal_row["high"])
        breakout_high = float(breakout_row["high"])
        atr = max(float(signal_row["atr"]), 1e-9)

        stop_floor = entry_price + backtest_config.min_stop_atr * atr
        stop_price = max(
            range_high,
            signal_high,
            breakout_high,
        ) + backtest_config.stop_buffer_atr * atr
        stop_price = max(stop_price, stop_floor)
        risk_per_unit = stop_price - entry_price
        target_mid = range_mid
        target_second = max(
            range_low + range_height * backtest_config.second_target_fraction,
            range_low,
        )
        space_to_mid_r = (entry_price - target_mid) / risk_per_unit if risk_per_unit > 0 else 0.0

        hard_gate_checks: list[tuple[str, bool]] = [
            ("snapshot 完整", str(breakout_row["snapshot_status"]) == "complete"),
            ("处于横向区间上下文", bool(breakout_row["range_context"])),
            (
                "被测试边线至少 2 次分离触碰",
                int(breakout_row["resistance_touch_count"]) >= config.min_touches,
            ),
            (
                "突破后 4 根内重新回到区间内部",
                signal_row["reentry_idx"] is not None
                and int(signal_row["reentry_latency"]) <= config.reentry_window,
            ),
            (
                "回归当根或下一根出现反向确认 K 线",
                bool(signal_row["signal_bar"]),
            ),
            ("只在确认 bar close 产出", signal_idx >= reentry_idx),
            ("入场到止损风险为正", risk_per_unit > 0),
        ]
        hard_gates = [label for label, passed in hard_gate_checks if passed]
        hard_gate_pass = all(passed for _, passed in hard_gate_checks)

        breakout_depth_atr = float(signal_row["breakout_depth_atr"] or 0.0)
        htf_mid = breakout_row.get("htf_mid")
        near_higher_tf_magnet = False
        if htf_mid is not None:
            near_higher_tf_magnet = (
                abs(float(htf_mid) - range_high) / max(range_height, atr)
                <= config.htf_magnet_gap_ratio_max
            )

        rejection_or_engulf = (
            float(breakout_row["upper_wick_ratio"] or 0.0) >= 0.35
            or bool(reentry_row["bearish_engulf"])
            or bool(signal_row["bearish_engulf"])
        )

        strengtheners: list[str] = []
        if int(breakout_row["resistance_touch_count"]) >= 3:
            strengtheners.append("边线有 3 次及以上有效触碰")
        if int(signal_row["reentry_latency"]) <= 2:
            strengtheners.append("假突破后 2 根内快速回归区间")
        if rejection_or_engulf:
            strengtheners.append("突破端出现明显拒绝尾巴或反包")
        if space_to_mid_r >= config.target_mid_min_r:
            strengtheners.append("入场到区间中轴至少有 1R 空间")
        if near_higher_tf_magnet:
            strengtheners.append("假突破发生在更大级别磁体附近")

        middle_third_low = range_low + range_height / 3.0
        middle_third_high = range_high - range_height / 3.0

        cautions: list[str] = []
        if middle_third_low <= entry_price <= middle_third_high:
            cautions.append("入场时价格已进入区间中间三分之一")
        if (
            breakout_depth_atr >= config.max_breakout_depth_atr
            or int(signal_row["reentry_latency"]) >= config.reentry_window
        ):
            cautions.append("假突破过深，回归已明显迟滞")
        if (
            float(signal_row["body_ratio"] or 0.0) <= config.weak_confirm_body_ratio_max
            or float(signal_row["close_pos"] or 0.5) >= config.weak_confirm_close_pos_max
        ):
            cautions.append("确认 K 实体弱、收盘位置差")
        if int(signal_row["recent_bull_pressure_3"] or 0) >= 2:
            cautions.append("最近 3 根出现反方向强压力序列")
        if (
            float(breakout_row["range_height_atr"] or 0.0) >= config.caution_range_height_atr
            or float(breakout_row["range_expansion_ratio"] or 1.0) >= config.caution_expansion_ratio
        ):
            cautions.append("区间过宽或明显扩张，失去均衡回归特征")

        score_value = _clip_score(70 + 5 * len(strengtheners) - 5 * len(cautions))
        route = _route(score_value)
        score_band = _score_band(score_value)

        summary = (
            f"{TITLE_ZH}：阻力触碰 {int(breakout_row['resistance_touch_count'])} 次，"
            f"上破后 {int(signal_row['reentry_latency'])} 根回归区间，"
            f"确认 K 于 {_iso(signal_row['timestamp'])} 收盘给出。"
            f"评分 {score_value}，分档 {score_band}，路由 {route}。"
        )

        scored.append(
            {
                "event_id": f"evt_{_stable_id(SPEC_ID, signal_row['timestamp'], 'short')}",
                "candidate_id": f"cand_{_stable_id(SPEC_ID, signal_row['timestamp'], 'short')}",
                "signal_idx": signal_idx,
                "breakout_idx": breakout_idx,
                "reentry_idx": reentry_idx,
                "side": "short",
                "snapshot_status": str(breakout_row["snapshot_status"]),
                "hard_gate_pass": hard_gate_pass,
                "hard_gates": hard_gates,
                "strengtheners": strengtheners,
                "cautions": cautions,
                "score": score_value,
                "score_band": score_band,
                "route": route,
                "entry_price": entry_price,
                "entry_zone_low": min(entry_price, range_high - 0.05 * atr),
                "entry_zone_high": max(entry_price, signal_high),
                "stop_price": stop_price,
                "take_profit_prices": [target_mid, target_second],
                "invalidation_price": stop_price,
                "space_to_mid_r": space_to_mid_r,
                "range_high": range_high,
                "range_low": range_low,
                "range_mid": range_mid,
                "range_height": range_height,
                "range_height_atr": float(breakout_row["range_height_atr"] or 0.0),
                "breakout_depth_atr": breakout_depth_atr,
                "signal_bar_time": _iso(signal_row["timestamp"]),
                "analysis_summary": summary,
                "tags": [
                    CONCEPT_ID,
                    DEFAULT_STRATEGY_CODE,
                    "range",
                    "failed_breakout",
                    "reversion",
                    "short",
                    route,
                    score_band.lower(),
                ],
            }
        )

    return scored


def _build_points_and_overlays(
    rows_by_idx: dict[int, dict[str, Any]],
    record: dict[str, Any],
) -> tuple[list[KeyPoint], list[Overlay]]:
    breakout_row = rows_by_idx[record["breakout_idx"]]
    reentry_row = rows_by_idx[record["reentry_idx"]]
    signal_row = rows_by_idx[record["signal_idx"]]
    start_row = rows_by_idx[int(breakout_row["range_window_start_idx"])]

    key_points: list[KeyPoint] = []
    for slot in range(1, 4):
        resistance_idx = breakout_row.get(f"resistance_touch_{slot}_idx")
        if resistance_idx is None:
            continue
        resistance_row = rows_by_idx[int(resistance_idx)]
        key_points.append(
            _make_key_point(
                f"{record['event_id']}-r{slot}",
                f"resistance_touch_{slot}",
                resistance_row["timestamp"],
                float(resistance_row["high"]),
            )
        )

    key_points.extend(
        [
            _make_key_point(
                f"{record['event_id']}-breakout",
                "breakout",
                breakout_row["timestamp"],
                float(breakout_row["high"]),
            ),
            _make_key_point(
                f"{record['event_id']}-reentry",
                "reentry",
                reentry_row["timestamp"],
                float(reentry_row["close"]),
            ),
            _make_key_point(
                f"{record['event_id']}-signal",
                "signal",
                signal_row["timestamp"],
                float(signal_row["close"]),
            ),
            _make_key_point(
                f"{record['event_id']}-mid",
                "target_mid",
                signal_row["timestamp"],
                float(record["range_mid"]),
            ),
        ]
    )

    resistance_start = _make_key_point(
        f"{record['event_id']}-res-start",
        "resistance_start",
        start_row["timestamp"],
        float(record["range_high"]),
    )
    resistance_end = _make_key_point(
        f"{record['event_id']}-res-end",
        "resistance_end",
        signal_row["timestamp"],
        float(record["range_high"]),
    )
    support_start = _make_key_point(
        f"{record['event_id']}-sup-start",
        "support_start",
        start_row["timestamp"],
        float(record["range_low"]),
    )
    support_end = _make_key_point(
        f"{record['event_id']}-sup-end",
        "support_end",
        signal_row["timestamp"],
        float(record["range_low"]),
    )
    stop_start = _make_key_point(
        f"{record['event_id']}-stop-start",
        "stop_start",
        signal_row["timestamp"],
        float(record["stop_price"]),
    )
    stop_end = _make_key_point(
        f"{record['event_id']}-stop-end",
        "stop_end",
        signal_row["timestamp"],
        float(record["stop_price"]),
    )

    overlays = [
        _make_segment_overlay(
            f"{record['event_id']}-res",
            "range_resistance",
            resistance_start,
            resistance_end,
            "#b9770e",
        ),
        _make_segment_overlay(
            f"{record['event_id']}-sup",
            "range_support",
            support_start,
            support_end,
            "#2874a6",
        ),
        _make_segment_overlay(
            f"{record['event_id']}-stop",
            "stop_line",
            stop_start,
            stop_end,
            "#cb4335",
            dash="dash",
        ),
        _make_zone_overlay(
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

    rows_by_idx = _index_rows(df)
    scored_records: list[dict[str, Any]] = run_ctx["scored_records"]
    strategy_meta: dict[str, str] = run_ctx.get("strategy_meta", _strategy_meta())
    candidates: list[OpportunityCandidate] = []
    events: list[QuantEvent] = []
    dedupe_keys: set[tuple[str, str, str, str, str]] = set()

    for record in scored_records:
        key_points, overlays = _build_points_and_overlays(rows_by_idx, record)
        signal_row = rows_by_idx[record["signal_idx"]]

        trade_plan = TradePlan(
            direction="bearish",
            entry_trigger="确认 K 收盘后做空或小幅回抽进场",
            entry_price=float(record["entry_price"]),
            stop_price=float(record["stop_price"]),
            target_prices=[float(price) for price in record["take_profit_prices"]],
            timeout_bars=run_ctx["backtest_config"].max_holding_bars,
            invalidation_rule="重新站回假突破高点上方",
            tags=["mean_reversion", "failed_breakout"],
        )

        event = QuantEvent(
            event_id=record["event_id"],
            run_id=run_ctx["run_id"],
            concept_id=CONCEPT_ID,
            spec_id=SPEC_ID,
            symbol=run_ctx["symbol"],
            timeframe=run_ctx["timeframe"],
            direction="bearish",
            stage="confirmed",
            detected_at=record["signal_bar_time"],
            start_time=_iso(rows_by_idx[record["breakout_idx"]]["timestamp"]),
            end_time=record["signal_bar_time"],
            confidence=round(record["score"] / 100.0, 4),
            score=float(record["score"]),
            family="range_failed_breakout",
            features={
                "hard_gate_pass": record["hard_gate_pass"],
                "hard_gates": record["hard_gates"],
                "strengtheners": record["strengtheners"],
                "cautions": record["cautions"],
                "route": record["route"],
                "range_height_atr": record["range_height_atr"],
                "breakout_depth_atr": record["breakout_depth_atr"],
                "space_to_mid_r": round(record["space_to_mid_r"], 4),
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
                "reentry_idx": record["reentry_idx"],
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
                side="short",
                setup_type="failed_breakout_reversion",
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
    params: FailedBreakoutConfig | dict[str, Any] | None = None,
    backtest_params: BacktestConfig | dict[str, Any] | None = None,
) -> list[QuantEvent]:
    """只做研究/回测事后标签，明确使用未来 K 线。"""

    config = _coerce_config(params)
    backtest_config = _coerce_backtest_config(backtest_params)
    rows_by_idx = _index_rows(df)
    highs = [float(value) for value in df["high"].to_list()]
    lows = [float(value) for value in df["low"].to_list()]
    closes = [float(value) for value in df["close"].to_list()]

    labeled: list[QuantEvent] = []
    for event in events:
        signal_idx = int(event.meta["signal_idx"])
        entry_price = float(event.trade_plan.entry_price or 0.0)
        stop_price = float(event.trade_plan.stop_price or 0.0)
        target_price = float(event.trade_plan.target_prices[0]) if event.trade_plan.target_prices else entry_price
        risk_per_unit = max(stop_price - entry_price, 1e-9)

        lookahead_end = min(len(highs) - 1, signal_idx + min(config.label_lookahead, backtest_config.max_holding_bars))
        exit_idx = lookahead_end
        exit_price = closes[lookahead_end]
        exit_reason = "时间止盈/止损"
        hit_target = False
        hit_stop = False

        for idx in range(signal_idx + 1, lookahead_end + 1):
            high = highs[idx]
            low = lows[idx]
            if high >= stop_price and low <= target_price:
                exit_idx = idx
                exit_price = stop_price
                exit_reason = "同柱先按止损"
                hit_stop = True
                break
            if high >= stop_price:
                exit_idx = idx
                exit_price = stop_price
                exit_reason = "止损"
                hit_stop = True
                break
            if low <= target_price:
                exit_idx = idx
                exit_price = target_price
                exit_reason = "止盈"
                hit_target = True
                break

        future_high = max(highs[signal_idx + 1 : lookahead_end + 1], default=entry_price)
        future_low = min(lows[signal_idx + 1 : lookahead_end + 1], default=entry_price)
        mfe = max(0.0, (entry_price - future_low) / risk_per_unit)
        mae = max(0.0, (future_high - entry_price) / risk_per_unit)
        pnl_r = (entry_price - exit_price) / risk_per_unit

        quantity = backtest_config.notional_usdt / max(entry_price, 1e-9)
        fee_rate = (backtest_config.fee_bps_round_trip / 2.0) / 10_000.0
        gross_pnl = (entry_price - exit_price) * quantity
        fees = quantity * (entry_price + exit_price) * fee_rate
        net_pnl = gross_pnl - fees

        outcome_class = "time_exit"
        if hit_target:
            outcome_class = "target_hit"
        elif hit_stop:
            outcome_class = "stop_hit"
        elif pnl_r > 0:
            outcome_class = "time_exit_positive"
        elif pnl_r < 0:
            outcome_class = "time_exit_negative"

        labeled.append(
            replace(
                event,
                outcome=OutcomeLabel(
                    lookahead_bars=lookahead_end - signal_idx,
                    outcome_class=outcome_class,
                    hit_target=hit_target,
                    hit_stop=hit_stop,
                    mfe=round(mfe, 6),
                    mae=round(mae, 6),
                    pnl_r=round(pnl_r, 6),
                    meta={
                        "uses_future_bars": True,
                        "exit_idx": exit_idx,
                        "exit_time": _iso(rows_by_idx[exit_idx]["timestamp"]),
                        "exit_price": round(exit_price, 6),
                        "exit_reason": exit_reason,
                        "gross_pnl": round(gross_pnl, 6),
                        "net_pnl": round(net_pnl, 6),
                        "fees": round(fees, 6),
                        "holding_bars": exit_idx - signal_idx,
                    },
                ),
            )
        )

    return labeled


def build_quant_spec(
    registry_entry: dict[str, Any] | None = None,
) -> QuantSpec:
    strategy_meta = _strategy_meta(registry_entry)
    return QuantSpec(
        spec_id=SPEC_ID,
        concept_id=CONCEPT_ID,
        version="v1",
        title=strategy_meta["strategy_name_std"],
        input_columns=[
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ],
        feature_columns=[
            "atr",
            "ema20",
            "ema50",
            "ema200",
            "overlap_ratio",
            "range_high_pre",
            "range_low_pre",
            "range_mid_pre",
            "range_height_pre",
            "range_height_atr",
            "resistance_touch_count",
            "snapshot_status",
            "range_context",
            "breakout_depth_atr",
        ],
        event_columns=[
            "upside_breakout_bar",
            "signal_bar",
            "breakout_idx",
            "reentry_idx",
            "signal_idx",
            "reentry_latency",
            "score",
            "score_band",
            "route",
        ],
        required_context_ids=["trading_range", "ema_balance"],
        required_primitive_ids=["range_edge_touch", "failed_breakout", "bearish_confirmation"],
        params={
            **asdict(FailedBreakoutConfig()),
            "backtest": asdict(BacktestConfig()),
            "strategy_meta": strategy_meta,
            "registry_entry": registry_entry or {},
        },
        detector=DetectorBinding(
            module_path="strategylets.range_edge_failed_breakout",
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


def _event_to_trade_row(event: QuantEvent) -> dict[str, Any]:
    outcome = event.outcome
    if outcome is None:
        raise ValueError("event 缺少 outcome，无法生成 trade row。")

    meta = outcome.meta
    entry_price = float(event.trade_plan.entry_price or 0.0)
    stop_price = float(event.trade_plan.stop_price or 0.0)
    target_price = float(event.trade_plan.target_prices[0]) if event.trade_plan.target_prices else entry_price
    return {
        "event_id": event.event_id,
        "candidate_id": event.meta["candidate_id"],
        "signal_time": event.detected_at,
        "exit_time": meta["exit_time"],
        "side": "short",
        "score": float(event.score or 0.0),
        "score_band": event.meta["score_band"],
        "snapshot_status": event.meta["snapshot_status"],
        "entry_price": round(entry_price, 6),
        "exit_price": round(float(meta["exit_price"]), 6),
        "stop_price": round(stop_price, 6),
        "target_price": round(target_price, 6),
        "holding_bars": int(meta["holding_bars"]),
        "exit_reason": meta["exit_reason"],
        "gross_r": round(float(outcome.pnl_r or 0.0), 6),
        "mfe": round(float(outcome.mfe or 0.0), 6),
        "mae": round(float(outcome.mae or 0.0), 6),
        "gross_pnl": round(float(meta["gross_pnl"]), 6),
        "net_pnl": round(float(meta["net_pnl"]), 6),
        "fees": round(float(meta["fees"]), 6),
        "outcome_class": outcome.outcome_class,
        "route": "drop" if float(event.score or 0.0) < 60 else ("primary" if float(event.score or 0.0) >= 75 else "secondary"),
    }


def _empty_trades_df() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "event_id": pl.String,
            "candidate_id": pl.String,
            "signal_time": pl.String,
            "exit_time": pl.String,
            "side": pl.String,
            "score": pl.Float64,
            "score_band": pl.String,
            "snapshot_status": pl.String,
            "entry_price": pl.Float64,
            "exit_price": pl.Float64,
            "stop_price": pl.Float64,
            "target_price": pl.Float64,
            "holding_bars": pl.Int64,
            "exit_reason": pl.String,
            "gross_r": pl.Float64,
            "mfe": pl.Float64,
            "mae": pl.Float64,
            "gross_pnl": pl.Float64,
            "net_pnl": pl.Float64,
            "fees": pl.Float64,
            "outcome_class": pl.String,
            "route": pl.String,
        }
    )


def run_backtest(
    events: list[QuantEvent],
    backtest_params: BacktestConfig | dict[str, Any] | None = None,
) -> tuple[pl.DataFrame, dict[str, Any], pl.DataFrame]:
    backtest_config = _coerce_backtest_config(backtest_params)
    trade_rows = [
        _event_to_trade_row(event)
        for event in events
        if (
            event.outcome is not None
            and float(event.score or 0.0) >= 60
            and bool(event.features.get("hard_gate_pass"))
        )
    ]
    trades_df = pl.DataFrame(trade_rows) if trade_rows else _empty_trades_df()

    if trades_df.is_empty():
        equity_df = pl.DataFrame(
            schema={
                "trade_no": pl.Int64,
                "exit_time": pl.String,
                "net_pnl": pl.Float64,
                "gross_r": pl.Float64,
                "cumulative_net_pnl": pl.Float64,
                "cumulative_gross_r": pl.Float64,
                "nav": pl.Float64,
                "peak_nav": pl.Float64,
                "drawdown": pl.Float64,
                "drawdown_pct": pl.Float64,
            }
        )
        return (
            trades_df,
            {
                "trade_count": 0,
                "win_rate": 0.0,
                "avg_r": 0.0,
                "total_r": 0.0,
                "avg_net_pnl": 0.0,
                "total_net_pnl": 0.0,
                "max_drawdown": 0.0,
                "profit_factor": None,
                "ending_nav": 1.0,
                "max_drawdown_pct": 0.0,
            },
            equity_df,
        )

    trades_df = trades_df.sort("exit_time")
    wins = trades_df.filter(pl.col("net_pnl") > 0)
    losses = trades_df.filter(pl.col("net_pnl") < 0)
    gross_profit = float(wins["net_pnl"].sum()) if wins.height else 0.0
    gross_loss = abs(float(losses["net_pnl"].sum())) if losses.height else 0.0

    equity_df = (
        trades_df.with_row_index("trade_no", offset=1)
        .with_columns(
            pl.col("net_pnl").cum_sum().alias("cumulative_net_pnl"),
            pl.col("gross_r").cum_sum().alias("cumulative_gross_r"),
        )
        .with_columns(
            (
                1.0 + pl.col("cumulative_net_pnl") / backtest_config.notional_usdt
            ).alias("nav")
        )
        .with_columns(pl.col("nav").cum_max().alias("peak_nav"))
        .with_columns(
            (pl.col("nav") - pl.col("peak_nav")).alias("drawdown"),
            pl.when(pl.col("peak_nav") > 0)
            .then(pl.col("nav") / pl.col("peak_nav") - 1.0)
            .otherwise(0.0)
            .alias("drawdown_pct"),
        )
        .select(
            "trade_no",
            "exit_time",
            "net_pnl",
            "gross_r",
            "cumulative_net_pnl",
            "cumulative_gross_r",
            "nav",
            "peak_nav",
            "drawdown",
            "drawdown_pct",
        )
    )

    summary = {
        "trade_count": trades_df.height,
        "win_rate": round((wins.height / trades_df.height) * 100.0, 4),
        "avg_r": round(float(trades_df["gross_r"].mean() or 0.0), 6),
        "total_r": round(float(trades_df["gross_r"].sum() or 0.0), 6),
        "avg_net_pnl": round(float(trades_df["net_pnl"].mean() or 0.0), 6),
        "total_net_pnl": round(float(trades_df["net_pnl"].sum() or 0.0), 6),
        "max_drawdown": round(float(equity_df["drawdown"].min() or 0.0), 6),
        "profit_factor": round(gross_profit / gross_loss, 6) if gross_loss else None,
        "ending_nav": round(float(equity_df["nav"].tail(1).item()), 6),
        "max_drawdown_pct": round(float(equity_df["drawdown_pct"].min() or 0.0) * 100.0, 6),
    }
    return trades_df, summary, equity_df


def _dataclass_to_dict(item: Any) -> dict[str, Any]:
    return asdict(item)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, default=_json_default))
            handle.write("\n")


def _candidate_parquet_rows(candidates: list[OpportunityCandidate]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        row = _dataclass_to_dict(candidate)
        rows.append(
            {
                "candidate_id": row["candidate_id"],
                "run_id": row["run_id"],
                "spec_id": row["spec_id"],
                "concept_id": row["concept_id"],
                "symbol": row["symbol"],
                "timeframe": row["timeframe"],
                "signal_bar_time": row["signal_bar_time"],
                "side": row["side"],
                "setup_type": row["setup_type"],
                "entry_type": row["entry_type"],
                "entry_zone_low": row["entry_zone_low"],
                "entry_zone_high": row["entry_zone_high"],
                "stop_price": row["stop_price"],
                "invalidation_price": row["invalidation_price"],
                "score": row["score"],
                "score_band": row["score_band"],
                "candidate_tier": row["candidate_tier"],
                "snapshot_status": row["snapshot_status"],
                "realtime_safe": row["realtime_safe"],
                "analysis_summary": row["analysis_summary"],
                "take_profit_prices_json": json.dumps(row["take_profit_prices"], ensure_ascii=False),
                "hard_gates_json": json.dumps(row["hard_gates"], ensure_ascii=False),
                "strengtheners_json": json.dumps(row["strengtheners"], ensure_ascii=False),
                "cautions_json": json.dumps(row["cautions"], ensure_ascii=False),
                "key_points_json": json.dumps(row["key_points"], ensure_ascii=False, default=_json_default),
                "overlays_json": json.dumps(row["overlays"], ensure_ascii=False, default=_json_default),
                "tags_json": json.dumps(row["tags"], ensure_ascii=False),
            }
        )
    return rows


def _attach_scored_columns(
    df: pl.DataFrame,
    scored_records: list[dict[str, Any]],
) -> pl.DataFrame:
    if not scored_records:
        return df.with_columns(
            pl.lit(None).cast(pl.String).alias("event_id"),
            pl.lit(None).cast(pl.String).alias("candidate_id"),
            pl.lit(None).cast(pl.Int64).alias("score"),
            pl.lit(None).cast(pl.String).alias("score_band"),
            pl.lit(None).cast(pl.String).alias("route"),
            pl.lit(None).cast(pl.Boolean).alias("hard_gate_pass"),
        )

    scored_df = pl.DataFrame(
        [
            {
                "signal_idx": int(item["signal_idx"]),
                "event_id": item["event_id"],
                "candidate_id": item["candidate_id"],
                "score": int(item["score"]),
                "score_band": item["score_band"],
                "route": item["route"],
                "hard_gate_pass": bool(item["hard_gate_pass"]),
            }
            for item in scored_records
        ]
    )
    return df.join(
        scored_df,
        left_on="idx",
        right_on="signal_idx",
        how="left",
    )


def _pick_example_events(events: list[QuantEvent], example_count: int) -> list[QuantEvent]:
    candidate_events = [
        event
        for event in events
        if float(event.score or 0.0) >= 60 and bool(event.features.get("hard_gate_pass"))
    ]
    if len(candidate_events) <= example_count:
        return candidate_events
    sorted_events = sorted(candidate_events, key=lambda item: (float(item.score or 0.0), item.detected_at), reverse=True)
    picked: list[QuantEvent] = []
    for bucket_idx in range(example_count):
        source_idx = math.floor(bucket_idx * len(sorted_events) / example_count)
        picked.append(sorted_events[source_idx])
    picked = sorted({event.event_id: event for event in picked}.values(), key=lambda item: item.detected_at)
    return picked[:example_count]


def _plot_event(
    df: pl.DataFrame,
    event: QuantEvent,
    output_path: Path,
) -> None:
    strategy_name_std = str(event.meta.get("strategy_name_std", DEFAULT_STRATEGY_NAME_STD))
    signal_idx = int(event.meta["signal_idx"])
    start_idx = max(0, signal_idx - 80)
    end_idx = min(df.height - 1, signal_idx + 40)
    plot_df = df.filter((pl.col("idx") >= start_idx) & (pl.col("idx") <= end_idx))

    timestamps = [
        value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        for value in plot_df["timestamp"].to_list()
    ]
    x_values = mdates.date2num(timestamps)
    bar_width = 0.003
    if len(x_values) > 1:
        diffs = [max(x_values[i] - x_values[i - 1], 1e-6) for i in range(1, len(x_values))]
        bar_width = min(diffs) * 0.72

    fig, ax = plt.subplots(figsize=(16, 8))
    for x, open_price, high_price, low_price, close_price in zip(
        x_values,
        plot_df["open"].to_list(),
        plot_df["high"].to_list(),
        plot_df["low"].to_list(),
        plot_df["close"].to_list(),
        strict=False,
    ):
        color = "#1d8348" if close_price >= open_price else "#b03a2e"
        ax.vlines(x, low_price, high_price, color=color, linewidth=1.2, alpha=0.85)
        body_low = min(open_price, close_price)
        body_height = max(abs(close_price - open_price), 0.01)
        ax.add_patch(
            Rectangle(
                (x - bar_width / 2, body_low),
                bar_width,
                body_height,
                facecolor=color,
                edgecolor=color,
                linewidth=1.0,
                alpha=0.85,
            )
        )

    for overlay in event.overlays:
        if overlay.kind == "segment" and len(overlay.points) >= 2:
            overlay_times = [
                datetime.fromisoformat(overlay.points[0].timestamp),
                datetime.fromisoformat(overlay.points[1].timestamp),
            ]
            ax.plot(
                mdates.date2num(overlay_times),
                [overlay.points[0].price, overlay.points[1].price],
                color=overlay.style.get("color", "#34495e"),
                linewidth=overlay.style.get("width", 2),
                linestyle="--" if overlay.style.get("dash") else "-",
            )
        elif overlay.kind == "zone" and len(overlay.points) >= 2:
            start = datetime.fromisoformat(overlay.points[0].timestamp)
            end = datetime.fromisoformat(overlay.points[1].timestamp)
            x0 = mdates.date2num(start) - bar_width / 2
            x1 = mdates.date2num(end) + bar_width / 2
            y0 = min(overlay.points[0].price, overlay.points[1].price)
            y1 = max(overlay.points[0].price, overlay.points[1].price)
            ax.add_patch(
                Rectangle(
                    (x0, y0),
                    max(x1 - x0, bar_width),
                    max(y1 - y0, 0.01),
                    facecolor=overlay.style.get("fillcolor", "#f5b041"),
                    edgecolor=overlay.style.get("fillcolor", "#f5b041"),
                    alpha=overlay.style.get("opacity", 0.15),
                )
            )

    offsets = [(0, 14), (0, -18), (18, 12), (-18, 12), (18, -18), (-18, -18)]
    for idx, point in enumerate(event.key_points):
        point_time = datetime.fromisoformat(point.timestamp)
        point_x = mdates.date2num(point_time)
        ax.scatter(point_x, point.price, s=35, color="#34495e", zorder=4)
        dx, dy = offsets[idx % len(offsets)]
        ax.annotate(
            _point_display_label(point.role),
            xy=(point_x, point.price),
            xytext=(dx, dy),
            textcoords="offset points",
            ha="center",
            va="bottom" if dy >= 0 else "top",
            fontsize=8.5,
            color="#1f2328",
            bbox={
                "boxstyle": "round,pad=0.18",
                "facecolor": "white",
                "edgecolor": "#d0d7de",
                "alpha": 0.9,
            },
            arrowprops={
                "arrowstyle": "-",
                "color": "#7f8c8d",
                "linewidth": 0.8,
                "alpha": 0.7,
            },
        )

    trade_plan = event.trade_plan
    summary_lines = [
        f"方向: {'做空' if trade_plan and trade_plan.direction == 'bearish' else event.direction}",
        f"评分: {float(event.score or 0.0):.0f} / {event.meta.get('score_band', '-')}",
        f"入场区: {float(event.meta.get('entry_zone_low', 0.0)):.2f} ~ {float(event.meta.get('entry_zone_high', 0.0)):.2f}",
        f"止损: {float(trade_plan.stop_price if trade_plan and trade_plan.stop_price is not None else 0.0):.2f}",
        f"目标1: {float(trade_plan.target_prices[0] if trade_plan and trade_plan.target_prices else 0.0):.2f}",
    ]
    if event.outcome is not None:
        summary_lines.append(f"结果: {event.outcome.outcome_class}")
        summary_lines.append(f"R: {float(event.outcome.pnl_r or 0.0):.2f}")
    fig.text(
        0.84,
        0.86,
        "\n".join(summary_lines),
        ha="left",
        va="top",
        fontsize=9,
        color="#1f2328",
        bbox={
            "boxstyle": "round,pad=0.35",
            "facecolor": "#fffdf8",
            "edgecolor": "#d8ccbb",
            "alpha": 0.96,
        },
    )

    ax.set_title(f"{strategy_name_std} | {event.detected_at}")
    ax.set_ylabel("价格")
    ax.set_xlabel("时间")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    ax.grid(True, linestyle="--", alpha=0.25)
    fig.autofmt_xdate()
    fig.tight_layout(rect=[0, 0, 0.82, 1])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_equity_curve(equity_df: pl.DataFrame, output_dir: Path) -> dict[str, str]:
    plots_dir = output_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    png_path = plots_dir / "equity-curve.png"
    times = [
        datetime.fromisoformat(str(value))
        for value in (equity_df["exit_time"].to_list() if equity_df.height else [])
    ]
    nav = equity_df["nav"].to_list() if equity_df.height else []
    drawdown_pct = [
        value * 100.0 for value in (equity_df["drawdown_pct"].to_list() if equity_df.height else [])
    ]

    fig, (ax_nav, ax_dd) = plt.subplots(
        2,
        1,
        figsize=(14, 8),
        sharex=True,
        gridspec_kw={"height_ratios": [2.2, 1]},
    )
    ax_nav.plot(times, nav, color="#7d4f35", linewidth=2.2, marker="o", markersize=3)
    ax_nav.set_title("净值曲线")
    ax_nav.set_ylabel("NAV")
    ax_nav.grid(True, linestyle="--", alpha=0.25)

    ax_dd.fill_between(times, drawdown_pct, 0, color="#c0392b", alpha=0.18)
    ax_dd.plot(times, drawdown_pct, color="#c0392b", linewidth=1.6)
    ax_dd.set_ylabel("DD %")
    ax_dd.grid(True, linestyle="--", alpha=0.25)
    ax_dd.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))

    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(png_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return {
        "equity_curve_png": str(png_path.resolve()),
    }


def plot_outputs(
    df: pl.DataFrame,
    events: list[QuantEvent],
    equity_df: pl.DataFrame,
    output_dir: Path,
    example_count: int = 6,
) -> dict[str, Any]:
    plots_dir = output_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    picked_events = _pick_example_events(events, example_count=example_count)
    example_paths: list[str] = []
    for idx, event in enumerate(picked_events, start=1):
        output_path = plots_dir / f"example-{idx:03d}.png"
        _plot_event(df, event, output_path)
        example_paths.append(str(output_path.resolve()))

    payload = {
        "example_count": len(example_paths),
        "example_paths": example_paths,
    }
    if equity_df.height:
        payload.update(_plot_equity_curve(equity_df, output_dir))
    return payload


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

这是一个区间上沿假突破后的回归策略。先要求市场处在横向交易区间，上沿被多次测试；随后价格向上突破区间边线，但在 4 根 K 内重新回到区间内部，并在回归当根或下一根出现看空确认 K，信号只在确认 bar 收盘时发出。

## 成立条件

- snapshot 必须完整，`snapshot_status == complete`
- 当前处于横向区间上下文：区间高度受限、相邻 K 重叠率高、EMA 偏离不过大
- 被测试边线至少 2 次分离触碰
- 出现向区间外的突破，并在 4 根内回到区间内部
- 回归当根或下一根出现反向确认 K 线
- 信号只在确认 bar close 时产出，不做盘中预告

## 加强条件

- 边线有 3 次及以上有效触碰
- 假突破后 2 根内快速回归区间
- 突破端出现明显拒绝尾巴或反包
- 入场到区间中轴至少有 1R 空间
- 假突破发生在更大级别磁体附近

说明：第一版里“更大级别磁体”使用 `96` 根滚动高低点中轴作为代理特征，后续可换成更明确的高周期结构点。

## 谨慎信号

- 入场时价格已进入区间中间三分之一
- 假突破过深，回归已明显迟滞
- 确认 K 实体弱、收盘位置差
- 最近 3 根出现反方向强压力序列
- 区间过宽或明显扩张，失去均衡回归特征

## 买卖点与风控

- 方向：看空
- 入场：确认 K 收盘后做空，直播输出为 `entry_zone_low ~ entry_zone_high`
- 止损：假突破高点 / 确认 K 高点 / 区间上沿三者取高，再加 `0.15 ATR`
- 目标位：
  - 第一目标：区间中轴
  - 第二目标：靠近区间下沿的回归位
- 失效：价格重新站回假突破高点上方

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

## 备注

- `QuantEvent` 保留研究信息与事后 outcome 标签。
- `OpportunityCandidate` 为直播规则层对象，不依赖 AI 解释即可给下游展示。
- 第一版验证按 `ETHUSDT 5m` 运行，但接口口径保留为 `BTCUSDT 5m` 的 `spec_id`。
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
    run_id = f"{started_at.strftime('%Y%m%dT%H%M%S')}-{_stable_id(SPEC_ID, symbol, timeframe, started_at.date())}"
    output_dir = output_root / SPEC_ID / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    config = FailedBreakoutConfig()
    backtest_config = BacktestConfig()
    raw_df = read_ohlcv(input_path)
    base_df = prepare_ohlcv(raw_df)
    signal_df = detect(base_df, params=config)
    scored_records = score(signal_df, params=config, backtest_params=backtest_config)
    signal_df = _attach_scored_columns(signal_df, scored_records)

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
        sample_start=_iso(sample_info["sample_start"]),
        sample_end=_iso(sample_info["sample_end"]),
        params={
            "detect": asdict(config),
            "backtest": asdict(backtest_config),
            "strategy_meta": strategy_meta,
        },
        summary=run_summary,
        events=labeled_events,
        artifacts=artifacts,
    )

    candidate_rows = [_dataclass_to_dict(item) for item in candidates]

    _write_json(strategy_spec_path, _dataclass_to_dict(quant_spec))
    _write_jsonl(candidates_jsonl_path, candidate_rows)
    if candidate_rows:
        pl.DataFrame(_candidate_parquet_rows(candidates)).write_parquet(candidates_parquet_path)
    else:
        pl.DataFrame(schema={"candidate_id": pl.String}).write_parquet(candidates_parquet_path)
    signal_df.write_parquet(signals_path)
    trades_df.write_csv(trades_path)
    _write_json(
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
    _write_json(quant_run_path, quant_run.to_dict())

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
