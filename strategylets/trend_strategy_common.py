from __future__ import annotations

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
    KeyPoint,
    OpportunityCandidate,
    OutcomeLabel,
    Overlay,
    QuantEvent,
    TradePlan,
)


@dataclass(frozen=True)
class BacktestConfig:
    stop_buffer_atr: float = 0.15
    min_stop_atr: float = 0.60
    max_holding_bars: int = 32
    cooldown_bars: int = 1
    fee_bps_round_trip: float = 2.0
    notional_usdt: float = 10_000.0


def coerce_backtest_config(
    params: BacktestConfig | dict[str, Any] | None,
) -> BacktestConfig:
    if params is None:
        return BacktestConfig()
    if isinstance(params, BacktestConfig):
        return params
    return BacktestConfig(**params)


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


def add_common_features(
    df: pl.DataFrame,
    *,
    atr_window: int = 20,
    breakout_window: int = 20,
) -> pl.DataFrame:
    prev_close = pl.col("close").shift(1)
    prev_high = pl.col("high").shift(1)
    prev_low = pl.col("low").shift(1)

    base = (
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
            pl.col("true_range").rolling_mean(window_size=atr_window).alias("atr"),
            pl.when(pl.col("bar_range") > 0)
            .then((pl.col("close") - pl.col("low")) / pl.col("bar_range"))
            .otherwise(0.5)
            .alias("close_pos"),
            pl.when(pl.col("bar_range") > 0)
            .then(pl.col("body_size") / pl.col("bar_range"))
            .otherwise(0.0)
            .alias("body_ratio"),
            pl.when(pl.col("bar_range") > 0)
            .then(pl.col("upper_wick") / pl.col("bar_range"))
            .otherwise(0.0)
            .alias("upper_wick_ratio"),
            pl.when(pl.col("bar_range") > 0)
            .then(pl.col("lower_wick") / pl.col("bar_range"))
            .otherwise(0.0)
            .alias("lower_wick_ratio"),
            (pl.col("close") > pl.col("open")).alias("bull_bar"),
            (pl.col("close") < pl.col("open")).alias("bear_bar"),
            prev_high.alias("prev_high"),
            prev_low.alias("prev_low"),
        )
    )
    base = base.with_columns(
        pl.col("high").shift(1).rolling_max(window_size=breakout_window).alias("recent_high_pre"),
        pl.col("low").shift(1).rolling_min(window_size=breakout_window).alias("recent_low_pre"),
        pl.col("high").shift(1).rolling_max(window_size=10).alias("recent_high_10"),
        pl.col("low").shift(1).rolling_min(window_size=10).alias("recent_low_10"),
        pl.col("high").shift(1).rolling_max(window_size=50).alias("recent_high_50"),
        pl.col("low").shift(1).rolling_min(window_size=50).alias("recent_low_50"),
        pl.col("high").shift(1).rolling_max(window_size=96).alias("recent_high_96"),
        pl.col("low").shift(1).rolling_min(window_size=96).alias("recent_low_96"),
        pl.when(pl.col("atr") > 0)
        .then((pl.col("ema20") - pl.col("ema20").shift(5)) / pl.col("atr"))
        .otherwise(None)
        .alias("ema20_slope_atr"),
        pl.when(pl.col("atr") > 0)
        .then((pl.col("ema50") - pl.col("ema50").shift(8)) / pl.col("atr"))
        .otherwise(None)
        .alias("ema50_slope_atr"),
        pl.when(pl.col("atr") > 0)
        .then((pl.col("close") - pl.col("ema20")) / pl.col("atr"))
        .otherwise(None)
        .alias("distance_ema20_atr"),
        pl.when(pl.col("atr") > 0)
        .then((pl.col("ema20") - pl.col("ema50")).abs() / pl.col("atr"))
        .otherwise(None)
        .alias("ema_gap_atr"),
        (
            (pl.col("close") > pl.col("open"))
            & (pl.col("close_pos") >= 0.68)
            & (pl.col("body_ratio") >= 0.52)
        ).alias("bull_trend_bar"),
        (
            (pl.col("close") < pl.col("open"))
            & (pl.col("close_pos") <= 0.32)
            & (pl.col("body_ratio") >= 0.52)
        ).alias("bear_trend_bar"),
        (pl.col("low") > pl.col("ema20")).alias("bar_above_ema20"),
        (pl.col("high") < pl.col("ema20")).alias("bar_below_ema20"),
        (pl.col("close") >= pl.col("ema20")).alias("close_above_ema20"),
        (pl.col("close") <= pl.col("ema20")).alias("close_below_ema20"),
        (pl.col("close") >= pl.col("ema50")).alias("close_above_ema50"),
        (pl.col("close") <= pl.col("ema50")).alias("close_below_ema50"),
    )
    base = base.with_columns(
        pl.when(pl.col("atr") > 0)
        .then((pl.col("close") - pl.col("recent_high_pre")) / pl.col("atr"))
        .otherwise(None)
        .alias("close_vs_recent_high_atr"),
        pl.when(pl.col("atr") > 0)
        .then((pl.col("recent_low_pre") - pl.col("close")) / pl.col("atr"))
        .otherwise(None)
        .alias("close_vs_recent_low_atr"),
        pl.col("bull_trend_bar").cast(pl.Int64).rolling_sum(window_size=3).alias("recent_bull_pressure_3"),
        pl.col("bear_trend_bar").cast(pl.Int64).rolling_sum(window_size=3).alias("recent_bear_pressure_3"),
        pl.when(
            (pl.col("idx") >= max(atr_window, breakout_window, 50))
            & pl.col("atr").is_not_null()
            & pl.col("recent_high_pre").is_not_null()
            & pl.col("recent_low_pre").is_not_null()
        )
        .then(pl.lit("complete"))
        .otherwise(pl.lit("degraded"))
        .alias("snapshot_status"),
    )
    return base


def clip_score(value: int) -> int:
    return max(0, min(100, value))


def score_band(score: int) -> str:
    if score >= 85:
        return "A"
    if score >= 75:
        return "B"
    return "C"


def route(score: int) -> str:
    if score >= 75:
        return "primary"
    if score >= 60:
        return "secondary"
    return "drop"


def iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def stable_id(*parts: object) -> str:
    import hashlib

    digest = hashlib.sha1(
        "|".join("" if part is None else str(part) for part in parts).encode("utf-8")
    ).hexdigest()
    return digest[:20]


def json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def make_key_point(
    point_id: str,
    role: str,
    timestamp: Any,
    price: float,
    display_label: str | None = None,
    meta: dict[str, Any] | None = None,
) -> KeyPoint:
    point_meta = dict(meta or {})
    if display_label:
        point_meta["display_label"] = display_label
    return KeyPoint(
        point_id=point_id,
        role=role,
        timestamp=iso(timestamp),
        price=float(price),
        meta=point_meta,
    )


def point_display_label(point: KeyPoint) -> str:
    display = point.meta.get("display_label")
    if display:
        return str(display)
    return point.role


def make_segment_overlay(
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


def make_zone_overlay(
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
            make_key_point(f"{overlay_id}-start", "zone_start", start_time, low, "区起"),
            make_key_point(f"{overlay_id}-end", "zone_end", end_time, high, "区终"),
        ],
        style={"fillcolor": color, "opacity": 0.15},
        meta={},
    )


def index_rows(df: pl.DataFrame) -> dict[int, dict[str, Any]]:
    return {
        int(row["idx"]): row
        for row in df.to_dicts()
    }


def label_outcomes(
    df: pl.DataFrame,
    events: list[QuantEvent],
    *,
    lookahead_bars: int,
    backtest_params: BacktestConfig | dict[str, Any] | None = None,
) -> list[QuantEvent]:
    """只做研究/回测事后标签，明确使用未来 K 线。"""

    backtest_config = coerce_backtest_config(backtest_params)
    rows_by_idx = index_rows(df)
    highs = [float(value) for value in df["high"].to_list()]
    lows = [float(value) for value in df["low"].to_list()]
    closes = [float(value) for value in df["close"].to_list()]

    labeled: list[QuantEvent] = []
    for event in events:
        signal_idx = int(event.meta["signal_idx"])
        trade_plan = event.trade_plan
        if trade_plan is None:
            labeled.append(event)
            continue

        direction = trade_plan.direction
        entry_price = float(trade_plan.entry_price or 0.0)
        stop_price = float(trade_plan.stop_price or 0.0)
        target_price = (
            float(trade_plan.target_prices[0])
            if trade_plan.target_prices
            else entry_price
        )
        risk_per_unit = max(abs(entry_price - stop_price), 1e-9)

        lookahead_end = min(
            len(highs) - 1,
            signal_idx + min(lookahead_bars, backtest_config.max_holding_bars),
        )
        exit_idx = lookahead_end
        exit_price = closes[lookahead_end]
        exit_reason = "时间止盈/止损"
        hit_target = False
        hit_stop = False

        for idx in range(signal_idx + 1, lookahead_end + 1):
            high = highs[idx]
            low = lows[idx]
            if direction == "bullish":
                if low <= stop_price and high >= target_price:
                    exit_idx = idx
                    exit_price = stop_price
                    exit_reason = "同柱先按止损"
                    hit_stop = True
                    break
                if low <= stop_price:
                    exit_idx = idx
                    exit_price = stop_price
                    exit_reason = "止损"
                    hit_stop = True
                    break
                if high >= target_price:
                    exit_idx = idx
                    exit_price = target_price
                    exit_reason = "止盈"
                    hit_target = True
                    break
            else:
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
        quantity = backtest_config.notional_usdt / max(entry_price, 1e-9)
        if direction == "bullish":
            mfe = max(0.0, (future_high - entry_price) / risk_per_unit)
            mae = max(0.0, (entry_price - future_low) / risk_per_unit)
            pnl_r = (exit_price - entry_price) / risk_per_unit
            gross_pnl = (exit_price - entry_price) * quantity
        else:
            mfe = max(0.0, (entry_price - future_low) / risk_per_unit)
            mae = max(0.0, (future_high - entry_price) / risk_per_unit)
            pnl_r = (entry_price - exit_price) / risk_per_unit
            gross_pnl = (entry_price - exit_price) * quantity

        fee_rate = (backtest_config.fee_bps_round_trip / 2.0) / 10_000.0
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
                    lookahead_bars=lookahead_bars,
                    outcome_class=outcome_class,
                    hit_target=hit_target,
                    hit_stop=hit_stop,
                    mfe=round(mfe, 6),
                    mae=round(mae, 6),
                    pnl_r=round(pnl_r, 6),
                    meta={
                        "exit_idx": exit_idx,
                        "exit_time": iso(rows_by_idx[exit_idx]["timestamp"]),
                        "exit_price": exit_price,
                        "exit_reason": exit_reason,
                        "gross_pnl": gross_pnl,
                        "net_pnl": net_pnl,
                        "fees": fees,
                        "holding_bars": exit_idx - signal_idx,
                    },
                ),
            )
        )

    return labeled


def _event_to_trade_row(event: QuantEvent) -> dict[str, Any]:
    outcome = event.outcome
    if outcome is None:
        raise ValueError("event 缺少 outcome，无法生成 trade row。")

    trade_plan = event.trade_plan
    if trade_plan is None:
        raise ValueError("event 缺少 trade_plan，无法生成 trade row。")

    side = "long" if trade_plan.direction == "bullish" else "short"
    entry_price = float(trade_plan.entry_price or 0.0)
    stop_price = float(trade_plan.stop_price or 0.0)
    target_price = (
        float(trade_plan.target_prices[0])
        if trade_plan.target_prices
        else entry_price
    )
    meta = outcome.meta
    score_value = float(event.score or 0.0)
    trade_route = "drop" if score_value < 60 else ("primary" if score_value >= 75 else "secondary")

    return {
        "event_id": event.event_id,
        "candidate_id": event.meta["candidate_id"],
        "signal_time": event.detected_at,
        "exit_time": meta["exit_time"],
        "side": side,
        "score": score_value,
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
        "route": trade_route,
    }


def empty_trades_df() -> pl.DataFrame:
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
    backtest_config = coerce_backtest_config(backtest_params)
    trade_rows = [
        _event_to_trade_row(event)
        for event in events
        if (
            event.outcome is not None
            and float(event.score or 0.0) >= 60
            and bool(event.features.get("hard_gate_pass"))
        )
    ]
    trades_df = pl.DataFrame(trade_rows) if trade_rows else empty_trades_df()

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


def dataclass_to_dict(item: Any) -> dict[str, Any]:
    return asdict(item)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=json_default),
        encoding="utf-8",
    )


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, default=json_default))
            handle.write("\n")


def candidate_parquet_rows(candidates: list[OpportunityCandidate]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        row = dataclass_to_dict(candidate)
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
                "key_points_json": json.dumps(row["key_points"], ensure_ascii=False, default=json_default),
                "overlays_json": json.dumps(row["overlays"], ensure_ascii=False, default=json_default),
                "tags_json": json.dumps(row["tags"], ensure_ascii=False),
            }
        )
    return rows


def attach_scored_columns(
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


def pick_example_events(events: list[QuantEvent], example_count: int) -> list[QuantEvent]:
    candidate_events = [
        event
        for event in events
        if float(event.score or 0.0) >= 60 and bool(event.features.get("hard_gate_pass"))
    ]
    if len(candidate_events) <= example_count:
        return candidate_events
    sorted_events = sorted(
        candidate_events,
        key=lambda item: (float(item.score or 0.0), item.detected_at),
        reverse=True,
    )
    picked: list[QuantEvent] = []
    for bucket_idx in range(example_count):
        source_idx = math.floor(bucket_idx * len(sorted_events) / example_count)
        picked.append(sorted_events[source_idx])
    picked = sorted(
        {event.event_id: event for event in picked}.values(),
        key=lambda item: item.detected_at,
    )
    return picked[:example_count]


def _plot_event(
    df: pl.DataFrame,
    event: QuantEvent,
    output_path: Path,
) -> None:
    signal_idx = int(event.meta["signal_idx"])
    exit_idx_raw = event.outcome.meta.get("exit_idx") if event.outcome is not None else None
    exit_idx = int(exit_idx_raw) if exit_idx_raw is not None else signal_idx
    start_idx = max(0, signal_idx - 80)
    end_idx = min(df.height - 1, max(signal_idx + 40, exit_idx + 6))
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
            point_display_label(point),
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

    def _annotate_trade_marker(
        timestamp_text: str,
        price: float,
        label: str,
        color: str,
        marker: str,
        y_offset: int,
    ) -> None:
        marker_time = datetime.fromisoformat(timestamp_text)
        marker_x = mdates.date2num(marker_time)
        ax.scatter(
            marker_x,
            price,
            s=120,
            color=color,
            marker=marker,
            edgecolors="white",
            linewidths=1.2,
            zorder=6,
        )
        ax.annotate(
            label,
            xy=(marker_x, price),
            xytext=(0, y_offset),
            textcoords="offset points",
            ha="center",
            va="bottom" if y_offset >= 0 else "top",
            fontsize=9.5,
            fontweight="bold",
            color=color,
            bbox={
                "boxstyle": "round,pad=0.20",
                "facecolor": "white",
                "edgecolor": color,
                "alpha": 0.95,
            },
            arrowprops={
                "arrowstyle": "-",
                "color": color,
                "linewidth": 1.0,
                "alpha": 0.85,
            },
        )

    entry_time = event.detected_at
    entry_price = float(trade_plan.entry_price) if trade_plan and trade_plan.entry_price is not None else float(
        event.meta.get("entry_zone_low", 0.0)
    )
    entry_color = "#1d8348" if trade_plan and trade_plan.direction == "bullish" else "#b03a2e"
    _annotate_trade_marker(
        entry_time,
        entry_price,
        "入场",
        entry_color,
        "^" if trade_plan and trade_plan.direction == "bullish" else "v",
        18 if trade_plan and trade_plan.direction == "bullish" else -22,
    )

    if event.outcome is not None:
        exit_time_text = str(event.outcome.meta.get("exit_time", event.detected_at))
        exit_price = float(event.outcome.meta.get("exit_price", entry_price))
        exit_reason = str(event.outcome.meta.get("exit_reason", "出场"))
        exit_color = "#2874a6" if float(event.outcome.pnl_r or 0.0) >= 0 else "#7f8c8d"
        _annotate_trade_marker(
            exit_time_text,
            exit_price,
            f"出场\n{exit_reason}",
            exit_color,
            "X",
            -30 if trade_plan and trade_plan.direction == "bullish" else 28,
        )
    direction_text = "做多" if trade_plan and trade_plan.direction == "bullish" else "做空"
    summary_lines = [
        f"方向: {direction_text}",
        f"评分: {float(event.score or 0.0):.0f} / {event.meta.get('score_band', '-')}",
        f"入场区: {float(event.meta.get('entry_zone_low', 0.0)):.2f} ~ {float(event.meta.get('entry_zone_high', 0.0)):.2f}",
        f"入场价: {entry_price:.2f}",
        f"止损: {float(trade_plan.stop_price if trade_plan and trade_plan.stop_price is not None else 0.0):.2f}",
        f"目标1: {float(trade_plan.target_prices[0] if trade_plan and trade_plan.target_prices else 0.0):.2f}",
    ]
    if event.outcome is not None:
        summary_lines.append(f"出场价: {float(event.outcome.meta.get('exit_price', 0.0)):.2f}")
        summary_lines.append(f"出场因子: {event.outcome.meta.get('exit_reason', '-')}")
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

    ax.set_title(f"{event.meta.get('strategy_name_std', event.spec_id)} | {event.detected_at}")
    ax.set_ylabel("价格")
    ax.set_xlabel("时间")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    ax.grid(True, linestyle="--", alpha=0.25)
    fig.autofmt_xdate()
    fig.tight_layout(rect=[0, 0, 0.82, 1])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_equity_curve(equity_df: pl.DataFrame, output_dir: Path) -> dict[str, str]:
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

    picked_events = pick_example_events(events, example_count=example_count)
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
        payload.update(plot_equity_curve(equity_df, output_dir))
    return payload
