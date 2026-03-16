from __future__ import annotations

import argparse
import json
from pathlib import Path

import polars as pl

from detect_flag import FlagConfig, detect_flag, label_flag_outcomes, prepare_ohlcv, read_ohlcv


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = REPO_ROOT / "data" / "binance_um_perp" / "ETHUSDT" / "5m" / "ETHUSDT-5m-history.parquet"
OUTPUT_DIR = Path(__file__).resolve().parent
DEFAULT_BREAKOUT_TRADES = OUTPUT_DIR / "breakout_strategy_trades.parquet"
DEFAULT_FINAL_REVERSAL_TRADES = OUTPUT_DIR / "final_flag_reversal_research_trades.parquet"
DEFAULT_SUMMARY_JSON = OUTPUT_DIR / "strategy_summary.json"

BREAKOUT_HOLD_BARS = 12
FINAL_REVERSAL_HOLD_BARS = 16
STOP_BUFFER_ATR = 0.5
TARGET_R = 2.0


def _simulate_long(df: pl.DataFrame, entry_idx: int, exit_idx_limit: int, stop: float, target: float) -> tuple[int, float, str]:
    for idx in range(entry_idx, exit_idx_limit + 1):
        row = df.row(idx, named=True)
        if row["low"] <= stop:
            return idx, stop, "stop"
        if row["high"] >= target:
            return idx, target, "target"
    last_close = float(df.row(exit_idx_limit, named=True)["close"])
    return exit_idx_limit, last_close, "time"


def _simulate_short(df: pl.DataFrame, entry_idx: int, exit_idx_limit: int, stop: float, target: float) -> tuple[int, float, str]:
    for idx in range(entry_idx, exit_idx_limit + 1):
        row = df.row(idx, named=True)
        if row["high"] >= stop:
            return idx, stop, "stop"
        if row["low"] <= target:
            return idx, target, "target"
    last_close = float(df.row(exit_idx_limit, named=True)["close"])
    return exit_idx_limit, last_close, "time"


def build_breakout_strategy(df: pl.DataFrame) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    total_rows = df.height

    signal_df = df.filter(pl.col("bull_flag_breakout") | pl.col("bear_flag_breakout"))
    for row in signal_df.iter_rows(named=True):
        signal_idx = int(row["idx"])
        entry_idx = signal_idx + 1
        if entry_idx >= total_rows:
            continue

        entry_bar = df.row(entry_idx, named=True)
        atr = float(row["atr"])
        entry_price = float(entry_bar["open"])
        exit_limit = min(total_rows - 1, entry_idx + BREAKOUT_HOLD_BARS)

        if bool(row["bull_flag_breakout"]):
            side = "long"
            signal_name = "bull_flag_breakout"
            stop_price = float(row["rolling_low_flag"]) - STOP_BUFFER_ATR * atr
            risk = entry_price - stop_price
            if risk <= 0:
                continue
            target_price = entry_price + TARGET_R * risk
            exit_idx, exit_price, exit_reason = _simulate_long(df, entry_idx, exit_limit, stop_price, target_price)
            realized_r = (exit_price - entry_price) / risk
        else:
            side = "short"
            signal_name = "bear_flag_breakout"
            stop_price = float(row["rolling_high_flag"]) + STOP_BUFFER_ATR * atr
            risk = stop_price - entry_price
            if risk <= 0:
                continue
            target_price = entry_price - TARGET_R * risk
            exit_idx, exit_price, exit_reason = _simulate_short(df, entry_idx, exit_limit, stop_price, target_price)
            realized_r = (entry_price - exit_price) / risk

        exit_bar = df.row(exit_idx, named=True)
        rows.append(
            {
                "strategy": "breakout_trend",
                "signal_name": signal_name,
                "signal_idx": signal_idx,
                "entry_idx": entry_idx,
                "exit_idx": exit_idx,
                "side": side,
                "signal_time": row["timestamp"],
                "entry_time": entry_bar["timestamp"],
                "exit_time": exit_bar["timestamp"],
                "entry_price": entry_price,
                "exit_price": float(exit_price),
                "stop_price": stop_price,
                "target_price": target_price,
                "risk": risk,
                "realized_r": float(realized_r),
                "bars_held": int(exit_idx - entry_idx + 1),
                "exit_reason": exit_reason,
            }
        )

    return pl.DataFrame(rows) if rows else pl.DataFrame(schema={"strategy": pl.String})


def _find_failure_confirm_idx(df: pl.DataFrame, row: dict[str, object], config: FlagConfig) -> int | None:
    signal_idx = int(row["idx"])
    end_idx = min(df.height - 1, signal_idx + config.failure_lookahead)
    if bool(row["final_bull_flag_confirmed"]):
        threshold = float(row["rolling_low_flag"])
        for idx in range(signal_idx + 1, end_idx + 1):
            if float(df.row(idx, named=True)["low"]) < threshold:
                return idx
    if bool(row["final_bear_flag_confirmed"]):
        threshold = float(row["rolling_high_flag"])
        for idx in range(signal_idx + 1, end_idx + 1):
            if float(df.row(idx, named=True)["high"]) > threshold:
                return idx
    return None


def build_final_flag_reversal_research(df: pl.DataFrame, config: FlagConfig = FlagConfig()) -> pl.DataFrame:
    rows: list[dict[str, object]] = []

    signal_df = df.filter(pl.col("final_bull_flag_confirmed") | pl.col("final_bear_flag_confirmed"))
    for row in signal_df.iter_rows(named=True):
        confirm_idx = _find_failure_confirm_idx(df, row, config=config)
        if confirm_idx is None or confirm_idx + 1 >= df.height:
            continue

        entry_idx = confirm_idx + 1
        entry_bar = df.row(entry_idx, named=True)
        atr = float(row["atr"])
        entry_price = float(entry_bar["open"])
        exit_limit = min(df.height - 1, entry_idx + FINAL_REVERSAL_HOLD_BARS)

        if bool(row["final_bull_flag_confirmed"]):
            side = "short"
            signal_name = "final_bull_flag_reversal"
            stop_price = max(float(row["high"]), float(row["rolling_high_flag"])) + STOP_BUFFER_ATR * atr
            risk = stop_price - entry_price
            if risk <= 0:
                continue
            target_price = entry_price - TARGET_R * risk
            exit_idx, exit_price, exit_reason = _simulate_short(df, entry_idx, exit_limit, stop_price, target_price)
            realized_r = (entry_price - exit_price) / risk
        else:
            side = "long"
            signal_name = "final_bear_flag_reversal"
            stop_price = min(float(row["low"]), float(row["rolling_low_flag"])) - STOP_BUFFER_ATR * atr
            risk = entry_price - stop_price
            if risk <= 0:
                continue
            target_price = entry_price + TARGET_R * risk
            exit_idx, exit_price, exit_reason = _simulate_long(df, entry_idx, exit_limit, stop_price, target_price)
            realized_r = (exit_price - entry_price) / risk

        exit_bar = df.row(exit_idx, named=True)
        confirm_bar = df.row(confirm_idx, named=True)
        rows.append(
            {
                "strategy": "final_flag_reversal_research",
                "signal_name": signal_name,
                "signal_idx": int(row["idx"]),
                "confirm_idx": confirm_idx,
                "entry_idx": entry_idx,
                "exit_idx": exit_idx,
                "side": side,
                "signal_time": row["timestamp"],
                "confirm_time": confirm_bar["timestamp"],
                "entry_time": entry_bar["timestamp"],
                "exit_time": exit_bar["timestamp"],
                "entry_price": entry_price,
                "exit_price": float(exit_price),
                "stop_price": stop_price,
                "target_price": target_price,
                "risk": risk,
                "realized_r": float(realized_r),
                "bars_held": int(exit_idx - entry_idx + 1),
                "exit_reason": exit_reason,
            }
        )

    return pl.DataFrame(rows) if rows else pl.DataFrame(schema={"strategy": pl.String})


def summarize_trades(trades: pl.DataFrame) -> dict[str, float | int]:
    if trades.is_empty():
        return {
            "trades": 0,
            "win_rate": 0.0,
            "avg_r": 0.0,
            "median_r": 0.0,
            "total_r": 0.0,
            "avg_bars_held": 0.0,
        }

    stats = trades.select(
        pl.len().alias("trades"),
        (pl.col("realized_r") > 0).mean().alias("win_rate"),
        pl.col("realized_r").mean().alias("avg_r"),
        pl.col("realized_r").median().alias("median_r"),
        pl.col("realized_r").sum().alias("total_r"),
        pl.col("bars_held").mean().alias("avg_bars_held"),
    ).to_dicts()[0]
    return {
        "trades": int(stats["trades"]),
        "win_rate": round(float(stats["win_rate"] or 0.0), 4),
        "avg_r": round(float(stats["avg_r"] or 0.0), 4),
        "median_r": round(float(stats["median_r"] or 0.0), 4),
        "total_r": round(float(stats["total_r"] or 0.0), 4),
        "avg_bars_held": round(float(stats["avg_bars_held"] or 0.0), 2),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build flag strategy research outputs.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Path to OHLCV input.")
    parser.add_argument("--breakout-output", type=Path, default=DEFAULT_BREAKOUT_TRADES, help="Output parquet for breakout strategy trades.")
    parser.add_argument("--final-output", type=Path, default=DEFAULT_FINAL_REVERSAL_TRADES, help="Output parquet for final flag reversal trades.")
    parser.add_argument("--summary-json", type=Path, default=DEFAULT_SUMMARY_JSON, help="Strategy summary json path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = prepare_ohlcv(read_ohlcv(args.input))
    df = label_flag_outcomes(detect_flag(df))

    breakout_trades = build_breakout_strategy(df)
    final_trades = build_final_flag_reversal_research(df)

    args.breakout_output.parent.mkdir(parents=True, exist_ok=True)
    breakout_trades.write_parquet(args.breakout_output)
    final_trades.write_parquet(args.final_output)

    summary = {
        "breakout_trend": summarize_trades(breakout_trades),
        "final_flag_reversal_research": summarize_trades(final_trades),
    }
    args.summary_json.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"saved: {args.breakout_output.resolve()}")
    print(f"saved: {args.final_output.resolve()}")
    print(f"summary: {args.summary_json.resolve()}")


if __name__ == "__main__":
    main()
