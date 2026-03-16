from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

import polars as pl


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = REPO_ROOT / "data" / "binance_um_perp" / "ETHUSDT" / "5m" / "ETHUSDT-5m-history.parquet"
DEFAULT_OUTPUT = Path(__file__).resolve().parent / "flag_signals.parquet"
DEFAULT_SUMMARY = Path(__file__).resolve().parent / "flag_signal_summary.json"


@dataclass(frozen=True)
class FlagConfig:
    atr_window: int = 20
    flag_window: int = 10
    late_flag_window: int = 20
    trend_score_window: int = 20
    failure_lookahead: int = 6
    flag_range_atr: float = 3.0
    late_flag_range_atr: float = 4.0
    resistance_tol_atr: float = 0.5
    breakout_close_pos: float = 0.70
    climax_distance_atr: float = 1.5
    late_trend_score_min: int = 14


def read_ohlcv(path: Path) -> pl.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pl.read_parquet(path)
    return pl.read_csv(path)


def prepare_ohlcv(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize OHLCV columns.

    Real-time safe. This function uses only the current row and past schema information.
    """

    if "open_time" in df.columns and "timestamp" not in df.columns:
        df = df.rename({"open_time": "timestamp"})

    exprs: list[pl.Expr] = []
    if "timestamp" in df.columns and df.schema["timestamp"] != pl.Datetime:
        exprs.append(pl.from_epoch("timestamp", time_unit="ms").alias("timestamp"))
    if "close_time" in df.columns and df.schema["close_time"] != pl.Datetime:
        exprs.append(pl.from_epoch("close_time", time_unit="ms").alias("close_time"))
    if "timestamp" in df.columns:
        exprs.append(pl.col("timestamp").dt.replace_time_zone(None).alias("timestamp"))
    if "close_time" in df.columns:
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
    existing_float_cols = [col for col in float_cols if col in df.columns]
    if existing_float_cols:
        exprs.append(pl.col(existing_float_cols).cast(pl.Float64))
    if "count" in df.columns:
        exprs.append(pl.col("count").cast(pl.Int64))

    out = df.with_columns(*exprs) if exprs else df
    return out.drop("ignore", strict=False).sort("timestamp")


def add_flag_features(df: pl.DataFrame, config: FlagConfig = FlagConfig()) -> pl.DataFrame:
    """Add reusable flag features. Real-time safe."""

    prev_close = pl.col("close").shift(1)
    return (
        df.with_row_index("idx")
        .with_columns(
            (pl.col("high") - pl.col("low")).alias("bar_range"),
            (pl.col("close") - pl.col("open")).abs().alias("body_size"),
            pl.max_horizontal(
                pl.col("high") - pl.col("low"),
                (pl.col("high") - prev_close).abs(),
                (pl.col("low") - prev_close).abs(),
            ).alias("true_range"),
            pl.col("close").ewm_mean(span=20, adjust=False).alias("ema20"),
            pl.col("close").ewm_mean(span=50, adjust=False).alias("ema50"),
            pl.col("high").rolling_max(window_size=config.flag_window).alias("rolling_high_flag"),
            pl.col("low").rolling_min(window_size=config.flag_window).alias("rolling_low_flag"),
            pl.col("high").rolling_max(window_size=config.late_flag_window).alias("rolling_high_late"),
            pl.col("low").rolling_min(window_size=config.late_flag_window).alias("rolling_low_late"),
            pl.col("high").rolling_max(window_size=60).alias("rolling_high_60"),
            pl.col("low").rolling_min(window_size=60).alias("rolling_low_60"),
        )
        .with_columns(
            pl.col("true_range").rolling_mean(window_size=config.atr_window).alias("atr"),
            pl.when(pl.col("bar_range") > 0)
            .then((pl.col("close") - pl.col("low")) / pl.col("bar_range"))
            .otherwise(0.5)
            .alias("close_pos"),
            (pl.col("rolling_high_flag") - pl.col("rolling_low_flag")).alias("flag_range"),
            (pl.col("rolling_high_late") - pl.col("rolling_low_late")).alias("late_flag_range"),
            (
                (pl.col("close") > pl.col("ema20"))
                & (pl.col("ema20") > pl.col("ema50"))
                & (pl.col("ema20").diff() > 0)
            ).alias("trend_up"),
            (
                (pl.col("close") < pl.col("ema20"))
                & (pl.col("ema20") < pl.col("ema50"))
                & (pl.col("ema20").diff() < 0)
            ).alias("trend_down"),
        )
        .with_columns(
            pl.col("trend_up").cast(pl.Int16).rolling_sum(window_size=config.trend_score_window).alias("uptrend_score"),
            pl.col("trend_down").cast(pl.Int16).rolling_sum(window_size=config.trend_score_window).alias("downtrend_score"),
            (pl.col("flag_range") <= config.flag_range_atr * pl.col("atr")).alias("is_flag_range"),
            (pl.col("late_flag_range") <= config.late_flag_range_atr * pl.col("atr")).alias("is_late_flag_range"),
            ((pl.col("rolling_high_60") - pl.col("close")).abs() <= config.resistance_tol_atr * pl.col("atr")).alias("near_resistance"),
            ((pl.col("close") - pl.col("rolling_low_60")).abs() <= config.resistance_tol_atr * pl.col("atr")).alias("near_support"),
            ((pl.col("close") - pl.col("ema20")) > config.climax_distance_atr * pl.col("atr")).alias("buy_climax"),
            ((pl.col("ema20") - pl.col("close")) > config.climax_distance_atr * pl.col("atr")).alias("sell_climax"),
        )
    )


def detect_flag(df: pl.DataFrame, config: FlagConfig = FlagConfig()) -> pl.DataFrame:
    """Real-time safe detection. No future bars are used."""

    df = add_flag_features(df, config=config)
    return (
        df.with_columns(
            (
                pl.col("trend_up").shift(config.flag_window).fill_null(False)
                & pl.col("is_flag_range")
                & (pl.col("close") > pl.col("rolling_low_flag"))
            ).alias("bull_flag_candidate"),
            (
                pl.col("trend_down").shift(config.flag_window).fill_null(False)
                & pl.col("is_flag_range")
                & (pl.col("close") < pl.col("rolling_high_flag"))
            ).alias("bear_flag_candidate"),
            (
                pl.col("trend_up").shift(config.flag_window).fill_null(False)
                & pl.col("is_late_flag_range")
                & (pl.col("close") > pl.col("rolling_low_late"))
            ).alias("late_bull_flag_candidate"),
            (
                pl.col("trend_down").shift(config.flag_window).fill_null(False)
                & pl.col("is_late_flag_range")
                & (pl.col("close") < pl.col("rolling_high_late"))
            ).alias("late_bear_flag_candidate"),
        )
        .with_columns(
            (
                pl.col("bull_flag_candidate")
                & (pl.col("close") > pl.col("rolling_high_flag").shift(1))
                & (pl.col("close_pos") >= config.breakout_close_pos)
            ).alias("bull_flag_breakout"),
            (
                pl.col("bear_flag_candidate")
                & (pl.col("close") < pl.col("rolling_low_flag").shift(1))
                & (pl.col("close_pos") <= (1.0 - config.breakout_close_pos))
            ).alias("bear_flag_breakout"),
            (
                (pl.col("uptrend_score") >= config.late_trend_score_min)
                & pl.col("late_bull_flag_candidate")
                & (pl.col("near_resistance") | pl.col("buy_climax"))
            ).alias("final_bull_flag_setup"),
            (
                (pl.col("downtrend_score") >= config.late_trend_score_min)
                & pl.col("late_bear_flag_candidate")
                & (pl.col("near_support") | pl.col("sell_climax"))
            ).alias("final_bear_flag_setup"),
        )
    )


def label_flag_outcomes(df: pl.DataFrame, config: FlagConfig = FlagConfig()) -> pl.DataFrame:
    """Research-only hindsight labels. Uses future bars."""

    if "bull_flag_breakout" not in df.columns or "bear_flag_breakout" not in df.columns:
        df = detect_flag(df, config=config)

    return (
        df.with_columns(
            pl.col("low").shift(-1).rolling_min(window_size=config.failure_lookahead).alias("future_low_n"),
            pl.col("high").shift(-1).rolling_max(window_size=config.failure_lookahead).alias("future_high_n"),
        )
        .with_columns(
            (
                pl.col("bull_flag_breakout")
                & (pl.col("future_low_n") < pl.col("rolling_low_flag"))
            ).alias("bull_breakout_failed"),
            (
                pl.col("bear_flag_breakout")
                & (pl.col("future_high_n") > pl.col("rolling_high_flag"))
            ).alias("bear_breakout_failed"),
        )
        .with_columns(
            (
                pl.col("final_bull_flag_setup") & pl.col("bull_breakout_failed")
            ).alias("final_bull_flag_confirmed"),
            (
                pl.col("final_bear_flag_setup") & pl.col("bear_breakout_failed")
            ).alias("final_bear_flag_confirmed"),
        )
    )


def summary_dict(df: pl.DataFrame) -> dict[str, int]:
    summary = df.select(
        pl.len().alias("rows"),
        pl.col("bull_flag_candidate").sum().alias("bull_flag_candidate"),
        pl.col("bear_flag_candidate").sum().alias("bear_flag_candidate"),
        pl.col("bull_flag_breakout").sum().alias("bull_flag_breakout"),
        pl.col("bear_flag_breakout").sum().alias("bear_flag_breakout"),
        pl.col("final_bull_flag_setup").sum().alias("final_bull_flag_setup"),
        pl.col("final_bear_flag_setup").sum().alias("final_bear_flag_setup"),
        *(
            [
                pl.col("bull_breakout_failed").sum().alias("bull_breakout_failed"),
                pl.col("bear_breakout_failed").sum().alias("bear_breakout_failed"),
                pl.col("final_bull_flag_confirmed").sum().alias("final_bull_flag_confirmed"),
                pl.col("final_bear_flag_confirmed").sum().alias("final_bear_flag_confirmed"),
            ]
            if "bull_breakout_failed" in df.columns
            else []
        ),
    ).to_dicts()[0]
    return {key: int(value) for key, value in summary.items() if value is not None}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detect flag patterns from OHLCV data.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Path to CSV or Parquet input.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output parquet path.")
    parser.add_argument("--summary-json", type=Path, default=DEFAULT_SUMMARY, help="Output summary JSON path.")
    parser.add_argument("--with-outcomes", action="store_true", help="Add hindsight label columns.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = prepare_ohlcv(read_ohlcv(args.input))
    df = detect_flag(df)
    if args.with_outcomes:
        df = label_flag_outcomes(df)

    summary = summary_dict(df)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(b"")
    df.write_parquet(args.output)
    args.summary_json.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"saved: {args.output.resolve()}")
    print(f"summary: {args.summary_json.resolve()}")


if __name__ == "__main__":
    main()
