from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import polars as pl


@dataclass(frozen=True)
class FinalFlagConfig:
    atr_window: int = 20
    trend_window: int = 20
    flag_window: int = 12
    late_flag_window: int = 20
    sr_window: int = 60
    failure_lookahead: int = 8
    range_width_atr: float = 3.2
    late_range_width_atr: float = 4.4
    flat_drift_atr: float = 1.1
    overlap_ratio_min: float = 0.55
    trend_score_min: int = 14
    climax_count_min: int = 5
    breakout_close_pos: float = 0.62
    breakout_body_ratio_min: float = 0.45
    sr_tolerance_atr: float = 0.60
    climax_distance_atr: float = 1.40


def get_config(preset: str) -> FinalFlagConfig:
    if preset == "adjusted":
        return FinalFlagConfig(
            range_width_atr=3.8,
            late_range_width_atr=5.0,
            overlap_ratio_min=0.48,
            trend_score_min=12,
            climax_count_min=4,
            breakout_close_pos=0.58,
        )
    return FinalFlagConfig()


def read_ohlcv(path: Path) -> pl.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pl.read_parquet(path)
    return pl.read_csv(path)


def prepare_ohlcv(df: pl.DataFrame) -> pl.DataFrame:
    """标准化 OHLCV 字段。实时安全，不使用未来 K 线。"""

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
    present_float_cols = [col for col in float_cols if col in df.columns]
    if present_float_cols:
        exprs.append(pl.col(present_float_cols).cast(pl.Float64))
    if "count" in df.columns:
        exprs.append(pl.col("count").cast(pl.Int64))

    out = df.with_columns(*exprs) if exprs else df
    if "ignore" in out.columns:
        out = out.drop("ignore")
    return out.sort("timestamp")


def add_final_flag_features(
    df: pl.DataFrame,
    config: FinalFlagConfig,
) -> pl.DataFrame:
    """生成最终旗形识别特征。实时安全，不使用未来 K 线。"""

    prev_close = pl.col("close").shift(1)
    prev_high = pl.col("high").shift(1)
    prev_low = pl.col("low").shift(1)

    df = (
        df.with_row_index("idx")
        .with_columns(
            (pl.col("high") - pl.col("low")).alias("bar_range"),
            (pl.col("close") - pl.col("open")).abs().alias("body_size"),
            (
                pl.col("high")
                - pl.max_horizontal(pl.col("open"), pl.col("close"))
            ).alias("upper_wick"),
            (
                pl.min_horizontal(pl.col("open"), pl.col("close"))
                - pl.col("low")
            ).alias("lower_wick"),
            pl.max_horizontal(
                pl.col("high") - pl.col("low"),
                (pl.col("high") - prev_close).abs(),
                (pl.col("low") - prev_close).abs(),
            ).alias("true_range"),
            pl.col("close").ewm_mean(span=20, adjust=False).alias("ema20"),
            pl.col("close").ewm_mean(span=50, adjust=False).alias("ema50"),
            pl.col("high").rolling_max(window_size=config.flag_window).alias("flag_high"),
            pl.col("low").rolling_min(window_size=config.flag_window).alias("flag_low"),
            pl.col("high").rolling_max(window_size=config.late_flag_window).alias("late_flag_high"),
            pl.col("low").rolling_min(window_size=config.late_flag_window).alias("late_flag_low"),
            pl.col("high").rolling_max(window_size=config.sr_window).alias("sr_high"),
            pl.col("low").rolling_min(window_size=config.sr_window).alias("sr_low"),
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
                (pl.col("low") <= prev_high) & (pl.col("high") >= prev_low)
            )
            .cast(pl.Float64)
            .alias("overlap_with_prev"),
            (
                (pl.col("close") > pl.col("ema20"))
                & (pl.col("ema20") > pl.col("ema50"))
                & (pl.col("ema20").diff() > 0)
            ).alias("bull_trend_bar"),
            (
                (pl.col("close") < pl.col("ema20"))
                & (pl.col("ema20") < pl.col("ema50"))
                & (pl.col("ema20").diff() < 0)
            ).alias("bear_trend_bar"),
        )
        .with_columns(
            pl.col("bull_trend_bar")
            .cast(pl.Int16)
            .rolling_sum(window_size=config.trend_window)
            .alias("uptrend_score"),
            pl.col("bear_trend_bar")
            .cast(pl.Int16)
            .rolling_sum(window_size=config.trend_window)
            .alias("downtrend_score"),
            (
                (pl.col("close") > pl.col("open"))
                & (pl.col("close_pos") >= 0.70)
                & (pl.col("body_ratio") >= 0.55)
            ).alias("bull_climax_bar"),
            (
                (pl.col("close") < pl.col("open"))
                & (pl.col("close_pos") <= 0.30)
                & (pl.col("body_ratio") >= 0.55)
            ).alias("bear_climax_bar"),
            (pl.col("flag_high") - pl.col("flag_low")).alias("flag_height"),
            (pl.col("late_flag_high") - pl.col("late_flag_low")).alias("late_flag_height"),
            ((pl.col("flag_high") + pl.col("flag_low")) / 2.0).alias("flag_mid"),
            ((pl.col("late_flag_high") + pl.col("late_flag_low")) / 2.0).alias("late_flag_mid"),
        )
        .with_columns(
            pl.col("bull_climax_bar")
            .cast(pl.Int16)
            .rolling_sum(window_size=config.trend_window)
            .alias("buy_climax_count"),
            pl.col("bear_climax_bar")
            .cast(pl.Int16)
            .rolling_sum(window_size=config.trend_window)
            .alias("sell_climax_count"),
            pl.col("overlap_with_prev")
            .rolling_mean(window_size=config.late_flag_window)
            .alias("overlap_ratio"),
            (
                (
                    pl.col("late_flag_mid")
                    - pl.col("late_flag_mid").shift(config.late_flag_window - 1)
                ).abs()
            ).alias("late_flag_drift"),
            (pl.col("close") - pl.col("ema20")).alias("ema_extension"),
        )
        .with_columns(
            (pl.col("flag_height") / pl.col("atr")).alias("flag_height_atr"),
            (pl.col("late_flag_height") / pl.col("atr")).alias("late_flag_height_atr"),
            (pl.col("late_flag_drift") / pl.col("atr")).alias("late_flag_drift_atr"),
            (
                (pl.col("sr_high") - pl.col("close"))
                <= config.sr_tolerance_atr * pl.col("atr")
            ).alias("near_resistance"),
            (
                (pl.col("close") - pl.col("sr_low"))
                <= config.sr_tolerance_atr * pl.col("atr")
            ).alias("near_support"),
            (
                pl.col("ema_extension")
                >= config.climax_distance_atr * pl.col("atr")
            ).alias("buy_climax_extension"),
            (
                (-pl.col("ema_extension"))
                >= config.climax_distance_atr * pl.col("atr")
            ).alias("sell_climax_extension"),
            pl.col("flag_high").shift(1).alias("flag_high_pre"),
            pl.col("flag_low").shift(1).alias("flag_low_pre"),
            pl.col("flag_mid").shift(1).alias("flag_mid_pre"),
        )
    )
    return df


def detect_final_flag(
    df: pl.DataFrame,
    config: FinalFlagConfig,
) -> pl.DataFrame:
    """检测最终旗形上下文与突破。实时安全，不使用未来 K 线。"""

    df = add_final_flag_features(df, config=config)

    df = df.with_columns(
        (
            (pl.col("uptrend_score") >= config.trend_score_min)
            & (pl.col("late_flag_height_atr") <= config.late_range_width_atr)
            & (pl.col("late_flag_drift_atr") <= config.flat_drift_atr)
            & (pl.col("overlap_ratio") >= config.overlap_ratio_min)
            & (pl.col("close") > pl.col("ema50"))
        ).alias("late_bull_context"),
        (
            (pl.col("downtrend_score") >= config.trend_score_min)
            & (pl.col("late_flag_height_atr") <= config.late_range_width_atr)
            & (pl.col("late_flag_drift_atr") <= config.flat_drift_atr)
            & (pl.col("overlap_ratio") >= config.overlap_ratio_min)
            & (pl.col("close") < pl.col("ema50"))
        ).alias("late_bear_context"),
    )

    df = df.with_columns(
        (
            pl.col("late_bull_context")
            & (
                pl.col("near_resistance")
                | pl.col("buy_climax_extension")
                | (pl.col("buy_climax_count") >= config.climax_count_min)
            )
        ).alias("final_bull_flag_setup"),
        (
            pl.col("late_bear_context")
            & (
                pl.col("near_support")
                | pl.col("sell_climax_extension")
                | (pl.col("sell_climax_count") >= config.climax_count_min)
            )
        ).alias("final_bear_flag_setup"),
    )

    return df.with_columns(
        (
            pl.col("final_bull_flag_setup")
            & (pl.col("flag_height_atr") <= config.range_width_atr)
            & (pl.col("close") > pl.col("flag_high_pre"))
            & (pl.col("close_pos") >= config.breakout_close_pos)
            & (pl.col("body_ratio") >= config.breakout_body_ratio_min)
        ).alias("final_bull_flag_breakout"),
        (
            pl.col("final_bear_flag_setup")
            & (pl.col("flag_height_atr") <= config.range_width_atr)
            & (pl.col("close") < pl.col("flag_low_pre"))
            & (pl.col("close_pos") <= (1.0 - config.breakout_close_pos))
            & (pl.col("body_ratio") >= config.breakout_body_ratio_min)
        ).alias("final_bear_flag_breakout"),
    )


def label_final_flag_outcomes(
    df: pl.DataFrame,
    config: FinalFlagConfig,
) -> pl.DataFrame:
    """添加事后标签。使用未来 K 线，只用于研究与评估。"""

    if "final_bull_flag_breakout" not in df.columns:
        df = detect_final_flag(df, config=config)

    df = df.with_columns(
        pl.col("low")
        .shift(-1)
        .rolling_min(window_size=config.failure_lookahead)
        .alias("future_low_n"),
        pl.col("high")
        .shift(-1)
        .rolling_max(window_size=config.failure_lookahead)
        .alias("future_high_n"),
        pl.col("close")
        .shift(-1)
        .rolling_min(window_size=config.failure_lookahead)
        .alias("future_close_min_n"),
        pl.col("close")
        .shift(-1)
        .rolling_max(window_size=config.failure_lookahead)
        .alias("future_close_max_n"),
    )

    return df.with_columns(
        (
            pl.col("final_bull_flag_breakout")
            & (pl.col("future_close_min_n") < pl.col("flag_high_pre"))
            & (pl.col("future_low_n") <= pl.col("flag_mid_pre"))
        ).alias("bull_breakout_failed"),
        (
            pl.col("final_bear_flag_breakout")
            & (pl.col("future_close_max_n") > pl.col("flag_low_pre"))
            & (pl.col("future_high_n") >= pl.col("flag_mid_pre"))
        ).alias("bear_breakout_failed"),
    ).with_columns(
        pl.col("bull_breakout_failed").alias("final_bull_flag_confirmed"),
        pl.col("bear_breakout_failed").alias("final_bear_flag_confirmed"),
    )


def summarize_detection(df: pl.DataFrame) -> dict[str, int]:
    summary = df.select(
        pl.len().alias("rows"),
        pl.col("final_bull_flag_setup").sum().alias("final_bull_flag_setup"),
        pl.col("final_bear_flag_setup").sum().alias("final_bear_flag_setup"),
        pl.col("final_bull_flag_breakout").sum().alias("final_bull_flag_breakout"),
        pl.col("final_bear_flag_breakout").sum().alias("final_bear_flag_breakout"),
        *(
            [
                pl.col("final_bull_flag_confirmed").sum().alias("final_bull_flag_confirmed"),
                pl.col("final_bear_flag_confirmed").sum().alias("final_bear_flag_confirmed"),
            ]
            if "final_bull_flag_confirmed" in df.columns
            else []
        ),
    ).to_dicts()[0]
    return {key: int(value) for key, value in summary.items()}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="检测阿布课程中的最终旗形。")
    parser.add_argument("--input", type=Path, required=True, help="OHLCV 数据路径")
    parser.add_argument("--output", type=Path, help="可选的输出 parquet 路径")
    parser.add_argument(
        "--preset",
        choices=["baseline", "adjusted"],
        default="baseline",
        help="参数预设",
    )
    parser.add_argument(
        "--with-outcomes",
        action="store_true",
        help="是否添加使用未来 K 线的事后标签",
    )
    parser.add_argument(
        "--summary-json",
        type=Path,
        help="可选的检测摘要 JSON 输出路径",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    config = get_config(args.preset)

    df = prepare_ohlcv(read_ohlcv(args.input))
    df = detect_final_flag(df, config=config)
    if args.with_outcomes:
        df = label_final_flag_outcomes(df, config=config)

    summary = summarize_detection(df)
    print(json.dumps({"preset": args.preset, **summary}, ensure_ascii=False, indent=2))

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(args.output)
        print(f"saved: {args.output.resolve()}")

    if args.summary_json:
        args.summary_json.parent.mkdir(parents=True, exist_ok=True)
        payload = {"preset": args.preset, "config": asdict(config), "summary": summary}
        args.summary_json.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"summary: {args.summary_json.resolve()}")


if __name__ == "__main__":
    main()
