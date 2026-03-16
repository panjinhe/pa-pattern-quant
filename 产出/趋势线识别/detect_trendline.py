from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import polars as pl


@dataclass(frozen=True)
class TrendlineConfig:
    atr_window: int = 20
    line_window: int = 48
    min_touches: int = 3
    touch_tolerance_atr: float = 0.18
    breakout_tolerance_atr: float = 0.20
    touch_separation_bars: int = 3
    min_channel_height_atr: float = 1.20
    max_channel_height_atr: float = 12.00
    slope_flat_threshold_atr_per_bar: float = 0.03
    label_lookahead: int = 24
    measured_move_factor: float = 0.80


def get_config(preset: str) -> TrendlineConfig:
    if preset == "adjusted":
        return TrendlineConfig(
            min_touches=2,
            touch_tolerance_atr=0.22,
            breakout_tolerance_atr=0.16,
            min_channel_height_atr=1.00,
        )
    return TrendlineConfig()


def read_ohlcv(path: Path) -> pl.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pl.read_parquet(path)
    return pl.read_csv(path)


def prepare_ohlcv(df: pl.DataFrame) -> pl.DataFrame:
    """标准化 OHLCV 字段。实时安全，不使用未来 K 线。"""

    if "open_time" in df.columns and "timestamp" not in df.columns:
        df = df.rename({"open_time": "timestamp"})

    exprs: list[pl.Expr] = []
    if "timestamp" in df.columns:
        if df.schema["timestamp"] != pl.Datetime:
            exprs.append(pl.from_epoch("timestamp", time_unit="ms").alias("timestamp"))
        exprs.append(pl.col("timestamp").dt.replace_time_zone(None).alias("timestamp"))
    if "close_time" in df.columns:
        if df.schema["close_time"] != pl.Datetime:
            exprs.append(pl.from_epoch("close_time", time_unit="ms").alias("close_time"))
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


def _ols_slope_intercept(values: list[float], sum_x: float, sum_x2: float) -> tuple[float, float]:
    n = len(values)
    sum_y = 0.0
    sum_xy = 0.0
    for idx, value in enumerate(values):
        sum_y += value
        sum_xy += idx * value

    denominator = n * sum_x2 - sum_x * sum_x
    if abs(denominator) < 1e-12:
        intercept = values[-1] if values else 0.0
        return 0.0, intercept

    slope = (n * sum_xy - sum_x * sum_y) / denominator
    intercept = (sum_y - slope * sum_x) / n
    return slope, intercept


def _fit_channel_window(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    atr: float,
    start_idx: int,
    end_idx: int,
    config: TrendlineConfig,
    sum_x: float,
    sum_x2: float,
) -> dict[str, object]:
    window_high = highs[start_idx:end_idx]
    window_low = lows[start_idx:end_idx]
    window_close = closes[start_idx:end_idx]
    window_len = len(window_close)
    slope, intercept = _ols_slope_intercept(window_close, sum_x=sum_x, sum_x2=sum_x2)

    support_offset = float("inf")
    resistance_offset = float("-inf")
    for idx in range(window_len):
        base = slope * idx + intercept
        support_offset = min(support_offset, window_low[idx] - base)
        resistance_offset = max(resistance_offset, window_high[idx] - base)

    touch_tol = config.touch_tolerance_atr * atr
    support_candidates: list[int] = []
    resistance_candidates: list[int] = []
    for idx in range(window_len):
        base = slope * idx + intercept
        support_price = base + support_offset
        resistance_price = base + resistance_offset
        if abs(window_low[idx] - support_price) <= touch_tol:
            support_candidates.append(start_idx + idx)
        if abs(window_high[idx] - resistance_price) <= touch_tol:
            resistance_candidates.append(start_idx + idx)

    support_touches = _group_touch_indices(support_candidates, config.touch_separation_bars)
    resistance_touches = _group_touch_indices(resistance_candidates, config.touch_separation_bars)

    support_start = intercept + support_offset
    support_prev = slope * (window_len - 1) + intercept + support_offset
    support_curr = slope * window_len + intercept + support_offset
    resistance_start = intercept + resistance_offset
    resistance_prev = slope * (window_len - 1) + intercept + resistance_offset
    resistance_curr = slope * window_len + intercept + resistance_offset
    channel_height = resistance_curr - support_curr
    channel_height_atr = channel_height / atr if atr > 0 else 0.0
    slope_atr = slope / atr if atr > 0 else 0.0

    if slope_atr > config.slope_flat_threshold_atr_per_bar:
        channel_type = "ascending"
    elif slope_atr < -config.slope_flat_threshold_atr_per_bar:
        channel_type = "descending"
    else:
        channel_type = "flat"

    return {
        "window_start_idx": start_idx,
        "window_end_idx": end_idx - 1,
        "slope": slope,
        "intercept": intercept,
        "support_offset": support_offset,
        "resistance_offset": resistance_offset,
        "support_line_start": support_start,
        "support_line_prev": support_prev,
        "support_line_current": support_curr,
        "resistance_line_start": resistance_start,
        "resistance_line_prev": resistance_prev,
        "resistance_line_current": resistance_curr,
        "channel_height": channel_height,
        "channel_height_atr": channel_height_atr,
        "slope_atr": slope_atr,
        "channel_type": channel_type,
        "support_touch_indices": support_touches,
        "resistance_touch_indices": resistance_touches,
        "support_touch_count": len(support_touches),
        "resistance_touch_count": len(resistance_touches),
    }


def _compute_trendline_state(
    df: pl.DataFrame,
    config: TrendlineConfig,
) -> dict[str, list[object]]:
    highs = [float(value) for value in df["high"].to_list()]
    lows = [float(value) for value in df["low"].to_list()]
    closes = [float(value) for value in df["close"].to_list()]
    atrs = [float(value) if value is not None else 0.0 for value in df["atr"].to_list()]
    n = len(closes)

    x_values = list(range(config.line_window))
    sum_x = float(sum(x_values))
    sum_x2 = float(sum(value * value for value in x_values))

    columns: dict[str, list[object]] = {
        "trend_window_start_idx": [None] * n,
        "trend_window_end_idx": [None] * n,
        "trend_slope": [None] * n,
        "trend_slope_atr": [None] * n,
        "trend_channel_type": [None] * n,
        "support_line_start": [None] * n,
        "support_line_prev": [None] * n,
        "support_line_current": [None] * n,
        "resistance_line_start": [None] * n,
        "resistance_line_prev": [None] * n,
        "resistance_line_current": [None] * n,
        "channel_height": [None] * n,
        "channel_height_atr": [None] * n,
        "support_touch_count": [0] * n,
        "resistance_touch_count": [0] * n,
        "support_touch_1_idx": [None] * n,
        "support_touch_2_idx": [None] * n,
        "support_touch_3_idx": [None] * n,
        "resistance_touch_1_idx": [None] * n,
        "resistance_touch_2_idx": [None] * n,
        "resistance_touch_3_idx": [None] * n,
    }

    for current_idx in range(config.line_window, n):
        atr = max(atrs[current_idx], 1e-9)
        start_idx = current_idx - config.line_window
        fit = _fit_channel_window(
            highs=highs,
            lows=lows,
            closes=closes,
            atr=atr,
            start_idx=start_idx,
            end_idx=current_idx,
            config=config,
            sum_x=sum_x,
            sum_x2=sum_x2,
        )

        columns["trend_window_start_idx"][current_idx] = fit["window_start_idx"]
        columns["trend_window_end_idx"][current_idx] = fit["window_end_idx"]
        columns["trend_slope"][current_idx] = fit["slope"]
        columns["trend_slope_atr"][current_idx] = fit["slope_atr"]
        columns["trend_channel_type"][current_idx] = fit["channel_type"]
        columns["support_line_start"][current_idx] = fit["support_line_start"]
        columns["support_line_prev"][current_idx] = fit["support_line_prev"]
        columns["support_line_current"][current_idx] = fit["support_line_current"]
        columns["resistance_line_start"][current_idx] = fit["resistance_line_start"]
        columns["resistance_line_prev"][current_idx] = fit["resistance_line_prev"]
        columns["resistance_line_current"][current_idx] = fit["resistance_line_current"]
        columns["channel_height"][current_idx] = fit["channel_height"]
        columns["channel_height_atr"][current_idx] = fit["channel_height_atr"]
        columns["support_touch_count"][current_idx] = fit["support_touch_count"]
        columns["resistance_touch_count"][current_idx] = fit["resistance_touch_count"]

        support_touches = fit["support_touch_indices"]
        resistance_touches = fit["resistance_touch_indices"]
        for slot, value in enumerate(support_touches[:3], start=1):
            columns[f"support_touch_{slot}_idx"][current_idx] = value
        for slot, value in enumerate(resistance_touches[:3], start=1):
            columns[f"resistance_touch_{slot}_idx"][current_idx] = value

    return columns


def add_trendline_features(
    df: pl.DataFrame,
    config: TrendlineConfig,
) -> pl.DataFrame:
    """生成趋势线识别特征。实时安全，不使用未来 K 线。"""

    prev_close = pl.col("close").shift(1)

    df = (
        df.with_row_index("idx")
        .with_columns(
            (pl.col("high") - pl.col("low")).alias("bar_range"),
            (pl.col("close") - pl.col("open")).abs().alias("body_size"),
            pl.max_horizontal(
                pl.col("high") - pl.col("low"),
                (pl.col("high") - prev_close).abs(),
                (pl.col("low") - prev_close).abs(),
            ).alias("true_range"),
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
        )
    )

    state_cols = _compute_trendline_state(df, config=config)
    return df.with_columns(
        *[
            pl.Series(name=column_name, values=values)
            for column_name, values in state_cols.items()
        ]
    )


def detect_trendline_breakouts(
    df: pl.DataFrame,
    config: TrendlineConfig,
) -> pl.DataFrame:
    """检测趋势线与突破信号。实时安全，不使用未来 K 线。"""

    df = add_trendline_features(df, config=config)

    df = df.with_columns(
        (
            (pl.col("support_touch_count") >= config.min_touches)
            & (pl.col("channel_height_atr") >= config.min_channel_height_atr)
            & (pl.col("channel_height_atr") <= config.max_channel_height_atr)
        ).alias("support_line_valid"),
        (
            (pl.col("resistance_touch_count") >= config.min_touches)
            & (pl.col("channel_height_atr") >= config.min_channel_height_atr)
            & (pl.col("channel_height_atr") <= config.max_channel_height_atr)
        ).alias("resistance_line_valid"),
    ).with_columns(
        (
            pl.col("support_line_valid") & pl.col("resistance_line_valid")
        ).alias("trend_channel_valid"),
    )

    df = df.with_columns(
        (
            pl.col("trend_channel_valid")
            & (pl.col("close").shift(1) <= pl.col("resistance_line_prev") + config.breakout_tolerance_atr * pl.col("atr"))
            & (pl.col("close") > pl.col("resistance_line_current") + config.breakout_tolerance_atr * pl.col("atr"))
            & (pl.col("close_pos") >= 0.60)
            & (pl.col("body_ratio") >= 0.45)
        ).alias("raw_bullish_breakout_signal"),
        (
            pl.col("trend_channel_valid")
            & (pl.col("close").shift(1) >= pl.col("support_line_prev") - config.breakout_tolerance_atr * pl.col("atr"))
            & (pl.col("close") < pl.col("support_line_current") - config.breakout_tolerance_atr * pl.col("atr"))
            & (pl.col("close_pos") <= 0.40)
            & (pl.col("body_ratio") >= 0.45)
        ).alias("raw_bearish_breakdown_signal"),
    )

    return df.with_columns(
        (
            pl.col("raw_bullish_breakout_signal")
            & ~pl.col("raw_bullish_breakout_signal").shift(1).fill_null(False)
        ).alias("bullish_breakout_signal"),
        (
            pl.col("raw_bearish_breakdown_signal")
            & ~pl.col("raw_bearish_breakdown_signal").shift(1).fill_null(False)
        ).alias("bearish_breakdown_signal"),
    )


def label_trendline_outcomes(
    df: pl.DataFrame,
    config: TrendlineConfig,
) -> pl.DataFrame:
    """添加事后标签。使用未来 K 线，只用于研究与评估。"""

    if "bullish_breakout_signal" not in df.columns:
        df = detect_trendline_breakouts(df, config=config)

    future_low_expr = pl.min_horizontal(
        *[pl.col("low").shift(-step) for step in range(1, config.label_lookahead + 1)]
    )
    future_high_expr = pl.max_horizontal(
        *[pl.col("high").shift(-step) for step in range(1, config.label_lookahead + 1)]
    )

    df = df.with_columns(
        future_low_expr.alias("future_low_n"),
        future_high_expr.alias("future_high_n"),
    )

    measured_distance = pl.max_horizontal(
        pl.col("atr"),
        pl.col("channel_height") * config.measured_move_factor,
    )

    return df.with_columns(
        (
            pl.col("bullish_breakout_signal")
            & (pl.col("future_high_n") >= pl.col("close") + measured_distance)
        ).alias("bullish_measured_move_hit"),
        (
            pl.col("bearish_breakdown_signal")
            & (pl.col("future_low_n") <= pl.col("close") - measured_distance)
        ).alias("bearish_measured_move_hit"),
        (
            pl.col("bullish_breakout_signal")
            & (pl.col("future_low_n") <= pl.col("resistance_line_current"))
        ).alias("bullish_breakout_failure"),
        (
            pl.col("bearish_breakdown_signal")
            & (pl.col("future_high_n") >= pl.col("support_line_current"))
        ).alias("bearish_breakdown_failure"),
    )


def summarize_detection(df: pl.DataFrame) -> dict[str, int]:
    summary = df.select(
        pl.len().alias("rows"),
        pl.col("support_line_valid").sum().alias("support_line_valid"),
        pl.col("resistance_line_valid").sum().alias("resistance_line_valid"),
        pl.col("trend_channel_valid").sum().alias("trend_channel_valid"),
        pl.col("bullish_breakout_signal").sum().alias("bullish_breakout_signal"),
        pl.col("bearish_breakdown_signal").sum().alias("bearish_breakdown_signal"),
        *(
            [
                pl.col("bullish_measured_move_hit").sum().alias("bullish_measured_move_hit"),
                pl.col("bearish_measured_move_hit").sum().alias("bearish_measured_move_hit"),
                pl.col("bullish_breakout_failure").sum().alias("bullish_breakout_failure"),
                pl.col("bearish_breakdown_failure").sum().alias("bearish_breakdown_failure"),
            ]
            if "bullish_measured_move_hit" in df.columns
            else []
        ),
    ).to_dicts()[0]
    return {key: int(value) for key, value in summary.items()}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="检测趋势线与趋势线突破。")
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
    df = detect_trendline_breakouts(df, config=config)
    if args.with_outcomes:
        df = label_trendline_outcomes(df, config=config)

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
