from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import polars as pl


@dataclass(frozen=True)
class WedgeConfig:
    atr_window: int = 20
    trend_window: int = 20
    overlap_window: int = 12
    breakout_lookback: int = 5
    min_push_spacing: int = 3
    max_push_age: int = 40
    push_pullback_atr: float = 0.35
    trend_score_min: int = 12
    climax_count_min: int = 4
    overlap_ratio_min: float = 0.45
    compression_ratio_max: float = 0.90
    wedge_window: int = 24
    recent_window: int = 8
    sr_window: int = 60
    sr_tolerance_atr: float = 0.80
    label_lookahead: int = 10
    signal_fresh_bars: int = 6


def get_config(preset: str) -> WedgeConfig:
    if preset == "adjusted":
        return WedgeConfig(
            trend_score_min=10,
            climax_count_min=3,
            overlap_ratio_min=0.35,
            compression_ratio_max=1.00,
            signal_fresh_bars=8,
        )
    return WedgeConfig()


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


def _compute_push_state(
    df: pl.DataFrame,
    config: WedgeConfig,
    side: str,
) -> dict[str, list[int | float | None]]:
    highs = df["high"].to_list()
    lows = df["low"].to_list()
    atrs = df["atr"].to_list()
    n = len(highs)

    count_values: list[int] = []
    first_idx_values: list[int | None] = []
    second_idx_values: list[int | None] = []
    third_idx_values: list[int | None] = []
    third_price_values: list[float | None] = []
    bars_since_third_values: list[int | None] = []

    pushes: list[tuple[int, float]] = []

    for i in range(n):
        atr = float(atrs[i] or 0.0)
        new_price = float(highs[i] if side == "up" else lows[i])
        reference = highs if side == "up" else lows

        breakout = False
        if i >= config.breakout_lookback:
            ref_slice = reference[i - config.breakout_lookback : i]
            if side == "up":
                breakout = new_price >= max(ref_slice)
            else:
                breakout = new_price <= min(ref_slice)

        if breakout:
            if not pushes:
                pushes.append((i, new_price))
            else:
                prev_idx, prev_price = pushes[-1]
                if i - prev_idx < config.min_push_spacing:
                    if (side == "up" and new_price >= prev_price) or (
                        side == "down" and new_price <= prev_price
                    ):
                        pushes[-1] = (i, new_price)
                else:
                    if side == "up":
                        pullback_price = min(float(value) for value in lows[prev_idx : i + 1])
                        pullback_ok = pullback_price <= prev_price - config.push_pullback_atr * atr
                        extension_ok = new_price >= prev_price + 0.10 * atr
                    else:
                        pullback_price = max(float(value) for value in highs[prev_idx : i + 1])
                        pullback_ok = pullback_price >= prev_price + config.push_pullback_atr * atr
                        extension_ok = new_price <= prev_price - 0.10 * atr

                    if pullback_ok and extension_ok:
                        pushes.append((i, new_price))
                    elif (side == "up" and new_price > prev_price) or (
                        side == "down" and new_price < prev_price
                    ):
                        pushes[-1] = (i, new_price)

        pushes = [push for push in pushes if i - push[0] <= config.max_push_age]
        if len(pushes) > 3:
            pushes = pushes[-3:]

        count_values.append(min(len(pushes), 3))

        idx_slots: list[int | None] = [None, None, None]
        price_slots: list[float | None] = [None, None, None]
        for slot, (push_idx, push_price) in enumerate(pushes[-3:]):
            idx_slots[slot] = push_idx
            price_slots[slot] = push_price

        first_idx_values.append(idx_slots[0])
        second_idx_values.append(idx_slots[1])
        third_idx_values.append(idx_slots[2])
        third_price_values.append(price_slots[2])
        if idx_slots[2] is None:
            bars_since_third_values.append(None)
        else:
            bars_since_third_values.append(i - idx_slots[2])

    prefix = "up" if side == "up" else "down"
    return {
        f"{prefix}_push_count": count_values,
        f"{prefix}_first_push_idx": first_idx_values,
        f"{prefix}_second_push_idx": second_idx_values,
        f"{prefix}_third_push_idx": third_idx_values,
        f"{prefix}_third_push_price": third_price_values,
        f"bars_since_{prefix}_third": bars_since_third_values,
    }


def add_wedge_features(
    df: pl.DataFrame,
    config: WedgeConfig,
) -> pl.DataFrame:
    """生成楔形所需基础特征。实时安全，不使用未来 K 线。"""

    prev_close = pl.col("close").shift(1)
    prev_high = pl.col("high").shift(1)
    prev_low = pl.col("low").shift(1)

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
            pl.col("close").ewm_mean(span=20, adjust=False).alias("ema20"),
            pl.col("close").ewm_mean(span=50, adjust=False).alias("ema50"),
            pl.col("high").rolling_max(window_size=config.wedge_window).alias("wedge_high"),
            pl.col("low").rolling_min(window_size=config.wedge_window).alias("wedge_low"),
            pl.col("high").rolling_max(window_size=config.recent_window).alias("recent_high"),
            pl.col("low").rolling_min(window_size=config.recent_window).alias("recent_low"),
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
            (pl.col("wedge_high") - pl.col("wedge_low")).alias("wedge_range"),
            (pl.col("recent_high") - pl.col("recent_low")).alias("recent_range"),
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
            .rolling_mean(window_size=config.overlap_window)
            .alias("overlap_ratio"),
            pl.when(pl.col("wedge_range") > 0)
            .then(pl.col("recent_range") / pl.col("wedge_range"))
            .otherwise(1.0)
            .alias("compression_ratio"),
            (
                (pl.col("sr_high") - pl.col("close"))
                <= config.sr_tolerance_atr * pl.col("atr")
            ).alias("near_resistance"),
            (
                (pl.col("close") - pl.col("sr_low"))
                <= config.sr_tolerance_atr * pl.col("atr")
            ).alias("near_support"),
        )
    )

    push_cols = {
        **_compute_push_state(df, config=config, side="up"),
        **_compute_push_state(df, config=config, side="down"),
    }
    return df.with_columns(
        *[
            pl.Series(name=column_name, values=values)
            for column_name, values in push_cols.items()
        ]
    )


def detect_wedge(
    df: pl.DataFrame,
    config: WedgeConfig,
) -> pl.DataFrame:
    """检测楔形顶/底及反向触发。实时安全，不使用未来 K 线。"""

    df = add_wedge_features(df, config=config)

    df = df.with_columns(
        (
            (pl.col("up_push_count") >= 3)
            & (pl.col("bars_since_up_third") <= config.signal_fresh_bars)
            & (pl.col("uptrend_score") >= config.trend_score_min)
            & (pl.col("overlap_ratio") >= config.overlap_ratio_min)
            & (pl.col("compression_ratio") <= config.compression_ratio_max)
            & (
                pl.col("near_resistance")
                | (pl.col("buy_climax_count") >= config.climax_count_min)
            )
        ).alias("wedge_top_setup"),
        (
            (pl.col("down_push_count") >= 3)
            & (pl.col("bars_since_down_third") <= config.signal_fresh_bars)
            & (pl.col("downtrend_score") >= config.trend_score_min)
            & (pl.col("overlap_ratio") >= config.overlap_ratio_min)
            & (pl.col("compression_ratio") <= config.compression_ratio_max)
            & (
                pl.col("near_support")
                | (pl.col("sell_climax_count") >= config.climax_count_min)
            )
        ).alias("wedge_bottom_setup"),
    )

    df = df.with_columns(
        (
            pl.col("wedge_top_setup")
            & (pl.col("close") < pl.col("low").shift(1))
            & (pl.col("close") < pl.col("ema20"))
            & (pl.col("close_pos") <= 0.45)
        ).alias("raw_wedge_top_signal"),
        (
            pl.col("wedge_bottom_setup")
            & (pl.col("close") > pl.col("high").shift(1))
            & (pl.col("close") > pl.col("ema20"))
            & (pl.col("close_pos") >= 0.55)
        ).alias("raw_wedge_bottom_signal"),
    )

    return df.with_columns(
        (
            pl.col("raw_wedge_top_signal")
            & ~pl.col("raw_wedge_top_signal").shift(1).fill_null(False)
        ).alias("wedge_top_signal"),
        (
            pl.col("raw_wedge_bottom_signal")
            & ~pl.col("raw_wedge_bottom_signal").shift(1).fill_null(False)
        ).alias("wedge_bottom_signal"),
    )


def label_wedge_outcomes(
    df: pl.DataFrame,
    config: WedgeConfig,
) -> pl.DataFrame:
    """添加事后标签。使用未来 K 线，只用于研究与评估。"""

    if "wedge_top_signal" not in df.columns:
        df = detect_wedge(df, config=config)

    df = df.with_columns(
        pl.col("low")
        .shift(-1)
        .rolling_min(window_size=config.label_lookahead)
        .alias("future_low_n"),
        pl.col("high")
        .shift(-1)
        .rolling_max(window_size=config.label_lookahead)
        .alias("future_high_n"),
    )

    return df.with_columns(
        (
            pl.col("wedge_top_signal")
            & (
                pl.col("future_low_n")
                <= pl.col("close")
                - pl.max_horizontal(pl.col("atr"), pl.col("wedge_range") * 0.50)
            )
        ).alias("wedge_top_follow_through"),
        (
            pl.col("wedge_bottom_signal")
            & (
                pl.col("future_high_n")
                >= pl.col("close")
                + pl.max_horizontal(pl.col("atr"), pl.col("wedge_range") * 0.50)
            )
        ).alias("wedge_bottom_follow_through"),
    )


def summarize_detection(df: pl.DataFrame) -> dict[str, int]:
    summary = df.select(
        pl.len().alias("rows"),
        pl.col("wedge_top_setup").sum().alias("wedge_top_setup"),
        pl.col("wedge_bottom_setup").sum().alias("wedge_bottom_setup"),
        pl.col("wedge_top_signal").sum().alias("wedge_top_signal"),
        pl.col("wedge_bottom_signal").sum().alias("wedge_bottom_signal"),
        *(
            [
                pl.col("wedge_top_follow_through").sum().alias("wedge_top_follow_through"),
                pl.col("wedge_bottom_follow_through").sum().alias("wedge_bottom_follow_through"),
            ]
            if "wedge_top_follow_through" in df.columns
            else []
        ),
    ).to_dicts()[0]
    return {key: int(value) for key, value in summary.items()}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="检测阿布课程中的楔形。")
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
    df = detect_wedge(df, config=config)
    if args.with_outcomes:
        df = label_wedge_outcomes(df, config=config)

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
