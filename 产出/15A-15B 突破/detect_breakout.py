from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import polars as pl


@dataclass(frozen=True)
class BreakoutConfig:
    atr_window: int = 20
    range_window: int = 20
    confirm_bars: int = 4
    min_touches: int = 2
    touch_tolerance_atr: float = 0.18
    touch_separation_bars: int = 3
    breakout_buffer_atr: float = 0.08
    breakout_close_pos: float = 0.65
    breakout_body_ratio: float = 0.55
    followthrough_close_pos: float = 0.58
    followthrough_body_ratio: float = 0.35
    followthrough_buffer_atr: float = 0.04
    failure_reentry_atr: float = 0.12
    range_height_atr_min: float = 1.00
    range_height_atr_max: float = 7.00
    range_overlap_min: float = 0.55
    range_ema_gap_atr_max: float = 0.90
    label_lookahead: int = 24
    continuation_measured_move_factor: float = 0.85
    reversal_measured_move_factor: float = 0.55


def get_config(preset: str) -> BreakoutConfig:
    if preset == "adjusted":
        return BreakoutConfig(
            min_touches=2,
            touch_tolerance_atr=0.22,
            breakout_buffer_atr=0.05,
            breakout_close_pos=0.60,
            breakout_body_ratio=0.48,
            followthrough_close_pos=0.54,
            range_overlap_min=0.50,
            range_ema_gap_atr_max=1.05,
        )
    return BreakoutConfig()


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


def _compute_range_state(
    df: pl.DataFrame,
    config: BreakoutConfig,
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
        range_height_atr = range_height / atr if atr > 0 else 0.0
        touch_tol = config.touch_tolerance_atr * atr

        support_candidates: list[int] = []
        resistance_candidates: list[int] = []
        for idx in range(start_idx, end_idx):
            if abs(lows[idx] - range_low) <= touch_tol:
                support_candidates.append(idx)
            if abs(highs[idx] - range_high) <= touch_tol:
                resistance_candidates.append(idx)

        support_touches = _group_touch_indices(support_candidates, config.touch_separation_bars)
        resistance_touches = _group_touch_indices(resistance_candidates, config.touch_separation_bars)
        support_tail = support_touches[-3:]
        resistance_tail = resistance_touches[-3:]

        columns["range_window_start_idx"][current_idx] = start_idx
        columns["range_window_end_idx"][current_idx] = end_idx - 1
        columns["range_high_pre"][current_idx] = range_high
        columns["range_low_pre"][current_idx] = range_low
        columns["range_mid_pre"][current_idx] = range_mid
        columns["range_height_pre"][current_idx] = range_height
        columns["range_height_atr"][current_idx] = range_height_atr
        columns["support_touch_count"][current_idx] = len(support_touches)
        columns["resistance_touch_count"][current_idx] = len(resistance_touches)

        for slot, value in enumerate(support_tail, start=1):
            columns[f"support_touch_{slot}_idx"][current_idx] = value
        for slot, value in enumerate(resistance_tail, start=1):
            columns[f"resistance_touch_{slot}_idx"][current_idx] = value

    return columns


def add_breakout_features(
    df: pl.DataFrame,
    config: BreakoutConfig,
) -> pl.DataFrame:
    """生成突破识别特征。实时安全，不使用未来 K 线。"""

    prev_close = pl.col("close").shift(1)
    prev_high = pl.col("high").shift(1)
    prev_low = pl.col("low").shift(1)

    df = (
        df.with_row_index("idx")
        .with_columns(
            (pl.col("high") - pl.col("low")).alias("bar_range"),
            (pl.col("close") - pl.col("open")).abs().alias("body_size"),
            (
                pl.col("high") - pl.max_horizontal(pl.col("open"), pl.col("close"))
            ).alias("upper_wick"),
            (
                pl.min_horizontal(pl.col("open"), pl.col("close")) - pl.col("low")
            ).alias("lower_wick"),
            pl.max_horizontal(
                pl.col("high") - pl.col("low"),
                (pl.col("high") - prev_close).abs(),
                (pl.col("low") - prev_close).abs(),
            ).alias("true_range"),
            pl.col("close").ewm_mean(span=20, adjust=False).alias("ema20"),
            pl.col("close").ewm_mean(span=50, adjust=False).alias("ema50"),
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
        )
        .with_columns(
            pl.col("overlap_with_prev")
            .rolling_mean(window_size=config.range_window)
            .alias("overlap_ratio"),
            pl.when(pl.col("atr") > 0)
            .then((pl.col("ema20") - pl.col("ema50")).abs() / pl.col("atr"))
            .otherwise(None)
            .alias("ema_gap_atr"),
        )
    )

    state_cols = _compute_range_state(df, config=config)
    df = df.with_columns(
        *[
            pl.Series(name=column_name, values=values)
            for column_name, values in state_cols.items()
        ]
    )

    return df.with_columns(
        (
            (pl.col("range_height_atr") >= config.range_height_atr_min)
            & (pl.col("range_height_atr") <= config.range_height_atr_max)
            & (pl.col("support_touch_count") >= config.min_touches)
            & (pl.col("resistance_touch_count") >= config.min_touches)
        ).alias("range_boundary_valid"),
        (
            (pl.col("range_height_atr") >= config.range_height_atr_min)
            & (pl.col("range_height_atr") <= config.range_height_atr_max)
            & (pl.col("overlap_ratio") >= config.range_overlap_min)
            & (pl.col("ema_gap_atr") <= config.range_ema_gap_atr_max)
        ).alias("range_context"),
    )


def _scan_breakouts(
    df: pl.DataFrame,
    config: BreakoutConfig,
) -> dict[str, list[object]]:
    data = {
        "open": [float(value) for value in df["open"].to_list()],
        "high": [float(value) for value in df["high"].to_list()],
        "low": [float(value) for value in df["low"].to_list()],
        "close": [float(value) for value in df["close"].to_list()],
        "atr": [float(value) if value is not None else 0.0 for value in df["atr"].to_list()],
        "close_pos": [float(value) if value is not None else 0.5 for value in df["close_pos"].to_list()],
        "body_ratio": [float(value) if value is not None else 0.0 for value in df["body_ratio"].to_list()],
        "range_high_pre": [value for value in df["range_high_pre"].to_list()],
        "range_low_pre": [value for value in df["range_low_pre"].to_list()],
        "range_boundary_valid": [bool(value) if value is not None else False for value in df["range_boundary_valid"].to_list()],
        "range_context": [bool(value) if value is not None else False for value in df["range_context"].to_list()],
    }
    n = len(data["close"])

    columns: dict[str, list[object]] = {
        "bull_breakout_bar": [False] * n,
        "bear_breakout_bar": [False] * n,
        "bull_followthrough_signal": [False] * n,
        "bear_followthrough_signal": [False] * n,
        "bull_failed_breakout_signal": [False] * n,
        "bear_failed_breakout_signal": [False] * n,
        "bull_followthrough_breakout_idx": [None] * n,
        "bear_followthrough_breakout_idx": [None] * n,
        "bull_failed_breakout_idx": [None] * n,
        "bear_failed_breakout_idx": [None] * n,
    }

    active: list[dict[str, object]] = []
    last_breakout_idx = {"up": -10_000, "down": -10_000}

    for current_idx in range(n):
        next_active: list[dict[str, object]] = []
        for candidate in active:
            breakout_idx = int(candidate["breakout_idx"])
            direction = str(candidate["direction"])
            range_context = bool(candidate["range_context"])
            if current_idx <= breakout_idx:
                next_active.append(candidate)
                continue
            if current_idx > int(candidate["expires"]):
                continue

            atr = max(data["atr"][current_idx], 1e-9)
            close = data["close"][current_idx]
            open_price = data["open"][current_idx]
            close_pos = data["close_pos"][current_idx]
            body_ratio = data["body_ratio"][current_idx]
            breakout_high = data["high"][breakout_idx]
            breakout_low = data["low"][breakout_idx]
            range_high = float(data["range_high_pre"][breakout_idx])
            range_low = float(data["range_low_pre"][breakout_idx])

            signaled = False
            if direction == "up":
                followthrough = (
                    close >= data["close"][breakout_idx] + config.followthrough_buffer_atr * atr
                    and (
                        data["high"][current_idx] > breakout_high
                        or close >= breakout_high - config.followthrough_buffer_atr * atr
                    )
                    and close_pos >= config.followthrough_close_pos
                    and (body_ratio >= config.followthrough_body_ratio or close > breakout_high)
                )
                failed = (
                    range_context
                    and close < range_high - config.failure_reentry_atr * atr
                    and ((close < open_price) or (close_pos <= 0.45))
                )
                if followthrough:
                    columns["bull_followthrough_signal"][current_idx] = True
                    columns["bull_followthrough_breakout_idx"][current_idx] = breakout_idx
                    signaled = True
                elif failed:
                    columns["bull_failed_breakout_signal"][current_idx] = True
                    columns["bull_failed_breakout_idx"][current_idx] = breakout_idx
                    signaled = True
            else:
                followthrough = (
                    close <= data["close"][breakout_idx] - config.followthrough_buffer_atr * atr
                    and (
                        data["low"][current_idx] < breakout_low
                        or close <= breakout_low + config.followthrough_buffer_atr * atr
                    )
                    and close_pos <= (1.0 - config.followthrough_close_pos)
                    and (body_ratio >= config.followthrough_body_ratio or close < breakout_low)
                )
                failed = (
                    range_context
                    and close > range_low + config.failure_reentry_atr * atr
                    and ((close > open_price) or (close_pos >= 0.55))
                )
                if followthrough:
                    columns["bear_followthrough_signal"][current_idx] = True
                    columns["bear_followthrough_breakout_idx"][current_idx] = breakout_idx
                    signaled = True
                elif failed:
                    columns["bear_failed_breakout_signal"][current_idx] = True
                    columns["bear_failed_breakout_idx"][current_idx] = breakout_idx
                    signaled = True

            if not signaled and current_idx < int(candidate["expires"]):
                next_active.append(candidate)

        active = next_active

        atr = max(data["atr"][current_idx], 1e-9)
        range_high = data["range_high_pre"][current_idx]
        range_low = data["range_low_pre"][current_idx]
        if range_high is None or range_low is None or not data["range_boundary_valid"][current_idx]:
            continue

        bull_breakout = (
            current_idx - last_breakout_idx["up"] >= 2
            and data["close"][current_idx] > float(range_high) + config.breakout_buffer_atr * atr
            and data["close_pos"][current_idx] >= config.breakout_close_pos
            and data["body_ratio"][current_idx] >= config.breakout_body_ratio
        )
        bear_breakout = (
            current_idx - last_breakout_idx["down"] >= 2
            and data["close"][current_idx] < float(range_low) - config.breakout_buffer_atr * atr
            and data["close_pos"][current_idx] <= (1.0 - config.breakout_close_pos)
            and data["body_ratio"][current_idx] >= config.breakout_body_ratio
        )

        if bull_breakout:
            columns["bull_breakout_bar"][current_idx] = True
            last_breakout_idx["up"] = current_idx
            active.append(
                {
                    "direction": "up",
                    "breakout_idx": current_idx,
                    "expires": current_idx + config.confirm_bars,
                    "range_context": data["range_context"][current_idx],
                }
            )
        if bear_breakout:
            columns["bear_breakout_bar"][current_idx] = True
            last_breakout_idx["down"] = current_idx
            active.append(
                {
                    "direction": "down",
                    "breakout_idx": current_idx,
                    "expires": current_idx + config.confirm_bars,
                    "range_context": data["range_context"][current_idx],
                }
            )

    return columns


def detect_breakout(
    df: pl.DataFrame,
    config: BreakoutConfig,
) -> pl.DataFrame:
    """检测突破、后续走势确认与失败突破。实时安全，不使用未来 K 线。"""

    df = add_breakout_features(df, config=config)
    scan_cols = _scan_breakouts(df, config=config)
    return df.with_columns(
        *[
            pl.Series(name=column_name, values=values)
            for column_name, values in scan_cols.items()
        ]
    )


def label_breakout_outcomes(
    df: pl.DataFrame,
    config: BreakoutConfig,
) -> pl.DataFrame:
    """添加事后标签。使用未来 K 线，只用于研究与评估。"""

    if "bull_breakout_bar" not in df.columns:
        df = detect_breakout(df, config=config)

    highs = [float(value) for value in df["high"].to_list()]
    lows = [float(value) for value in df["low"].to_list()]
    closes = [float(value) for value in df["close"].to_list()]
    atrs = [float(value) if value is not None else 0.0 for value in df["atr"].to_list()]
    range_highs = [float(value) if value is not None else 0.0 for value in df["range_high_pre"].to_list()]
    range_lows = [float(value) if value is not None else 0.0 for value in df["range_low_pre"].to_list()]
    range_mids = [float(value) if value is not None else 0.0 for value in df["range_mid_pre"].to_list()]
    range_heights = [float(value) if value is not None else 0.0 for value in df["range_height_pre"].to_list()]

    n = len(closes)
    columns: dict[str, list[bool]] = {
        "bull_followthrough_measured_move_hit": [False] * n,
        "bear_followthrough_measured_move_hit": [False] * n,
        "bull_failed_breakout_reversal_hit": [False] * n,
        "bear_failed_breakout_reversal_hit": [False] * n,
        "bull_followthrough_failure": [False] * n,
        "bear_followthrough_failure": [False] * n,
        "bull_failed_breakout_failure": [False] * n,
        "bear_failed_breakout_failure": [False] * n,
    }

    bull_follow_idx = df["bull_followthrough_breakout_idx"].to_list()
    bear_follow_idx = df["bear_followthrough_breakout_idx"].to_list()
    bull_fail_idx = df["bull_failed_breakout_idx"].to_list()
    bear_fail_idx = df["bear_failed_breakout_idx"].to_list()

    for current_idx in range(n):
        future_slice = slice(current_idx + 1, min(n, current_idx + 1 + config.label_lookahead))
        future_high = max(highs[future_slice], default=None)
        future_low = min(lows[future_slice], default=None)
        if future_high is None or future_low is None:
            continue

        if df["bull_followthrough_signal"][current_idx]:
            breakout_idx = int(bull_follow_idx[current_idx])
            measured_distance = max(
                atrs[current_idx],
                range_heights[breakout_idx] * config.continuation_measured_move_factor,
            )
            columns["bull_followthrough_measured_move_hit"][current_idx] = (
                future_high >= closes[current_idx] + measured_distance
            )
            columns["bull_followthrough_failure"][current_idx] = (
                future_low <= range_highs[breakout_idx]
            )

        if df["bear_followthrough_signal"][current_idx]:
            breakout_idx = int(bear_follow_idx[current_idx])
            measured_distance = max(
                atrs[current_idx],
                range_heights[breakout_idx] * config.continuation_measured_move_factor,
            )
            columns["bear_followthrough_measured_move_hit"][current_idx] = (
                future_low <= closes[current_idx] - measured_distance
            )
            columns["bear_followthrough_failure"][current_idx] = (
                future_high >= range_lows[breakout_idx]
            )

        if df["bull_failed_breakout_signal"][current_idx]:
            breakout_idx = int(bull_fail_idx[current_idx])
            reversal_distance = max(
                atrs[current_idx],
                range_heights[breakout_idx] * config.reversal_measured_move_factor,
            )
            columns["bull_failed_breakout_reversal_hit"][current_idx] = (
                future_low <= min(range_mids[breakout_idx], closes[current_idx] - reversal_distance)
            )
            columns["bull_failed_breakout_failure"][current_idx] = (
                future_high >= highs[breakout_idx]
            )

        if df["bear_failed_breakout_signal"][current_idx]:
            breakout_idx = int(bear_fail_idx[current_idx])
            reversal_distance = max(
                atrs[current_idx],
                range_heights[breakout_idx] * config.reversal_measured_move_factor,
            )
            columns["bear_failed_breakout_reversal_hit"][current_idx] = (
                future_high >= max(range_mids[breakout_idx], closes[current_idx] + reversal_distance)
            )
            columns["bear_failed_breakout_failure"][current_idx] = (
                future_low <= lows[breakout_idx]
            )

    return df.with_columns(
        *[
            pl.Series(name=column_name, values=values)
            for column_name, values in columns.items()
        ]
    )


def summarize_detection(df: pl.DataFrame) -> dict[str, int]:
    summary = df.select(
        pl.len().alias("rows"),
        pl.col("range_boundary_valid").sum().alias("range_boundary_valid"),
        pl.col("range_context").sum().alias("range_context"),
        pl.col("bull_breakout_bar").sum().alias("bull_breakout_bar"),
        pl.col("bear_breakout_bar").sum().alias("bear_breakout_bar"),
        pl.col("bull_followthrough_signal").sum().alias("bull_followthrough_signal"),
        pl.col("bear_followthrough_signal").sum().alias("bear_followthrough_signal"),
        pl.col("bull_failed_breakout_signal").sum().alias("bull_failed_breakout_signal"),
        pl.col("bear_failed_breakout_signal").sum().alias("bear_failed_breakout_signal"),
        *(
            [
                pl.col("bull_followthrough_measured_move_hit")
                .sum()
                .alias("bull_followthrough_measured_move_hit"),
                pl.col("bear_followthrough_measured_move_hit")
                .sum()
                .alias("bear_followthrough_measured_move_hit"),
                pl.col("bull_failed_breakout_reversal_hit")
                .sum()
                .alias("bull_failed_breakout_reversal_hit"),
                pl.col("bear_failed_breakout_reversal_hit")
                .sum()
                .alias("bear_failed_breakout_reversal_hit"),
                pl.col("bull_followthrough_failure")
                .sum()
                .alias("bull_followthrough_failure"),
                pl.col("bear_followthrough_failure")
                .sum()
                .alias("bear_followthrough_failure"),
                pl.col("bull_failed_breakout_failure")
                .sum()
                .alias("bull_failed_breakout_failure"),
                pl.col("bear_failed_breakout_failure")
                .sum()
                .alias("bear_failed_breakout_failure"),
            ]
            if "bull_followthrough_measured_move_hit" in df.columns
            else []
        ),
    ).to_dicts()[0]
    return {key: int(value) for key, value in summary.items()}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="检测突破、后续走势与失败突破。")
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
    df = detect_breakout(df, config=config)
    if args.with_outcomes:
        df = label_breakout_outcomes(df, config=config)

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
