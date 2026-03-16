from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import polars as pl


@dataclass(frozen=True)
class PatternConfig:
    order: int = 10
    hold_multiplier: float = 1.0
    max_flag_width_ratio: float = 0.50
    max_flag_height_ratio: float = 0.75


@dataclass
class FlagPattern:
    base_x: int
    base_y: float
    tip_x: int = -1
    tip_y: float = -1.0
    conf_x: int = -1
    conf_y: float = -1.0
    pennant: bool = False
    flag_width: int = -1
    flag_height: float = -1.0
    pole_width: int = -1
    pole_height: float = -1.0
    support_intercept: float = -1.0
    support_slope: float = -1.0
    resist_intercept: float = -1.0
    resist_slope: float = -1.0
    support_pivot_offset: int = -1
    resist_pivot_offset: int = -1
    flag_extreme_offset: int = -1


def get_config(preset: str) -> PatternConfig:
    if preset == "adjusted":
        return PatternConfig(order=8)
    return PatternConfig()


def read_ohlcv(path: Path) -> pl.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pl.read_parquet(path)
    return pl.read_csv(path)


def prepare_ohlcv(df: pl.DataFrame) -> pl.DataFrame:
    """标准化 OHLCV 字段。信号确认时实时安全，不使用确认时点之后的数据。"""

    if "open_time" in df.columns and "timestamp" not in df.columns:
        df = df.rename({"open_time": "timestamp"})
    if "date" in df.columns and "timestamp" not in df.columns:
        df = df.rename({"date": "timestamp"})

    exprs: list[pl.Expr] = []
    if "timestamp" in df.columns:
        if df.schema["timestamp"] == pl.String:
            timestamp_expr = pl.col("timestamp").str.to_datetime(strict=False)
        elif df.schema["timestamp"] != pl.Datetime:
            timestamp_expr = pl.from_epoch("timestamp", time_unit="ms")
        else:
            timestamp_expr = pl.col("timestamp")
        exprs.append(timestamp_expr.dt.replace_time_zone(None).alias("timestamp"))
    if "close_time" in df.columns:
        if df.schema["close_time"] == pl.String:
            close_time_expr = pl.col("close_time").str.to_datetime(strict=False)
        elif df.schema["close_time"] != pl.Datetime:
            close_time_expr = pl.from_epoch("close_time", time_unit="ms")
        else:
            close_time_expr = pl.col("close_time")
        exprs.append(close_time_expr.dt.replace_time_zone(None).alias("close_time"))

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
    return out.sort("timestamp").with_row_index("idx")


def _rw_top(data: list[float], curr_index: int, order: int) -> bool:
    if curr_index < order * 2 + 1:
        return False

    k = curr_index - order
    v = data[k]
    for offset in range(1, order + 1):
        if data[k + offset] > v or data[k - offset] > v:
            return False
    return True


def _rw_bottom(data: list[float], curr_index: int, order: int) -> bool:
    if curr_index < order * 2 + 1:
        return False

    k = curr_index - order
    v = data[k]
    for offset in range(1, order + 1):
        if data[k + offset] < v or data[k - offset] < v:
            return False
    return True


def _check_trend_line(support: bool, pivot: int, slope: float, y: list[float]) -> float:
    intercept = -slope * pivot + y[pivot]
    line_vals = [slope * idx + intercept for idx in range(len(y))]
    diffs = [line_vals[idx] - y[idx] for idx in range(len(y))]

    if support and max(diffs) > 1e-5:
        return -1.0
    if not support and min(diffs) < -1e-5:
        return -1.0
    return sum(diff * diff for diff in diffs)


def _optimize_slope(support: bool, pivot: int, init_slope: float, y: list[float]) -> tuple[float, float]:
    slope_unit = (max(y) - min(y)) / max(len(y), 1)
    opt_step = 1.0
    min_step = 0.0001
    curr_step = opt_step

    best_slope = init_slope
    best_err = _check_trend_line(support, pivot, init_slope, y)
    if best_err < 0:
        raise ValueError("初始斜率无效，无法拟合趋势线。")

    get_derivative = True
    derivative: float | None = None
    while curr_step > min_step:
        if get_derivative:
            slope_change = best_slope + slope_unit * min_step
            test_err = _check_trend_line(support, pivot, slope_change, y)
            derivative = test_err - best_err

            if test_err < 0:
                slope_change = best_slope - slope_unit * min_step
                test_err = _check_trend_line(support, pivot, slope_change, y)
                derivative = best_err - test_err

            if test_err < 0:
                raise ValueError("数值求导失败，无法拟合趋势线。")
            get_derivative = False

        if derivative is None:
            break

        if derivative > 0.0:
            test_slope = best_slope - slope_unit * curr_step
        else:
            test_slope = best_slope + slope_unit * curr_step

        test_err = _check_trend_line(support, pivot, test_slope, y)
        if test_err < 0 or test_err >= best_err:
            curr_step *= 0.5
        else:
            best_err = test_err
            best_slope = test_slope
            get_derivative = True

    return best_slope, -best_slope * pivot + y[pivot]


def _fit_trendlines_single(
    data: list[float],
) -> tuple[tuple[float, float], tuple[float, float], int, int]:
    n = len(data)
    x = list(range(n))
    x_mean = sum(x) / n
    y_mean = sum(data) / n
    numerator = sum((x[idx] - x_mean) * (data[idx] - y_mean) for idx in range(n))
    denominator = sum((x[idx] - x_mean) ** 2 for idx in range(n))
    slope = numerator / denominator if denominator else 0.0
    intercept = y_mean - slope * x_mean

    line_points = [slope * idx + intercept for idx in range(n)]
    upper_pivot = max(range(n), key=lambda idx: data[idx] - line_points[idx])
    lower_pivot = min(range(n), key=lambda idx: data[idx] - line_points[idx])

    support_coefs = _optimize_slope(True, lower_pivot, slope, data)
    resist_coefs = _optimize_slope(False, upper_pivot, slope, data)
    return support_coefs, resist_coefs, lower_pivot, upper_pivot


def _check_bull_pattern_trendline(
    pending: FlagPattern,
    data: list[float],
    current_idx: int,
    config: PatternConfig,
) -> bool:
    if max(data[pending.tip_x + 1 : current_idx], default=float("-inf")) > pending.tip_y:
        return False

    flag_slice = data[pending.tip_x:current_idx]
    if len(flag_slice) < 2:
        return False

    flag_min = min(flag_slice)
    pole_height = pending.tip_y - pending.base_y
    pole_width = pending.tip_x - pending.base_x
    flag_height = pending.tip_y - flag_min
    flag_width = current_idx - pending.tip_x

    if flag_width > pole_width * config.max_flag_width_ratio:
        return False
    if flag_height > pole_height * config.max_flag_height_ratio:
        return False

    support_coefs, resist_coefs, lower_pivot, upper_pivot = _fit_trendlines_single(flag_slice)
    support_slope, support_intercept = support_coefs
    resist_slope, resist_intercept = resist_coefs

    current_resist = resist_intercept + resist_slope * (flag_width + 1)
    if data[current_idx] <= current_resist:
        return False

    pending.pennant = support_slope > 0
    pending.conf_x = current_idx
    pending.conf_y = data[current_idx]
    pending.flag_width = flag_width
    pending.flag_height = flag_height
    pending.pole_width = pole_width
    pending.pole_height = pole_height
    pending.support_slope = support_slope
    pending.support_intercept = support_intercept
    pending.resist_slope = resist_slope
    pending.resist_intercept = resist_intercept
    pending.support_pivot_offset = lower_pivot
    pending.resist_pivot_offset = upper_pivot
    pending.flag_extreme_offset = min(range(len(flag_slice)), key=lambda idx: flag_slice[idx])
    return True


def _check_bear_pattern_trendline(
    pending: FlagPattern,
    data: list[float],
    current_idx: int,
    config: PatternConfig,
) -> bool:
    if min(data[pending.tip_x + 1 : current_idx], default=float("inf")) < pending.tip_y:
        return False

    flag_slice = data[pending.tip_x:current_idx]
    if len(flag_slice) < 2:
        return False

    flag_max = max(flag_slice)
    pole_height = pending.base_y - pending.tip_y
    pole_width = pending.tip_x - pending.base_x
    flag_height = flag_max - pending.tip_y
    flag_width = current_idx - pending.tip_x

    if flag_width > pole_width * config.max_flag_width_ratio:
        return False
    if flag_height > pole_height * config.max_flag_height_ratio:
        return False

    support_coefs, resist_coefs, lower_pivot, upper_pivot = _fit_trendlines_single(flag_slice)
    support_slope, support_intercept = support_coefs
    resist_slope, resist_intercept = resist_coefs

    current_support = support_intercept + support_slope * (flag_width + 1)
    if data[current_idx] >= current_support:
        return False

    pending.pennant = resist_slope < 0
    pending.conf_x = current_idx
    pending.conf_y = data[current_idx]
    pending.flag_width = flag_width
    pending.flag_height = flag_height
    pending.pole_width = pole_width
    pending.pole_height = pole_height
    pending.support_slope = support_slope
    pending.support_intercept = support_intercept
    pending.resist_slope = resist_slope
    pending.resist_intercept = resist_intercept
    pending.support_pivot_offset = lower_pivot
    pending.resist_pivot_offset = upper_pivot
    pending.flag_extreme_offset = max(range(len(flag_slice)), key=lambda idx: flag_slice[idx])
    return True


def extract_flags_pennants(
    df: pl.DataFrame,
    config: PatternConfig,
) -> list[tuple[str, FlagPattern]]:
    """复刻 upstream 的 trendline 版旗形/旗幡检测。信号在 conf_x 当根确认，可实时使用。"""

    log_close = [math.log(float(value)) for value in df["close"].to_list()]
    pending_bull: FlagPattern | None = None
    pending_bear: FlagPattern | None = None
    last_bottom = -1
    last_top = -1
    patterns: list[tuple[str, FlagPattern]] = []

    for current_idx in range(len(log_close)):
        if _rw_top(log_close, current_idx, config.order):
            last_top = current_idx - config.order
            if last_bottom != -1 and last_bottom < last_top:
                pending_bull = FlagPattern(last_bottom, log_close[last_bottom], tip_x=last_top, tip_y=log_close[last_top])

        if _rw_bottom(log_close, current_idx, config.order):
            last_bottom = current_idx - config.order
            if last_top != -1 and last_top < last_bottom:
                pending_bear = FlagPattern(last_top, log_close[last_top], tip_x=last_bottom, tip_y=log_close[last_bottom])

        if pending_bear is not None:
            if _check_bear_pattern_trendline(pending_bear, log_close, current_idx, config):
                family = "bear_pennant" if pending_bear.pennant else "bear_flag"
                patterns.append((family, pending_bear))
                pending_bear = None

        if pending_bull is not None:
            if _check_bull_pattern_trendline(pending_bull, log_close, current_idx, config):
                family = "bull_pennant" if pending_bull.pennant else "bull_flag"
                patterns.append((family, pending_bull))
                pending_bull = None

    return patterns


def patterns_to_frame(
    df: pl.DataFrame,
    patterns: list[tuple[str, FlagPattern]],
    config: PatternConfig,
) -> pl.DataFrame:
    timestamps = df["timestamp"].to_list()
    closes = [float(value) for value in df["close"].to_list()]
    rows: list[dict[str, Any]] = []

    for family, pattern in patterns:
        hold_bars = max(1, int(round(pattern.flag_width * config.hold_multiplier)))
        exit_idx = pattern.conf_x + hold_bars
        if exit_idx >= len(closes):
            exit_idx = len(closes) - 1

        direction = "long" if family.startswith("bull") else "short"
        pattern_kind = "pennant" if "pennant" in family else "flag"
        entry_price = closes[pattern.conf_x]
        exit_price = closes[exit_idx]
        forward_log_return = (
            math.log(exit_price) - math.log(entry_price)
            if direction == "long"
            else math.log(entry_price) - math.log(exit_price)
        )
        support_pivot_idx = (
            pattern.tip_x + pattern.support_pivot_offset
            if pattern.support_pivot_offset >= 0
            else None
        )
        resist_pivot_idx = (
            pattern.tip_x + pattern.resist_pivot_offset
            if pattern.resist_pivot_offset >= 0
            else None
        )
        flag_extreme_idx = (
            pattern.tip_x + pattern.flag_extreme_offset
            if pattern.flag_extreme_offset >= 0
            else None
        )
        support_tip_price = math.exp(pattern.support_intercept)
        support_conf_price = math.exp(
            pattern.support_intercept + pattern.support_slope * pattern.flag_width
        )
        resist_tip_price = math.exp(pattern.resist_intercept)
        resist_conf_price = math.exp(
            pattern.resist_intercept + pattern.resist_slope * pattern.flag_width
        )
        structure_stop_price = support_conf_price if direction == "long" else resist_conf_price

        flag_quality = (
            min(pattern.pole_width / max(pattern.flag_width, 1), 10.0) * 0.4
            + min(pattern.pole_height / max(pattern.flag_height, 1e-9), 10.0) * 0.6
        )

        rows.append(
            {
                "family": family,
                "direction": direction,
                "pattern_kind": pattern_kind,
                "base_idx": pattern.base_x,
                "tip_idx": pattern.tip_x,
                "conf_idx": pattern.conf_x,
                "flag_end_idx": pattern.conf_x - 1,
                "exit_idx": exit_idx,
                "support_pivot_idx": support_pivot_idx,
                "resist_pivot_idx": resist_pivot_idx,
                "flag_extreme_idx": flag_extreme_idx,
                "base_time": timestamps[pattern.base_x].isoformat(),
                "tip_time": timestamps[pattern.tip_x].isoformat(),
                "conf_time": timestamps[pattern.conf_x].isoformat(),
                "exit_time": timestamps[exit_idx].isoformat(),
                "base_price": math.exp(pattern.base_y),
                "tip_price": math.exp(pattern.tip_y),
                "conf_price": math.exp(pattern.conf_y),
                "exit_price": exit_price,
                "support_tip_price": support_tip_price,
                "support_conf_price": support_conf_price,
                "resist_tip_price": resist_tip_price,
                "resist_conf_price": resist_conf_price,
                "flag_extreme_price": closes[flag_extreme_idx] if flag_extreme_idx is not None else None,
                "structure_stop_price": structure_stop_price,
                "flag_width": pattern.flag_width,
                "flag_height_log": pattern.flag_height,
                "flag_height_ratio": pattern.flag_height / max(pattern.pole_height, 1e-9),
                "pole_width": pattern.pole_width,
                "pole_height_log": pattern.pole_height,
                "support_intercept_log": pattern.support_intercept,
                "support_slope": pattern.support_slope,
                "resist_intercept_log": pattern.resist_intercept,
                "resist_slope": pattern.resist_slope,
                "hold_bars": hold_bars,
                "forward_log_return": forward_log_return,
                "flag_quality_score": flag_quality,
            }
        )

    if not rows:
        return pl.DataFrame(
            schema={
                "family": pl.String,
                "direction": pl.String,
                "pattern_kind": pl.String,
                "base_idx": pl.Int64,
                "tip_idx": pl.Int64,
                "conf_idx": pl.Int64,
                "flag_end_idx": pl.Int64,
                "exit_idx": pl.Int64,
                "support_pivot_idx": pl.Int64,
                "resist_pivot_idx": pl.Int64,
                "flag_extreme_idx": pl.Int64,
                "base_time": pl.String,
                "tip_time": pl.String,
                "conf_time": pl.String,
                "exit_time": pl.String,
                "base_price": pl.Float64,
                "tip_price": pl.Float64,
                "conf_price": pl.Float64,
                "exit_price": pl.Float64,
                "support_tip_price": pl.Float64,
                "support_conf_price": pl.Float64,
                "resist_tip_price": pl.Float64,
                "resist_conf_price": pl.Float64,
                "flag_extreme_price": pl.Float64,
                "structure_stop_price": pl.Float64,
                "flag_width": pl.Int64,
                "flag_height_log": pl.Float64,
                "flag_height_ratio": pl.Float64,
                "pole_width": pl.Int64,
                "pole_height_log": pl.Float64,
                "support_intercept_log": pl.Float64,
                "support_slope": pl.Float64,
                "resist_intercept_log": pl.Float64,
                "resist_slope": pl.Float64,
                "hold_bars": pl.Int64,
                "forward_log_return": pl.Float64,
                "flag_quality_score": pl.Float64,
            }
        )

    return pl.DataFrame(rows)


def build_signal_frame(
    df: pl.DataFrame,
    patterns_df: pl.DataFrame,
) -> pl.DataFrame:
    n = df.height
    columns: dict[str, list[Any]] = {
        "bull_flag_breakout": [False] * n,
        "bear_flag_breakout": [False] * n,
        "bull_pennant_breakout": [False] * n,
        "bear_pennant_breakout": [False] * n,
        "pattern_base_idx": [None] * n,
        "pattern_tip_idx": [None] * n,
        "pattern_exit_idx": [None] * n,
        "pattern_kind": [None] * n,
        "pattern_family": [None] * n,
        "flag_width": [None] * n,
        "pole_width": [None] * n,
        "flag_quality_score": [None] * n,
        "support_intercept_log": [None] * n,
        "support_slope": [None] * n,
        "resist_intercept_log": [None] * n,
        "resist_slope": [None] * n,
    }

    for row in patterns_df.to_dicts():
        conf_idx = int(row["conf_idx"])
        family = str(row["family"])
        columns[f"{family}_breakout"][conf_idx] = True
        columns["pattern_base_idx"][conf_idx] = int(row["base_idx"])
        columns["pattern_tip_idx"][conf_idx] = int(row["tip_idx"])
        columns["pattern_exit_idx"][conf_idx] = int(row["exit_idx"])
        columns["pattern_kind"][conf_idx] = str(row["pattern_kind"])
        columns["pattern_family"][conf_idx] = family
        columns["flag_width"][conf_idx] = int(row["flag_width"])
        columns["pole_width"][conf_idx] = int(row["pole_width"])
        columns["flag_quality_score"][conf_idx] = float(row["flag_quality_score"])
        columns["support_intercept_log"][conf_idx] = float(row["support_intercept_log"])
        columns["support_slope"][conf_idx] = float(row["support_slope"])
        columns["resist_intercept_log"][conf_idx] = float(row["resist_intercept_log"])
        columns["resist_slope"][conf_idx] = float(row["resist_slope"])

    return df.with_columns(
        *[
            pl.Series(name=column_name, values=values)
            for column_name, values in columns.items()
        ]
    )


def label_flags_pennants(
    df: pl.DataFrame,
    patterns_df: pl.DataFrame,
    config: PatternConfig,
) -> pl.DataFrame:
    """添加事后标签。使用未来持有窗口，只用于研究与评估。"""

    if patterns_df.is_empty():
        return patterns_df.with_columns(
            pl.lit(None, dtype=pl.Boolean).alias("target_hit"),
            pl.lit(None, dtype=pl.Float64).alias("future_return_r"),
        )

    highs = [float(value) for value in df["high"].to_list()]
    lows = [float(value) for value in df["low"].to_list()]
    closes = [float(value) for value in df["close"].to_list()]
    result_rows: list[dict[str, Any]] = []

    for row in patterns_df.to_dicts():
        conf_idx = int(row["conf_idx"])
        exit_idx = int(row["exit_idx"])
        direction = str(row["direction"])
        conf_price = float(row["conf_price"])
        pole_height_log = float(row["pole_height_log"])
        target_price = math.exp(math.log(conf_price) + pole_height_log) if direction == "long" else math.exp(math.log(conf_price) - pole_height_log)

        future_high = max(highs[conf_idx + 1 : exit_idx + 1], default=conf_price)
        future_low = min(lows[conf_idx + 1 : exit_idx + 1], default=conf_price)
        if direction == "long":
            target_hit = future_high >= target_price
            max_favorable = math.log(max(closes[conf_idx: exit_idx + 1])) - math.log(conf_price)
            future_return_r = row["forward_log_return"] / max(pole_height_log, 1e-9)
        else:
            target_hit = future_low <= target_price
            max_favorable = math.log(conf_price) - math.log(min(closes[conf_idx: exit_idx + 1]))
            future_return_r = row["forward_log_return"] / max(pole_height_log, 1e-9)

        result = dict(row)
        result["target_price"] = target_price
        result["target_hit"] = target_hit
        result["max_favorable_log_return"] = max_favorable
        result["future_return_r"] = future_return_r
        result_rows.append(result)

    return pl.DataFrame(result_rows)


def summarize_patterns(patterns_df: pl.DataFrame) -> dict[str, int]:
    if patterns_df.is_empty():
        return {
            "pattern_count": 0,
            "bull_flag": 0,
            "bear_flag": 0,
            "bull_pennant": 0,
            "bear_pennant": 0,
        }

    summary = patterns_df.select(
        pl.len().alias("pattern_count"),
        pl.col("family").eq("bull_flag").sum().alias("bull_flag"),
        pl.col("family").eq("bear_flag").sum().alias("bear_flag"),
        pl.col("family").eq("bull_pennant").sum().alias("bull_pennant"),
        pl.col("family").eq("bear_pennant").sum().alias("bear_pennant"),
        *(
            [
                pl.col("target_hit").sum().alias("target_hit"),
            ]
            if "target_hit" in patterns_df.columns
            else []
        ),
    ).to_dicts()[0]
    return {key: int(value) for key, value in summary.items() if value is not None}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="复刻 TechnicalAnalysisAutomation 的旗形/旗幡检测。")
    parser.add_argument("--input", type=Path, required=True, help="OHLCV 数据路径")
    parser.add_argument("--output-dir", type=Path, required=True, help="产出目录")
    parser.add_argument(
        "--preset",
        choices=["baseline", "adjusted"],
        default="baseline",
        help="参数预设",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    config = get_config(args.preset)

    df = prepare_ohlcv(read_ohlcv(args.input))
    patterns = extract_flags_pennants(df, config=config)
    patterns_df = patterns_to_frame(df, patterns, config=config)
    patterns_df = label_flags_pennants(df, patterns_df, config=config)
    signal_df = build_signal_frame(df, patterns_df)

    patterns_path = args.output_dir / f"patterns_{args.preset}.csv"
    signals_path = args.output_dir / f"signals_{args.preset}.parquet"
    summary_path = args.output_dir / f"detection-summary-{args.preset}.json"

    patterns_df.write_csv(patterns_path)
    signal_df.write_parquet(signals_path)

    summary = {
        "preset": args.preset,
        "config": asdict(config),
        "summary": summarize_patterns(patterns_df),
        "patterns_path": str(patterns_path.resolve()),
        "signals_path": str(signals_path.resolve()),
    }
    _write_json(summary_path, summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
