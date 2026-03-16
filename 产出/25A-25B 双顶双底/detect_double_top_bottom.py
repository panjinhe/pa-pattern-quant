from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import polars as pl


@dataclass(frozen=True)
class DoublePatternConfig:
    atr_window: int = 20
    trend_window: int = 20
    sr_window: int = 80
    extreme_lookback: int = 12
    min_pattern_bars: int = 4
    max_pattern_bars: int = 60
    min_pullback_atr: float = 0.90
    retest_tolerance_atr: float = 0.45
    retest_tolerance_pct: float = 0.0030
    breakout_reset_atr: float = 0.80
    min_pattern_height_atr: float = 0.90
    max_pattern_height_atr: float = 12.00
    trigger_rejection_ratio: float = 0.25
    signal_fresh_bars: int = 6
    flag_bar_threshold: int = 20
    label_lookahead: int = 24
    measured_move_factor: float = 0.75
    failure_break_factor: float = 0.25


def get_config(preset: str) -> DoublePatternConfig:
    if preset == "adjusted":
        return DoublePatternConfig(
            min_pullback_atr=0.75,
            retest_tolerance_atr=0.60,
            trigger_rejection_ratio=0.20,
            signal_fresh_bars=8,
            min_pattern_height_atr=0.75,
        )
    return DoublePatternConfig()


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


def _reset_top_state(
    idx: int | None = None,
    price: float | None = None,
    low: float | None = None,
) -> dict[str, int | float | bool | None]:
    return {
        "first_idx": idx,
        "first_price": price,
        "pullback_low_idx": idx,
        "pullback_low_price": low,
        "pullback_confirmed": False,
        "second_idx": None,
        "second_price": None,
        "neckline_idx": None,
        "neckline_price": None,
    }


def _clear_top_state() -> dict[str, int | float | bool | None]:
    return _reset_top_state()


def _compute_double_top_state(
    df: pl.DataFrame,
    config: DoublePatternConfig,
) -> dict[str, list[int | float | None]]:
    highs = [float(value) for value in df["high"].to_list()]
    lows = [float(value) for value in df["low"].to_list()]
    atrs = [float(value) if value is not None else 0.0 for value in df["atr"].to_list()]
    n = len(highs)

    outputs: dict[str, list[int | float | None]] = {
        "dt_first_top_idx": [],
        "dt_second_top_idx": [],
        "dt_neckline_idx": [],
        "dt_first_top_price": [],
        "dt_second_top_price": [],
        "dt_neckline_price": [],
        "dt_pattern_span": [],
        "dt_pattern_height": [],
        "dt_price_delta": [],
        "bars_since_dt_second": [],
    }

    state = _clear_top_state()

    for i in range(n):
        high = highs[i]
        low = lows[i]
        atr = max(atrs[i], 1e-9)
        prev_highs = highs[max(0, i - config.extreme_lookback) : i]
        is_new_local_high = bool(prev_highs) and high >= max(prev_highs)

        first_idx = state["first_idx"]
        first_price = state["first_price"]
        second_idx = state["second_idx"]

        if first_idx is None:
            if is_new_local_high:
                state = _reset_top_state(idx=i, price=high, low=low)
        else:
            age = i - int(first_idx)
            if age > config.max_pattern_bars:
                if is_new_local_high:
                    state = _reset_top_state(idx=i, price=high, low=low)
                else:
                    state = _clear_top_state()
            else:
                pullback_low_price = state["pullback_low_price"]
                if pullback_low_price is None or low <= float(pullback_low_price):
                    state["pullback_low_price"] = low
                    state["pullback_low_idx"] = i

                if second_idx is None:
                    if not bool(state["pullback_confirmed"]):
                        if high >= float(first_price):
                            state = _reset_top_state(idx=i, price=high, low=low)
                        else:
                            depth = float(first_price) - float(state["pullback_low_price"])
                            if depth >= config.min_pullback_atr * atr and age >= config.min_pattern_bars:
                                state["pullback_confirmed"] = True
                                state["neckline_idx"] = state["pullback_low_idx"]
                                state["neckline_price"] = state["pullback_low_price"]
                    else:
                        tolerance = max(
                            config.retest_tolerance_pct * float(first_price),
                            config.retest_tolerance_atr * atr,
                        )
                        if high > float(first_price) + config.breakout_reset_atr * atr:
                            state = _reset_top_state(idx=i, price=high, low=low)
                        elif age >= config.min_pattern_bars and abs(high - float(first_price)) <= tolerance:
                            state["second_idx"] = i
                            state["second_price"] = high
                            state["neckline_idx"] = state["pullback_low_idx"]
                            state["neckline_price"] = state["pullback_low_price"]
                else:
                    tolerance = max(
                        config.retest_tolerance_pct * float(first_price),
                        config.retest_tolerance_atr * atr,
                    )
                    second_age = i - int(second_idx)
                    if second_age <= config.signal_fresh_bars and high >= float(state["second_price"]) and high <= float(first_price) + tolerance:
                        state["second_idx"] = i
                        state["second_price"] = high
                        low_slice = lows[int(first_idx) : i + 1]
                        neckline_price = min(low_slice)
                        neckline_offset = low_slice.index(neckline_price)
                        state["neckline_price"] = neckline_price
                        state["neckline_idx"] = int(first_idx) + neckline_offset
                    elif high > float(first_price) + config.breakout_reset_atr * atr:
                        state = _reset_top_state(idx=i, price=high, low=low)
                    elif second_age > config.signal_fresh_bars:
                        if is_new_local_high:
                            state = _reset_top_state(idx=i, price=high, low=low)
                        else:
                            state = _clear_top_state()

        first_idx = state["first_idx"]
        second_idx = state["second_idx"]
        first_price = state["first_price"]
        second_price = state["second_price"]
        neckline_idx = state["neckline_idx"]
        neckline_price = state["neckline_price"]

        pattern_span = None
        pattern_height = None
        price_delta = None
        bars_since_second = None
        if first_idx is not None and second_idx is not None and neckline_price is not None:
            pattern_span = int(second_idx) - int(first_idx)
            pattern_height = max(float(first_price), float(second_price)) - float(neckline_price)
            price_delta = float(second_price) - float(first_price)
            bars_since_second = i - int(second_idx)

        outputs["dt_first_top_idx"].append(int(first_idx) if first_idx is not None else None)
        outputs["dt_second_top_idx"].append(int(second_idx) if second_idx is not None else None)
        outputs["dt_neckline_idx"].append(int(neckline_idx) if neckline_idx is not None else None)
        outputs["dt_first_top_price"].append(float(first_price) if first_price is not None else None)
        outputs["dt_second_top_price"].append(float(second_price) if second_price is not None else None)
        outputs["dt_neckline_price"].append(float(neckline_price) if neckline_price is not None else None)
        outputs["dt_pattern_span"].append(pattern_span)
        outputs["dt_pattern_height"].append(pattern_height)
        outputs["dt_price_delta"].append(price_delta)
        outputs["bars_since_dt_second"].append(bars_since_second)

    return outputs


def _reset_bottom_state(
    idx: int | None = None,
    price: float | None = None,
    high: float | None = None,
) -> dict[str, int | float | bool | None]:
    return {
        "first_idx": idx,
        "first_price": price,
        "pullback_high_idx": idx,
        "pullback_high_price": high,
        "pullback_confirmed": False,
        "second_idx": None,
        "second_price": None,
        "neckline_idx": None,
        "neckline_price": None,
    }


def _clear_bottom_state() -> dict[str, int | float | bool | None]:
    return _reset_bottom_state()


def _compute_double_bottom_state(
    df: pl.DataFrame,
    config: DoublePatternConfig,
) -> dict[str, list[int | float | None]]:
    highs = [float(value) for value in df["high"].to_list()]
    lows = [float(value) for value in df["low"].to_list()]
    atrs = [float(value) if value is not None else 0.0 for value in df["atr"].to_list()]
    n = len(highs)

    outputs: dict[str, list[int | float | None]] = {
        "db_first_bottom_idx": [],
        "db_second_bottom_idx": [],
        "db_neckline_idx": [],
        "db_first_bottom_price": [],
        "db_second_bottom_price": [],
        "db_neckline_price": [],
        "db_pattern_span": [],
        "db_pattern_height": [],
        "db_price_delta": [],
        "bars_since_db_second": [],
    }

    state = _clear_bottom_state()

    for i in range(n):
        high = highs[i]
        low = lows[i]
        atr = max(atrs[i], 1e-9)
        prev_lows = lows[max(0, i - config.extreme_lookback) : i]
        is_new_local_low = bool(prev_lows) and low <= min(prev_lows)

        first_idx = state["first_idx"]
        first_price = state["first_price"]
        second_idx = state["second_idx"]

        if first_idx is None:
            if is_new_local_low:
                state = _reset_bottom_state(idx=i, price=low, high=high)
        else:
            age = i - int(first_idx)
            if age > config.max_pattern_bars:
                if is_new_local_low:
                    state = _reset_bottom_state(idx=i, price=low, high=high)
                else:
                    state = _clear_bottom_state()
            else:
                pullback_high_price = state["pullback_high_price"]
                if pullback_high_price is None or high >= float(pullback_high_price):
                    state["pullback_high_price"] = high
                    state["pullback_high_idx"] = i

                if second_idx is None:
                    if not bool(state["pullback_confirmed"]):
                        if low <= float(first_price):
                            state = _reset_bottom_state(idx=i, price=low, high=high)
                        else:
                            depth = float(state["pullback_high_price"]) - float(first_price)
                            if depth >= config.min_pullback_atr * atr and age >= config.min_pattern_bars:
                                state["pullback_confirmed"] = True
                                state["neckline_idx"] = state["pullback_high_idx"]
                                state["neckline_price"] = state["pullback_high_price"]
                    else:
                        tolerance = max(
                            config.retest_tolerance_pct * max(float(first_price), 1.0),
                            config.retest_tolerance_atr * atr,
                        )
                        if low < float(first_price) - config.breakout_reset_atr * atr:
                            state = _reset_bottom_state(idx=i, price=low, high=high)
                        elif age >= config.min_pattern_bars and abs(low - float(first_price)) <= tolerance:
                            state["second_idx"] = i
                            state["second_price"] = low
                            state["neckline_idx"] = state["pullback_high_idx"]
                            state["neckline_price"] = state["pullback_high_price"]
                else:
                    tolerance = max(
                        config.retest_tolerance_pct * max(float(first_price), 1.0),
                        config.retest_tolerance_atr * atr,
                    )
                    second_age = i - int(second_idx)
                    if second_age <= config.signal_fresh_bars and low <= float(state["second_price"]) and low >= float(first_price) - tolerance:
                        state["second_idx"] = i
                        state["second_price"] = low
                        high_slice = highs[int(first_idx) : i + 1]
                        neckline_price = max(high_slice)
                        neckline_offset = high_slice.index(neckline_price)
                        state["neckline_price"] = neckline_price
                        state["neckline_idx"] = int(first_idx) + neckline_offset
                    elif low < float(first_price) - config.breakout_reset_atr * atr:
                        state = _reset_bottom_state(idx=i, price=low, high=high)
                    elif second_age > config.signal_fresh_bars:
                        if is_new_local_low:
                            state = _reset_bottom_state(idx=i, price=low, high=high)
                        else:
                            state = _clear_bottom_state()

        first_idx = state["first_idx"]
        second_idx = state["second_idx"]
        first_price = state["first_price"]
        second_price = state["second_price"]
        neckline_idx = state["neckline_idx"]
        neckline_price = state["neckline_price"]

        pattern_span = None
        pattern_height = None
        price_delta = None
        bars_since_second = None
        if first_idx is not None and second_idx is not None and neckline_price is not None:
            pattern_span = int(second_idx) - int(first_idx)
            pattern_height = float(neckline_price) - min(float(first_price), float(second_price))
            price_delta = float(second_price) - float(first_price)
            bars_since_second = i - int(second_idx)

        outputs["db_first_bottom_idx"].append(int(first_idx) if first_idx is not None else None)
        outputs["db_second_bottom_idx"].append(int(second_idx) if second_idx is not None else None)
        outputs["db_neckline_idx"].append(int(neckline_idx) if neckline_idx is not None else None)
        outputs["db_first_bottom_price"].append(float(first_price) if first_price is not None else None)
        outputs["db_second_bottom_price"].append(float(second_price) if second_price is not None else None)
        outputs["db_neckline_price"].append(float(neckline_price) if neckline_price is not None else None)
        outputs["db_pattern_span"].append(pattern_span)
        outputs["db_pattern_height"].append(pattern_height)
        outputs["db_price_delta"].append(price_delta)
        outputs["bars_since_db_second"].append(bars_since_second)

    return outputs


def add_double_pattern_features(
    df: pl.DataFrame,
    config: DoublePatternConfig,
) -> pl.DataFrame:
    """生成双顶双底特征列。实时安全，不使用未来 K 线。"""

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
                (pl.col("sr_high") - pl.col("close"))
                <= 0.8 * pl.col("atr")
            ).alias("near_resistance"),
            (
                (pl.col("close") - pl.col("sr_low"))
                <= 0.8 * pl.col("atr")
            ).alias("near_support"),
        )
    )

    state_cols = {
        **_compute_double_top_state(df, config=config),
        **_compute_double_bottom_state(df, config=config),
    }
    df = df.with_columns(
        *[
            pl.Series(name=column_name, values=values)
            for column_name, values in state_cols.items()
        ]
    )

    return df.with_columns(
        pl.when(pl.col("atr") > 0)
        .then(pl.col("dt_pattern_height") / pl.col("atr"))
        .otherwise(None)
        .alias("dt_pattern_height_atr"),
        pl.when(pl.col("atr") > 0)
        .then(pl.col("db_pattern_height") / pl.col("atr"))
        .otherwise(None)
        .alias("db_pattern_height_atr"),
        pl.when(pl.col("dt_price_delta").abs() > 0.10 * pl.col("atr"))
        .then(pl.col("dt_price_delta") > 0)
        .otherwise(None)
        .alias("dt_is_higher_high"),
        pl.when(pl.col("db_price_delta").abs() > 0.10 * pl.col("atr"))
        .then(pl.col("db_price_delta") < 0)
        .otherwise(None)
        .alias("db_is_lower_low"),
        pl.max_horizontal(
            pl.col("dt_first_top_price"),
            pl.col("dt_second_top_price"),
        ).alias("dt_reference_top"),
        pl.min_horizontal(
            pl.col("db_first_bottom_price"),
            pl.col("db_second_bottom_price"),
        ).alias("db_reference_bottom"),
    )


def detect_double_top_bottom(
    df: pl.DataFrame,
    config: DoublePatternConfig,
) -> pl.DataFrame:
    """检测双顶双底与实时触发。实时安全，不使用未来 K 线。"""

    df = add_double_pattern_features(df, config=config)

    df = df.with_columns(
        (
            pl.col("dt_second_top_idx").is_not_null()
            & (pl.col("bars_since_dt_second") <= config.signal_fresh_bars)
            & (pl.col("dt_pattern_span") >= config.min_pattern_bars)
            & (pl.col("dt_pattern_height_atr") >= config.min_pattern_height_atr)
            & (pl.col("dt_pattern_height_atr") <= config.max_pattern_height_atr)
        ).alias("double_top_setup"),
        (
            pl.col("db_second_bottom_idx").is_not_null()
            & (pl.col("bars_since_db_second") <= config.signal_fresh_bars)
            & (pl.col("db_pattern_span") >= config.min_pattern_bars)
            & (pl.col("db_pattern_height_atr") >= config.min_pattern_height_atr)
            & (pl.col("db_pattern_height_atr") <= config.max_pattern_height_atr)
        ).alias("double_bottom_setup"),
    )

    df = df.with_columns(
        (
            pl.col("double_top_setup")
            & (pl.col("close") < pl.col("low").shift(1))
            & (pl.col("close") < pl.col("ema20"))
            & (pl.col("close_pos") <= 0.45)
            & (
                (pl.col("dt_second_top_price") - pl.col("close"))
                >= config.trigger_rejection_ratio * pl.col("dt_pattern_height")
            )
        ).alias("raw_double_top_signal"),
        (
            pl.col("double_bottom_setup")
            & (pl.col("close") > pl.col("high").shift(1))
            & (pl.col("close") > pl.col("ema20"))
            & (pl.col("close_pos") >= 0.55)
            & (
                (pl.col("close") - pl.col("db_second_bottom_price"))
                >= config.trigger_rejection_ratio * pl.col("db_pattern_height")
            )
        ).alias("raw_double_bottom_signal"),
        (
            pl.col("double_top_setup")
            & (pl.col("downtrend_score") > pl.col("uptrend_score"))
            & (pl.col("dt_pattern_span") <= config.flag_bar_threshold)
        ).alias("double_top_bear_flag_setup"),
        (
            pl.col("double_bottom_setup")
            & (pl.col("uptrend_score") > pl.col("downtrend_score"))
            & (pl.col("db_pattern_span") <= config.flag_bar_threshold)
        ).alias("double_bottom_bull_flag_setup"),
        (
            pl.col("double_top_setup")
            & ~(
                (pl.col("downtrend_score") > pl.col("uptrend_score"))
                & (pl.col("dt_pattern_span") <= config.flag_bar_threshold)
            )
        ).alias("double_top_reversal_setup"),
        (
            pl.col("double_bottom_setup")
            & ~(
                (pl.col("uptrend_score") > pl.col("downtrend_score"))
                & (pl.col("db_pattern_span") <= config.flag_bar_threshold)
            )
        ).alias("double_bottom_reversal_setup"),
    )

    return df.with_columns(
        (
            pl.col("raw_double_top_signal")
            & ~pl.col("raw_double_top_signal").shift(1).fill_null(False)
        ).alias("double_top_signal"),
        (
            pl.col("raw_double_bottom_signal")
            & ~pl.col("raw_double_bottom_signal").shift(1).fill_null(False)
        ).alias("double_bottom_signal"),
    )


def label_double_top_bottom_outcomes(
    df: pl.DataFrame,
    config: DoublePatternConfig,
) -> pl.DataFrame:
    """添加事后标签。使用未来 K 线，只用于研究与评估。"""

    if "double_top_signal" not in df.columns:
        df = detect_double_top_bottom(df, config=config)

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

    measured_top_distance = pl.max_horizontal(
        pl.col("atr"),
        pl.col("dt_pattern_height") * config.measured_move_factor,
    )
    measured_bottom_distance = pl.max_horizontal(
        pl.col("atr"),
        pl.col("db_pattern_height") * config.measured_move_factor,
    )
    failure_top_distance = pl.max_horizontal(
        0.25 * pl.col("atr"),
        pl.col("dt_pattern_height") * config.failure_break_factor,
    )
    failure_bottom_distance = pl.max_horizontal(
        0.25 * pl.col("atr"),
        pl.col("db_pattern_height") * config.failure_break_factor,
    )

    return df.with_columns(
        (
            pl.col("double_top_signal")
            & (pl.col("future_low_n") <= pl.col("close") - measured_top_distance)
        ).alias("double_top_measured_move_hit"),
        (
            pl.col("double_bottom_signal")
            & (pl.col("future_high_n") >= pl.col("close") + measured_bottom_distance)
        ).alias("double_bottom_measured_move_hit"),
        (
            pl.col("double_top_signal")
            & (pl.col("future_high_n") >= pl.col("dt_reference_top") + failure_top_distance)
        ).alias("double_top_failure_breakout"),
        (
            pl.col("double_bottom_signal")
            & (pl.col("future_low_n") <= pl.col("db_reference_bottom") - failure_bottom_distance)
        ).alias("double_bottom_failure_breakdown"),
    )


def summarize_detection(df: pl.DataFrame) -> dict[str, int]:
    summary = df.select(
        pl.len().alias("rows"),
        pl.col("double_top_setup").sum().alias("double_top_setup"),
        pl.col("double_bottom_setup").sum().alias("double_bottom_setup"),
        pl.col("double_top_bear_flag_setup").sum().alias("double_top_bear_flag_setup"),
        pl.col("double_bottom_bull_flag_setup").sum().alias("double_bottom_bull_flag_setup"),
        pl.col("double_top_reversal_setup").sum().alias("double_top_reversal_setup"),
        pl.col("double_bottom_reversal_setup").sum().alias("double_bottom_reversal_setup"),
        pl.col("double_top_signal").sum().alias("double_top_signal"),
        pl.col("double_bottom_signal").sum().alias("double_bottom_signal"),
        *(
            [
                pl.col("double_top_measured_move_hit").sum().alias("double_top_measured_move_hit"),
                pl.col("double_bottom_measured_move_hit").sum().alias("double_bottom_measured_move_hit"),
                pl.col("double_top_failure_breakout").sum().alias("double_top_failure_breakout"),
                pl.col("double_bottom_failure_breakdown").sum().alias("double_bottom_failure_breakdown"),
            ]
            if "double_top_measured_move_hit" in df.columns
            else []
        ),
    ).to_dicts()[0]
    return {key: int(value) for key, value in summary.items()}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="检测阿布课程中的双顶双底。")
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
    df = detect_double_top_bottom(df, config=config)
    if args.with_outcomes:
        df = label_double_top_bottom_outcomes(df, config=config)

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
