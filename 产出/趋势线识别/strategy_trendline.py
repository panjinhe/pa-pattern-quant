from __future__ import annotations

import argparse
import json
import statistics
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import polars as pl


HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.append(str(HERE))

from detect_trendline import (  # noqa: E402
    TrendlineConfig,
    detect_trendline_breakouts,
    get_config,
    label_trendline_outcomes,
    prepare_ohlcv,
    read_ohlcv,
    summarize_detection,
)


@dataclass(frozen=True)
class BacktestConfig:
    stop_buffer_atr: float = 0.12
    min_stop_atr: float = 0.70
    target_height_multiplier: float = 0.90
    target_r_multiple: float = 1.80
    max_holding_bars: int = 48
    cooldown_bars: int = 6
    fee_bps_per_side: float = 2.00
    notional_usdt: float = 10_000.0
    min_trades_for_baseline: int = 20


def _simulate_exit(
    data: dict[str, list[Any]],
    entry_idx: int,
    side: str,
    stop_price: float,
    target_price: float,
    config: BacktestConfig,
) -> tuple[int, float, str]:
    end_idx = min(len(data["idx"]) - 1, entry_idx + config.max_holding_bars)

    for idx in range(entry_idx + 1, end_idx + 1):
        high = float(data["high"][idx])
        low = float(data["low"][idx])

        if side == "short":
            stop_hit = high >= stop_price
            target_hit = low <= target_price
            if stop_hit and target_hit:
                return idx, stop_price, "同柱先按止损"
            if stop_hit:
                return idx, stop_price, "止损"
            if target_hit:
                return idx, target_price, "止盈"
        else:
            stop_hit = low <= stop_price
            target_hit = high >= target_price
            if stop_hit and target_hit:
                return idx, stop_price, "同柱先按止损"
            if stop_hit:
                return idx, stop_price, "止损"
            if target_hit:
                return idx, target_price, "止盈"

    return end_idx, float(data["close"][end_idx]), "时间止盈/止损"


def summarize_backtest(trades_df: pl.DataFrame) -> dict[str, Any]:
    if trades_df.is_empty():
        return {
            "trade_count": 0,
            "win_rate": 0.0,
            "avg_net_pnl": 0.0,
            "total_net_pnl": 0.0,
            "avg_r": 0.0,
            "total_r": 0.0,
            "max_drawdown": 0.0,
            "profit_factor": 0.0,
            "long_trades": 0,
            "short_trades": 0,
            "avg_holding_bars": 0.0,
            "median_holding_bars": 0,
            "stop_exit_count": 0,
            "target_exit_count": 0,
            "time_exit_count": 0,
        }

    pnl_list = [float(value) for value in trades_df["net_pnl"].to_list()]
    r_list = [float(value) for value in trades_df["gross_r"].to_list()]
    holding_list = [int(value) for value in trades_df["holding_bars"].to_list()]
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for pnl in pnl_list:
        equity += pnl
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity - peak)

    wins = [value for value in pnl_list if value > 0]
    losses = [value for value in pnl_list if value < 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    profit_factor = gross_profit / gross_loss if gross_loss else None

    trade_count = trades_df.height
    win_count = len(wins)
    return {
        "trade_count": trade_count,
        "win_rate": round((win_count / trade_count) * 100.0, 4),
        "avg_net_pnl": round(sum(pnl_list) / trade_count, 6),
        "total_net_pnl": round(sum(pnl_list), 6),
        "avg_r": round(sum(r_list) / trade_count, 6),
        "total_r": round(sum(r_list), 6),
        "max_drawdown": round(max_drawdown, 6),
        "profit_factor": round(profit_factor, 6) if profit_factor is not None else None,
        "long_trades": trades_df.filter(pl.col("side") == "long").height,
        "short_trades": trades_df.filter(pl.col("side") == "short").height,
        "avg_holding_bars": round(sum(holding_list) / trade_count, 2),
        "median_holding_bars": int(statistics.median(holding_list)),
        "stop_exit_count": trades_df.filter(
            pl.col("exit_reason").is_in(["止损", "同柱先按止损"])
        ).height,
        "target_exit_count": trades_df.filter(pl.col("exit_reason") == "止盈").height,
        "time_exit_count": trades_df.filter(pl.col("exit_reason") == "时间止盈/止损").height,
    }


def build_equity_curve(
    trades_df: pl.DataFrame,
    backtest_config: BacktestConfig,
) -> pl.DataFrame:
    if trades_df.is_empty():
        return pl.DataFrame(
            schema={
                "trade_no": pl.Int64,
                "exit_idx": pl.Int64,
                "exit_time": pl.String,
                "side": pl.String,
                "gross_r": pl.Float64,
                "net_pnl": pl.Float64,
                "fees": pl.Float64,
                "cumulative_gross_r": pl.Float64,
                "cumulative_net_pnl": pl.Float64,
                "nav": pl.Float64,
                "peak_nav": pl.Float64,
                "drawdown": pl.Float64,
                "drawdown_pct": pl.Float64,
            }
        )

    return (
        trades_df.sort("exit_idx")
        .with_row_index("trade_no", offset=1)
        .with_columns(
            pl.col("gross_r").cum_sum().alias("cumulative_gross_r"),
            pl.col("net_pnl").cum_sum().alias("cumulative_net_pnl"),
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
            "exit_idx",
            "exit_time",
            "side",
            "gross_r",
            "net_pnl",
            "fees",
            "cumulative_gross_r",
            "cumulative_net_pnl",
            "nav",
            "peak_nav",
            "drawdown",
            "drawdown_pct",
        )
    )


def summarize_equity_curve(equity_curve_df: pl.DataFrame) -> dict[str, float]:
    if equity_curve_df.is_empty():
        return {
            "ending_nav": 1.0,
            "min_nav": 1.0,
            "max_drawdown_pct": 0.0,
        }

    last_row = equity_curve_df.tail(1).to_dicts()[0]
    return {
        "ending_nav": round(float(last_row["nav"]), 6),
        "min_nav": round(float(equity_curve_df["nav"].min()), 6),
        "max_drawdown_pct": round(float(equity_curve_df["drawdown_pct"].min()) * 100.0, 6),
    }


def _build_trade_record(
    data: dict[str, list[Any]],
    signal_idx: int,
    exit_idx: int,
    side: str,
    entry_price: float,
    exit_price: float,
    stop_price: float,
    target_price: float,
    risk_per_unit: float,
    exit_reason: str,
    preset: str,
    config: BacktestConfig,
) -> dict[str, Any]:
    quantity = config.notional_usdt / entry_price
    fee_rate = config.fee_bps_per_side / 10_000.0

    gross_pnl = (exit_price - entry_price) * quantity if side == "long" else (entry_price - exit_price) * quantity
    gross_r = (exit_price - entry_price) / risk_per_unit if side == "long" else (entry_price - exit_price) / risk_per_unit
    fees = quantity * (entry_price + exit_price) * fee_rate
    net_pnl = gross_pnl - fees
    net_return_pct = (net_pnl / config.notional_usdt) * 100.0

    pattern_role = "上破阻力趋势线" if side == "long" else "下破支撑趋势线"
    return {
        "preset": preset,
        "side": side,
        "pattern_role": pattern_role,
        "channel_type": data["trend_channel_type"][signal_idx],
        "signal_idx": signal_idx,
        "exit_idx": exit_idx,
        "signal_time": data["timestamp"][signal_idx].isoformat(),
        "entry_time": data["timestamp"][signal_idx].isoformat(),
        "exit_time": data["timestamp"][exit_idx].isoformat(),
        "entry_price": round(entry_price, 6),
        "exit_price": round(exit_price, 6),
        "stop_price": round(stop_price, 6),
        "target_price": round(target_price, 6),
        "window_start_idx": int(data["trend_window_start_idx"][signal_idx]),
        "window_end_idx": int(data["trend_window_end_idx"][signal_idx]),
        "support_line_start": round(float(data["support_line_start"][signal_idx]), 6),
        "support_line_current": round(float(data["support_line_current"][signal_idx]), 6),
        "resistance_line_start": round(float(data["resistance_line_start"][signal_idx]), 6),
        "resistance_line_current": round(float(data["resistance_line_current"][signal_idx]), 6),
        "support_touch_count": int(data["support_touch_count"][signal_idx]),
        "resistance_touch_count": int(data["resistance_touch_count"][signal_idx]),
        "support_touch_1_idx": data["support_touch_1_idx"][signal_idx],
        "support_touch_2_idx": data["support_touch_2_idx"][signal_idx],
        "support_touch_3_idx": data["support_touch_3_idx"][signal_idx],
        "resistance_touch_1_idx": data["resistance_touch_1_idx"][signal_idx],
        "resistance_touch_2_idx": data["resistance_touch_2_idx"][signal_idx],
        "resistance_touch_3_idx": data["resistance_touch_3_idx"][signal_idx],
        "channel_height": round(float(data["channel_height"][signal_idx]), 6),
        "channel_height_atr": round(float(data["channel_height_atr"][signal_idx]), 6),
        "trend_slope": round(float(data["trend_slope"][signal_idx]), 6),
        "trend_slope_atr": round(float(data["trend_slope_atr"][signal_idx]), 6),
        "atr_at_entry": round(float(data["atr"][signal_idx]), 6),
        "holding_bars": exit_idx - signal_idx,
        "exit_reason": exit_reason,
        "gross_r": round(gross_r, 6),
        "net_pnl": round(net_pnl, 6),
        "net_return_pct": round(net_return_pct, 6),
        "fees": round(fees, 6),
        "measured_move_hit_label": bool(
            data["bullish_measured_move_hit"][signal_idx]
            if side == "long"
            else data["bearish_measured_move_hit"][signal_idx]
        ),
        "failure_label": bool(
            data["bullish_breakout_failure"][signal_idx]
            if side == "long"
            else data["bearish_breakdown_failure"][signal_idx]
        ),
    }


def run_backtest(
    df: pl.DataFrame,
    preset: str,
    detect_config: TrendlineConfig,
    backtest_config: BacktestConfig,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    cols = [
        "idx",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "atr",
        "trend_window_start_idx",
        "trend_window_end_idx",
        "trend_slope",
        "trend_slope_atr",
        "trend_channel_type",
        "support_line_start",
        "support_line_prev",
        "support_line_current",
        "resistance_line_start",
        "resistance_line_prev",
        "resistance_line_current",
        "channel_height",
        "channel_height_atr",
        "support_touch_count",
        "resistance_touch_count",
        "support_touch_1_idx",
        "support_touch_2_idx",
        "support_touch_3_idx",
        "resistance_touch_1_idx",
        "resistance_touch_2_idx",
        "resistance_touch_3_idx",
        "support_line_valid",
        "resistance_line_valid",
        "trend_channel_valid",
        "bullish_breakout_signal",
        "bearish_breakdown_signal",
        "bullish_measured_move_hit",
        "bearish_measured_move_hit",
        "bullish_breakout_failure",
        "bearish_breakdown_failure",
    ]
    data = {col: df[col].to_list() for col in cols}

    long_signals = df.filter(pl.col("bullish_breakout_signal"))["idx"].to_list()
    short_signals = df.filter(pl.col("bearish_breakdown_signal"))["idx"].to_list()
    events = [(idx, "long") for idx in long_signals] + [(idx, "short") for idx in short_signals]
    events.sort(key=lambda item: item[0])

    trades: list[dict[str, Any]] = []
    next_available_idx = 0

    for signal_idx, side in events:
        if signal_idx < next_available_idx:
            continue

        atr = float(data["atr"][signal_idx] or 0.0)
        channel_height = float(data["channel_height"][signal_idx] or 0.0)
        if atr <= 0 or channel_height <= 0:
            continue

        entry_price = float(data["close"][signal_idx])
        if side == "long":
            stop_price = min(
                float(data["support_line_current"][signal_idx]) - backtest_config.stop_buffer_atr * atr,
                entry_price - backtest_config.min_stop_atr * atr,
            )
            risk_per_unit = entry_price - stop_price
            target_distance = max(
                channel_height * backtest_config.target_height_multiplier,
                risk_per_unit * backtest_config.target_r_multiple,
            )
            target_price = entry_price + target_distance
        else:
            stop_price = max(
                float(data["resistance_line_current"][signal_idx]) + backtest_config.stop_buffer_atr * atr,
                entry_price + backtest_config.min_stop_atr * atr,
            )
            risk_per_unit = stop_price - entry_price
            target_distance = max(
                channel_height * backtest_config.target_height_multiplier,
                risk_per_unit * backtest_config.target_r_multiple,
            )
            target_price = entry_price - target_distance

        if risk_per_unit <= 0:
            continue

        exit_idx, exit_price, exit_reason = _simulate_exit(
            data=data,
            entry_idx=signal_idx,
            side=side,
            stop_price=stop_price,
            target_price=target_price,
            config=backtest_config,
        )
        next_available_idx = exit_idx + backtest_config.cooldown_bars

        trades.append(
            _build_trade_record(
                data=data,
                signal_idx=signal_idx,
                exit_idx=exit_idx,
                side=side,
                entry_price=entry_price,
                exit_price=exit_price,
                stop_price=stop_price,
                target_price=target_price,
                risk_per_unit=risk_per_unit,
                exit_reason=exit_reason,
                preset=preset,
                config=backtest_config,
            )
        )

    if trades:
        trades_df = pl.DataFrame(trades)
    else:
        trades_df = pl.DataFrame(
            schema={
                "preset": pl.String,
                "side": pl.String,
                "pattern_role": pl.String,
                "channel_type": pl.String,
                "signal_idx": pl.Int64,
                "exit_idx": pl.Int64,
                "signal_time": pl.String,
                "entry_time": pl.String,
                "exit_time": pl.String,
                "entry_price": pl.Float64,
                "exit_price": pl.Float64,
                "stop_price": pl.Float64,
                "target_price": pl.Float64,
                "window_start_idx": pl.Int64,
                "window_end_idx": pl.Int64,
                "support_line_start": pl.Float64,
                "support_line_current": pl.Float64,
                "resistance_line_start": pl.Float64,
                "resistance_line_current": pl.Float64,
                "support_touch_count": pl.Int64,
                "resistance_touch_count": pl.Int64,
                "support_touch_1_idx": pl.Int64,
                "support_touch_2_idx": pl.Int64,
                "support_touch_3_idx": pl.Int64,
                "resistance_touch_1_idx": pl.Int64,
                "resistance_touch_2_idx": pl.Int64,
                "resistance_touch_3_idx": pl.Int64,
                "channel_height": pl.Float64,
                "channel_height_atr": pl.Float64,
                "trend_slope": pl.Float64,
                "trend_slope_atr": pl.Float64,
                "atr_at_entry": pl.Float64,
                "holding_bars": pl.Int64,
                "exit_reason": pl.String,
                "gross_r": pl.Float64,
                "net_pnl": pl.Float64,
                "net_return_pct": pl.Float64,
                "fees": pl.Float64,
                "measured_move_hit_label": pl.Boolean,
                "failure_label": pl.Boolean,
            }
        )

    summary = summarize_backtest(trades_df)
    summary.update(
        {
            "preset": preset,
            "detect_config": asdict(detect_config),
            "backtest_config": asdict(backtest_config),
            "detection_summary": summarize_detection(df),
        }
    )
    return trades_df, summary


def _save_signal_frame(df: pl.DataFrame, path: Path) -> None:
    export_cols = [
        "idx",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "atr",
        "close_pos",
        "body_ratio",
        "trend_window_start_idx",
        "trend_window_end_idx",
        "trend_slope",
        "trend_slope_atr",
        "trend_channel_type",
        "support_line_start",
        "support_line_prev",
        "support_line_current",
        "resistance_line_start",
        "resistance_line_prev",
        "resistance_line_current",
        "channel_height",
        "channel_height_atr",
        "support_touch_count",
        "resistance_touch_count",
        "support_touch_1_idx",
        "support_touch_2_idx",
        "support_touch_3_idx",
        "resistance_touch_1_idx",
        "resistance_touch_2_idx",
        "resistance_touch_3_idx",
        "support_line_valid",
        "resistance_line_valid",
        "trend_channel_valid",
        "bullish_breakout_signal",
        "bearish_breakdown_signal",
        "bullish_measured_move_hit",
        "bearish_measured_move_hit",
        "bullish_breakout_failure",
        "bearish_breakdown_failure",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    df.select(export_cols).write_parquet(path)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def execute_preset(
    df_source: pl.DataFrame,
    output_dir: Path,
    preset: str,
    backtest_config: BacktestConfig,
) -> dict[str, Any]:
    detect_config = get_config(preset)
    df = detect_trendline_breakouts(df_source, config=detect_config)
    df = label_trendline_outcomes(df, config=detect_config)

    trades_df, summary = run_backtest(
        df=df,
        preset=preset,
        detect_config=detect_config,
        backtest_config=backtest_config,
    )
    equity_curve_df = build_equity_curve(trades_df, backtest_config)

    signals_path = output_dir / f"signals_{preset}.parquet"
    trades_path = output_dir / f"trades_{preset}.csv"
    equity_curve_path = output_dir / f"equity-curve-{preset}.csv"
    summary_path = output_dir / f"backtest-summary-{preset}.json"

    _save_signal_frame(df, signals_path)
    trades_df.write_csv(trades_path)
    equity_curve_df.write_csv(equity_curve_path)
    summary.update(summarize_equity_curve(equity_curve_df))
    _write_json(summary_path, summary)

    summary["signals_path"] = str(signals_path.resolve())
    summary["trades_path"] = str(trades_path.resolve())
    summary["equity_curve_path"] = str(equity_curve_path.resolve())
    summary["summary_path"] = str(summary_path.resolve())
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="趋势线突破策略回测。")
    parser.add_argument("--input", type=Path, required=True, help="OHLCV 数据路径")
    parser.add_argument("--output-dir", type=Path, required=True, help="产出目录")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    base_df = prepare_ohlcv(read_ohlcv(args.input))
    sample_info = base_df.select(
        pl.len().alias("rows"),
        pl.col("timestamp").min().alias("start"),
        pl.col("timestamp").max().alias("end"),
    ).to_dicts()[0]

    backtest_config = BacktestConfig()
    baseline_summary = execute_preset(
        df_source=base_df,
        output_dir=args.output_dir,
        preset="baseline",
        backtest_config=backtest_config,
    )

    combined_summary: dict[str, Any] = {
        "reference_repos": [
            "https://github.com/neurotrader888/TrendLineAutomation",
            "https://github.com/ednunezg/pytrendline",
        ],
        "data_path": str(args.input.resolve()),
        "sample_info": {
            "rows": int(sample_info["rows"]),
            "start": sample_info["start"].isoformat(),
            "end": sample_info["end"].isoformat(),
        },
        "fee_slippage_assumption": "双边合计按每边 2 bps 计入，含手续费与滑点",
        "capital_assumption": "固定名义仓位 10000 USDT，每笔独立，不做复利",
        "parameter_adjustment_triggered": False,
        "selected_preset": "baseline",
        "baseline": baseline_summary,
    }

    if baseline_summary["trade_count"] < backtest_config.min_trades_for_baseline:
        adjusted_summary = execute_preset(
            df_source=base_df,
            output_dir=args.output_dir,
            preset="adjusted",
            backtest_config=backtest_config,
        )
        combined_summary["parameter_adjustment_triggered"] = True
        combined_summary["selected_preset"] = "adjusted"
        combined_summary["adjustment_reason"] = (
            f"基线交易笔数仅 {baseline_summary['trade_count']} 笔，低于阈值 "
            f"{backtest_config.min_trades_for_baseline} 笔。"
        )
        combined_summary["adjusted"] = adjusted_summary

    _write_json(args.output_dir / "backtest-summary.json", combined_summary)
    print(json.dumps(combined_summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
