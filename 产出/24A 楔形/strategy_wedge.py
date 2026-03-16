from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import polars as pl


HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.append(str(HERE))

from detect_wedge import (  # noqa: E402
    WedgeConfig,
    detect_wedge,
    get_config,
    label_wedge_outcomes,
    prepare_ohlcv,
    read_ohlcv,
    summarize_detection,
)


@dataclass(frozen=True)
class BacktestConfig:
    stop_buffer_atr: float = 0.15
    min_stop_atr: float = 0.70
    target_r_multiple: float = 1.80
    target_range_multiplier: float = 0.80
    max_holding_bars: int = 40
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

    if side == "short":
        gross_pnl = (entry_price - exit_price) * quantity
        gross_r = (entry_price - exit_price) / risk_per_unit
        first_push_idx = data["up_first_push_idx"][signal_idx]
        second_push_idx = data["up_second_push_idx"][signal_idx]
        third_push_idx = data["up_third_push_idx"][signal_idx]
        third_push_price = data["up_third_push_price"][signal_idx]
    else:
        gross_pnl = (exit_price - entry_price) * quantity
        gross_r = (exit_price - entry_price) / risk_per_unit
        first_push_idx = data["down_first_push_idx"][signal_idx]
        second_push_idx = data["down_second_push_idx"][signal_idx]
        third_push_idx = data["down_third_push_idx"][signal_idx]
        third_push_price = data["down_third_push_price"][signal_idx]

    fees = quantity * (entry_price + exit_price) * fee_rate
    net_pnl = gross_pnl - fees
    net_return_pct = (net_pnl / config.notional_usdt) * 100.0

    return {
        "preset": preset,
        "side": side,
        "signal_idx": signal_idx,
        "exit_idx": exit_idx,
        "signal_time": data["timestamp"][signal_idx].isoformat(),
        "signal_price": round(float(data["close"][signal_idx]), 6),
        "entry_time": data["timestamp"][signal_idx].isoformat(),
        "exit_time": data["timestamp"][exit_idx].isoformat(),
        "entry_price": round(entry_price, 6),
        "exit_price": round(exit_price, 6),
        "stop_price": round(stop_price, 6),
        "target_price": round(target_price, 6),
        "wedge_high": round(float(data["wedge_high"][signal_idx]), 6),
        "wedge_low": round(float(data["wedge_low"][signal_idx]), 6),
        "wedge_range": round(float(data["wedge_range"][signal_idx]), 6),
        "third_push_price": round(float(third_push_price), 6),
        "first_push_idx": int(first_push_idx),
        "second_push_idx": int(second_push_idx),
        "third_push_idx": int(third_push_idx),
        "atr_at_entry": round(float(data["atr"][signal_idx]), 6),
        "holding_bars": exit_idx - signal_idx,
        "exit_reason": exit_reason,
        "gross_r": round(gross_r, 6),
        "net_pnl": round(net_pnl, 6),
        "net_return_pct": round(net_return_pct, 6),
        "fees": round(fees, 6),
    }


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
        }

    pnl_list = [float(value) for value in trades_df["net_pnl"].to_list()]
    r_list = [float(value) for value in trades_df["gross_r"].to_list()]
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
                1.0
                + pl.col("cumulative_net_pnl") / backtest_config.notional_usdt
            ).alias("nav")
        )
        .with_columns(
            pl.col("nav").cum_max().alias("peak_nav"),
        )
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


def run_backtest(
    df: pl.DataFrame,
    preset: str,
    detect_config: WedgeConfig,
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
        "wedge_high",
        "wedge_low",
        "wedge_range",
        "up_first_push_idx",
        "up_second_push_idx",
        "up_third_push_idx",
        "up_third_push_price",
        "down_first_push_idx",
        "down_second_push_idx",
        "down_third_push_idx",
        "down_third_push_price",
        "wedge_top_setup",
        "wedge_bottom_setup",
        "wedge_top_signal",
        "wedge_bottom_signal",
        "wedge_top_follow_through",
        "wedge_bottom_follow_through",
    ]
    data = {col: df[col].to_list() for col in cols}

    top_signals = df.filter(pl.col("wedge_top_signal"))["idx"].to_list()
    bottom_signals = df.filter(pl.col("wedge_bottom_signal"))["idx"].to_list()
    events = [(idx, "short") for idx in top_signals] + [(idx, "long") for idx in bottom_signals]
    events.sort(key=lambda item: item[0])

    trades: list[dict[str, Any]] = []
    next_available_idx = 0

    for signal_idx, side in events:
        if signal_idx < next_available_idx:
            continue

        atr = float(data["atr"][signal_idx] or 0.0)
        if atr <= 0:
            continue

        entry_price = float(data["close"][signal_idx])
        wedge_range = float(data["wedge_range"][signal_idx] or 0.0)
        if side == "short":
            third_push_price = float(data["up_third_push_price"][signal_idx])
            stop_price = max(
                third_push_price + backtest_config.stop_buffer_atr * atr,
                entry_price + backtest_config.min_stop_atr * atr,
            )
            risk_per_unit = stop_price - entry_price
            if risk_per_unit <= 0:
                continue
            target_distance = max(
                wedge_range * backtest_config.target_range_multiplier,
                risk_per_unit * backtest_config.target_r_multiple,
            )
            target_price = entry_price - target_distance
        else:
            third_push_price = float(data["down_third_push_price"][signal_idx])
            stop_price = min(
                third_push_price - backtest_config.stop_buffer_atr * atr,
                entry_price - backtest_config.min_stop_atr * atr,
            )
            risk_per_unit = entry_price - stop_price
            if risk_per_unit <= 0:
                continue
            target_distance = max(
                wedge_range * backtest_config.target_range_multiplier,
                risk_per_unit * backtest_config.target_r_multiple,
            )
            target_price = entry_price + target_distance

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
                "signal_idx": pl.Int64,
                "exit_idx": pl.Int64,
                "signal_time": pl.String,
                "signal_price": pl.Float64,
                "entry_time": pl.String,
                "exit_time": pl.String,
                "entry_price": pl.Float64,
                "exit_price": pl.Float64,
                "stop_price": pl.Float64,
                "target_price": pl.Float64,
                "wedge_high": pl.Float64,
                "wedge_low": pl.Float64,
                "wedge_range": pl.Float64,
                "third_push_price": pl.Float64,
                "first_push_idx": pl.Int64,
                "second_push_idx": pl.Int64,
                "third_push_idx": pl.Int64,
                "atr_at_entry": pl.Float64,
                "holding_bars": pl.Int64,
                "exit_reason": pl.String,
                "gross_r": pl.Float64,
                "net_pnl": pl.Float64,
                "net_return_pct": pl.Float64,
                "fees": pl.Float64,
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
        "wedge_high",
        "wedge_low",
        "wedge_range",
        "up_first_push_idx",
        "up_second_push_idx",
        "up_third_push_idx",
        "up_third_push_price",
        "down_first_push_idx",
        "down_second_push_idx",
        "down_third_push_idx",
        "down_third_push_price",
        "wedge_top_setup",
        "wedge_bottom_setup",
        "wedge_top_signal",
        "wedge_bottom_signal",
        "wedge_top_follow_through",
        "wedge_bottom_follow_through",
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
    df = detect_wedge(df_source, config=detect_config)
    df = label_wedge_outcomes(df, config=detect_config)

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
    parser = argparse.ArgumentParser(description="楔形反转策略回测。")
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
