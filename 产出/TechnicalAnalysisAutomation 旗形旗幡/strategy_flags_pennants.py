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

from detect_flags_pennants import (  # noqa: E402
    PatternConfig,
    build_signal_frame,
    extract_flags_pennants,
    get_config,
    label_flags_pennants,
    patterns_to_frame,
    prepare_ohlcv,
    read_ohlcv,
    summarize_patterns,
)


SOURCE_URL = "https://github.com/neurotrader888/TechnicalAnalysisAutomation/blob/main/flags_pennants.py"
SOURCE_COMMIT = "da99c20bf3d977b639451258cd6cfca9baa1dcc3"
SOURCE_METHOD = "find_flags_pennants_trendline"


@dataclass(frozen=True)
class BacktestConfig:
    fee_bps_per_side: float = 2.0
    notional_usdt: float = 10_000.0
    min_trades_for_baseline: int = 20
    allow_overlap_samples: bool = True


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
            "target_hit_count": 0,
            "target_hit_rate": 0.0,
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
    target_hit_count = trades_df.filter(pl.col("target_hit")).height
    return {
        "trade_count": trade_count,
        "win_rate": round((len(wins) / trade_count) * 100.0, 4),
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
        "target_hit_count": target_hit_count,
        "target_hit_rate": round((target_hit_count / trade_count) * 100.0, 4),
        "bull_flag_trades": trades_df.filter(pl.col("family") == "bull_flag").height,
        "bear_flag_trades": trades_df.filter(pl.col("family") == "bear_flag").height,
        "bull_pennant_trades": trades_df.filter(pl.col("family") == "bull_pennant").height,
        "bear_pennant_trades": trades_df.filter(pl.col("family") == "bear_pennant").height,
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
                "family": pl.String,
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
        trades_df.sort(["exit_idx", "conf_idx"])
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
            "conf_idx",
            "exit_idx",
            "entry_time",
            "exit_time",
            "side",
            "family",
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
    row: dict[str, Any],
    preset: str,
    config: BacktestConfig,
) -> dict[str, Any]:
    side = str(row["direction"])
    entry_price = float(row["conf_price"])
    exit_price = float(row["exit_price"])
    quantity = config.notional_usdt / entry_price
    fee_rate = config.fee_bps_per_side / 10_000.0

    if side == "long":
        gross_pnl = (exit_price - entry_price) * quantity
        reference_stop_distance = max(entry_price - float(row["structure_stop_price"]), 1e-9)
    else:
        gross_pnl = (entry_price - exit_price) * quantity
        reference_stop_distance = max(float(row["structure_stop_price"]) - entry_price, 1e-9)

    fees = quantity * (entry_price + exit_price) * fee_rate
    net_pnl = gross_pnl - fees
    gross_return_pct = (gross_pnl / config.notional_usdt) * 100.0
    net_return_pct = (net_pnl / config.notional_usdt) * 100.0
    structure_r = gross_pnl / max((reference_stop_distance * quantity), 1e-9)
    pole_r = float(row["future_return_r"])

    return {
        "preset": preset,
        "family": str(row["family"]),
        "pattern_kind": str(row["pattern_kind"]),
        "side": side,
        "base_idx": int(row["base_idx"]),
        "tip_idx": int(row["tip_idx"]),
        "conf_idx": int(row["conf_idx"]),
        "flag_end_idx": int(row["flag_end_idx"]),
        "exit_idx": int(row["exit_idx"]),
        "support_pivot_idx": row["support_pivot_idx"],
        "resist_pivot_idx": row["resist_pivot_idx"],
        "flag_extreme_idx": row["flag_extreme_idx"],
        "base_time": str(row["base_time"]),
        "tip_time": str(row["tip_time"]),
        "entry_time": str(row["conf_time"]),
        "exit_time": str(row["exit_time"]),
        "base_price": round(float(row["base_price"]), 6),
        "tip_price": round(float(row["tip_price"]), 6),
        "entry_price": round(entry_price, 6),
        "exit_price": round(exit_price, 6),
        "support_tip_price": round(float(row["support_tip_price"]), 6),
        "support_conf_price": round(float(row["support_conf_price"]), 6),
        "resist_tip_price": round(float(row["resist_tip_price"]), 6),
        "resist_conf_price": round(float(row["resist_conf_price"]), 6),
        "flag_extreme_price": round(float(row["flag_extreme_price"]), 6)
        if row["flag_extreme_price"] is not None
        else None,
        "structure_stop_price": round(float(row["structure_stop_price"]), 6),
        "target_price": round(float(row["target_price"]), 6),
        "target_hit": bool(row["target_hit"]),
        "flag_width": int(row["flag_width"]),
        "pole_width": int(row["pole_width"]),
        "hold_bars": int(row["hold_bars"]),
        "holding_bars": int(row["exit_idx"]) - int(row["conf_idx"]),
        "flag_height_log": round(float(row["flag_height_log"]), 6),
        "flag_height_ratio": round(float(row["flag_height_ratio"]), 6),
        "pole_height_log": round(float(row["pole_height_log"]), 6),
        "support_intercept_log": round(float(row["support_intercept_log"]), 6),
        "support_slope": round(float(row["support_slope"]), 6),
        "resist_intercept_log": round(float(row["resist_intercept_log"]), 6),
        "resist_slope": round(float(row["resist_slope"]), 6),
        "flag_quality_score": round(float(row["flag_quality_score"]), 6),
        "gross_log_return": round(float(row["forward_log_return"]), 6),
        "gross_return_pct": round(gross_return_pct, 6),
        "gross_r": round(pole_r, 6),
        "structure_r": round(structure_r, 6),
        "net_pnl": round(net_pnl, 6),
        "net_return_pct": round(net_return_pct, 6),
        "fees": round(fees, 6),
        "exit_reason": "固定持有期",
    }


def build_flags_pennants_strategy(
    patterns_df: pl.DataFrame,
    preset: str,
    backtest_config: BacktestConfig,
) -> pl.DataFrame:
    if patterns_df.is_empty():
        return pl.DataFrame(
            schema={
                "preset": pl.String,
                "family": pl.String,
                "pattern_kind": pl.String,
                "side": pl.String,
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
                "entry_time": pl.String,
                "exit_time": pl.String,
                "base_price": pl.Float64,
                "tip_price": pl.Float64,
                "entry_price": pl.Float64,
                "exit_price": pl.Float64,
                "support_tip_price": pl.Float64,
                "support_conf_price": pl.Float64,
                "resist_tip_price": pl.Float64,
                "resist_conf_price": pl.Float64,
                "flag_extreme_price": pl.Float64,
                "structure_stop_price": pl.Float64,
                "target_price": pl.Float64,
                "target_hit": pl.Boolean,
                "flag_width": pl.Int64,
                "pole_width": pl.Int64,
                "hold_bars": pl.Int64,
                "holding_bars": pl.Int64,
                "flag_height_log": pl.Float64,
                "flag_height_ratio": pl.Float64,
                "pole_height_log": pl.Float64,
                "support_intercept_log": pl.Float64,
                "support_slope": pl.Float64,
                "resist_intercept_log": pl.Float64,
                "resist_slope": pl.Float64,
                "flag_quality_score": pl.Float64,
                "gross_log_return": pl.Float64,
                "gross_return_pct": pl.Float64,
                "gross_r": pl.Float64,
                "structure_r": pl.Float64,
                "net_pnl": pl.Float64,
                "net_return_pct": pl.Float64,
                "fees": pl.Float64,
                "exit_reason": pl.String,
            }
        )

    active_until = -1
    rows: list[dict[str, Any]] = []
    for row in patterns_df.sort("conf_idx").to_dicts():
        conf_idx = int(row["conf_idx"])
        exit_idx = int(row["exit_idx"])
        if not backtest_config.allow_overlap_samples and conf_idx <= active_until:
            continue
        rows.append(_build_trade_record(row, preset=preset, config=backtest_config))
        active_until = max(active_until, exit_idx)

    return pl.DataFrame(rows).sort(["conf_idx", "exit_idx"])


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def run_preset(
    df: pl.DataFrame,
    output_dir: Path,
    preset: str,
    backtest_config: BacktestConfig,
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, Any]]:
    detect_config = get_config(preset)
    patterns = extract_flags_pennants(df, config=detect_config)
    patterns_df = patterns_to_frame(df, patterns, config=detect_config)
    patterns_df = label_flags_pennants(df, patterns_df, config=detect_config)
    signals_df = build_signal_frame(df, patterns_df)
    trades_df = build_flags_pennants_strategy(
        patterns_df=patterns_df,
        preset=preset,
        backtest_config=backtest_config,
    )
    equity_curve_df = build_equity_curve(trades_df, backtest_config=backtest_config)

    patterns_path = output_dir / f"patterns_{preset}.csv"
    signals_path = output_dir / f"signals_{preset}.parquet"
    trades_path = output_dir / f"trades_{preset}.csv"
    equity_curve_path = output_dir / f"equity-curve-{preset}.csv"
    detection_summary_path = output_dir / f"detection-summary-{preset}.json"
    summary_path = output_dir / f"backtest-summary-{preset}.json"

    patterns_df.write_csv(patterns_path)
    signals_df.write_parquet(signals_path)
    trades_df.write_csv(trades_path)
    equity_curve_df.write_csv(equity_curve_path)

    detection_summary = summarize_patterns(patterns_df)
    _write_json(
        detection_summary_path,
        {
            "preset": preset,
            "config": asdict(detect_config),
            "summary": detection_summary,
            "patterns_path": str(patterns_path.resolve()),
            "signals_path": str(signals_path.resolve()),
        },
    )

    summary = summarize_backtest(trades_df)
    summary.update(
        {
            "preset": preset,
            "detect_config": asdict(detect_config),
            "backtest_config": asdict(backtest_config),
            "detection_summary": detection_summary,
            "patterns_path": str(patterns_path.resolve()),
            "signals_path": str(signals_path.resolve()),
            "trades_path": str(trades_path.resolve()),
            "equity_curve_path": str(equity_curve_path.resolve()),
            "detection_summary_path": str(detection_summary_path.resolve()),
        }
    )
    summary.update(summarize_equity_curve(equity_curve_df))
    _write_json(summary_path, summary)
    summary["summary_path"] = str(summary_path.resolve())
    return trades_df, equity_curve_df, summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="复刻 TechnicalAnalysisAutomation 的旗形/旗幡固定持有期策略。")
    parser.add_argument("--input", type=Path, required=True, help="OHLCV 数据路径")
    parser.add_argument("--output-dir", type=Path, required=True, help="产出目录")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    backtest_config = BacktestConfig()

    df = prepare_ohlcv(read_ohlcv(args.input))
    sample_info = {
        "rows": df.height,
        "start": df["timestamp"].min().isoformat(),
        "end": df["timestamp"].max().isoformat(),
    }

    _, _, baseline_summary = run_preset(
        df=df,
        output_dir=args.output_dir,
        preset="baseline",
        backtest_config=backtest_config,
    )

    parameter_adjustment_triggered = baseline_summary["trade_count"] < backtest_config.min_trades_for_baseline
    adjusted_summary: dict[str, Any] | None = None
    selected_preset = "baseline"
    if parameter_adjustment_triggered:
        _, _, adjusted_summary = run_preset(
            df=df,
            output_dir=args.output_dir,
            preset="adjusted",
            backtest_config=backtest_config,
        )
        selected_preset = "adjusted"

    overall_summary: dict[str, Any] = {
        "source_reference": {
            "url": SOURCE_URL,
            "repo_commit": SOURCE_COMMIT,
            "upstream_method": SOURCE_METHOD,
        },
        "data_path": str(args.input.resolve()),
        "sample_info": sample_info,
        "fee_slippage_assumption": "双边合计按每边 2 bps 计入，含手续费与滑点",
        "capital_assumption": (
            "固定名义仓位 10000 USDT，每笔独立计盈亏；默认允许信号样本重叠，"
            "以贴近上游脚本的逐样本统计方式，不做资金占用约束。"
        ),
        "r_definition": "R 使用 pole_height_log 归一化，属于结构归一化收益，不是基于硬止损的风险倍数。",
        "parameter_adjustment_triggered": parameter_adjustment_triggered,
        "selected_preset": selected_preset,
        "baseline": baseline_summary,
    }
    if adjusted_summary is not None:
        overall_summary["adjusted"] = adjusted_summary

    _write_json(args.output_dir / "backtest-summary.json", overall_summary)
    print(json.dumps(overall_summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
