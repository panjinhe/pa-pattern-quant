from __future__ import annotations

import argparse
from pathlib import Path

import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots

from detect_flag import FlagConfig, detect_flag, label_flag_outcomes, prepare_ohlcv, read_ohlcv
from strategy_flag import (
    build_breakout_strategy,
    build_final_flag_reversal_research,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = Path(__file__).resolve().parent
PLOTS_DIR = OUTPUT_DIR / "plots"
DEFAULT_INPUT = REPO_ROOT / "data" / "binance_um_perp" / "ETHUSDT" / "5m" / "ETHUSDT-5m-history.parquet"
BREAKOUT_PNG = PLOTS_DIR / "flag-breakouts.png"
BREAKOUT_HTML = PLOTS_DIR / "flag-breakouts.html"
REVERSAL_PNG = PLOTS_DIR / "final-flag-reversals.png"
REVERSAL_HTML = PLOTS_DIR / "final-flag-reversals.html"

CONFIG = FlagConfig()
LOOKBACK_BARS = 36
LOOKFORWARD_BARS = 18


def _pick_best_trade(trades: pl.DataFrame, signal_name: str) -> dict[str, object]:
    return (
        trades.filter(pl.col("signal_name") == signal_name)
        .sort("realized_r", descending=True)
        .row(0, named=True)
    )


def _slice_plot_df(df: pl.DataFrame, start_idx: int, end_idx: int) -> pl.DataFrame:
    return df.filter((pl.col("idx") >= start_idx) & (pl.col("idx") <= end_idx))


def _add_trade_panel(
    fig: go.Figure,
    plot_df: pl.DataFrame,
    signal_row: dict[str, object],
    trade_row: dict[str, object],
    row: int,
    title: str,
    color: str,
) -> None:
    xref = "x" if row == 1 else f"x{row}"
    yref = "y" if row == 1 else f"y{row}"

    fig.add_trace(
        go.Candlestick(
            x=plot_df["timestamp"].to_list(),
            open=plot_df["open"].to_list(),
            high=plot_df["high"].to_list(),
            low=plot_df["low"].to_list(),
            close=plot_df["close"].to_list(),
            name=title,
            showlegend=False,
        ),
        row=row,
        col=1,
    )

    signal_idx = int(signal_row["idx"])
    flag_start_idx = max(0, signal_idx - CONFIG.flag_window)
    flag_df = plot_df.filter((pl.col("idx") >= flag_start_idx) & (pl.col("idx") <= signal_idx - 1))
    flag_high = float(flag_df["high"].max())
    flag_low = float(flag_df["low"].min())
    flag_start_time = flag_df["timestamp"].min()
    flag_end_time = flag_df["timestamp"].max()

    fig.add_shape(
        type="rect",
        x0=flag_start_time,
        x1=flag_end_time,
        y0=flag_low,
        y1=flag_high,
        fillcolor=color,
        opacity=0.12,
        line=dict(color=color, width=2),
        xref=xref,
        yref=yref,
    )

    signal_time = signal_row["timestamp"]
    signal_price = float(signal_row["close"])
    entry_time = trade_row["entry_time"]
    exit_time = trade_row["exit_time"]
    entry_price = float(trade_row["entry_price"])
    exit_price = float(trade_row["exit_price"])
    stop_price = float(trade_row["stop_price"])
    target_price = float(trade_row["target_price"])

    fig.add_trace(
        go.Scatter(
            x=[signal_time, entry_time, exit_time],
            y=[signal_price, entry_price, exit_price],
            mode="markers+text",
            text=["信号", "入场", "离场"],
            textposition="top center",
            marker=dict(size=10, color=[color, "#1f4e79", "#444444"], symbol=["diamond", "circle", "x"]),
            showlegend=False,
        ),
        row=row,
        col=1,
    )

    fig.add_shape(
        type="line",
        x0=plot_df["timestamp"].min(),
        x1=plot_df["timestamp"].max(),
        y0=stop_price,
        y1=stop_price,
        line=dict(color="#c0392b", width=1.5, dash="dot"),
        xref=xref,
        yref=yref,
    )
    fig.add_shape(
        type="line",
        x0=plot_df["timestamp"].min(),
        x1=plot_df["timestamp"].max(),
        y0=target_price,
        y1=target_price,
        line=dict(color="#1f9d55", width=1.5, dash="dot"),
        xref=xref,
        yref=yref,
    )

    fig.add_annotation(
        x=entry_time,
        y=entry_price,
        xref=xref,
        yref=yref,
        text=(
            f"{title}<br>"
            f"方向: {trade_row['side']}<br>"
            f"R: {float(trade_row['realized_r']):.2f}<br>"
            f"止损: {stop_price:.2f}<br>"
            f"目标: {target_price:.2f}"
        ),
        showarrow=True,
        arrowhead=2,
        arrowsize=1,
        arrowwidth=1.2,
        arrowcolor=color,
        ax=20,
        ay=-60 if row == 1 else 60,
        bgcolor="rgba(255,255,255,0.86)",
        bordercolor=color,
    )


def _make_figure(
    df: pl.DataFrame,
    trades: pl.DataFrame,
    specs: list[tuple[str, str, str]],
    png_path: Path,
    html_path: Path,
    figure_title: str,
) -> None:
    available_specs = [spec for spec in specs if trades.filter(pl.col("signal_name") == spec[0]).height > 0]
    if not available_specs:
        return

    fig = make_subplots(
        rows=len(available_specs),
        cols=1,
        shared_xaxes=False,
        vertical_spacing=0.08,
        subplot_titles=[spec[1] for spec in available_specs],
    )

    for row_index, (signal_name, title, color) in enumerate(available_specs, start=1):
        trade_row = _pick_best_trade(trades, signal_name)
        signal_row = df.row(int(trade_row["signal_idx"]), named=True)
        start_idx = max(0, int(trade_row["signal_idx"]) - LOOKBACK_BARS)
        end_idx = min(df.height - 1, int(trade_row["exit_idx"]) + LOOKFORWARD_BARS)
        plot_df = _slice_plot_df(df, start_idx, end_idx)
        _add_trade_panel(fig, plot_df, signal_row, trade_row, row_index, title, color)

    fig.update_layout(
        title=figure_title,
        width=1500,
        height=max(600, 520 * len(available_specs)),
        template="plotly_white",
        xaxis_rangeslider_visible=False,
        margin=dict(l=60, r=40, t=80, b=40),
    )
    for index in range(2, len(available_specs) + 1):
        fig.update_layout(**{f"xaxis{index}_rangeslider_visible": False})
    html_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(html_path)
    fig.write_image(png_path, scale=2)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render flag pattern case charts.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Path to OHLCV input.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    df = prepare_ohlcv(read_ohlcv(args.input))
    df = label_flag_outcomes(detect_flag(df))

    breakout_trades = build_breakout_strategy(df)
    final_trades = build_final_flag_reversal_research(df)

    _make_figure(
        df=df,
        trades=breakout_trades,
        specs=[
            ("bull_flag_breakout", "ETHUSDT 5m 多头旗形顺势突破案例", "#1f9d55"),
            ("bear_flag_breakout", "ETHUSDT 5m 空头旗形顺势突破案例", "#c0392b"),
        ],
        png_path=BREAKOUT_PNG,
        html_path=BREAKOUT_HTML,
        figure_title="23A 旗形 - 顺势旗形突破案例",
    )

    _make_figure(
        df=df,
        trades=final_trades,
        specs=[
            ("final_bull_flag_reversal", "ETHUSDT 5m 最终多头旗形失败突破反转案例", "#d35400"),
            ("final_bear_flag_reversal", "ETHUSDT 5m 最终空头旗形失败突破反转案例", "#2980b9"),
        ],
        png_path=REVERSAL_PNG,
        html_path=REVERSAL_HTML,
        figure_title="23A 旗形 - 最终旗形失败突破反转案例",
    )

    print(f"PNG: {BREAKOUT_PNG.resolve()}")
    print(f"HTML: {BREAKOUT_HTML.resolve()}")
    print(f"PNG: {REVERSAL_PNG.resolve()}")
    print(f"HTML: {REVERSAL_HTML.resolve()}")


if __name__ == "__main__":
    main()
