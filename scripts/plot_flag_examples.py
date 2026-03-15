from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots

from detect_flag import FlagConfig, detect_flag, prepare_ohlcv


DATA_PATH = Path("data/binance_um_perp/ETHUSDT/5m/ETHUSDT-5m-2026-02.csv")
OUTPUT_DIR = Path("outputs")
PNG_PATH = OUTPUT_DIR / "eth-5m-flag-examples.png"
HTML_PATH = OUTPUT_DIR / "eth-5m-flag-examples.html"

CONFIG = FlagConfig()
PLOT_LOOKBACK = 36
PLOT_LOOKFORWARD = 18


def pick_candidate(df: pl.DataFrame, side: str) -> dict[str, object]:
    if side == "bull":
        filtered = (
            df.filter(pl.col("bull_flag_breakout"))
            .with_columns(
                (
                    (pl.col("uptrend_score") / CONFIG.trend_score_window)
                    + (pl.col("body_size") / pl.col("atr"))
                    + pl.col("close_pos")
                    - (pl.col("flag_range") / (pl.col("atr") * CONFIG.flag_range_atr))
                ).alias("score")
            )
            .sort("score", descending=True)
        )
    else:
        filtered = (
            df.filter(pl.col("bear_flag_breakout"))
            .with_columns(
                (
                    (pl.col("downtrend_score") / CONFIG.trend_score_window)
                    + (pl.col("body_size") / pl.col("atr"))
                    + (1 - pl.col("close_pos"))
                    - (pl.col("flag_range") / (pl.col("atr") * CONFIG.flag_range_atr))
                ).alias("score")
            )
            .sort("score", descending=True)
        )

    row = filtered.select(
        [
            "idx",
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "rolling_high_flag",
            "rolling_low_flag",
            "atr",
            "score",
        ]
    ).row(0, named=True)
    return row


def add_flag_panel(fig: go.Figure, plot_df: pl.DataFrame, candidate: dict[str, object], row: int, title: str, color: str) -> None:
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

    breakout_idx = int(candidate["idx"])
    flag_start_idx = breakout_idx - CONFIG.flag_window
    flag_end_idx = breakout_idx - 1

    flag_df = plot_df.filter(
        (pl.col("idx") >= flag_start_idx) & (pl.col("idx") <= flag_end_idx)
    )
    flag_high = float(flag_df["high"].max())
    flag_low = float(flag_df["low"].min())
    flag_start_time = flag_df["timestamp"].min()
    flag_end_time = flag_df["timestamp"].max()
    breakout_time = candidate["timestamp"]
    breakout_price = float(candidate["close"])

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
    fig.add_shape(
        type="line",
        x0=flag_start_time,
        x1=flag_end_time,
        y0=flag_high,
        y1=flag_high,
        line=dict(color=color, width=2, dash="dash"),
        xref=xref,
        yref=yref,
    )
    fig.add_shape(
        type="line",
        x0=flag_start_time,
        x1=flag_end_time,
        y0=flag_low,
        y1=flag_low,
        line=dict(color=color, width=2, dash="dash"),
        xref=xref,
        yref=yref,
    )

    fig.add_trace(
        go.Scatter(
            x=[breakout_time],
            y=[breakout_price],
            mode="markers+text",
            text=["突破点"],
            textposition="top center",
            marker=dict(size=11, color=color, symbol="diamond"),
            name=f"{title} 突破",
            showlegend=False,
        ),
        row=row,
        col=1,
    )

    fig.add_annotation(
        x=breakout_time,
        y=flag_high if row == 1 else flag_low,
        xref=xref,
        yref=yref,
        text=(
            f"{title}<br>"
            f"旗形区间: {flag_low:.2f} - {flag_high:.2f}<br>"
            f"突破收盘: {breakout_price:.2f}"
        ),
        showarrow=True,
        arrowhead=2,
        arrowsize=1,
        arrowwidth=1.2,
        arrowcolor=color,
        ax=20,
        ay=-60 if row == 1 else 60,
        bgcolor="rgba(255,255,255,0.85)",
        bordercolor=color,
    )


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = prepare_ohlcv(pl.read_csv(DATA_PATH))
    df = detect_flag(df, config=CONFIG)

    bull = pick_candidate(df, "bull")
    bear = pick_candidate(df, "bear")

    bull_slice = df.filter(
        (pl.col("idx") >= int(bull["idx"]) - PLOT_LOOKBACK)
        & (pl.col("idx") <= int(bull["idx"]) + PLOT_LOOKFORWARD)
    )
    bear_slice = df.filter(
        (pl.col("idx") >= int(bear["idx"]) - PLOT_LOOKBACK)
        & (pl.col("idx") <= int(bear["idx"]) + PLOT_LOOKFORWARD)
    )

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=False,
        vertical_spacing=0.08,
        subplot_titles=[
            f"ETHUSDT 5m 多头旗形示例 - {bull['timestamp']}",
            f"ETHUSDT 5m 空头旗形示例 - {bear['timestamp']}",
        ],
    )

    add_flag_panel(fig, bull_slice, bull, row=1, title="多头旗形", color="#1f9d55")
    add_flag_panel(fig, bear_slice, bear, row=2, title="空头旗形", color="#c0392b")

    fig.update_layout(
        title="ETHUSDT 5m 旗形示例图",
        width=1500,
        height=1100,
        template="plotly_white",
        xaxis_rangeslider_visible=False,
        xaxis2_rangeslider_visible=False,
        margin=dict(l=60, r=40, t=80, b=40),
    )

    fig.write_html(HTML_PATH)
    fig.write_image(PNG_PATH, scale=2)

    print(f"PNG: {PNG_PATH.resolve()}")
    print(f"HTML: {HTML_PATH.resolve()}")
    print(f"Bull idx: {bull['idx']} @ {bull['timestamp']}")
    print(f"Bear idx: {bear['idx']} @ {bear['timestamp']}")


if __name__ == "__main__":
    main()
