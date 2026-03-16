from __future__ import annotations

import argparse
import json
from pathlib import Path

import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots


def _load_summary(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _pick_examples(trades_df: pl.DataFrame) -> list[dict]:
    if trades_df.is_empty():
        return []

    examples: list[dict] = []
    short_df = trades_df.filter(pl.col("side") == "short").sort("gross_r", descending=True)
    long_df = trades_df.filter(pl.col("side") == "long").sort("gross_r", descending=True)

    if not short_df.is_empty():
        examples.append(short_df.row(0, named=True))
    if not long_df.is_empty():
        examples.append(long_df.row(0, named=True))

    if not examples:
        return trades_df.sort("gross_r", descending=True).head(2).to_dicts()

    if len(examples) == 1 and trades_df.height > 1:
        fallback = (
            trades_df.sort("gross_r", descending=True)
            .filter(pl.col("entry_time") != examples[0]["entry_time"])
            .head(1)
            .to_dicts()
        )
        examples.extend(fallback)

    return examples[:2]


def _add_trade_panel(
    fig: go.Figure,
    row: int,
    plot_df: pl.DataFrame,
    trade: dict,
) -> None:
    xref = "x" if row == 1 else f"x{row}"
    yref = "y" if row == 1 else f"y{row}"
    side = trade["side"]
    title = "多头趋势末端失败上破做空" if side == "short" else "空头趋势末端失败下破做多"
    color = "#c0392b" if side == "short" else "#1f9d55"

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

    flag_start_idx = int(trade["breakout_idx"]) - 12
    flag_end_idx = int(trade["breakout_idx"]) - 1
    flag_df = plot_df.filter(
        (pl.col("idx") >= flag_start_idx) & (pl.col("idx") <= flag_end_idx)
    )
    if not flag_df.is_empty():
        fig.add_shape(
            type="rect",
            x0=flag_df["timestamp"].min(),
            x1=flag_df["timestamp"].max(),
            y0=float(trade["flag_low"]),
            y1=float(trade["flag_high"]),
            fillcolor=color,
            opacity=0.12,
            line=dict(color=color, width=2),
            xref=xref,
            yref=yref,
        )

    marker_specs = [
        ("突破", "diamond", trade["breakout_time"], trade["breakout_price"], color),
        ("入场", "circle", trade["entry_time"], trade["entry_price"], "#000000"),
        ("离场", "x", trade["exit_time"], trade["exit_price"], "#34495e"),
    ]
    for label, symbol, x_value, y_value, marker_color in marker_specs:
        fig.add_trace(
            go.Scatter(
                x=[x_value],
                y=[y_value],
                mode="markers+text",
                text=[label],
                textposition="top center",
                marker=dict(size=10, symbol=symbol, color=marker_color),
                showlegend=False,
            ),
            row=row,
            col=1,
        )

    for price, line_color, label in [
        (float(trade["stop_price"]), "#d35454", "止损"),
        (float(trade["target_price"]), "#2980b9", "目标"),
    ]:
        fig.add_shape(
            type="line",
            x0=plot_df["timestamp"].min(),
            x1=plot_df["timestamp"].max(),
            y0=price,
            y1=price,
            line=dict(color=line_color, width=1.5, dash="dash"),
            xref=xref,
            yref=yref,
        )
        fig.add_annotation(
            x=plot_df["timestamp"].max(),
            y=price,
            xref=xref,
            yref=yref,
            text=label,
            showarrow=False,
            bgcolor="rgba(255,255,255,0.75)",
            bordercolor=line_color,
            xanchor="left",
        )

    fig.add_annotation(
        x=plot_df["timestamp"].min(),
        y=float(plot_df["high"].max()),
        xref=xref,
        yref=yref,
        text=(
            f"{title}<br>"
            f"R: {trade['gross_r']:.2f}<br>"
            f"净盈亏: {trade['net_pnl']:.2f} USDT<br>"
            f"持仓: {trade['holding_bars']} 根"
        ),
        showarrow=False,
        align="left",
        bgcolor="rgba(255,255,255,0.85)",
        bordercolor=color,
        xanchor="left",
        yanchor="top",
    )


def _plot_equity_curve(
    equity_curve_df: pl.DataFrame,
    preset: str,
    summary: dict,
    plots_dir: Path,
) -> tuple[Path, Path]:
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        row_heights=[0.7, 0.3],
        subplot_titles=["累计净值", "回撤 (%)"],
    )

    x_values = equity_curve_df["exit_time"].to_list()
    nav_values = equity_curve_df["nav"].to_list()
    drawdown_pct_values = [value * 100.0 for value in equity_curve_df["drawdown_pct"].to_list()]

    fig.add_trace(
        go.Scatter(
            x=x_values,
            y=nav_values,
            mode="lines+markers",
            name="净值",
            line=dict(color="#1f4e79", width=2.5),
            marker=dict(size=5, color="#1f4e79"),
            hovertemplate="时间=%{x}<br>净值=%{y:.4f}<extra></extra>",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=x_values,
            y=drawdown_pct_values,
            mode="lines",
            name="回撤",
            line=dict(color="#c0392b", width=2),
            fill="tozeroy",
            fillcolor="rgba(192,57,43,0.15)",
            hovertemplate="时间=%{x}<br>回撤=%{y:.2f}%<extra></extra>",
        ),
        row=2,
        col=1,
    )

    fig.add_annotation(
        x=x_values[0],
        y=max(nav_values),
        xref="x",
        yref="y",
        text=(
            f"期末净值: {summary['ending_nav']:.4f}<br>"
            f"总净盈亏: {summary['total_net_pnl']:.2f} USDT<br>"
            f"最大回撤: {summary['max_drawdown']:.2f} USDT / {summary['max_drawdown_pct']:.2f}%"
        ),
        showarrow=False,
        align="left",
        bgcolor="rgba(255,255,255,0.88)",
        bordercolor="#1f4e79",
        xanchor="left",
        yanchor="top",
    )

    fig.update_yaxes(title_text="净值", row=1, col=1)
    fig.update_yaxes(title_text="回撤 (%)", row=2, col=1)
    fig.update_layout(
        title=f"最终旗形策略净值曲线 ({preset})",
        width=1600,
        height=900,
        template="plotly_white",
        margin=dict(l=60, r=40, t=80, b=40),
        xaxis_rangeslider_visible=False,
        xaxis2_rangeslider_visible=False,
    )

    html_path = plots_dir / "final-flag-equity-curve.html"
    png_path = plots_dir / "final-flag-equity-curve.png"
    fig.write_html(html_path)
    fig.write_image(png_path, scale=2)
    return html_path, png_path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="绘制最终旗形案例图。")
    parser.add_argument("--output-dir", type=Path, required=True, help="产出目录")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    summary = _load_summary(args.output_dir / "backtest-summary.json")
    preset = summary["selected_preset"]
    preset_summary = summary[preset]

    signals_df = pl.read_parquet(preset_summary["signals_path"])
    trades_df = pl.read_csv(preset_summary["trades_path"], try_parse_dates=True)
    equity_curve_df = pl.read_csv(preset_summary["equity_curve_path"], try_parse_dates=True)
    examples = _pick_examples(trades_df)
    if not examples:
        raise SystemExit("没有可绘制的交易样本。")

    plots_dir = args.output_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)
    lookback = 48
    lookforward = 24

    fig = make_subplots(
        rows=len(examples),
        cols=1,
        shared_xaxes=False,
        vertical_spacing=0.08,
        subplot_titles=[f"{item['side']} | 入场 {item['entry_time']}" for item in examples],
    )

    for row, trade in enumerate(examples, start=1):
        start_idx = max(0, int(trade["breakout_idx"]) - lookback)
        end_idx = int(trade["exit_idx"]) + lookforward
        plot_df = signals_df.filter(
            (pl.col("idx") >= start_idx) & (pl.col("idx") <= end_idx)
        )
        _add_trade_panel(fig, row=row, plot_df=plot_df, trade=trade)

    fig.update_layout(
        title=f"最终旗形失败突破反转案例图 ({preset})",
        width=1600,
        height=700 * len(examples),
        template="plotly_white",
        margin=dict(l=60, r=80, t=80, b=40),
        xaxis_rangeslider_visible=False,
    )
    for axis_index in range(2, len(examples) + 1):
        fig.update_layout({f"xaxis{axis_index}_rangeslider_visible": False})

    html_path = plots_dir / "final-flag-examples.html"
    png_path = plots_dir / "final-flag-examples.png"
    fig.write_html(html_path)
    fig.write_image(png_path, scale=2)
    equity_html_path, equity_png_path = _plot_equity_curve(
        equity_curve_df=equity_curve_df,
        preset=preset,
        summary=preset_summary,
        plots_dir=plots_dir,
    )

    print(f"html: {html_path.resolve()}")
    print(f"png: {png_path.resolve()}")
    print(f"equity html: {equity_html_path.resolve()}")
    print(f"equity png: {equity_png_path.resolve()}")


if __name__ == "__main__":
    main()
