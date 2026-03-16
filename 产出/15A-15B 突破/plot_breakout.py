from __future__ import annotations

import argparse
import html
import json
import logging
import warnings
from datetime import datetime
from pathlib import Path

import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots

try:
    from choreographer.utils._tmpfile import TmpDirWarning
except Exception:  # pragma: no cover - 仅用于静默第三方非阻塞 warning
    TmpDirWarning = None

if TmpDirWarning is not None:
    warnings.filterwarnings("ignore", category=TmpDirWarning)

logging.getLogger("choreographer").setLevel(logging.ERROR)
logging.getLogger("choreographer.utils._tmpfile").setLevel(logging.ERROR)

DEFAULT_CASE_COUNT = 100
LOOKBACK_BARS = 64
LOOKFORWARD_BARS = 32


def _load_summary(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _format_ts(value: object) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M")
    return str(value).replace("T", " ")


def _signal_label(signal_family: str) -> str:
    mapping = {
        "bull_followthrough": "上破后续做多",
        "bear_followthrough": "下破后续做空",
        "bull_failed_breakout": "上破失败做空",
        "bear_failed_breakout": "下破失败做多",
    }
    return mapping[signal_family]


def _build_selection_score(trade: dict) -> float:
    height_score = min(float(trade["range_height_atr"]), 8.0) / 8.0
    breakout_score = float(trade["breakout_body_ratio"]) * 0.35 + float(trade["signal_body_ratio"]) * 0.25
    move_bonus = 0.25 if trade.get("measured_move_hit_label") else 0.0
    failure_penalty = 0.10 if trade.get("failure_label") else 0.0
    return height_score * 0.45 + breakout_score + move_bonus - failure_penalty


def _select_time_dispersed(records: list[dict], target_count: int) -> list[dict]:
    if len(records) <= target_count:
        return records

    sorted_records = sorted(records, key=lambda item: item["entry_time"])
    buckets: list[list[dict]] = [[] for _ in range(target_count)]

    for index, record in enumerate(sorted_records):
        bucket = min(target_count - 1, index * target_count // len(sorted_records))
        buckets[bucket].append(record)

    selected: list[dict] = []
    for bucket_records in buckets:
        best_record = max(
            bucket_records,
            key=lambda item: (
                item["selection_score"],
                float(item["range_height_atr"]),
                -int(item["signal_idx"]),
            ),
        )
        selected.append(best_record)

    selected_ids = {int(item["signal_idx"]) for item in selected}
    if len(selected) < target_count:
        remainder = [
            item
            for item in sorted(records, key=lambda row: row["selection_score"], reverse=True)
            if int(item["signal_idx"]) not in selected_ids
        ]
        selected.extend(remainder[: target_count - len(selected)])

    return sorted(selected[:target_count], key=lambda item: item["entry_time"])


def _allocate_family_targets(trades_df: pl.DataFrame, total_count: int) -> dict[str, int]:
    families = trades_df["signal_family"].unique().to_list()
    family_counts = {
        family: trades_df.filter(pl.col("signal_family") == family).height
        for family in families
    }
    active_families = [family for family, count in family_counts.items() if count > 0]
    if not active_families:
        return {}

    allocation = {family: 0 for family in active_families}
    remaining = min(total_count, sum(family_counts[family] for family in active_families))
    base = remaining // len(active_families)
    for family in active_families:
        allocation[family] = min(base, family_counts[family])

    allocated = sum(allocation.values())
    while allocated < remaining:
        for family in sorted(active_families, key=lambda item: family_counts[item], reverse=True):
            if allocated >= remaining:
                break
            if allocation[family] < family_counts[family]:
                allocation[family] += 1
                allocated += 1

    return allocation


def _pick_examples(trades_df: pl.DataFrame, target_count: int = DEFAULT_CASE_COUNT) -> list[dict]:
    if trades_df.is_empty():
        return []

    allocations = _allocate_family_targets(trades_df, target_count)
    selected: list[dict] = []
    for family, family_target in allocations.items():
        if family_target <= 0:
            continue
        family_records = trades_df.filter(pl.col("signal_family") == family).to_dicts()
        for record in family_records:
            record["selection_score"] = _build_selection_score(record)
        selected.extend(_select_time_dispersed(family_records, family_target))

    selected = sorted(selected, key=lambda item: item["entry_time"])
    for example_no, record in enumerate(selected, start=1):
        record["example_no"] = example_no
        record["example_code"] = f"{example_no:03d}"
        record["example_file"] = f"breakout-example-{example_no:03d}.png"
    return selected


def _row_at_idx(plot_df: pl.DataFrame, idx: int | None) -> dict | None:
    if idx is None:
        return None
    row = plot_df.filter(pl.col("idx") == idx)
    if row.is_empty():
        return None
    return row.to_dicts()[0]


def _add_connected_points(
    fig: go.Figure,
    row: int,
    points: list[tuple[object, float, str]],
    color: str,
    textposition: str,
    width: float = 2.0,
    dash: str | None = None,
    marker_symbol: str = "circle",
) -> None:
    if not points:
        return

    line_kwargs: dict[str, object] = {"color": color, "width": width}
    if dash:
        line_kwargs["dash"] = dash

    mode = "markers+text" if len(points) == 1 else "lines+markers+text"
    fig.add_trace(
        go.Scatter(
            x=[point[0] for point in points],
            y=[point[1] for point in points],
            mode=mode,
            text=[point[2] for point in points],
            textposition=textposition,
            marker=dict(size=9, color=color, symbol=marker_symbol),
            line=line_kwargs,
            showlegend=False,
            hovertemplate="%{text}<br>时间=%{x}<br>价格=%{y:.2f}<extra></extra>",
        ),
        row=row,
        col=1,
    )


def _add_boundary_line(
    fig: go.Figure,
    row: int,
    start_x: object,
    end_x: object,
    y_value: float,
    color: str,
    dash: str | None = None,
    width: float = 2.0,
) -> None:
    line_kwargs: dict[str, object] = {"color": color, "width": width}
    if dash:
        line_kwargs["dash"] = dash

    fig.add_trace(
        go.Scatter(
            x=[start_x, end_x],
            y=[y_value, y_value],
            mode="lines",
            line=line_kwargs,
            showlegend=False,
            hoverinfo="skip",
        ),
        row=row,
        col=1,
    )


def _collect_touch_points(
    plot_df: pl.DataFrame,
    trade: dict,
    prefix: str,
) -> list[tuple[object, float, str]]:
    points: list[tuple[object, float, str]] = []
    for slot in range(1, 4):
        idx_value = trade.get(f"{prefix}_touch_{slot}_idx")
        if idx_value is None:
            continue
        row = _row_at_idx(plot_df, int(idx_value))
        if row is None:
            continue
        price = float(row["low"]) if prefix == "support" else float(row["high"])
        label = f"{'S' if prefix == 'support' else 'R'}{slot}"
        points.append((row["timestamp"], price, label))
    return points


def _add_trade_panel(
    fig: go.Figure,
    row: int,
    plot_df: pl.DataFrame,
    trade: dict,
) -> None:
    xref = "x" if row == 1 else f"x{row}"
    yref = "y" if row == 1 else f"y{row}"
    side = trade["side"]
    signal_family = trade["signal_family"]
    main_color = "#b03a2e" if side == "short" else "#1d8348"
    support_color = "#2874a6"
    resistance_color = "#b9770e"
    neutral_color = "#34495e"

    fig.add_trace(
        go.Candlestick(
            x=plot_df["timestamp"].to_list(),
            open=plot_df["open"].to_list(),
            high=plot_df["high"].to_list(),
            low=plot_df["low"].to_list(),
            close=plot_df["close"].to_list(),
            showlegend=False,
            name=_signal_label(signal_family),
        ),
        row=row,
        col=1,
    )

    start_row = _row_at_idx(plot_df, int(trade["range_window_start_idx"]))
    breakout_row = _row_at_idx(plot_df, int(trade["breakout_idx"]))
    signal_row = _row_at_idx(plot_df, int(trade["signal_idx"]))
    exit_row = _row_at_idx(plot_df, int(trade["exit_idx"]))
    if start_row and breakout_row:
        fig.add_shape(
            type="rect",
            x0=start_row["timestamp"],
            x1=breakout_row["timestamp"],
            y0=float(trade["range_low"]),
            y1=float(trade["range_high"]),
            fillcolor=main_color,
            opacity=0.08,
            line=dict(color=main_color, width=1.2),
            xref=xref,
            yref=yref,
        )
        _add_boundary_line(
            fig=fig,
            row=row,
            start_x=start_row["timestamp"],
            end_x=breakout_row["timestamp"],
            y_value=float(trade["range_low"]),
            color=support_color,
        )
        _add_boundary_line(
            fig=fig,
            row=row,
            start_x=start_row["timestamp"],
            end_x=breakout_row["timestamp"],
            y_value=float(trade["range_high"]),
            color=resistance_color,
        )
        extension_end = exit_row["timestamp"] if exit_row else plot_df["timestamp"].max()
        _add_boundary_line(
            fig=fig,
            row=row,
            start_x=breakout_row["timestamp"],
            end_x=extension_end,
            y_value=float(trade["range_low"]),
            color=support_color,
            dash="dot",
            width=1.4,
        )
        _add_boundary_line(
            fig=fig,
            row=row,
            start_x=breakout_row["timestamp"],
            end_x=extension_end,
            y_value=float(trade["range_high"]),
            color=resistance_color,
            dash="dot",
            width=1.4,
        )

    support_points = _collect_touch_points(plot_df, trade, "support")
    resistance_points = _collect_touch_points(plot_df, trade, "resistance")
    _add_connected_points(
        fig=fig,
        row=row,
        points=support_points,
        color=support_color,
        textposition="bottom center",
        width=1.8,
    )
    _add_connected_points(
        fig=fig,
        row=row,
        points=resistance_points,
        color=resistance_color,
        textposition="top center",
        width=1.8,
    )

    if breakout_row:
        fig.add_trace(
            go.Scatter(
                x=[breakout_row["timestamp"]],
                y=[float(plot_df.filter(pl.col("idx") == int(trade["breakout_idx"]))["close"].item())],
                mode="markers+text",
                text=["突破"],
                textposition="top center",
                marker=dict(size=12, color=main_color, symbol="diamond"),
                showlegend=False,
            ),
            row=row,
            col=1,
        )

    if signal_row:
        confirm_label = "后续确认" if "followthrough" in signal_family else "失败回归"
        fig.add_trace(
            go.Scatter(
                x=[signal_row["timestamp"]],
                y=[float(trade["entry_price"])],
                mode="markers+text",
                text=[confirm_label],
                textposition="top center" if side == "long" else "bottom center",
                marker=dict(size=12, color=neutral_color, symbol="circle"),
                showlegend=False,
            ),
            row=row,
            col=1,
        )

    if exit_row:
        fig.add_trace(
            go.Scatter(
                x=[exit_row["timestamp"]],
                y=[float(trade["exit_price"])],
                mode="markers+text",
                text=["离场"],
                textposition="top center",
                marker=dict(size=10, color=neutral_color, symbol="x"),
                showlegend=False,
            ),
            row=row,
            col=1,
        )

    if breakout_row and signal_row:
        if signal_family in {"bull_followthrough", "bull_failed_breakout"} and resistance_points:
            structure_points = [
                resistance_points[-1],
                (breakout_row["timestamp"], float(plot_df.filter(pl.col("idx") == int(trade["breakout_idx"]))["close"].item()), ""),
                (signal_row["timestamp"], float(trade["entry_price"]), ""),
            ]
        elif signal_family in {"bear_followthrough", "bear_failed_breakout"} and support_points:
            structure_points = [
                support_points[-1],
                (breakout_row["timestamp"], float(plot_df.filter(pl.col("idx") == int(trade["breakout_idx"]))["close"].item()), ""),
                (signal_row["timestamp"], float(trade["entry_price"]), ""),
            ]
        else:
            structure_points = [
                (breakout_row["timestamp"], float(plot_df.filter(pl.col("idx") == int(trade["breakout_idx"]))["close"].item()), ""),
                (signal_row["timestamp"], float(trade["entry_price"]), ""),
            ]
        _add_connected_points(
            fig=fig,
            row=row,
            points=structure_points,
            color=neutral_color,
            textposition="top center",
            width=1.6,
            dash="dot",
            marker_symbol="circle-open",
        )

    for price, line_color, label in [
        (float(trade["stop_price"]), "#cb4335", "止损"),
        (float(trade["target_price"]), "#2e86c1", "目标"),
    ]:
        fig.add_shape(
            type="line",
            x0=plot_df["timestamp"].min(),
            x1=plot_df["timestamp"].max(),
            y0=price,
            y1=price,
            line=dict(color=line_color, width=1.4, dash="dash"),
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
            font=dict(color=line_color, size=12),
            xanchor="right",
            yanchor="bottom",
            bgcolor="rgba(255,255,255,0.72)",
        )

    fig.add_annotation(
        x=plot_df["timestamp"].min(),
        y=float(plot_df["high"].max()),
        xref=xref,
        yref=yref,
        text=(
            f"#{trade['example_code']} {_signal_label(signal_family)}<br>"
            f"区间高度: {float(trade['range_height_atr']):.2f} ATR | 触碰: 支撑 {int(trade['support_touch_count'])} / 阻力 {int(trade['resistance_touch_count'])}<br>"
            f"突破实体: {float(trade['breakout_body_ratio']):.2f} | 确认实体: {float(trade['signal_body_ratio']):.2f}<br>"
            f"R: {float(trade['gross_r']):.2f} | 净盈亏: {float(trade['net_pnl']):.2f} USDT<br>"
            f"持仓: {int(trade['holding_bars'])} 根"
        ),
        showarrow=False,
        align="left",
        bgcolor="rgba(255,255,255,0.88)",
        bordercolor=main_color,
        xanchor="left",
        yanchor="top",
    )


def _build_trade_figure(plot_df: pl.DataFrame, trade: dict, preset: str) -> go.Figure:
    fig = make_subplots(rows=1, cols=1, shared_xaxes=False)
    _add_trade_panel(fig, row=1, plot_df=plot_df, trade=trade)
    fig.update_layout(
        title=f"突破案例 #{trade['example_code']} ({preset}) | 入场 {_format_ts(trade['entry_time'])}",
        width=1600,
        height=760,
        template="plotly_white",
        margin=dict(l=60, r=80, t=80, b=40),
        xaxis_rangeslider_visible=False,
    )
    return fig


def _cleanup_old_case_images(plots_dir: Path) -> None:
    for path in plots_dir.glob("breakout-example-*.png"):
        path.unlink(missing_ok=True)
    (plots_dir / "breakout-equity-curve.png").unlink(missing_ok=True)
    (plots_dir / "breakout-equity-curve.html").unlink(missing_ok=True)
    (plots_dir / "breakout-examples.html").unlink(missing_ok=True)
    (plots_dir / "breakout-examples-manifest.csv").unlink(missing_ok=True)


def _write_manifest(examples: list[dict], manifest_path: Path) -> None:
    manifest_rows = [
        {
            "example_no": item["example_no"],
            "example_file": item["example_file"],
            "side": item["side"],
            "signal_family": item["signal_family"],
            "entry_time": _format_ts(item["entry_time"]),
            "exit_time": _format_ts(item["exit_time"]),
            "range_height_atr": round(float(item["range_height_atr"]), 6),
            "gross_r": round(float(item["gross_r"]), 6),
            "net_pnl": round(float(item["net_pnl"]), 6),
            "selection_score": round(float(item["selection_score"]), 6),
        }
        for item in examples
    ]
    pl.DataFrame(manifest_rows).write_csv(manifest_path)


def _write_gallery_html(
    examples: list[dict],
    html_path: Path,
    preset: str,
    preset_summary: dict,
) -> None:
    cards = []
    for item in examples:
        cards.append(
            f"""
            <article class="card">
              <img src="{html.escape(item['example_file'])}" alt="案例 {item['example_code']}" loading="lazy">
              <div class="meta">
                <h2>#{item['example_code']} {html.escape(_signal_label(item['signal_family']))}</h2>
                <p>入场：{html.escape(_format_ts(item['entry_time']))}</p>
                <p>区间高度：{float(item['range_height_atr']):.2f} ATR，方向：{html.escape(item['side'])}</p>
                <p>R：{float(item['gross_r']):.2f}，净盈亏：{float(item['net_pnl']):.2f} USDT</p>
              </div>
            </article>
            """
        )

    html_text = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>突破案例巡检</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f5f1e8;
      --card: #fffdf8;
      --ink: #1f2328;
      --muted: #695f51;
      --line: #d8ccbb;
      --accent: #7d4f35;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Microsoft YaHei", "PingFang SC", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(125, 79, 53, 0.12), transparent 34%),
        linear-gradient(180deg, #faf6ef 0%, var(--bg) 100%);
    }}
    main {{ max-width: 1600px; margin: 0 auto; padding: 32px 24px 48px; }}
    .summary {{
      background: rgba(255, 253, 248, 0.92);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 20px 24px;
      margin-bottom: 24px;
      box-shadow: 0 10px 30px rgba(69, 51, 33, 0.08);
    }}
    h1, h2 {{ margin: 0 0 12px; }}
    p {{ margin: 6px 0; color: var(--muted); }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(420px, 1fr));
      gap: 18px;
    }}
    .card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 18px;
      overflow: hidden;
      box-shadow: 0 8px 24px rgba(69, 51, 33, 0.08);
    }}
    .card img {{
      display: block;
      width: 100%;
      background: white;
    }}
    .meta {{
      padding: 16px 18px 18px;
    }}
    .meta h2 {{
      font-size: 20px;
      color: var(--accent);
    }}
  </style>
</head>
<body>
  <main>
    <section class="summary">
      <h1>突破案例巡检</h1>
      <p>参数版本：{html.escape(preset)}</p>
      <p>案例总数：{len(examples)} 张，按信号家族均衡、时间分散和结构清晰度抽样生成。</p>
      <p>交易笔数：{preset_summary['trade_count']}，胜率：{preset_summary['win_rate']:.4f}% ，期末净值：{preset_summary['ending_nav']:.4f}</p>
      <p>图中已连出区间触点、区间边界、突破点和确认点，便于人工巡检 follow-through 与失败突破逻辑。</p>
    </section>
    <section class="grid">
      {''.join(cards)}
    </section>
  </main>
</body>
</html>
"""
    html_path.write_text(html_text, encoding="utf-8")


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
    drawdown_pct_values = [
        value * 100.0 for value in equity_curve_df["drawdown_pct"].to_list()
    ]

    fig.add_trace(
        go.Scatter(
            x=x_values,
            y=nav_values,
            mode="lines+markers",
            name="净值",
            line=dict(color="#7d4f35", width=2.5),
            marker=dict(size=5, color="#7d4f35"),
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
        bordercolor="#7d4f35",
        xanchor="left",
        yanchor="top",
    )

    fig.update_yaxes(title_text="净值", row=1, col=1)
    fig.update_yaxes(title_text="回撤 (%)", row=2, col=1)
    fig.update_layout(
        title=f"突破策略净值曲线 ({preset})",
        width=1600,
        height=900,
        template="plotly_white",
        margin=dict(l=60, r=40, t=80, b=40),
        xaxis_rangeslider_visible=False,
        xaxis2_rangeslider_visible=False,
    )

    html_path = plots_dir / "breakout-equity-curve.html"
    png_path = plots_dir / "breakout-equity-curve.png"
    fig.write_html(html_path)
    fig.write_image(png_path, scale=2)
    return html_path, png_path


def _update_summary_files(
    output_dir: Path,
    summary: dict,
    preset: str,
    example_count: int,
    manifest_path: Path,
    gallery_html_path: Path,
    equity_html_path: Path,
    equity_png_path: Path,
) -> None:
    plot_summary = {
        "example_count": example_count,
        "per_example_png_glob": str((output_dir / "plots" / "breakout-example-*.png").resolve()),
        "gallery_html_path": str(gallery_html_path.resolve()),
        "manifest_path": str(manifest_path.resolve()),
        "equity_html_path": str(equity_html_path.resolve()),
        "equity_png_path": str(equity_png_path.resolve()),
    }

    summary[preset]["plot_summary"] = plot_summary
    summary_path = output_dir / "backtest-summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    detail_path = output_dir / f"backtest-summary-{preset}.json"
    if detail_path.exists():
        detail_summary = json.loads(detail_path.read_text(encoding="utf-8"))
        detail_summary["plot_summary"] = plot_summary
        detail_path.write_text(
            json.dumps(detail_summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="绘制突破案例图。")
    parser.add_argument("--output-dir", type=Path, required=True, help="产出目录")
    parser.add_argument(
        "--example-count",
        type=int,
        default=DEFAULT_CASE_COUNT,
        help="要输出的案例图数量，默认 100。",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    summary = _load_summary(args.output_dir / "backtest-summary.json")
    preset = summary["selected_preset"]
    preset_summary = summary[preset]

    signals_df = pl.read_parquet(preset_summary["signals_path"])
    trades_df = pl.read_csv(preset_summary["trades_path"], try_parse_dates=True)
    equity_curve_df = pl.read_csv(preset_summary["equity_curve_path"], try_parse_dates=True)
    examples = _pick_examples(trades_df, target_count=args.example_count)
    if not examples:
        raise SystemExit("没有可绘制的交易样本。")

    plots_dir = args.output_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)
    _cleanup_old_case_images(plots_dir)

    for trade in examples:
        start_idx = max(0, int(trade["range_window_start_idx"]) - LOOKBACK_BARS)
        end_idx = int(trade["exit_idx"]) + LOOKFORWARD_BARS
        plot_df = signals_df.filter(
            (pl.col("idx") >= start_idx) & (pl.col("idx") <= end_idx)
        )
        fig = _build_trade_figure(plot_df=plot_df, trade=trade, preset=preset)
        fig.write_image(plots_dir / trade["example_file"], scale=2)

    manifest_path = plots_dir / "breakout-examples-manifest.csv"
    _write_manifest(examples, manifest_path)
    gallery_html_path = plots_dir / "breakout-examples.html"
    _write_gallery_html(
        examples=examples,
        html_path=gallery_html_path,
        preset=preset,
        preset_summary=preset_summary,
    )
    equity_html_path, equity_png_path = _plot_equity_curve(
        equity_curve_df=equity_curve_df,
        preset=preset,
        summary=preset_summary,
        plots_dir=plots_dir,
    )
    _update_summary_files(
        output_dir=args.output_dir,
        summary=summary,
        preset=preset,
        example_count=len(examples),
        manifest_path=manifest_path,
        gallery_html_path=gallery_html_path,
        equity_html_path=equity_html_path,
        equity_png_path=equity_png_path,
    )

    print(f"example png count: {len(examples)}")
    print(f"example glob: {(plots_dir / 'breakout-example-*.png').resolve()}")
    print(f"gallery html: {gallery_html_path.resolve()}")
    print(f"manifest: {manifest_path.resolve()}")
    print(f"equity html: {equity_html_path.resolve()}")
    print(f"equity png: {equity_png_path.resolve()}")


if __name__ == "__main__":
    main()
