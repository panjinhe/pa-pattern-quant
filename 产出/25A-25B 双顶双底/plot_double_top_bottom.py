from __future__ import annotations

import argparse
import html
import json
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

DEFAULT_CASE_COUNT = 100
LOOKBACK_BARS = 48
LOOKFORWARD_BARS = 24


def _load_summary(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _format_ts(value: object) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M")
    return str(value).replace("T", " ")


def _pattern_title(side: str) -> str:
    return "双顶做空" if side == "short" else "双底做多"


def _build_selection_score(trade: dict) -> float:
    height_atr = max(float(trade["pattern_height_atr"]), 0.0)
    symmetry_score = 1.0 - min(
        1.0,
        abs(float(trade["second_price"]) - float(trade["first_price"]))
        / max(float(trade["pattern_height"]), 1e-6),
    )
    measured_move_bonus = 0.4 if trade.get("measured_move_hit_label") else 0.0
    return min(height_atr, 10.0) * 0.6 + symmetry_score * 0.4 + measured_move_bonus


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
                float(item["pattern_height_atr"]),
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


def _allocate_side_targets(trades_df: pl.DataFrame, total_count: int) -> dict[str, int]:
    side_counts = {
        side: trades_df.filter(pl.col("side") == side).height
        for side in ("short", "long")
    }
    active_sides = [side for side, count in side_counts.items() if count > 0]
    if not active_sides:
        return {}

    allocation = {side: 0 for side in active_sides}
    remaining = min(total_count, sum(side_counts[side] for side in active_sides))
    base = remaining // len(active_sides)
    for side in active_sides:
        allocation[side] = min(base, side_counts[side])

    allocated = sum(allocation.values())
    while allocated < remaining:
        for side in sorted(active_sides, key=lambda item: side_counts[item], reverse=True):
            if allocated >= remaining:
                break
            if allocation[side] < side_counts[side]:
                allocation[side] += 1
                allocated += 1

    return allocation


def _pick_examples(trades_df: pl.DataFrame, target_count: int = DEFAULT_CASE_COUNT) -> list[dict]:
    if trades_df.is_empty():
        return []

    allocations = _allocate_side_targets(trades_df, target_count)
    selected: list[dict] = []
    for side, side_target in allocations.items():
        if side_target <= 0:
            continue
        side_records = trades_df.filter(pl.col("side") == side).to_dicts()
        for record in side_records:
            record["selection_score"] = _build_selection_score(record)
        selected.extend(_select_time_dispersed(side_records, side_target))

    selected = sorted(selected, key=lambda item: item["entry_time"])
    for example_no, record in enumerate(selected, start=1):
        record["example_no"] = example_no
        record["example_code"] = f"{example_no:03d}"
        record["example_file"] = f"double-top-bottom-example-{example_no:03d}.png"
    return selected


def _row_at_idx(plot_df: pl.DataFrame, idx: int) -> dict | None:
    row = plot_df.filter(pl.col("idx") == idx)
    if row.is_empty():
        return None
    return row.to_dicts()[0]


def _add_connected_points(
    fig: go.Figure,
    row: int,
    points: list[tuple[object, float, str]],
    color: str,
    width: float = 2.4,
    dash: str | None = None,
) -> None:
    if len(points) < 2:
        return

    line_kwargs: dict[str, object] = {"color": color, "width": width}
    if dash:
        line_kwargs["dash"] = dash

    fig.add_trace(
        go.Scatter(
            x=[point[0] for point in points],
            y=[point[1] for point in points],
            mode="lines+markers+text",
            text=[point[2] for point in points],
            textposition="top center",
            marker=dict(size=8, color=color, symbol="circle"),
            line=line_kwargs,
            showlegend=False,
            hovertemplate="%{text}<br>时间=%{x}<br>价格=%{y:.2f}<extra></extra>",
        ),
        row=row,
        col=1,
    )


def _add_pattern_markers(
    fig: go.Figure,
    row: int,
    plot_df: pl.DataFrame,
    trade: dict,
) -> None:
    xref = "x" if row == 1 else f"x{row}"
    yref = "y" if row == 1 else f"y{row}"
    side = trade["side"]
    color = "#b03a2e" if side == "short" else "#1d8348"
    neutral = "#2e4053"

    fig.add_trace(
        go.Candlestick(
            x=plot_df["timestamp"].to_list(),
            open=plot_df["open"].to_list(),
            high=plot_df["high"].to_list(),
            low=plot_df["low"].to_list(),
            close=plot_df["close"].to_list(),
            showlegend=False,
            name=_pattern_title(side),
        ),
        row=row,
        col=1,
    )

    first_row = _row_at_idx(plot_df, int(trade["first_idx"]))
    second_row = _row_at_idx(plot_df, int(trade["second_idx"]))
    neckline_row = _row_at_idx(plot_df, int(trade["neckline_idx"]))
    signal_row = _row_at_idx(plot_df, int(trade["signal_idx"]))
    if first_row and second_row:
        upper = max(float(trade["first_price"]), float(trade["second_price"]))
        lower = min(float(trade["neckline_price"]), upper)
        fig.add_shape(
            type="rect",
            x0=first_row["timestamp"],
            x1=second_row["timestamp"],
            y0=lower,
            y1=upper if side == "short" else float(trade["neckline_price"]),
            fillcolor=color,
            opacity=0.10,
            line=dict(color=color, width=2),
            xref=xref,
            yref=yref,
        )

    structure_points: list[tuple[object, float, str]] = []
    if first_row:
        structure_points.append((first_row["timestamp"], float(trade["first_price"]), ""))
    if neckline_row:
        structure_points.append((neckline_row["timestamp"], float(trade["neckline_price"]), ""))
    if second_row:
        structure_points.append((second_row["timestamp"], float(trade["second_price"]), ""))
    _add_connected_points(
        fig=fig,
        row=row,
        points=structure_points,
        color=color,
        width=2.6,
    )

    if second_row and signal_row:
        _add_connected_points(
            fig=fig,
            row=row,
            points=[
                (
                    second_row["timestamp"],
                    float(trade["second_price"]),
                    "",
                ),
                (
                    signal_row["timestamp"],
                    float(trade["entry_price"]),
                    "",
                ),
            ],
            color=neutral,
            width=1.8,
            dash="dot",
        )

    if first_row:
        fig.add_trace(
            go.Scatter(
                x=[first_row["timestamp"]],
                y=[float(trade["first_price"])],
                mode="markers+text",
                text=["1"],
                textposition="top center" if side == "short" else "bottom center",
                marker=dict(size=10, color=color, symbol="diamond"),
                showlegend=False,
            ),
            row=row,
            col=1,
        )
    if second_row:
        fig.add_trace(
            go.Scatter(
                x=[second_row["timestamp"]],
                y=[float(trade["second_price"])],
                mode="markers+text",
                text=["2"],
                textposition="top center" if side == "short" else "bottom center",
                marker=dict(size=10, color=color, symbol="diamond"),
                showlegend=False,
            ),
            row=row,
            col=1,
        )
    if neckline_row:
        fig.add_trace(
            go.Scatter(
                x=[neckline_row["timestamp"]],
                y=[float(trade["neckline_price"])],
                mode="markers+text",
                text=["颈线"],
                textposition="middle right",
                marker=dict(size=10, color=neutral, symbol="circle"),
                showlegend=False,
            ),
            row=row,
            col=1,
        )
        fig.add_shape(
            type="line",
            x0=plot_df["timestamp"].min(),
            x1=plot_df["timestamp"].max(),
            y0=float(trade["neckline_price"]),
            y1=float(trade["neckline_price"]),
            line=dict(color=neutral, width=1.5, dash="dot"),
            xref=xref,
            yref=yref,
        )

    for price, line_color in [
        (float(trade["stop_price"]), "#cb4335"),
        (float(trade["target_price"]), "#2874a6"),
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

    marker_specs = [
        ("信号", "diamond", trade["signal_time"], trade["entry_price"], color),
        ("入场", "circle", trade["entry_time"], trade["entry_price"], "#111111"),
        ("离场", "x", trade["exit_time"], trade["exit_price"], "#566573"),
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

    fig.add_annotation(
        x=plot_df["timestamp"].min(),
        y=float(plot_df["high"].max()),
        xref=xref,
        yref=yref,
        text=(
            f"#{trade['example_code']} {_pattern_title(side)}<br>"
            f"{trade['pattern_role']} / {trade['variant_label']}<br>"
            f"结构分: {trade['selection_score']:.2f}<br>"
            f"R: {trade['gross_r']:.2f}，净盈亏: {trade['net_pnl']:.2f} USDT<br>"
            f"跨度: {trade['pattern_span']} 根，持仓: {trade['holding_bars']} 根"
        ),
        showarrow=False,
        align="left",
        bgcolor="rgba(255,255,255,0.90)",
        bordercolor=color,
        xanchor="left",
        yanchor="top",
    )


def _build_trade_figure(plot_df: pl.DataFrame, trade: dict, preset: str) -> go.Figure:
    fig = make_subplots(rows=1, cols=1, shared_xaxes=False)
    _add_pattern_markers(fig=fig, row=1, plot_df=plot_df, trade=trade)
    fig.update_layout(
        title=f"双顶双底案例 #{trade['example_code']} ({preset}) | 入场 {_format_ts(trade['entry_time'])}",
        width=1600,
        height=760,
        template="plotly_white",
        margin=dict(l=60, r=80, t=80, b=40),
        xaxis_rangeslider_visible=False,
    )
    return fig


def _cleanup_old_case_images(plots_dir: Path) -> None:
    for path in plots_dir.glob("double-top-bottom-example-*.png"):
        path.unlink(missing_ok=True)


def _write_manifest(examples: list[dict], manifest_path: Path) -> None:
    manifest_rows = [
        {
            "example_no": item["example_no"],
            "example_file": item["example_file"],
            "side": item["side"],
            "pattern_role": item["pattern_role"],
            "variant_label": item["variant_label"],
            "signal_time": _format_ts(item["signal_time"]),
            "entry_time": _format_ts(item["entry_time"]),
            "exit_time": _format_ts(item["exit_time"]),
            "gross_r": round(float(item["gross_r"]), 6),
            "net_pnl": round(float(item["net_pnl"]), 6),
            "holding_bars": int(item["holding_bars"]),
            "pattern_span": int(item["pattern_span"]),
            "pattern_height_atr": round(float(item["pattern_height_atr"]), 6),
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
                <h2>#{item['example_code']} {html.escape(_pattern_title(item['side']))}</h2>
                <p>{html.escape(item['pattern_role'])} / {html.escape(item['variant_label'])}</p>
                <p>入场：{html.escape(_format_ts(item['entry_time']))}</p>
                <p>R：{float(item['gross_r']):.2f}，净盈亏：{float(item['net_pnl']):.2f} USDT</p>
                <p>跨度：{int(item['pattern_span'])} 根，持仓：{int(item['holding_bars'])} 根</p>
              </div>
            </article>
            """
        )

    html_text = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>25A-25B 双顶双底案例巡检</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f6f2ea;
      --card: #fffdf8;
      --ink: #1f2328;
      --muted: #655d50;
      --line: #d9d1c2;
      --accent: #8b3a2b;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Microsoft YaHei", "PingFang SC", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(139, 58, 43, 0.12), transparent 32%),
        linear-gradient(180deg, #f9f4ed 0%, var(--bg) 100%);
    }}
    main {{ max-width: 1600px; margin: 0 auto; padding: 32px 24px 48px; }}
    .summary {{
      background: rgba(255, 253, 248, 0.94);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 20px 24px;
      margin-bottom: 24px;
      box-shadow: 0 10px 28px rgba(88, 63, 39, 0.08);
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
      box-shadow: 0 8px 22px rgba(88, 63, 39, 0.08);
    }}
    .card img {{ display: block; width: 100%; background: white; }}
    .meta {{ padding: 16px 18px 18px; }}
    .meta h2 {{ font-size: 20px; color: var(--accent); }}
  </style>
</head>
<body>
  <main>
    <section class="summary">
      <h1>25A-25B 双顶双底案例巡检</h1>
      <p>参数版本：{html.escape(preset)}</p>
      <p>案例总数：{len(examples)} 张，按多空均衡与时间分散抽样生成。</p>
      <p>交易笔数：{preset_summary['trade_count']}，胜率：{preset_summary['win_rate']:.4f}% ，期末净值：{preset_summary['ending_nav']:.4f}</p>
      <p>当前页只引用本目录下的 PNG 文件，便于本地快速翻阅。</p>
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
        title=f"双顶双底策略净值曲线 ({preset})",
        width=1600,
        height=900,
        template="plotly_white",
        margin=dict(l=60, r=40, t=80, b=40),
        xaxis_rangeslider_visible=False,
        xaxis2_rangeslider_visible=False,
    )

    html_path = plots_dir / "double-top-bottom-equity-curve.html"
    png_path = plots_dir / "double-top-bottom-equity-curve.png"
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
        "per_example_png_glob": str((output_dir / "plots" / "double-top-bottom-example-*.png").resolve()),
        "gallery_html_path": str(gallery_html_path.resolve()),
        "manifest_path": str(manifest_path.resolve()),
        "equity_html_path": str(equity_html_path.resolve()),
        "equity_png_path": str(equity_png_path.resolve()),
    }

    summary[preset]["plot_summary"] = plot_summary
    summary_path = output_dir / "backtest-summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    detail_path = output_dir / f"backtest-summary-{preset}.json"
    if detail_path.exists():
        detail_summary = json.loads(detail_path.read_text(encoding="utf-8"))
        detail_summary["plot_summary"] = plot_summary
        detail_path.write_text(json.dumps(detail_summary, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="绘制双顶双底案例图。")
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
        start_idx = max(0, int(trade["first_idx"]) - LOOKBACK_BARS)
        end_idx = int(trade["exit_idx"]) + LOOKFORWARD_BARS
        plot_df = signals_df.filter((pl.col("idx") >= start_idx) & (pl.col("idx") <= end_idx))
        fig = _build_trade_figure(plot_df=plot_df, trade=trade, preset=preset)
        fig.write_image(plots_dir / trade["example_file"], scale=2)

    manifest_path = plots_dir / "double-top-bottom-examples-manifest.csv"
    _write_manifest(examples, manifest_path)
    gallery_html_path = plots_dir / "double-top-bottom-examples.html"
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
    print(f"example glob: {(plots_dir / 'double-top-bottom-example-*.png').resolve()}")
    print(f"gallery html: {gallery_html_path.resolve()}")
    print(f"manifest: {manifest_path.resolve()}")
    print(f"equity html: {equity_html_path.resolve()}")
    print(f"equity png: {equity_png_path.resolve()}")


if __name__ == "__main__":
    main()
