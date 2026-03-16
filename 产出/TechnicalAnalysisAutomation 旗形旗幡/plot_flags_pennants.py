from __future__ import annotations

import argparse
import html
import json
import logging
import math
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any

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
FAMILY_ORDER = ["bull_flag", "bear_flag", "bull_pennant", "bear_pennant"]


def _load_summary(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _format_ts(value: object) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M")
    return str(value).replace("T", " ")


def _family_label(family: str) -> str:
    labels = {
        "bull_flag": "多头旗形",
        "bear_flag": "空头旗形",
        "bull_pennant": "多头旗幡",
        "bear_pennant": "空头旗幡",
    }
    return labels.get(family, family)


def _as_int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return int(value)


def _build_selection_score(trade: dict[str, Any]) -> float:
    quality_score = min(float(trade["flag_quality_score"]), 10.0) / 10.0
    width_score = min(float(trade["pole_width"]) / max(float(trade["flag_width"]), 1.0), 10.0) / 10.0
    target_bonus = 0.10 if bool(trade["target_hit"]) else 0.0
    pennant_bonus = 0.05 if str(trade["pattern_kind"]) == "pennant" else 0.0
    return quality_score * 0.6 + width_score * 0.25 + target_bonus + pennant_bonus


def _select_time_dispersed(records: list[dict[str, Any]], target_count: int) -> list[dict[str, Any]]:
    if len(records) <= target_count:
        return records

    sorted_records = sorted(records, key=lambda item: item["entry_time"])
    buckets: list[list[dict[str, Any]]] = [[] for _ in range(target_count)]
    for index, record in enumerate(sorted_records):
        bucket = min(target_count - 1, index * target_count // len(sorted_records))
        buckets[bucket].append(record)

    selected: list[dict[str, Any]] = []
    for bucket_records in buckets:
        best_record = max(
            bucket_records,
            key=lambda item: (
                item["selection_score"],
                float(item["flag_quality_score"]),
                -int(item["conf_idx"]),
            ),
        )
        selected.append(best_record)

    selected_ids = {int(item["conf_idx"]) for item in selected}
    if len(selected) < target_count:
        remainder = [
            item
            for item in sorted(records, key=lambda row: row["selection_score"], reverse=True)
            if int(item["conf_idx"]) not in selected_ids
        ]
        selected.extend(remainder[: target_count - len(selected)])

    return sorted(selected[:target_count], key=lambda item: item["entry_time"])


def _allocate_family_targets(trades_df: pl.DataFrame, total_count: int) -> dict[str, int]:
    family_counts = {
        family: trades_df.filter(pl.col("family") == family).height
        for family in FAMILY_ORDER
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


def _pick_examples(trades_df: pl.DataFrame, target_count: int = DEFAULT_CASE_COUNT) -> list[dict[str, Any]]:
    if trades_df.is_empty():
        return []

    allocations = _allocate_family_targets(trades_df, target_count)
    selected: list[dict[str, Any]] = []
    for family, family_target in allocations.items():
        if family_target <= 0:
            continue
        family_records = trades_df.filter(pl.col("family") == family).to_dicts()
        for record in family_records:
            record["selection_score"] = _build_selection_score(record)
        selected.extend(_select_time_dispersed(family_records, family_target))

    selected = sorted(selected, key=lambda item: item["entry_time"])
    for example_no, record in enumerate(selected, start=1):
        record["example_no"] = example_no
        record["example_code"] = f"{example_no:03d}"
        record["example_file"] = f"flags-pennants-example-{example_no:03d}.png"
    return selected


def _row_at_idx(plot_df: pl.DataFrame, idx: int | None) -> dict[str, Any] | None:
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

    fig.add_trace(
        go.Scatter(
            x=[point[0] for point in points],
            y=[point[1] for point in points],
            mode="lines+markers+text" if len(points) >= 2 else "markers+text",
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


def _add_horizontal_level(
    fig: go.Figure,
    row: int,
    plot_df: pl.DataFrame,
    price: float,
    color: str,
    label: str,
) -> None:
    xref = "x" if row == 1 else f"x{row}"
    yref = "y" if row == 1 else f"y{row}"
    fig.add_shape(
        type="line",
        x0=plot_df["timestamp"].min(),
        x1=plot_df["timestamp"].max(),
        y0=price,
        y1=price,
        line=dict(color=color, width=1.4, dash="dash"),
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
        font=dict(color=color, size=12),
        xanchor="right",
        yanchor="bottom",
        bgcolor="rgba(255,255,255,0.72)",
    )


def _line_price(intercept_log: float, slope: float, offset: int) -> float:
    return math.exp(intercept_log + slope * offset)


def _add_trade_panel(
    fig: go.Figure,
    row: int,
    plot_df: pl.DataFrame,
    trade: dict[str, Any],
) -> None:
    xref = "x" if row == 1 else f"x{row}"
    yref = "y" if row == 1 else f"y{row}"

    family = str(trade["family"])
    side = str(trade["side"])
    main_color = "#1d8348" if side == "long" else "#b03a2e"
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
            name=_family_label(family),
        ),
        row=row,
        col=1,
    )

    base_idx = int(trade["base_idx"])
    tip_idx = int(trade["tip_idx"])
    conf_idx = int(trade["conf_idx"])
    exit_idx = int(trade["exit_idx"])
    support_pivot_idx = _as_int_or_none(trade["support_pivot_idx"])
    resist_pivot_idx = _as_int_or_none(trade["resist_pivot_idx"])
    flag_extreme_idx = _as_int_or_none(trade["flag_extreme_idx"])

    base_row = _row_at_idx(plot_df, base_idx)
    tip_row = _row_at_idx(plot_df, tip_idx)
    conf_row = _row_at_idx(plot_df, conf_idx)
    exit_row = _row_at_idx(plot_df, exit_idx)
    support_pivot_row = _row_at_idx(plot_df, support_pivot_idx)
    resist_pivot_row = _row_at_idx(plot_df, resist_pivot_idx)
    flag_extreme_row = _row_at_idx(plot_df, flag_extreme_idx)

    flag_low = min(
        float(trade["support_tip_price"]),
        float(trade["support_conf_price"]),
        float(trade["flag_extreme_price"] or trade["support_conf_price"]),
    )
    flag_high = max(
        float(trade["resist_tip_price"]),
        float(trade["resist_conf_price"]),
        float(trade["flag_extreme_price"] or trade["resist_conf_price"]),
    )
    if tip_row and conf_row:
        fig.add_shape(
            type="rect",
            x0=tip_row["timestamp"],
            x1=conf_row["timestamp"],
            y0=flag_low,
            y1=flag_high,
            fillcolor=main_color,
            opacity=0.06,
            line=dict(color=main_color, width=1.2),
            xref=xref,
            yref=yref,
        )

    if base_row and tip_row:
        _add_connected_points(
            fig=fig,
            row=row,
            points=[
                (base_row["timestamp"], float(trade["base_price"]), "Base"),
                (tip_row["timestamp"], float(trade["tip_price"]), "Tip"),
            ],
            color=main_color,
            textposition="top center" if side == "long" else "bottom center",
            width=2.4,
            marker_symbol="diamond",
        )

    if tip_row and conf_row:
        support_points = [
            (tip_row["timestamp"], float(trade["support_tip_price"]), ""),
        ]
        if support_pivot_row is not None:
            support_offset = support_pivot_idx - tip_idx
            support_points.append(
                (
                    support_pivot_row["timestamp"],
                    _line_price(
                        float(trade["support_intercept_log"]),
                        float(trade["support_slope"]),
                        support_offset,
                    ),
                    "S1",
                )
            )
        support_points.append((conf_row["timestamp"], float(trade["support_conf_price"]), ""))
        _add_connected_points(
            fig=fig,
            row=row,
            points=support_points,
            color=support_color,
            textposition="bottom center",
            width=2.0,
        )

        resistance_points = [
            (tip_row["timestamp"], float(trade["resist_tip_price"]), ""),
        ]
        if resist_pivot_row is not None:
            resist_offset = resist_pivot_idx - tip_idx
            resistance_points.append(
                (
                    resist_pivot_row["timestamp"],
                    _line_price(
                        float(trade["resist_intercept_log"]),
                        float(trade["resist_slope"]),
                        resist_offset,
                    ),
                    "R1",
                )
            )
        resistance_points.append((conf_row["timestamp"], float(trade["resist_conf_price"]), ""))
        _add_connected_points(
            fig=fig,
            row=row,
            points=resistance_points,
            color=resistance_color,
            textposition="top center",
            width=2.0,
        )

        breakout_floor = float(trade["resist_conf_price"]) if side == "long" else float(trade["support_conf_price"])
        _add_connected_points(
            fig=fig,
            row=row,
            points=[
                (conf_row["timestamp"], breakout_floor, ""),
                (conf_row["timestamp"], float(trade["entry_price"]), "确认"),
            ],
            color=neutral_color,
            textposition="top center" if side == "long" else "bottom center",
            width=1.8,
            dash="dot",
            marker_symbol="circle-open",
        )

    if flag_extreme_row and trade.get("flag_extreme_price") is not None:
        extreme_label = "回撤低点" if side == "long" else "反抽高点"
        fig.add_trace(
            go.Scatter(
                x=[flag_extreme_row["timestamp"]],
                y=[float(trade["flag_extreme_price"])],
                mode="markers+text",
                text=[extreme_label],
                textposition="bottom center" if side == "long" else "top center",
                marker=dict(size=10, color="#7d6608", symbol="circle"),
                showlegend=False,
            ),
            row=row,
            col=1,
        )

    if conf_row:
        fig.add_trace(
            go.Scatter(
                x=[conf_row["timestamp"]],
                y=[float(trade["entry_price"])],
                mode="markers+text",
                text=["入场"],
                textposition="top center" if side == "long" else "bottom center",
                marker=dict(
                    size=12,
                    color=main_color,
                    symbol="triangle-up" if side == "long" else "triangle-down",
                ),
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

    _add_horizontal_level(
        fig=fig,
        row=row,
        plot_df=plot_df,
        price=float(trade["target_price"]),
        color="#2e86c1",
        label="测量目标",
    )
    _add_horizontal_level(
        fig=fig,
        row=row,
        plot_df=plot_df,
        price=float(trade["structure_stop_price"]),
        color="#d68910",
        label="失效参考",
    )

    fig.add_annotation(
        x=plot_df["timestamp"].min(),
        y=float(plot_df["high"].max()),
        xref=xref,
        yref=yref,
        text=(
            f"#{trade['example_code']} {_family_label(family)}<br>"
            f"旗杆/旗面: {int(trade['pole_width'])}/{int(trade['flag_width'])} 根<br>"
            f"质量分: {float(trade['flag_quality_score']):.2f} | 目标命中: {'是' if trade['target_hit'] else '否'}<br>"
            f"结构R: {float(trade['gross_r']):.2f} | 净盈亏: {float(trade['net_pnl']):.2f} USDT<br>"
            f"持有: {int(trade['holding_bars'])} 根 | 复刻方式: 固定持有期"
        ),
        showarrow=False,
        align="left",
        bgcolor="rgba(255,255,255,0.88)",
        bordercolor=main_color,
        xanchor="left",
        yanchor="top",
    )


def _build_trade_figure(plot_df: pl.DataFrame, trade: dict[str, Any], preset: str) -> go.Figure:
    fig = make_subplots(rows=1, cols=1, shared_xaxes=False)
    _add_trade_panel(fig, row=1, plot_df=plot_df, trade=trade)
    fig.update_layout(
        title=f"旗形/旗幡案例 #{trade['example_code']} ({preset}) | 入场 {_format_ts(trade['entry_time'])}",
        width=1600,
        height=760,
        template="plotly_white",
        margin=dict(l=60, r=80, t=80, b=40),
        xaxis_rangeslider_visible=False,
    )
    return fig


def _cleanup_old_case_images(plots_dir: Path) -> None:
    for path in plots_dir.glob("flags-pennants-example-*.png"):
        path.unlink(missing_ok=True)
    (plots_dir / "flags-pennants-equity-curve.png").unlink(missing_ok=True)
    (plots_dir / "flags-pennants-equity-curve.html").unlink(missing_ok=True)
    (plots_dir / "flags-pennants-examples.html").unlink(missing_ok=True)
    (plots_dir / "flags-pennants-examples-manifest.csv").unlink(missing_ok=True)


def _write_manifest(examples: list[dict[str, Any]], manifest_path: Path) -> None:
    manifest_rows = [
        {
            "example_no": item["example_no"],
            "example_file": item["example_file"],
            "family": item["family"],
            "side": item["side"],
            "entry_time": _format_ts(item["entry_time"]),
            "exit_time": _format_ts(item["exit_time"]),
            "flag_width": int(item["flag_width"]),
            "pole_width": int(item["pole_width"]),
            "flag_quality_score": round(float(item["flag_quality_score"]), 6),
            "gross_r": round(float(item["gross_r"]), 6),
            "net_pnl": round(float(item["net_pnl"]), 6),
            "target_hit": bool(item["target_hit"]),
            "selection_score": round(float(item["selection_score"]), 6),
        }
        for item in examples
    ]
    pl.DataFrame(manifest_rows).write_csv(manifest_path)


def _write_gallery_html(
    examples: list[dict[str, Any]],
    html_path: Path,
    preset: str,
    preset_summary: dict[str, Any],
) -> None:
    cards = []
    for item in examples:
        cards.append(
            f"""
            <article class="card">
              <img src="{html.escape(item['example_file'])}" alt="案例 {item['example_code']}" loading="lazy">
              <div class="meta">
                <h2>#{item['example_code']} {html.escape(_family_label(str(item['family'])))}</h2>
                <p>入场：{html.escape(_format_ts(item['entry_time']))}</p>
                <p>旗杆/旗面：{int(item['pole_width'])}/{int(item['flag_width'])} 根，质量分：{float(item['flag_quality_score']):.2f}</p>
                <p>目标命中：{'是' if item['target_hit'] else '否'}，结构R：{float(item['gross_r']):.2f}</p>
                <p>净盈亏：{float(item['net_pnl']):.2f} USDT</p>
              </div>
            </article>
            """
        )

    html_text = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>旗形/旗幡案例巡检</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f5f1e8;
      --card: #fffdf8;
      --ink: #1f2328;
      --muted: #695f51;
      --line: #d8ccbb;
      --accent: #355c7d;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Microsoft YaHei", "PingFang SC", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(53, 92, 125, 0.12), transparent 34%),
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
      <h1>旗形 / 旗幡案例巡检</h1>
      <p>参数版本：{html.escape(preset)}</p>
      <p>案例总数：{len(examples)} 张，按模式族均衡、时间分散与结构清晰度抽样生成。</p>
      <p>交易笔数：{preset_summary['trade_count']}，胜率：{preset_summary['win_rate']:.4f}% ，期末净值：{preset_summary['ending_nav']:.4f}</p>
      <p>图中已连出旗杆、旗面上下边界、边界锚点、回撤极值与确认点，便于人工核验上游识别逻辑。</p>
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
    summary: dict[str, Any],
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
            mode="lines",
            name="净值",
            line=dict(color="#355c7d", width=2.5),
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
        bordercolor="#355c7d",
        xanchor="left",
        yanchor="top",
    )

    fig.update_yaxes(title_text="净值", row=1, col=1)
    fig.update_yaxes(title_text="回撤 (%)", row=2, col=1)
    fig.update_layout(
        title=f"旗形/旗幡固定持有期净值曲线 ({preset})",
        width=1600,
        height=900,
        template="plotly_white",
        margin=dict(l=60, r=40, t=80, b=40),
        xaxis_rangeslider_visible=False,
        xaxis2_rangeslider_visible=False,
    )

    html_path = plots_dir / "flags-pennants-equity-curve.html"
    png_path = plots_dir / "flags-pennants-equity-curve.png"
    fig.write_html(html_path)
    fig.write_image(png_path, scale=2)
    return html_path, png_path


def _update_summary_files(
    output_dir: Path,
    summary: dict[str, Any],
    preset: str,
    example_count: int,
    manifest_path: Path,
    gallery_html_path: Path,
    equity_html_path: Path,
    equity_png_path: Path,
) -> None:
    plot_summary = {
        "example_count": example_count,
        "per_example_png_glob": str((output_dir / "plots" / "flags-pennants-example-*.png").resolve()),
        "gallery_html_path": str(gallery_html_path.resolve()),
        "manifest_path": str(manifest_path.resolve()),
        "equity_html_path": str(equity_html_path.resolve()),
        "equity_png_path": str(equity_png_path.resolve()),
    }

    summary[preset]["plot_summary"] = plot_summary
    summary_path = output_dir / "backtest-summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    preset_summary_path = output_dir / f"backtest-summary-{preset}.json"
    preset_summary = json.loads(preset_summary_path.read_text(encoding="utf-8"))
    preset_summary["plot_summary"] = plot_summary
    preset_summary_path.write_text(json.dumps(preset_summary, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="绘制 TechnicalAnalysisAutomation 旗形/旗幡案例图。")
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
        raise SystemExit("没有可绘制的旗形/旗幡样本。")

    plots_dir = args.output_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)
    _cleanup_old_case_images(plots_dir)

    for trade in examples:
        start_idx = max(0, int(trade["base_idx"]) - max(12, int(trade["pole_width"]) // 2))
        end_idx = min(
            signals_df.height - 1,
            int(trade["exit_idx"]) + max(12, int(trade["hold_bars"]) + 4),
        )
        plot_df = signals_df.filter(
            (pl.col("idx") >= start_idx) & (pl.col("idx") <= end_idx)
        )
        figure = _build_trade_figure(plot_df=plot_df, trade=trade, preset=preset)
        figure.write_image(plots_dir / trade["example_file"], scale=2)

    manifest_path = plots_dir / "flags-pennants-examples-manifest.csv"
    gallery_html_path = plots_dir / "flags-pennants-examples.html"
    _write_manifest(examples, manifest_path)
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

    print(f"gallery_html: {gallery_html_path.resolve()}")
    print(f"equity_png: {equity_png_path.resolve()}")
    print(f"example_count: {len(examples)}")


if __name__ == "__main__":
    main()
