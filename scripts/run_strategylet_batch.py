from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from strategylets.strategy_book_kpi import (  # noqa: E402
    build_manual_review_assessment,
    build_strategy_book_assessment,
    build_universal_strategy_book_assessment,
    render_strategy_book_assessment,
)
from strategylets.trend_strategy_common import (  # noqa: E402
    empty_trades_df,
    plot_equity_curve,
    summarize_trades,
    write_json,
    write_jsonl,
)


REGISTRY_PATH = ROOT / "strategylets" / "registry.json"
DEFAULT_DATA_ROOT = ROOT / "data" / "binance_um_perp"
DEFAULT_OUTPUT_ROOT = ROOT / "产出" / "标准策略"
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT"]


def load_registry(path: Path = REGISTRY_PATH) -> list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def _display_name(item: dict[str, Any]) -> str:
    return item.get("strategy_name_std") or item.get("strategy_name_zh") or item["spec_id"]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="批量运行标准化 strategylets，并默认输出 BTC + ETH 通用门禁评估。")
    parser.add_argument(
        "--spec",
        action="append",
        dest="spec_ids",
        help="指定要运行的 spec_id 或 strategy_code；可重复传入多个。",
    )
    parser.add_argument(
        "--implemented-only",
        action="store_true",
        help="运行 registry 中全部 implemented 策略。",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="仅列出 registry 中的策略状态，不执行。",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=DEFAULT_DATA_ROOT,
        help="默认数据根目录；不传 --input 时按 <data-root>/<symbol>/<timeframe>/<symbol>-<timeframe>-history.parquet 查找。",
    )
    parser.add_argument(
        "--input",
        action="append",
        dest="inputs",
        type=Path,
        help="按顺序提供与 --symbol 对齐的 OHLCV 文件；若使用该参数，必须同时显式传入对应的 --symbol。",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        dest="symbols",
        help="本次运行使用的 symbol 标签；不传时默认按 BTCUSDT + ETHUSDT 评估。",
    )
    parser.add_argument(
        "--timeframe",
        default="5m",
        help="本次运行使用的 timeframe 标签。",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="标准化产出根目录。",
    )
    parser.add_argument(
        "--example-count",
        type=int,
        default=6,
        help="每个单品种子运行生成的案例图数量上限。",
    )
    return parser.parse_args()


def _pick_specs(registry: list[dict[str, Any]], args: argparse.Namespace) -> list[dict[str, Any]]:
    if args.list:
        return []

    if args.spec_ids:
        wanted = set(args.spec_ids)
        picked = [
            item
            for item in registry
            if item["spec_id"] in wanted or item.get("strategy_code") in wanted
        ]
        matched_keys = {
            item["spec_id"]
            for item in picked
        } | {
            item.get("strategy_code")
            for item in picked
            if item.get("strategy_code")
        }
        missing = sorted(wanted - matched_keys)
        if missing:
            raise SystemExit(f"未在 registry 中找到这些 spec_id: {', '.join(missing)}")
        return picked

    if args.implemented_only:
        return [item for item in registry if item["status"] == "implemented"]

    return [item for item in registry if item["status"] == "implemented"]


def _default_history_path(symbol: str, timeframe: str, data_root: Path) -> Path:
    return data_root / symbol / timeframe / f"{symbol}-{timeframe}-history.parquet"


def _resolve_symbol_inputs(args: argparse.Namespace) -> dict[str, Path]:
    if args.inputs:
        if not args.symbols:
            raise SystemExit("使用 --input 时请同时传入对应的 --symbol，并保持顺序一致。")
        if len(args.inputs) != len(args.symbols):
            raise SystemExit("--input 与 --symbol 的数量必须一致。")
        return {
            symbol: Path(input_path)
            for symbol, input_path in zip(args.symbols, args.inputs, strict=True)
        }

    symbols = list(args.symbols or DEFAULT_SYMBOLS)
    resolved: dict[str, Path] = {}
    missing_paths: list[str] = []
    for symbol in symbols:
        data_path = _default_history_path(symbol, args.timeframe, Path(args.data_root))
        resolved[symbol] = data_path
        if not data_path.exists():
            missing_paths.append(f"{symbol}: {data_path}")
    if missing_paths:
        raise SystemExit(
            "缺少默认数据文件，无法完成通用门禁评估：\n- " + "\n- ".join(missing_paths)
        )
    return resolved


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed.replace(tzinfo=None)


def _infer_bars_per_day(timeframe: str) -> float | None:
    normalized = (timeframe or "").strip().lower()
    if len(normalized) < 2:
        return None
    unit = normalized[-1]
    raw_value = normalized[:-1]
    if not raw_value.isdigit():
        return None
    value = int(raw_value)
    if value <= 0:
        return None
    if unit == "m":
        return (24 * 60) / value
    if unit == "h":
        return 24 / value
    if unit == "d":
        return 1 / value
    return None


def _estimate_overlap_rows(
    timeframe: str,
    overlap_start: str | None,
    overlap_end: str | None,
    fallback_rows: int,
) -> int:
    start_dt = _parse_datetime(overlap_start)
    end_dt = _parse_datetime(overlap_end)
    bars_per_day = _infer_bars_per_day(timeframe)
    if start_dt is None or end_dt is None or bars_per_day is None or end_dt < start_dt:
        return fallback_rows
    sample_days = ((end_dt - start_dt).total_seconds() / 86_400.0) + (1.0 / bars_per_day)
    return max(int(round(sample_days * bars_per_day)), 1)


def _stable_run_id(spec_id: str, symbols: list[str], timeframe: str) -> str:
    started_at = datetime.now().replace(microsecond=0)
    digest = hashlib.sha1(
        "|".join([spec_id, timeframe, *symbols, str(started_at.date())]).encode("utf-8")
    ).hexdigest()[:20]
    return f"{started_at.strftime('%Y%m%dT%H%M%S')}-{digest}"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            rows.append(json.loads(text))
    return rows


def _collect_symbol_payload(symbol: str, output_dir: Path) -> dict[str, Any]:
    quant_run = _read_json(output_dir / "quant_run.json")
    backtest_summary = _read_json(output_dir / "backtest-summary.json")
    strategy_book_assessment = _read_json(output_dir / "strategy-book-assessment.json")
    strategy_spec = _read_json(output_dir / "strategy_spec.json")

    trades_path = output_dir / "trades.csv"
    trades_df = pl.read_csv(trades_path) if trades_path.exists() else empty_trades_df()
    signals_df = pl.read_parquet(output_dir / "signals.parquet")
    candidates_jsonl_path = output_dir / "candidates.jsonl"
    candidates_parquet_path = output_dir / "candidates.parquet"
    candidates_rows = _read_jsonl(candidates_jsonl_path)
    candidates_df = (
        pl.read_parquet(candidates_parquet_path)
        if candidates_parquet_path.exists()
        else pl.DataFrame(schema={"candidate_id": pl.String})
    )

    summary = quant_run.get("summary", {})
    return {
        "symbol": symbol,
        "output_dir": output_dir,
        "strategy_spec": strategy_spec,
        "quant_run": quant_run,
        "backtest_summary": backtest_summary,
        "strategy_book_assessment": strategy_book_assessment,
        "trades_df": trades_df.with_columns(pl.lit(symbol).alias("symbol")),
        "signals_df": signals_df.with_columns(pl.lit(symbol).alias("symbol")),
        "candidates_rows": candidates_rows,
        "candidates_df": candidates_df,
        "sample_rows": int(summary.get("rows", 0) or 0),
        "sample_start": quant_run.get("sample_start"),
        "sample_end": quant_run.get("sample_end"),
        "signal_count": int(summary.get("signal_count", 0) or 0),
        "event_count": int(summary.get("event_count", 0) or 0),
        "candidate_count": int(summary.get("extra", {}).get("candidate_count", len(candidates_rows)) or 0),
    }


def _build_combined_readme(
    *,
    spec: dict[str, Any],
    symbol_inputs: dict[str, Path],
    payloads: dict[str, dict[str, Any]],
    combined_summary: dict[str, Any],
    plot_summary: dict[str, Any],
    assessment: dict[str, Any],
) -> str:
    strategy_name = _display_name(spec)
    lines = [
        f"# {strategy_name}",
        "",
        f"- 策略代码：`{spec.get('strategy_code', spec['spec_id'])}`",
        f"- spec_id：`{spec['spec_id']}`",
        f"- 本次评估对象：`{', '.join(symbol_inputs.keys())}`",
        f"- timeframe：`{next(iter(payloads.values()))['quant_run'].get('timeframe', '5m')}`",
        "",
        "## 通用门禁摘要",
        "",
        f"- 自动分级：`{assessment['auto_stage']}`",
        f"- 最终归类：`{assessment['final_bucket']}`",
        f"- 合并交易笔数：`{combined_summary['trade_count']}`",
        f"- 合并平均 R：`{combined_summary['avg_r']}`",
        f"- 合并 Profit Factor：`{combined_summary['profit_factor']}`",
        f"- 合并最大回撤：`{combined_summary['max_drawdown']}`",
        "",
        render_strategy_book_assessment(assessment),
        "",
        "## 单品种子目录",
        "",
    ]
    for symbol, payload in payloads.items():
        lines.extend(
            [
                f"- `{symbol}`",
                f"  - 数据文件：`{symbol_inputs[symbol].resolve()}`",
                f"  - 输出目录：`{payload['output_dir'].resolve()}`",
            ]
        )
    lines.extend(
        [
            "",
            "## 图表产物",
            "",
            f"- 合并净值图：`{plot_summary.get('equity_curve_png', '未生成')}`",
        ]
    )
    return "\n".join(lines)


def _run_universal_strategylet(
    *,
    module: Any,
    spec: dict[str, Any],
    symbol_inputs: dict[str, Path],
    timeframe: str,
    output_root: Path,
    example_count: int,
) -> dict[str, Any]:
    symbols = list(symbol_inputs.keys())
    group_run_id = _stable_run_id(spec["spec_id"], symbols, timeframe)
    combined_dir = Path(output_root) / spec["spec_id"] / group_run_id
    combined_dir.mkdir(parents=True, exist_ok=True)

    run_strategylet = getattr(module, "run_strategylet", None)
    if run_strategylet is None:
        raise SystemExit(f"模块 {module.__name__} 没有 run_strategylet 入口。")

    for symbol, input_path in symbol_inputs.items():
        symbol_output_dir = combined_dir / "symbols" / symbol
        run_strategylet(
            input_path=Path(input_path),
            output_root=output_root,
            symbol=symbol,
            timeframe=timeframe,
            registry_entry=spec,
            example_count=example_count,
            run_id=f"{group_run_id}-{symbol.lower()}",
            output_dir=symbol_output_dir,
        )

    payloads = {
        symbol: _collect_symbol_payload(symbol, combined_dir / "symbols" / symbol)
        for symbol in symbols
    }

    combined_trades_frames = [payload["trades_df"] for payload in payloads.values() if payload["trades_df"].height]
    combined_trades_df = (
        pl.concat(combined_trades_frames, how="diagonal_relaxed")
        if combined_trades_frames
        else empty_trades_df().with_columns(pl.lit(None).cast(pl.String).alias("symbol"))
    )
    combined_trades_summary_input = (
        combined_trades_df.drop("symbol")
        if "symbol" in combined_trades_df.columns
        else combined_trades_df
    )
    combined_summary, combined_equity_df = summarize_trades(combined_trades_summary_input)

    signal_count = sum(payload["signal_count"] for payload in payloads.values())
    event_count = sum(payload["event_count"] for payload in payloads.values())
    candidate_count = sum(payload["candidate_count"] for payload in payloads.values())
    overlap_start_dt = max(
        (
            parsed
            for parsed in (
                _parse_datetime(payload["sample_start"])
                for payload in payloads.values()
            )
            if parsed is not None
        ),
        default=None,
    )
    overlap_end_dt = min(
        (
            parsed
            for parsed in (
                _parse_datetime(payload["sample_end"])
                for payload in payloads.values()
            )
            if parsed is not None
        ),
        default=None,
    )
    overlap_start = overlap_start_dt.isoformat() if overlap_start_dt is not None else None
    overlap_end = overlap_end_dt.isoformat() if overlap_end_dt is not None else None
    combined_sample_rows = _estimate_overlap_rows(
        timeframe,
        overlap_start,
        overlap_end,
        fallback_rows=min(payload["sample_rows"] for payload in payloads.values()),
    )

    strategy_spec = next(iter(payloads.values()))["strategy_spec"]
    quant_run_ref = next(iter(payloads.values()))["quant_run"]
    combined_assessment = build_strategy_book_assessment(
        timeframe=timeframe,
        sample_rows=combined_sample_rows,
        signal_count=signal_count,
        trade_count=combined_summary["trade_count"],
        avg_r=combined_summary["avg_r"],
        profit_factor=combined_summary["profit_factor"],
        max_drawdown=combined_summary["max_drawdown"],
        realtime_safe=all(
            payload["strategy_spec"].get("realtime_safe", True)
            for payload in payloads.values()
        ),
        uses_future_bars=any(
            payload["strategy_spec"].get("uses_future_bars", False)
            for payload in payloads.values()
        ),
        supports_visualization=all(
            payload["strategy_spec"].get("supports_visualization", True)
            for payload in payloads.values()
        ),
        symbol="BTC+ETH 合并",
        sample_start=overlap_start,
        sample_end=overlap_end,
    )
    manual_review = build_manual_review_assessment(
        status="pending",
        review_pool_counts={
            symbol: payload["candidate_count"]
            for symbol, payload in payloads.items()
        },
    )
    strategy_book_assessment = build_universal_strategy_book_assessment(
        timeframe=timeframe,
        symbol_assessments={
            symbol: payload["strategy_book_assessment"]
            for symbol, payload in payloads.items()
        },
        combined_assessment=combined_assessment,
        manual_review=manual_review,
        required_symbols=DEFAULT_SYMBOLS,
    )

    signals_frames = [payload["signals_df"] for payload in payloads.values()]
    combined_signals_df = pl.concat(signals_frames, how="diagonal_relaxed")

    candidate_rows: list[dict[str, Any]] = []
    candidate_frames: list[pl.DataFrame] = []
    all_events: list[dict[str, Any]] = []
    plot_summary = {
        "symbol_plots": {
            symbol: payload["backtest_summary"].get("plots", {})
            for symbol, payload in payloads.items()
        }
    }
    if combined_equity_df.height:
        plot_summary.update(plot_equity_curve(combined_equity_df, combined_dir))

    for payload in payloads.values():
        candidate_rows.extend(payload["candidates_rows"])
        if payload["candidates_df"].height:
            candidate_frames.append(payload["candidates_df"])
        all_events.extend(payload["quant_run"].get("events", []))

    combined_candidates_df = (
        pl.concat(candidate_frames, how="diagonal_relaxed")
        if candidate_frames
        else pl.DataFrame(schema={"candidate_id": pl.String})
    )

    strategy_spec_path = combined_dir / "strategy_spec.json"
    quant_run_path = combined_dir / "quant_run.json"
    candidates_jsonl_path = combined_dir / "candidates.jsonl"
    candidates_parquet_path = combined_dir / "candidates.parquet"
    signals_path = combined_dir / "signals.parquet"
    backtest_summary_path = combined_dir / "backtest-summary.json"
    strategy_book_assessment_path = combined_dir / "strategy-book-assessment.json"
    trades_path = combined_dir / "trades.csv"
    readme_path = combined_dir / "README.md"

    artifacts = {
        "strategy_spec": str(strategy_spec_path.resolve()),
        "quant_run": str(quant_run_path.resolve()),
        "candidates_jsonl": str(candidates_jsonl_path.resolve()),
        "candidates_parquet": str(candidates_parquet_path.resolve()),
        "signals": str(signals_path.resolve()),
        "backtest_summary": str(backtest_summary_path.resolve()),
        "strategy_book_assessment": str(strategy_book_assessment_path.resolve()),
        "trades": str(trades_path.resolve()),
        "readme": str(readme_path.resolve()),
        "symbol_dirs": {
            symbol: str(payload["output_dir"].resolve())
            for symbol, payload in payloads.items()
        },
    }

    write_json(strategy_spec_path, strategy_spec)
    write_jsonl(candidates_jsonl_path, candidate_rows)
    if combined_candidates_df.height:
        combined_candidates_df.write_parquet(candidates_parquet_path)
    else:
        pl.DataFrame(schema={"candidate_id": pl.String}).write_parquet(candidates_parquet_path)
    combined_signals_df.write_parquet(signals_path)
    combined_trades_df.write_csv(trades_path)
    write_json(strategy_book_assessment_path, strategy_book_assessment)
    write_json(
        backtest_summary_path,
        {
            "run_id": group_run_id,
            "strategy_code": spec.get("strategy_code"),
            "strategy_name_std": _display_name(spec),
            "spec_id": spec["spec_id"],
            "concept_id": quant_run_ref.get("concept_id"),
            "symbols": symbols,
            "timeframe": timeframe,
            "signal_count": signal_count,
            "event_count": event_count,
            "candidate_count": candidate_count,
            "combined_backtest": combined_summary,
            "per_symbol_backtest": {
                symbol: payload["backtest_summary"].get("backtest", {})
                for symbol, payload in payloads.items()
            },
            "plots": plot_summary,
            "strategy_book_assessment": strategy_book_assessment,
            "assumptions": {
                "required_symbols_for_universal_gate": DEFAULT_SYMBOLS,
                "combined_window_rule": "使用 BTC 与 ETH 重叠时间窗重算合并 KPI",
            },
        },
    )
    write_json(
        quant_run_path,
        {
            "run_id": group_run_id,
            "spec_id": spec["spec_id"],
            "concept_id": quant_run_ref.get("concept_id"),
            "symbol": "+".join(symbols),
            "timeframe": timeframe,
            "data_source": json.dumps(
                {
                    symbol: str(path.resolve())
                    for symbol, path in symbol_inputs.items()
                },
                ensure_ascii=False,
            ),
            "started_at": datetime.now().replace(microsecond=0).isoformat(),
            "ended_at": datetime.now().replace(microsecond=0).isoformat(),
            "sample_start": overlap_start,
            "sample_end": overlap_end,
            "params": {
                "symbols": symbols,
                "strategy_meta": quant_run_ref.get("params", {}).get("strategy_meta", {}),
            },
            "summary": {
                "rows": sum(payload["sample_rows"] for payload in payloads.values()),
                "event_count": event_count,
                "signal_count": signal_count,
                "trade_count": combined_summary["trade_count"],
                "win_rate": combined_summary["win_rate"],
                "avg_r": combined_summary["avg_r"],
                "total_r": combined_summary["total_r"],
                "total_net_pnl": combined_summary["total_net_pnl"],
                "max_drawdown": combined_summary["max_drawdown"],
                "profit_factor": combined_summary["profit_factor"],
                "extra": {
                    "candidate_count": candidate_count,
                    "strategy_book_assessment": strategy_book_assessment,
                    "per_symbol": {
                        symbol: payload["quant_run"].get("summary", {})
                        for symbol, payload in payloads.items()
                    },
                },
            },
            "events": all_events,
            "artifacts": artifacts,
        },
    )
    readme_path.write_text(
        _build_combined_readme(
            spec=spec,
            symbol_inputs=symbol_inputs,
            payloads=payloads,
            combined_summary=combined_summary,
            plot_summary=plot_summary,
            assessment=strategy_book_assessment,
        ),
        encoding="utf-8",
    )

    return {
        "strategy_code": spec.get("strategy_code"),
        "strategy_name_std": _display_name(spec),
        "spec_id": spec["spec_id"],
        "run_id": group_run_id,
        "output_dir": str(combined_dir.resolve()),
        "symbols": symbols,
        "signal_count": signal_count,
        "event_count": event_count,
        "candidate_count": candidate_count,
        "trade_count": combined_summary["trade_count"],
        "strategy_book_auto_stage": strategy_book_assessment["auto_stage"],
        "strategy_book_final_status": strategy_book_assessment["final_status"],
    }


def main() -> None:
    args = _parse_args()
    registry = load_registry()

    if args.list:
        for item in registry:
            print(
                f"{item['status']:<12} "
                f"{item.get('strategy_code', '-'):>14}  "
                f"{item['spec_id']}  "
                f"{_display_name(item)}"
            )
        return

    specs = _pick_specs(registry, args)
    if not specs:
        raise SystemExit("没有可运行的策略。")

    symbol_inputs = _resolve_symbol_inputs(args)
    for spec in specs:
        if spec["status"] != "implemented":
            raise SystemExit(
                f"策略 {spec['spec_id']} 当前状态为 {spec['status']}，尚未实现。"
            )
        module_path = spec.get("module_path")
        if not module_path:
            raise SystemExit(f"策略 {spec['spec_id']} 没有 module_path。")

        module = importlib.import_module(module_path)
        result = _run_universal_strategylet(
            module=module,
            spec=spec,
            symbol_inputs=symbol_inputs,
            timeframe=args.timeframe,
            output_root=Path(args.output_root),
            example_count=args.example_count,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
