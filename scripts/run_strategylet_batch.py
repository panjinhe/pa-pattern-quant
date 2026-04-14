from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

REGISTRY_PATH = ROOT / "strategylets" / "registry.json"
DEFAULT_INPUT = ROOT / "data" / "binance_um_perp" / "ETHUSDT" / "5m" / "ETHUSDT-5m-history.parquet"
DEFAULT_OUTPUT_ROOT = ROOT / "产出" / "标准策略"


def load_registry(path: Path = REGISTRY_PATH) -> list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def _display_name(item: dict[str, Any]) -> str:
    return item.get("strategy_name_std") or item.get("strategy_name_zh") or item["spec_id"]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="批量运行标准化 strategylets。")
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
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="输入 OHLCV 文件路径。",
    )
    parser.add_argument(
        "--symbol",
        default="ETHUSDT",
        help="本次运行使用的 symbol 标签；接口目标可与验证数据不同。",
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
        help="案例图数量上限。",
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

    for spec in specs:
        if spec["status"] != "implemented":
            raise SystemExit(
                f"策略 {spec['spec_id']} 当前状态为 {spec['status']}，尚未实现。"
            )
        module_path = spec.get("module_path")
        if not module_path:
            raise SystemExit(f"策略 {spec['spec_id']} 没有 module_path。")

        module = importlib.import_module(module_path)
        run_strategylet = getattr(module, "run_strategylet", None)
        if run_strategylet is None:
            raise SystemExit(f"模块 {module_path} 没有 run_strategylet 入口。")

        result = run_strategylet(
            input_path=args.input,
            output_root=args.output_root,
            symbol=args.symbol,
            timeframe=args.timeframe,
            registry_entry=spec,
            example_count=args.example_count,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
