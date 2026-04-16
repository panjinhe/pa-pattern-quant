from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from strategylets.strategy_book_kpi import (  # noqa: E402
    build_manual_review_assessment,
    build_strategy_book_assessment,
    build_universal_strategy_book_assessment,
)


def _symbol_assessment(
    *,
    symbol: str,
    signal_count: int,
    trade_count: int,
    avg_r: float,
    profit_factor: float,
    max_drawdown: float,
) -> dict:
    return build_strategy_book_assessment(
        timeframe="5m",
        sample_rows=2880,
        sample_start="2026-01-01T00:00:00",
        sample_end="2026-01-10T23:55:00",
        symbol=symbol,
        signal_count=signal_count,
        trade_count=trade_count,
        avg_r=avg_r,
        profit_factor=profit_factor,
        max_drawdown=max_drawdown,
        realtime_safe=True,
        uses_future_bars=False,
        supports_visualization=True,
    )


class StrategyBookKpiTests(unittest.TestCase):
    def test_frequency_boundary_values_follow_v21_thresholds(self) -> None:
        passed = _symbol_assessment(
            symbol="BTCUSDT",
            signal_count=8,
            trade_count=120,
            avg_r=0.01,
            profit_factor=1.02,
            max_drawdown=-0.10,
        )
        watch = _symbol_assessment(
            symbol="ETHUSDT",
            signal_count=1,
            trade_count=120,
            avg_r=0.01,
            profit_factor=1.02,
            max_drawdown=-0.10,
        )
        failed = _symbol_assessment(
            symbol="ETHUSDT",
            signal_count=0,
            trade_count=120,
            avg_r=0.01,
            profit_factor=1.02,
            max_drawdown=-0.10,
        )

        self.assertEqual(passed["metrics"][0]["status"], "通过")
        self.assertEqual(watch["metrics"][0]["status"], "观察")
        self.assertEqual(failed["metrics"][0]["status"], "不通过")

    def test_sample_boundary_values_follow_v21_thresholds(self) -> None:
        passed = _symbol_assessment(
            symbol="BTCUSDT",
            signal_count=10,
            trade_count=100,
            avg_r=0.01,
            profit_factor=1.02,
            max_drawdown=-0.10,
        )
        watch = _symbol_assessment(
            symbol="ETHUSDT",
            signal_count=10,
            trade_count=40,
            avg_r=0.01,
            profit_factor=1.02,
            max_drawdown=-0.10,
        )
        failed = _symbol_assessment(
            symbol="ETHUSDT",
            signal_count=10,
            trade_count=39,
            avg_r=0.01,
            profit_factor=1.02,
            max_drawdown=-0.10,
        )

        self.assertEqual(passed["metrics"][2]["status"], "通过")
        self.assertEqual(watch["metrics"][2]["status"], "观察")
        self.assertEqual(failed["metrics"][2]["status"], "不通过")

    def test_universal_pass_when_btc_pass_eth_watch_combined_pass_and_manual_pass(self) -> None:
        btc = _symbol_assessment(
            symbol="BTCUSDT",
            signal_count=16,
            trade_count=150,
            avg_r=0.08,
            profit_factor=1.2,
            max_drawdown=-0.12,
        )
        eth = _symbol_assessment(
            symbol="ETHUSDT",
            signal_count=6,
            trade_count=70,
            avg_r=-0.02,
            profit_factor=0.97,
            max_drawdown=-0.30,
        )
        combined = _symbol_assessment(
            symbol="BTC+ETH 合并",
            signal_count=24,
            trade_count=220,
            avg_r=0.03,
            profit_factor=1.08,
            max_drawdown=-0.18,
        )
        manual = build_manual_review_assessment(
            status="pass",
            review_pool_counts={"BTCUSDT": 28, "ETHUSDT": 24},
        )

        assessment = build_universal_strategy_book_assessment(
            timeframe="5m",
            symbol_assessments={"BTCUSDT": btc, "ETHUSDT": eth},
            combined_assessment=combined,
            manual_review=manual,
        )

        self.assertEqual(assessment["auto_stage"], "可进入人工复核")
        self.assertEqual(assessment["final_bucket"], "可纳入通用策略集")

    def test_specialized_bucket_when_one_required_symbol_fails(self) -> None:
        btc = _symbol_assessment(
            symbol="BTCUSDT",
            signal_count=16,
            trade_count=150,
            avg_r=0.08,
            profit_factor=1.2,
            max_drawdown=-0.12,
        )
        eth = _symbol_assessment(
            symbol="ETHUSDT",
            signal_count=2,
            trade_count=20,
            avg_r=-0.20,
            profit_factor=0.7,
            max_drawdown=-0.50,
        )
        combined = _symbol_assessment(
            symbol="BTC+ETH 合并",
            signal_count=18,
            trade_count=160,
            avg_r=0.02,
            profit_factor=1.05,
            max_drawdown=-0.20,
        )
        manual = build_manual_review_assessment(
            status="pass",
            review_pool_counts={"BTCUSDT": 20, "ETHUSDT": 12},
        )

        assessment = build_universal_strategy_book_assessment(
            timeframe="5m",
            symbol_assessments={"BTCUSDT": btc, "ETHUSDT": eth},
            combined_assessment=combined,
            manual_review=manual,
        )

        self.assertEqual(assessment["final_bucket"], "品种特化策略池")
        self.assertEqual(assessment["applicable_symbols"], ["BTCUSDT"])

    def test_watchlist_when_both_symbols_are_not_bad_but_combined_only_watch(self) -> None:
        btc = _symbol_assessment(
            symbol="BTCUSDT",
            signal_count=6,
            trade_count=75,
            avg_r=-0.02,
            profit_factor=0.97,
            max_drawdown=-0.28,
        )
        eth = _symbol_assessment(
            symbol="ETHUSDT",
            signal_count=7,
            trade_count=80,
            avg_r=-0.01,
            profit_factor=0.98,
            max_drawdown=-0.26,
        )
        combined = _symbol_assessment(
            symbol="BTC+ETH 合并",
            signal_count=6,
            trade_count=90,
            avg_r=-0.01,
            profit_factor=0.98,
            max_drawdown=-0.30,
        )

        assessment = build_universal_strategy_book_assessment(
            timeframe="5m",
            symbol_assessments={"BTCUSDT": btc, "ETHUSDT": eth},
            combined_assessment=combined,
            manual_review=build_manual_review_assessment(status="pending"),
        )

        self.assertEqual(assessment["auto_stage"], "观察名单")
        self.assertEqual(assessment["final_bucket"], "观察名单")

    def test_combined_still_cannot_enter_universal_bucket_when_baseline_fails(self) -> None:
        btc = _symbol_assessment(
            symbol="BTCUSDT",
            signal_count=8,
            trade_count=120,
            avg_r=0.01,
            profit_factor=1.01,
            max_drawdown=-0.18,
        )
        eth = _symbol_assessment(
            symbol="ETHUSDT",
            signal_count=8,
            trade_count=120,
            avg_r=0.00,
            profit_factor=1.00,
            max_drawdown=-0.20,
        )
        combined = _symbol_assessment(
            symbol="BTC+ETH 合并",
            signal_count=12,
            trade_count=160,
            avg_r=-0.01,
            profit_factor=0.94,
            max_drawdown=-0.22,
        )

        assessment = build_universal_strategy_book_assessment(
            timeframe="5m",
            symbol_assessments={"BTCUSDT": btc, "ETHUSDT": eth},
            combined_assessment=combined,
            manual_review=build_manual_review_assessment(status="pending"),
        )

        self.assertEqual(combined["auto_stage"], "不通过")
        self.assertEqual(assessment["final_bucket"], "暂不纳入")

    def test_manual_review_needs_samples_when_one_symbol_pool_is_insufficient(self) -> None:
        btc = _symbol_assessment(
            symbol="BTCUSDT",
            signal_count=16,
            trade_count=150,
            avg_r=0.08,
            profit_factor=1.2,
            max_drawdown=-0.12,
        )
        eth = _symbol_assessment(
            symbol="ETHUSDT",
            signal_count=7,
            trade_count=80,
            avg_r=-0.01,
            profit_factor=0.98,
            max_drawdown=-0.26,
        )
        combined = _symbol_assessment(
            symbol="BTC+ETH 合并",
            signal_count=24,
            trade_count=220,
            avg_r=0.03,
            profit_factor=1.08,
            max_drawdown=-0.18,
        )
        manual = build_manual_review_assessment(
            status="pending",
            review_pool_counts={"BTCUSDT": 15, "ETHUSDT": 8},
        )

        assessment = build_universal_strategy_book_assessment(
            timeframe="5m",
            symbol_assessments={"BTCUSDT": btc, "ETHUSDT": eth},
            combined_assessment=combined,
            manual_review=manual,
        )

        self.assertEqual(assessment["manual_review"]["status"], "待补样复核")
        self.assertEqual(assessment["final_bucket"], "待人工复核")


if __name__ == "__main__":
    unittest.main()
