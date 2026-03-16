# 23A 旗形运行报告

## 执行脚本

1. `uv run python 产出\23A 旗形\detect_flag.py --with-outcomes`
2. `uv run python 产出\23A 旗形\strategy_flag.py`
3. `uv run python 产出\23A 旗形\plot_flag.py`

## 使用的数据文件

- `data/binance_um_perp/ETHUSDT/5m/ETHUSDT-5m-history.parquet`
- 样本区间：`2022-01-01 00:00:00` 到 `2026-03-06 23:55:00`
- 样本总行数：`439,488`

## 执行状态

- `detect_flag.py`：成功
- `strategy_flag.py`：成功
- `plot_flag.py`：成功

## 信号摘要

- `bull_flag_candidate = 69,507`
- `bear_flag_candidate = 64,476`
- `bull_flag_breakout = 1,931`
- `bear_flag_breakout = 1,613`
- `final_bull_flag_setup = 2,210`
- `final_bear_flag_setup = 1,750`
- `bull_breakout_failed = 13`
- `bear_breakout_failed = 9`
- `final_bull_flag_confirmed = 3`
- `final_bear_flag_confirmed = 0`

## 策略摘要

### 顺势旗形突破策略

- `trades = 3,544`
- `win_rate = 43.26%`
- `avg_r = 0.0155`
- `median_r = -0.1267`
- `total_r = 54.8466`
- `avg_bars_held = 11.2`

### 最终旗形失败突破研究策略

- `trades = 3`
- `win_rate = 66.67%`
- `avg_r = 0.1754`
- `median_r = 0.4512`
- `total_r = 0.5262`
- `avg_bars_held = 17.0`

## 已生成文件

- `README.md`
- `detect_flag.py`
- `strategy_flag.py`
- `plot_flag.py`
- `flag_signals.parquet`
- `flag_signal_summary.json`
- `breakout_strategy_trades.parquet`
- `final_flag_reversal_research_trades.parquet`
- `strategy_summary.json`
- `plots/flag-breakouts.png`
- `plots/flag-breakouts.html`
- `plots/final-flag-reversals.png`
- `plots/final-flag-reversals.html`

## 图表示例

- 顺势多头旗形案例：`2022-01-31 14:30:00`
- 顺势空头旗形案例：`2022-01-07 02:30:00`
- 最终多头旗形失败突破反转案例：`2024-03-08 15:25:00`

## 运行中的注意事项

- `plot_flag.py` 生成图后出现了 `TmpDirWarning`，内容是临时目录没有被完全删除。
- 该警告没有影响 `png/html` 图的生成，图表文件已成功写出。
- 本次样本中没有 `final_bear_flag_confirmed`，因此没有生成最终空头旗形失败突破反转的独立案例面板。

## 下一步建议

- 为最终旗形单独引入更明确的支撑阻力和等距目标特征。
- 把策略拆成“实时可交易版本”和“研究标签版本”两套结果。
- 对 `ETHUSDT 5m` 做滚动窗口参数校准，避免用全样本固定阈值。
