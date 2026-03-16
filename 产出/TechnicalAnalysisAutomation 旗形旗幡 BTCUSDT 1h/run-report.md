# 执行摘要

- 任务：在 BTC 数据上复刻 [TechnicalAnalysisAutomation/flags_pennants.py](https://github.com/neurotrader888/TechnicalAnalysisAutomation/blob/main/flags_pennants.py) 的 `find_flags_pennants_trendline` 旗形/旗幡逻辑，并按本仓库标准产出检测、策略、图表和报告。
- 上游参考提交：`da99c20bf3d977b639451258cd6cfca9baa1dcc3`
- 本次实现目录：`产出/TechnicalAnalysisAutomation 旗形旗幡 BTCUSDT 1h/`
- 运行状态：检测、策略和绘图全部执行成功，无阻塞错误。

# 输入与环境

- 数据文件：`outputs/flag_refs/TechnicalAnalysisAutomation/BTCUSDT3600.csv`
- 数据频率：`1h`
- 样本区间：`2018-01-08T02:00:00` 到 `2022-12-31T23:00:00`
- 样本行数：`43654`
- Python 环境：`E:\pa-pattern-quant\.venv\Scripts\python.exe`
- 成本假设：每边 `2 bps`
- 仓位假设：每笔固定名义仓位 `10000 USDT`

# 执行命令

```powershell
& 'E:\pa-pattern-quant\.venv\Scripts\python.exe' 'E:\pa-pattern-quant\产出\TechnicalAnalysisAutomation 旗形旗幡 BTCUSDT 1h\detect_flags_pennants.py' --input 'E:\pa-pattern-quant\outputs\flag_refs\TechnicalAnalysisAutomation\BTCUSDT3600.csv' --output-dir 'E:\pa-pattern-quant\产出\TechnicalAnalysisAutomation 旗形旗幡 BTCUSDT 1h' --preset baseline

& 'E:\pa-pattern-quant\.venv\Scripts\python.exe' 'E:\pa-pattern-quant\产出\TechnicalAnalysisAutomation 旗形旗幡 BTCUSDT 1h\strategy_flags_pennants.py' --input 'E:\pa-pattern-quant\outputs\flag_refs\TechnicalAnalysisAutomation\BTCUSDT3600.csv' --output-dir 'E:\pa-pattern-quant\产出\TechnicalAnalysisAutomation 旗形旗幡 BTCUSDT 1h'

& 'E:\pa-pattern-quant\.venv\Scripts\python.exe' 'E:\pa-pattern-quant\产出\TechnicalAnalysisAutomation 旗形旗幡 BTCUSDT 1h\plot_flags_pennants.py' --output-dir 'E:\pa-pattern-quant\产出\TechnicalAnalysisAutomation 旗形旗幡 BTCUSDT 1h'
```

# 信号统计

- 基线参数：
  - `order = 10`
  - `hold_multiplier = 1.0`
  - `max_flag_width_ratio = 0.5`
  - `max_flag_height_ratio = 0.75`
- 命中总数：`576`
- `bull_flag`: `251`
- `bear_flag`: `236`
- `bull_pennant`: `62`
- `bear_pennant`: `27`
- `target_hit`: `34`
- 参数调整：未触发。原因是基线样本量已经充分。

# 回测表现

| 指标 | 数值 |
| --- | ---: |
| 交易笔数 | 576 |
| 胜率 | 53.2986% |
| 平均净盈亏 | 10.5571 USDT |
| 总净盈亏 | 6080.8722 USDT |
| 平均 R | 0.0378 |
| 总 R | 21.7927 |
| 最大回撤 | -3944.5922 USDT |
| Profit Factor | 1.1340 |
| 多头 / 空头 | 313 / 263 |
| 平均 / 中位持有根数 | 10.84 / 10 |
| 测量目标命中率 | 5.9028% |

补充说明：

- 基线策略仍是“固定持有期复刻”，不是带硬止损的实盘策略。
- 允许样本重叠，因此 `ending_nav = 1.6081` 只能理解为样本累计后的归一化结果，不是资金曲线。
- 这次结果为正，但不能直接证明“BTC 一定适合这个策略”，因为这里用的是上游的 BTC 1 小时样本，不是和 ETH 同口径的 5 分钟历史数据。

# 图表产物

- 案例 PNG：`plots/flags-pennants-example-001.png` 至 `plots/flags-pennants-example-100.png`
- 案例汇总页：`plots/flags-pennants-examples.html`
- 案例 manifest：`plots/flags-pennants-examples-manifest.csv`
- 净值曲线 PNG：`plots/flags-pennants-equity-curve.png`
- 净值曲线 HTML：`plots/flags-pennants-equity-curve.html`

图表说明：

- 每张案例图都包含旗杆、旗面上下边界、边界锚点 `S1/R1`、旗面极值点、确认点、离场点、失效参考线和测量目标线。
- 案例抽样按模式族均衡、时间分散和结构清晰度完成，实际输出 `100` 张。

# 文件清单

- `detect_flags_pennants.py`
- `strategy_flags_pennants.py`
- `plot_flags_pennants.py`
- `patterns_baseline.csv`
- `signals_baseline.parquet`
- `trades_baseline.csv`
- `equity-curve-baseline.csv`
- `detection-summary-baseline.json`
- `backtest-summary-baseline.json`
- `backtest-summary.json`
- `README.md`
- `run-report.md`

# 失败与后续建议

- 本次无运行失败。
- 如果下一步要跟 ETH 5m 做严格可比测试，建议先准备同口径的 `BTCUSDT 5m history.parquet`，再在相同数据管线下重跑一版。
