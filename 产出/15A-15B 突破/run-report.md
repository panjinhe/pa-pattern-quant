# 执行概览

- 状态：成功
- 文档来源：`阿布课程语音转文字/15A 突破.txt`、`阿布课程语音转文字/15B 突破.txt`
- 数据文件：`data/binance_um_perp/ETHUSDT/5m/ETHUSDT-5m-history.parquet`
- 样本区间：`2022-01-01 00:00:00` 到 `2026-03-06 23:55:00`
- 样本行数：`439,488`
- 选用参数版本：`baseline`
- 是否触发低机会参数调整：否

# 执行脚本

| 脚本 | 用途 | 状态 |
| --- | --- | --- |
| `detect_breakout.py` | 生成区间边界、边界触点、突破 K 线、follow-through 与失败突破信号 | 成功 |
| `strategy_breakout.py` | 四类突破信号回测、逐笔交易、净值序列与摘要输出 | 成功 |
| `plot_breakout.py` | 案例图、案例索引页和净值曲线 | 成功 |

执行命令：

```powershell
.venv\Scripts\python.exe "产出\15A-15B 突破\strategy_breakout.py" `
  --input "data\binance_um_perp\ETHUSDT\5m\ETHUSDT-5m-history.parquet" `
  --output-dir "产出\15A-15B 突破"

.venv\Scripts\python.exe "产出\15A-15B 突破\plot_breakout.py" `
  --output-dir "产出\15A-15B 突破"
```

# 关键统计摘要

## 信号统计

| 指标 | 数值 |
| --- | --- |
| 区间边界有效窗口 | `11,225` |
| 横向区间上下文 | `266,965` |
| 多头突破 K 线 | `475` |
| 空头突破 K 线 | `501` |
| 多头 follow-through | `221` |
| 空头 follow-through | `224` |
| 多头失败突破做空 | `132` |
| 空头失败突破做多 | `160` |
| 多头 follow-through 测量目标命中 | `93` |
| 空头 follow-through 测量目标命中 | `102` |
| 多头失败突破反手命中 | `72` |
| 空头失败突破反手命中 | `88` |

## 回测表现

| 指标 | 数值 |
| --- | --- |
| 交易笔数 | `721` |
| 胜率 | `40.9154%` |
| 平均净盈亏 | `-5.7806 USDT` |
| 总净盈亏 | `-4167.8078 USDT` |
| 平均 `R` | `-0.0302` |
| 总 `R` | `-21.7750` |
| 最大回撤 | `-4972.3510 USDT` |
| 最大回撤百分比 | `-49.1172%` |
| Profit Factor | `0.7908` |
| 期末净值 | `0.5832` |
| 样本内最低净值 | `0.5151` |
| 多头交易 | `374` |
| 空头交易 | `347` |
| 止损笔数 | `368` |
| 止盈笔数 | `277` |
| 时间离场笔数 | `76` |

分家族表现：

| 信号家族 | 交易数 | 胜率 | 平均净盈亏 | 平均 `R` |
| --- | --- | --- | --- | --- |
| `bear_failed_breakout` | `155` | `49.0323%` | `-2.7860 USDT` | `-0.0291` |
| `bull_failed_breakout` | `129` | `46.5116%` | `-3.7714 USDT` | `-0.0722` |
| `bull_followthrough` | `219` | `36.9863%` | `-6.3727 USDT` | `0.0007` |
| `bear_followthrough` | `218` | `35.7798%` | `-8.5039 USDT` | `-0.0372` |

结论：

- 课程里的两条核心语义都能被量化抓到，但真正更接近样本现实的是“区间失败突破反手”，而不是“直接追 follow-through”。
- follow-through 延续类的确认机制确实能筛掉不少弱突破，但仍不足以覆盖成本和较远止损。
- 当前最差的不是“没有信号”，而是“信号很多，但盈利质量不足”。

# 参数调整情况

| 项目 | 结果 |
| --- | --- |
| 是否触发调整 | 否 |
| 触发条件 | 基线交易笔数低于 `20` 笔时才放宽 |
| 基线交易笔数 | `721` |
| 调整后交易笔数 | 未执行 |
| 调整参数 | 无 |

# 图表与产物

| 文件 | 说明 |
| --- | --- |
| `README.md` | 量化定义、交易语义、策略规则、风险说明 |
| `backtest-summary.json` | 汇总回测摘要与产物路径 |
| `backtest-summary-baseline.json` | 基线版详细摘要 |
| `signals_baseline.parquet` | 基线信号与标签数据 |
| `trades_baseline.csv` | 基线逐笔交易 |
| `equity-curve-baseline.csv` | 按平仓时点累计的净值与回撤序列 |
| `plots/breakout-example-001.png` ... `plots/breakout-example-100.png` | `100` 张单独案例图 |
| `plots/breakout-examples.html` | 本地巡检索引页 |
| `plots/breakout-examples-manifest.csv` | 案例清单 |
| `plots/breakout-equity-curve.png` | 净值曲线静态图 |
| `plots/breakout-equity-curve.html` | 净值曲线交互图 |

案例图输出说明：

- 本次已实际生成 `100` 张案例 PNG。
- 图中已显式连出区间触点、区间边界、突破点与确认点，便于巡检“突破成功”与“失败突破回归”的两条路径。
- 绘图过程没有再出现 `choreographer` 临时目录清理刷屏提示。

# 失败原因和下一步建议

- 本次执行没有脚本失败。
- 当前最值得继续验证的方向有三项：
  1. 把延续类入场从“确认 K 收盘即追”改成“突破后回踩原边界再入场”。
  2. 对失败突破只保留更强的交易区间背景，例如更高重叠、更明显双边触碰。
  3. 按年份或波动 regime 分层，检查 `2022-2026` 不同阶段里哪类信号才真正稳定。

