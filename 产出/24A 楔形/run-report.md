# 执行概览

- 状态：成功
- 文档来源：`阿布课程语音转文字/24A 楔形.txt`
- 数据文件：`data/binance_um_perp/ETHUSDT/5m/ETHUSDT-5m-history.parquet`
- 样本区间：`2022-01-01 00:00:00` 到 `2026-03-06 23:55:00`
- 样本行数：`439,488`
- 选用参数版本：`baseline`
- 是否触发低机会参数调整：否

# 执行脚本

| 脚本 | 用途 | 状态 |
| --- | --- | --- |
| `detect_wedge.py` | 生成三推状态、楔形顶/底 setup、信号与事后标签 | 成功 |
| `strategy_wedge.py` | 楔形信号回测、逐笔交易、净值序列与摘要输出 | 成功 |
| `plot_wedge.py` | 案例图与净值曲线图 | 成功 |

执行命令：

```powershell
.venv\Scripts\python.exe "产出\24A 楔形\strategy_wedge.py" `
  --input "data\binance_um_perp\ETHUSDT\5m\ETHUSDT-5m-history.parquet" `
  --output-dir "产出\24A 楔形"

.venv\Scripts\python.exe "产出\24A 楔形\plot_wedge.py" `
  --output-dir "产出\24A 楔形"
```

# 关键统计摘要

## 信号统计

| 指标 | 数值 |
| --- | --- |
| 楔形顶 setup | `22,693` |
| 楔形底 setup | `19,592` |
| 楔形顶触发 | `710` |
| 楔形底触发 | `634` |
| 楔形顶事后 follow-through | `11` |
| 楔形底事后 follow-through | `10` |

## 回测表现

| 指标 | 数值 |
| --- | --- |
| 交易笔数 | `1,152` |
| 胜率 | `44.6181%` |
| 平均净盈亏 | `-0.7112 USDT` |
| 总净盈亏 | `-819.3201 USDT` |
| 平均 `R` | `0.0396` |
| 总 `R` | `45.5818` |
| 最大回撤 | `-4492.0798 USDT` |
| 最大回撤百分比 | `-44.8332%` |
| Profit Factor | `0.9805` |
| 期末净值 | `0.9181` |
| 样本内最低净值 | `0.5527` |
| 多头交易 | `541` |
| 空头交易 | `611` |
| 止损笔数 | `488` |
| 止盈笔数 | `222` |
| 时间离场笔数 | `442` |

结论：

- 广义楔形定义能够稳定找到大量“三推 + 反向触发”样本，但过宽的定义会引入明显噪声。
- 该样本下毛 `R` 为正，但固定名义仓位和双边 `4 bps` 成本后，净值仍然偏弱，暂不支持直接当成可上线策略。

# 参数调整情况

| 项目 | 结果 |
| --- | --- |
| 是否触发调整 | 否 |
| 触发条件 | 基线交易笔数低于 `20` 笔时才放宽 |
| 基线交易笔数 | `1,152` |
| 调整后交易笔数 | 未执行 |
| 调整参数 | 无 |

# 图表与产物

| 文件 | 说明 |
| --- | --- |
| `README.md` | 量化定义、交易语义、策略规则、风险说明 |
| `backtest-summary.json` | 汇总回测摘要与路径 |
| `backtest-summary-baseline.json` | 基线版详细摘要 |
| `signals_baseline.parquet` | 基线信号与标签数据 |
| `trades_baseline.csv` | 基线逐笔交易 |
| `equity-curve-baseline.csv` | 按平仓时点累计的净值与回撤序列 |
| `plots/wedge-example-001.png` ... `plots/wedge-example-100.png` | 100 张单独案例图 |
| `plots/wedge-examples.html` | 本地巡检索引页，汇总 100 张案例图 |
| `plots/wedge-examples-manifest.csv` | 100 张案例图的编号、方向、时间与结构分 |
| `plots/wedge-equity-curve.png` | 净值曲线静态图 |
| `plots/wedge-equity-curve.html` | 净值曲线交互图 |

案例图输出说明：

- 本次已实际生成 `100` 张案例 PNG，文件位于 `plots/wedge-example-001.png` 到 `plots/wedge-example-100.png`。
- 选图规则为“多空均衡 + 时间分散 + 结构分优先”，覆盖区间从 `2022-01-21 18:10` 到 `2026-02-27 22:40`。
- 每张图的方向、入场时间、盈亏和结构分明细见 `plots/wedge-examples-manifest.csv`。

# 失败原因和下一步建议

- 本次执行没有脚本失败；Plotly/Kaleido 在清理临时目录时出现了非阻塞警告，但图片与 HTML 均已成功生成。
- 当前最主要的问题不是“机会太少”，而是“定义过宽导致样本杂质过高”。
- 下一步建议优先验证三件事：
  1. 只保留更明确的收敛楔形，减少宽通道和弱嵌套三推。
  2. 把支撑阻力过滤从滚动高低点升级到更强的结构位，例如测量目标、前高前低、通道边界。
  3. 把入场从“信号 K 收盘即入场”改成“反向突破后的二次回踩 / 第二次入场”并重新做样本外验证。
