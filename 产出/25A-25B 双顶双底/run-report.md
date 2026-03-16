# 执行概览

- 状态：成功
- 文档来源：`阿布课程语音转文字/25A 双顶双底.txt`、`阿布课程语音转文字/25B 双顶双底.txt`
- 数据文件：`data/binance_um_perp/ETHUSDT/5m/ETHUSDT-5m-history.parquet`
- 样本区间：`2022-01-01 00:00:00` 到 `2026-03-06 23:55:00`
- 样本行数：`439,488`
- 选用参数版本：`baseline`
- 是否触发低机会参数调整：否

# 执行脚本

| 脚本 | 用途 | 状态 |
| --- | --- | --- |
| `detect_double_top_bottom.py` | 生成双顶/双底结构、旗形/反转区分、信号与事后标签 | 成功 |
| `strategy_double_top_bottom.py` | 回测双顶双底策略，输出逐笔交易与摘要 | 成功 |
| `plot_double_top_bottom.py` | 生成案例图、索引页和净值曲线 | 成功 |

执行命令：

```powershell
.venv\Scripts\python.exe "产出\25A-25B 双顶双底\strategy_double_top_bottom.py" `
  --input "data\binance_um_perp\ETHUSDT\5m\ETHUSDT-5m-history.parquet" `
  --output-dir "产出\25A-25B 双顶双底"

.venv\Scripts\python.exe "产出\25A-25B 双顶双底\plot_double_top_bottom.py" `
  --output-dir "产出\25A-25B 双顶双底"
```

# 关键统计摘要

## 信号统计

| 指标 | 数值 |
| --- | --- |
| 双顶 setup | `88,866` |
| 双底 setup | `80,991` |
| 双顶熊旗 setup | `27,772` |
| 双底牛旗 setup | `29,276` |
| 双顶反转 setup | `61,094` |
| 双底反转 setup | `51,715` |
| 双顶触发 | `8,888` |
| 双底触发 | `8,539` |
| 双顶等距运动命中 | `4,699` |
| 双底等距运动命中 | `4,231` |
| 双顶失败突破 | `3,164` |
| 双底失败跌破 | `3,053` |

## 回测表现

| 指标 | 数值 |
| --- | --- |
| 交易笔数 | `7,167` |
| 胜率 | `39.2912%` |
| 平均净盈亏 | `-4.8993 USDT` |
| 总净盈亏 | `-35113.2532 USDT` |
| 平均 `R` | `-0.0132` |
| 总 `R` | `-94.9087` |
| 最大回撤 | `-38462.8011 USDT` |
| 最大回撤百分比 | `-327.7549%` |
| Profit Factor | `0.8445` |
| 期末净值 | `-2.5113` |
| 样本内最低净值 | `-2.6728` |
| 多头交易 | `3,379` |
| 空头交易 | `3,788` |
| 止损笔数 | `5,535` |
| 止盈笔数 | `1,632` |
| 时间离场笔数 | `2,045` |

结论：

- 双顶双底在阿布课程定义下非常宽，能找到大量结构样本，但直接变成策略后噪声很高。
- 当前策略同时覆盖反转型与旗形型双顶双底，且都按同一套目标与止损执行，统计结果偏弱。
- 图表已经补充“关键识别点 + 简单连线”，人工巡检更容易判断识别是否合理。

# 参数调整情况

| 项目 | 结果 |
| --- | --- |
| 是否触发调整 | 否 |
| 触发条件 | 基线交易笔数低于 `20` 笔时才放宽 |
| 基线交易笔数 | `7,167` |
| 调整后交易笔数 | 未执行 |
| 调整参数 | 无 |

# 图表与产物

| 文件 | 说明 |
| --- | --- |
| `README.md` | 量化定义、交易语义、策略规则、风险说明 |
| `backtest-summary.json` | 汇总回测摘要与产物路径 |
| `backtest-summary-baseline.json` | 基线版详细摘要 |
| `signals_baseline.parquet` | 基线信号与结构点数据 |
| `trades_baseline.csv` | 基线逐笔交易 |
| `equity-curve-baseline.csv` | 基线净值与回撤序列 |
| `plots/double-top-bottom-example-001.png` ... `plots/double-top-bottom-example-100.png` | 100 张案例图 |
| `plots/double-top-bottom-examples.html` | 案例图索引页 |
| `plots/double-top-bottom-examples-manifest.csv` | 100 张案例的编号、时间、方向与结构分 |
| `plots/double-top-bottom-equity-curve.png` | 净值曲线静态图 |
| `plots/double-top-bottom-equity-curve.html` | 净值曲线交互图 |

案例图输出说明：

- 本次已实际生成 `100` 张案例 PNG。
- 案例覆盖区间从 `2022-01-30 00:00` 到 `2026-03-02 05:55`。
- 图中已包含关键感知点连线：双顶/双底结构折线、颈线、触发点、入场止损目标标记。

# 失败原因和下一步建议

- 本次没有脚本失败。
- 导图时 `plotly/kaleido` 在 Windows 上会触发 `TmpDirWarning`，这是临时目录清理失败的非阻塞 warning，不影响图片输出；脚本已加静默处理，后续不会刷屏。
- 下一步建议优先验证三件事：
  1. 把反转型与旗形型双顶双底拆成两套策略分别回测。
  2. 收紧结构定义，例如更严格的颈线突破确认、形态高度下限和趋势背景过滤。
  3. 单独研究失败双顶/失败双底后的反向等距运动，而不是和成功形态混在一起。
