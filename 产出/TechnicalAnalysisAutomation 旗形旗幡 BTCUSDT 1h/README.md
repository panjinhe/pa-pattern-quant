# 文档摘要

- 本目录复刻的是 [TechnicalAnalysisAutomation/flags_pennants.py](https://github.com/neurotrader888/TechnicalAnalysisAutomation/blob/main/flags_pennants.py) 中的 `find_flags_pennants_trendline` 分支，数据换成上游仓库自带的 `BTCUSDT3600.csv`。
- 由于仓库内没有现成的 `BTCUSDT 5m history.parquet`，本次采用 BTC 1 小时样本，时间跨度为 `2018-01-08` 到 `2022-12-31`。
- 识别逻辑仍是三段式：rolling-window 顶底点找旗杆、旗面宽高约束、旗面趋势线突破确认。
- 基线策略继续忠实复刻上游评估方式：确认当根收盘入场，固定持有 `flag_width * hold_multiplier` 根后离场，不在基线中启用硬止损。
- BTC 1 小时样本上的基线结果为正：`576` 笔，胜率 `53.2986%`，总净盈亏 `6080.8722 USDT`，`Profit Factor = 1.1340`。

# 形态归纳

| 原文线索 | 归一化模式名 | 为什么归到这里 |
| --- | --- | --- |
| `last_bottom -> last_top -> breakout above upper trendline` | 多头旗形 / 多头旗幡 | 先有向上旗杆，再有旗面整理，随后向上突破上边界确认。 |
| `last_top -> last_bottom -> breakout below lower trendline` | 空头旗形 / 空头旗幡 | 先有向下旗杆，再有旗面反抽或收敛，随后向下跌破下边界确认。 |
| `support_slope > 0` 或 `resist_slope < 0` | 旗幡 | 两条边界更收敛，更接近 pennant。 |
| 其余旗面趋势线斜率组合 | 旗形 | 仍是延续结构，但边界更接近平行回撤。 |

主模式族仍然是 `旗形/旗幡延续`，只是样本数据从 ETH 5m 切换到了 BTC 1h。

# 量化定义

## 所需字段

- `timestamp, open, high, low, close`

## 衍生特征

- `log(close)`：上游检测在对数价格上运行。
- `rw_top / rw_bottom`：延迟确认的 rolling-window 顶底点。
- `pole_width / pole_height`：旗杆宽度与高度。
- `flag_width / flag_height`：旗面宽度与高度。
- `support_slope / resist_slope`：旗面上下边界趋势线斜率。
- `support_pivot_idx / resist_pivot_idx / flag_extreme_idx`：用于绘图解释的关键结构点。

## 判定逻辑

### 多头

- 先用 `rw_bottom` 找最近底部，再用 `rw_top` 找后续顶部，构成候选旗杆。
- 旗面区间是 `tip_x` 到 `conf_x - 1`。
- 要求 `flag_width <= pole_width * 0.5`。
- 要求 `flag_height <= pole_height * 0.75`。
- 在旗面切片上拟合上下趋势线；当前收盘高于上边界外推值才确认。

### 空头

- 先用 `rw_top` 找最近顶部，再用 `rw_bottom` 找后续底部，构成候选旗杆。
- 同样要求旗面宽度、高度不超过上游阈值。
- 当前收盘低于下边界外推值才确认。

## 确认条件

- `bull_flag / bull_pennant`：确认 K 线收盘上破旗面上边界。
- `bear_flag / bear_pennant`：确认 K 线收盘下破旗面下边界。

## 失效条件

- 实时结构失效：旗面内部先走出新的更高高点或更低低点，或者宽高比例超限。
- 研究用参考失效线：多头取确认时的下边界，空头取确认时的上边界。该线会画在图上，但基线回测不把它作为提前出场规则。

## 待验证假设

- `order=10` 是否恰好适配 BTC 1 小时，而不是因为样本频率变化偶然有效。
- 宽高阈值直接继承上游实现，尚未针对 BTC 波动结构做校准。
- 本次仍只复刻 `trendline` 版，未落地 `pips` 版旗形识别。

# 交易语义

- 入场触发：确认 K 线收盘突破旗面边界，当根收盘入场。
- 止损位置：上游脚本未定义硬止损；这里只保留“结构失效参考线”供图表使用。
- 目标位/测量目标：以 `pole_height_log` 从确认价外推同等高度，作为研究标签 `target_price`。
- 失败条件：价格未能在固定持有窗口内延续，或者很快回到旗面另一侧。
- 适用品种或周期假设：当前样本是 `BTCUSDT 1h`，不能直接外推到 BTC 5m 或 ETH 5m。
- 建议统计指标：模式数、交易笔数、胜率、平均净盈亏、总净盈亏、平均 `R`、总 `R`、最大回撤、profit factor、测量目标命中率。

# 交易策略

- 方向：`bull_*` 做多，`bear_*` 做空。
- 入场规则：`conf_idx` 当根收盘入场。
- 出场规则：固定持有 `hold_bars = round(flag_width * hold_multiplier)` 根后按收盘离场。
- 止损规则：基线不使用硬止损，只保留结构参考失效线。
- 止盈/目标位规则：基线不在 `target_price` 提前止盈，只记录是否命中。
- 过滤条件：只保留满足旗杆/旗面宽高约束并实际突破边界的样本。
- 风险管理假设：固定名义仓位 `10000 USDT`，允许样本重叠，以贴近上游逐样本统计方式。
- 建议回测指标：除绝对盈亏外，同时报告 `R = forward_log_return / pole_height_log`。这里的 `R` 是结构归一化收益，不是基于硬止损的风险倍数。

# 代码与文件清单

- `detect_flags_pennants.py`：复刻 `trendline` 版旗形/旗幡检测，兼容上游 `date` 字符串时间列。
- `strategy_flags_pennants.py`：把模式表转成固定持有期交易，并输出交易表、净值曲线和回测摘要。
- `plot_flags_pennants.py`：生成 100 张案例图、案例巡检页、净值曲线图，并把识别关键点和连线画出来。
- `backtest-summary.json`：总摘要，含数据区间、参数、信号统计、回测表现和图表路径。
- `plots/flags-pennants-example-001.png` 至 `plots/flags-pennants-example-100.png`：案例图。
- `plots/flags-pennants-examples.html`：案例图库。
- `plots/flags-pennants-equity-curve.png` / `.html`：净值与回撤图。
- `run-report.md`：本次实际执行结果与文件清单。

# 运行结果

- 数据文件：`outputs/flag_refs/TechnicalAnalysisAutomation/BTCUSDT3600.csv`
- 样本区间：`2018-01-08 02:00:00` 到 `2022-12-31 23:00:00`
- 样本行数：`43,654`
- 命中的模式数量：`576`
- 四个模式族分布：
  - `bull_flag`: `251`
  - `bear_flag`: `236`
  - `bull_pennant`: `62`
  - `bear_pennant`: `27`
- 图表生成状态：成功生成 `100` 张案例 PNG、1 个案例 HTML、1 张净值曲线 PNG 和 1 个净值曲线 HTML。
- 运行失败：无。

# 回测表现

## 基线总表

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
| 中位持有根数 | 10 |
| 测量目标命中率 | 5.9028% |

手续费/滑点假设：每边 `2 bps`。  
资金或仓位假设：每笔固定名义仓位 `10000 USDT`，允许样本重叠。

## 按模式族拆分

| 模式族 | 交易笔数 | 胜率 | 平均净盈亏 (USDT) | 总净盈亏 (USDT) | 平均 R | 目标命中率 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `bull_flag` | 251 | 53.3865% | 4.7179 | 1184.2006 | 0.0229 | 5.5777% |
| `bear_flag` | 236 | 52.5424% | 10.2605 | 2421.4875 | 0.0368 | 5.9322% |
| `bull_pennant` | 62 | 51.6129% | 40.5236 | 2512.4634 | 0.1108 | 8.0645% |
| `bear_pennant` | 27 | 62.9630% | -1.3807 | -37.2792 | 0.0182 | 3.7037% |

解读：

- BTC 1 小时样本上的 `bull_pennant` 最强，`bear_pennant` 胜率虽高但样本太少，扣成本后仍略亏。
- 相比 ETH 5m，这份 BTC 1h 结果明显更好，说明时间框架和品种切换会显著改变该上游逻辑的表现。
- `ending_nav = 1.6081` 仍然只是允许样本重叠后的归一化累计结果，不代表真实资金曲线。

# 参数调整记录

- 未触发调整，沿用基线参数。
- 原因：基线已有 `576` 笔样本，远高于“样本过少才放宽参数”的阈值。

# 图表清单

- `plots/flags-pennants-example-001.png` 至 `plots/flags-pennants-example-100.png`
  - 展示内容：单个旗形/旗幡样本，包含旗杆、旗面边界、边界锚点、旗面极值、确认点、失效参考线、测量目标和离场点。
  - 实际数量：`100` 张。
- `plots/flags-pennants-examples.html`
  - 展示内容：100 张案例的汇总巡检页。
- `plots/flags-pennants-equity-curve.png`
  - 展示内容：固定持有期复刻策略的累计样本净值与回撤。
- `plots/flags-pennants-equity-curve.html`
  - 展示内容：净值曲线交互版。

# 验证与风险

- `target_hit`、`max_favorable_log_return`、`future_return_r`、`exit_idx` 都依赖未来 K 线，只能用于研究与评估，不能混入实时检测。
- 这次样本不是本仓库统一的 `history.parquet`，而是上游提供的 BTC 1 小时 CSV；和 ETH 5m 结果不能直接横向比较。
- `R` 的定义是 `pole_height_log` 归一化收益，不是“按硬止损计算的风险倍数”，不要误读成传统回测里的 `R-multiple`。
- 由于允许样本重叠，累计净值曲线不代表真实资金曲线；若要实盘化评估，应再追加“持仓互斥 + 资金占用约束”版本。
- 如果后续拿到 BTC 5m 或更长周期的统一 parquet，应优先在同样数据管线下重跑，再决定这个策略是否真的对 BTC 更友好。
