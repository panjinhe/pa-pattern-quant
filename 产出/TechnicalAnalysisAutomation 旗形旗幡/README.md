# 文档摘要

- 本目录复刻的是 [TechnicalAnalysisAutomation/flags_pennants.py](https://github.com/neurotrader888/TechnicalAnalysisAutomation/blob/main/flags_pennants.py) 中的 `find_flags_pennants_trendline` 分支，不使用本仓库既有的阿布旗形定义。
- 识别逻辑由三段组成：`rolling-window pivot` 找杆身起点与终点、旗面宽高约束、旗面趋势线被当前 K 线突破后确认。
- 模式输出分成四个家族：`bull_flag`、`bear_flag`、`bull_pennant`、`bear_pennant`；`flag` 与 `pennant` 的区分由旗面两条边界线的斜率符号决定。
- 基线策略忠实复刻上游脚本的评估方式：确认当根按收盘入场，固定持有 `flag_width * hold_multiplier` 根后离场，不在基线里启用硬止损。
- 绘图额外把识别时真正用到的关键点和连线落出来，包括旗杆、旗面上下边界、上下边界锚点、旗面极值点、确认点和测量目标。

# 形态归纳

| 原文线索 | 归一化模式名 | 为什么归到这里 |
| --- | --- | --- |
| `last_bottom -> last_top -> breakout above upper trendline` | 多头旗形 / 多头旗幡 | 先有向上旗杆，再有旗面收敛或回撤，最后向上突破上边界确认。 |
| `last_top -> last_bottom -> breakout below lower trendline` | 空头旗形 / 空头旗幡 | 先有向下旗杆，再有旗面反抽或收敛，最后向下跌破下边界确认。 |
| `support_slope > 0` 或 `resist_slope < 0` | 旗幡 | 两条边界线朝收敛方向倾斜，更接近 pennant。 |
| 其余旗面趋势线斜率组合 | 旗形 | 仍保留旗杆 + 短期整理 + 趋势延续，但边界更接近平行/反向回撤。 |

主模式族是 `旗形/旗幡延续`。次模式只体现在 `flag` 与 `pennant` 的内部标签拆分，不额外引入别的价格行为语义。

# 量化定义

## 所需字段

- `timestamp, open, high, low, close, volume`

## 衍生特征

- `log(close)`：上游检测在对数价格上完成。
- `rw_top / rw_bottom`：延迟确认的 rolling-window 顶底点。
- `pole_width / pole_height`：旗杆宽度与高度。
- `flag_width / flag_height`：旗面宽度与高度。
- `support_slope / resist_slope`：旗面下边界、上边界趋势线斜率。
- `support_pivot_idx / resist_pivot_idx / flag_extreme_idx`：用于画图解释的关键结构点。

## 判定逻辑

### 多头

- 用 `rw_bottom` 得到最近底部，用 `rw_top` 得到后续顶部，构成候选旗杆。
- 旗面区间是 `tip_x` 到 `conf_x - 1`。
- 约束 `flag_width <= pole_width * 0.5`。
- 约束 `flag_height <= pole_height * 0.75`。
- 对旗面切片拟合上下趋势线；当前收盘必须高于上边界外推值才确认。

### 空头

- 用 `rw_top` 得到最近顶部，用 `rw_bottom` 得到后续底部，构成候选旗杆。
- 同样要求旗面宽度、高度不超过上游阈值。
- 对旗面切片拟合上下趋势线；当前收盘必须低于下边界外推值才确认。

## 确认条件

- `bull_flag / bull_pennant`：确认 K 线收盘上破旗面上边界。
- `bear_flag / bear_pennant`：确认 K 线收盘下破旗面下边界。

## 失效条件

- 实时检测阶段的结构失效：旗面内部先走出新的更高高点（多头）或更低低点（空头），或宽高比超限。
- 研究用参考失效线：多头取确认时的下边界，空头取确认时的上边界。这个价格会落盘并画图，但基线回测不把它当成真实止损。

## 待验证假设

- `order=10` 是否适合 ETHUSDT 5m，而不只是原作者使用的数据频率。
- `flag_width <= 0.5 * pole_width` 与 `flag_height <= 0.75 * pole_height` 是否对高波动加密市场过紧/过松。
- 本次只复刻 `trendline` 分支，未包含同文件中的 `pips` 版旗形识别。

# 交易语义

- 入场触发：确认 K 线收盘突破旗面边界，当根收盘入场。
- 止损位置：上游脚本没有给硬止损；本目录只提供“结构失效参考线”供图表和后续扩展使用。
- 目标位/测量目标：以 `pole_height_log` 从确认价外推同等高度，作为研究标签 `target_price`。
- 失败条件：价格未能在固定持有窗口内延续，或者很快回到旗面另一侧。
- 适用品种或周期假设：当前只在 `ETHUSDT perpetual 5m` 上复刻，别的品种和周期不应直接外推。
- 建议统计指标：模式数、交易笔数、胜率、平均净盈亏、总净盈亏、平均 `R`、总 `R`、最大回撤、profit factor、测量目标命中率。

# 交易策略

- 方向：`bull_*` 做多，`bear_*` 做空。
- 入场规则：`conf_idx` 当根收盘入场。
- 出场规则：固定持有 `hold_bars = round(flag_width * hold_multiplier)` 根后按收盘离场。
- 止损规则：基线不使用硬止损，完全复刻上游“固定持有期统计”的思路；`structure_stop_price` 仅作为结构参考。
- 止盈/目标位规则：基线不提前在 `target_price` 止盈，只把是否命中测量目标记成标签。
- 过滤条件：沿用上游原始过滤，只保留满足旗杆/旗面宽高约束且真正突破边界的样本。
- 风险管理假设：固定名义仓位 `10000 USDT`，每笔独立计盈亏，默认允许样本重叠，以贴近上游逐样本统计方式。
- 建议回测指标：除绝对盈亏外，还报告 `R = forward_log_return / pole_height_log`。这里的 `R` 是结构归一化收益，不是基于硬止损的风险倍数。

# 代码与文件清单

- `detect_flags_pennants.py`：复刻 `trendline` 版旗形/旗幡检测，输出模式表、信号表和检测摘要。
- `strategy_flags_pennants.py`：把模式表转成固定持有期交易，输出交易表、净值曲线和回测摘要。
- `plot_flags_pennants.py`：生成 100 张案例图、案例巡检页、净值曲线图，并把识别关键点和连线画出来。
- `backtest-summary.json`：总摘要，含数据区间、参数、信号统计、回测表现和图表路径。
- `plots/flags-pennants-example-001.png` 至 `plots/flags-pennants-example-100.png`：案例图。
- `plots/flags-pennants-examples.html`：案例图库。
- `plots/flags-pennants-equity-curve.png` / `.html`：净值与回撤图。
- `run-report.md`：本次实际执行结果与文件清单。

# 运行结果

- 数据文件：`data/binance_um_perp/ETHUSDT/5m/ETHUSDT-5m-history.parquet`
- 样本区间：`2022-01-01 00:00:00` 到 `2026-03-06 23:55:00`
- 样本行数：`439,488`
- 命中的模式数量：`5,801`
- 四个模式族分布：
  - `bull_flag`: `2,610`
  - `bear_flag`: `2,524`
  - `bull_pennant`: `346`
  - `bear_pennant`: `321`
- 图表生成状态：成功生成 `100` 张案例 PNG、1 个案例 HTML、1 张净值曲线 PNG 和 1 个净值曲线 HTML。
- 运行失败：无。

# 回测表现

## 基线总表

| 指标 | 数值 |
| --- | ---: |
| 交易笔数 | 5,801 |
| 胜率 | 41.7169% |
| 平均净盈亏 | -4.3738 USDT |
| 总净盈亏 | -25,372.1243 USDT |
| 平均 R | -0.0040 |
| 总 R | -23.2123 |
| 最大回撤 | -27,965.3057 USDT |
| Profit Factor | 0.8092 |
| 多头 / 空头 | 2,956 / 2,845 |
| 中位持有根数 | 10 |
| 测量目标命中率 | 3.9993% |

手续费/滑点假设：每边 `2 bps`。  
资金或仓位假设：每笔固定名义仓位 `10000 USDT`，允许样本重叠。

## 按模式族拆分

| 模式族 | 交易笔数 | 胜率 | 平均净盈亏 (USDT) | 总净盈亏 (USDT) | 平均 R | 目标命中率 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `bull_flag` | 2,610 | 42.8352% | -4.1504 | -10,832.4905 | 0.0005 | 3.4866% |
| `bear_flag` | 2,524 | 41.0856% | -4.2887 | -10,824.6377 | -0.0090 | 3.8431% |
| `bull_pennant` | 346 | 39.3064% | -6.5079 | -2,251.7486 | -0.0202 | 4.9133% |
| `bear_pennant` | 321 | 40.1869% | -4.5584 | -1,463.2474 | 0.0163 | 8.4112% |

解读：

- 从结构归一化收益看，`bear_pennant` 最接近正向样本，`bull_pennant` 最弱。
- 但四个模式族在加入双边 `2 bps` 成本后都为负，说明“结构方向有时对，但可交易边际不够”。
- `ending_nav = -1.5372` 不是可实盘解释的真实净值，而是把重叠样本逐笔独立累计后的归一化结果；它主要用于说明该复刻在固定样本统计下整体为负。

# 参数调整记录

- 未触发调整，沿用基线参数。
- 原因：基线已产生 `5,801` 笔样本，远高于“样本过少才放宽参数”的阈值。

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
- 本次只复刻 `find_flags_pennants_trendline`，没有实现上游同文件中的 `find_flags_pennants_pips`；如果后续要完整对照，需要单独再落地一版。
- `R` 的定义是 `pole_height_log` 归一化收益，不是“按硬止损计算的风险倍数”，不要把它误读成传统 CTA 回测里的 `R-multiple`。
- 由于允许样本重叠，累计净值曲线不代表真实资金曲线；如果要做实盘化评估，应追加“持仓互斥 + 资金占用约束”版本。
- 宽高阈值、`order=10` 和测量目标高度都直接继承上游实现，存在参数迁移风险；更稳妥的做法是按时间切片做前向验证，再决定是否微调。
