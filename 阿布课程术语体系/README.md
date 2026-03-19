# 阿布课程字幕术语、概念与形态分层报告

## 1. 研究范围

- 主体语料：`英文文档/` 下 `193` 份阿布课程英文字幕文本，覆盖 `Video 01` 到 `Video 52` 的全部主题。
- 交叉校验：`书/Reading Price Charts Bar by Bar.pdf` 末尾 index，重点参考 PDF 第 `423` 到 `428` 页。
- 提取口径：只保留后续“可量化、可建模、可接口化”的术语、概念、形态、订单与风控语言；人物、市场品种示例、书页引用等不纳入母表。
- 标准化原则：先保留原始英文术语，再把同义词、别名、场景名归并到统一标准项；后续量化应面向标准项，不直接面向原始字幕词面。

## 2. 核心结论

- 阿布课程的原始词汇可以稳定归并为 `5` 层：`市场上下文`、`结构原语`、`计数与入场`、`复合形态/失败结构`、`执行/风控/管理`。
- 真正适合做统一接口的，不是字幕里的全部原句，而是“标准项 + alias + 量化抓手”的三元组。
- 很多课程标题其实是“应用场景”而不是新形态，例如 `Trading Tight Bull Channels`、`Trading Breakouts`、`Trading in Trading Ranges`，它们应映射回已有模式族，而不是再造新接口。
- 后续量化应遵守依赖顺序：`上下文状态 -> 结构原语 -> setup/trigger -> 复合形态 -> 交易管理`。
- 最适合优先统一的枢纽家族是：`趋势/通道/交易区间`、`突破/失败突破`、`双顶双底`、`楔形/三推`、`MTR`、`高潮反转`、`最终旗形`、`Measured Move`。

## 3. 五层分类框架

| 层级 | 含义 | 代表性内容 | 后续接口角色 |
| --- | --- | --- | --- |
| `L0` 市场上下文 | 描述市场当前处于什么状态 | `trend`、`trading range`、`always in`、`market cycle` | `ContextState` |
| `L1` 结构原语 | 组成形态的最小几何和 K 线单元 | `bar`、`doji`、`inside bar`、`trendline`、`gap` | `StructurePrimitive` |
| `L2` 计数与入场 | 用于把原语组织成 setup 的计数逻辑 | `H1/H2`、`L1/L2`、`ABC`、`second entry` | `SetupEvent` |
| `L3` 复合形态/失败结构 | 可以直接作为模式检测目标的家族 | `wedge`、`triangle`、`MTR`、`final flag`、`climax` | `PatternEvent` |
| `L4` 执行/风控/管理 | 把模式转换成交易动作的语言 | `stop`、`actual risk`、`scalp`、`scale in`、`trader's equation` | `TradePlan` |

## 4. 课程主题覆盖

### 4.1 `01` 到 `13`

| 视频 | 主题 | 主层级 | 主家族 |
| --- | --- | --- | --- |
| `01` | Terminology 基本术语 | `L0-L1` | 基础术语、K 线、趋势/区间、信号 |
| `02` | Chart basics and price action | `L0-L1` | 价格行为、上下文、bar 读取 |
| `03` | Forex Basics | `L4` | tick/pip/point、点差、交易场景 |
| `04` | My Setup | `L4` | 交易流程与执行偏好 |
| `05` | Program Trading | `L4` | 交易计划、流程化执行 |
| `06` | Personality Traits of Successful Traders | `L4` | 纪律、耐心、执行习惯 |
| `07` | Starting Out | `L4` | 新手流程、入场纪律 |
| `08` | Candles, Setups, and Signal Bars | `L1-L2` | K 线、signal/setup/entry |
| `09` | Pullbacks and Bar Counting | `L2` | 回调、数腿、H1/H2/L1/L2 |
| `10` | Buying and selling pressure | `L0` | 买压、卖压、动量失衡 |
| `11` | Gaps | `L1` | gap、gap open、gap EMA |
| `12` | Market Cycle | `L0` | breakout-channel-range 循环 |
| `13` | Always In | `L0` | always in long/short |

### 4.2 `14` 到 `26`

| 视频 | 主题 | 主层级 | 主家族 |
| --- | --- | --- | --- |
| `14` | Trends | `L0` | 趋势、强弱、趋势类型 |
| `15` | Breakouts | `L0-L3` | 突破、突破跟进、失败突破 |
| `16` | Channels | `L0-L3` | 通道、趋势中的双边交易 |
| `17` | Tight Channels & Micro Channels | `L0-L3` | 窄通道、微通道 |
| `18` | Trading Ranges | `L0-L3` | 交易区间、Barb Wire、突破模式 |
| `19` | Support and Resistance | `L0` | 支撑阻力、磁铁位 |
| `20` | Measured moves | `L3` | `AB=CD`、`Leg1=Leg2`、目标位 |
| `21` | Reversals | `L3` | 普通反转、minor reversal |
| `22` | Major Trend Reversals | `L3` | `MTR`、HL/LH MTR |
| `23` | Final Flags | `L3` | 最终旗形、失败最终旗形 |
| `24` | Wedges | `L3` | 楔形、三推、失败楔形 |
| `25` | Double Tops and Bottoms | `L2-L3` | 双顶双底、微双顶双底 |
| `26` | Triangles | `L3` | 三角形、扩张三角形 |

### 4.3 `27` 到 `39`

| 视频 | 主题 | 主层级 | 主家族 |
| --- | --- | --- | --- |
| `27` | Head and Shoulders | `L3` | 头肩顶/底 |
| `28` | Rounded Tops and Bottoms | `L3` | 圆弧顶/底、V 顶/V 底 |
| `29` | Climaxes | `L3` | 买入高潮、卖出高潮、parabolic |
| `30` | Trader’s Equation and Probability | `L4` | 胜率、赔率、风险收益 |
| `31` | Swing Trading and Scalping | `L4` | 波段、短线、持仓风格 |
| `32` | Orders | `L4` | stop/limit/market/bracket/OCO |
| `33` | Protective Stops | `L4` | 保护止损、滑点、黑天鹅 |
| `34` | Actual Risk | `L4` | 实际风险、最小风险与真实风险 |
| `35` | Scaling in | `L4` | 分批入场、均价管理 |
| `36` | Trade Management and Taking Profits | `L4` | 止盈、分批止盈、持仓管理 |
| `37` | How to trade | `L4` | 把 setup 变成交易动作 |
| `38` | Trading MTR Tops | `L3-L4` | 顶部 MTR 场景交易 |
| `39` | Trading MTR Bottoms | `L3-L4` | 底部 MTR 场景交易 |

### 4.4 `40` 到 `52`

| 视频 | 主题 | 主层级 | 主家族 |
| --- | --- | --- | --- |
| `40` | Entering Late in Trends | `L2-L4` | 晚入场、回调补票、趋势续航 |
| `41` | Trading Breakouts | `L3-L4` | 突破交易、突破跟进 |
| `42` | Trading Climactic Reversals | `L3-L4` | 高潮反转、Spike and Trading Range |
| `43` | Trading Tight Bull Channels | `L3-L4` | 紧密多头通道交易 |
| `44` | Trading Tight Bear Channels | `L3-L4` | 紧密空头通道交易 |
| `45` | Trading Broad Bull Channels | `L3-L4` | 宽阔多头通道交易 |
| `46` | Trading Broad Bear Channels | `L3-L4` | 宽阔空头通道交易 |
| `47` | Trading in Trading Ranges | `L3-L4` | 区间内高卖低买与失败突破 |
| `48` | Trading the Open / Middle / End of the Day | `L4` | 开盘、午盘、尾盘时段结构 |
| `49` | Swing Trading Examples | `L4` | 波段案例归纳 |
| `50` | Scalping | `L4` | 短线案例归纳 |
| `51` | Losing Because of Mistakes | `L4` | 错误分类、执行偏差 |
| `52` | Losing When Good Trade Goes Bad | `L4` | 好 setup 变坏交易的管理 |

## 5. 标准化术语母表

### 5.1 市场状态与上下文

| 标准项 | 原文别名/近义词 | 层级 | 量化抓手 |
| --- | --- | --- | --- |
| `price_action` | `price action` | `L0` | 所有后续状态与事件的总语境，不单独做形态 |
| `buying_selling_pressure` | `buying pressure`、`selling pressure` | `L0` | 连续实体、收盘位置、跟随强度、重叠率 |
| `context_and_bias` | `context`、`with trend`、`countertrend`、`fade` | `L0` | 把 setup 放回左侧结构与主方向 |
| `trend_family` | `trend`、`bull trend`、`bear trend`、`trend resumption`、`reversal day`、`trend resumption day` | `L0` | 更高高/更高低、均线斜率、连续单边 K 线 |
| `trading_range_family` | `trading range`、`tight trading range`、`Barb Wire`、`breakout mode`、`big up big down` | `L0` | 重叠率、上下边界、往返次数、区间高度/ATR |
| `channel_family` | `channel`、`tight bull/bear channel`、`broad bull/bear channel`、`micro channel`、`stairs`、`shrinking stairs`、`spike and channel` | `L0` | 边界斜率、宽度/ATR、回调深度、连续触边次数 |
| `breakout_family` | `breakout`、`breakout pullback`、`breakout test` | `L0-L3` | 突破幅度、突破 K 收盘、跟随 K、回踩 |
| `always_in_family` | `always in`、`always in long`、`always in short` | `L0` | 当前更容易做多还是做空的状态标签 |
| `market_cycle` | `market cycle`、`breakout phase`、`channel phase`、`trading range phase` | `L0` | 状态机：突破 -> 通道 -> 区间 -> 再突破 |
| `support_resistance_magnet` | `support`、`resistance`、`price magnet`、`breakout point`、`emotional numbers`、`targets`、`yesterday high/low` | `L0` | 水平边界、目标位、整数位、前高前低 |

### 5.2 K 线、图表与几何原语

| 标准项 | 原文别名/近义词 | 层级 | 量化抓手 |
| --- | --- | --- | --- |
| `bar_timeframe_family` | `bar`、`candle`、`bar chart`、`candle chart`、`time frame`、`tick`、`pip`、`point` | `L1` | OHLC、时间尺度、最小价格单位 |
| `bar_classification_family` | `bull bar`、`bear bar`、`trend bar`、`doji`、`trading range bar`、`nontrend bar` | `L1` | 实体占比、收盘位置、尾巴长度 |
| `tail_family` | `tail`、`wick`、`shadow`、`shaved bar`、`shaved body`、`exhaustion bar` | `L1` | 上下影线长度、极端实体、衰竭状态 |
| `signal_setup_entry_family` | `setup`、`setup bar`、`signal bar`、`entry bar`、`reversal bar` | `L1-L2` | setup K 与入场 K 的关系、确认方式 |
| `inside_outside_family` | `inside bar`、`outside bar`、`ii`、`iii`、`ioi` | `L1-L2` | 包含关系、压缩结构、内外包突破 |
| `twin_signal_family` | `double bottom twin`、`double top twin`、`opposite twins` | `L1-L2` | 相邻两根 K 的近似相同高低点 |
| `trendline_family` | `swing point`、`trendline`、`micro trendline`、`trend channel line`、`micro trend channel line`、`horizontal line`、`dueling lines` | `L1` | 摆点连线、通道边界、平行线、交叉线 |
| `overshoot_family` | `overshoot`、`undershoot`、`trend channel line overshoot` | `L1-L3` | 线外穿越深度、回归速度、失败测试 |
| `gap_ema_family` | `gap`、`gap opening`、`EMA`、`moving average gap bar`、`GAP EMA pullback` | `L1-L2` | gap 是否回补、价均线偏离、均线缺口回调 |

### 5.3 回调、计数与入场

| 标准项 | 原文别名/近义词 | 层级 | 量化抓手 |
| --- | --- | --- | --- |
| `leg_pullback_family` | `leg`、`swing`、`pullback`、`correction` | `L2` | 单腿长度、回调深度、腿间间隔 |
| `two_legged_family` | `two-legged pullback`、`ABC correction`、`ABC pullback`、`TBTL` | `L2` | 两腿结构、A/B/C 摆点、时间与 bar 数 |
| `bar_counting_family` | `bar counting`、`counting the legs of a trend` | `L2` | 连续 K 线数、趋势推进段计数 |
| `high_low_counting_family` | `High 1/2/3/4`、`Low 1/2/3/4`、`higher high`、`lower low` | `L2` | 次数计数、二次尝试与失败/成功 |
| `second_entry_family` | `second entry`、`High/Low 2`、`M2B`、`M2S` | `L2` | 第二次尝试的触发与确认 |
| `first_pullback_family` | `first pullback`、`first pullback sequence`、`2HM`、`11:30 stop run pullback` | `L2` | 首次回调、均线首次回踩、长时间离均线 |
| `double_flag_pullback_family` | `double top bear flag`、`double bottom bull flag`、`double top pullback`、`double bottom pullback` | `L2-L3` | 双测结构是否为续势回调而非反转 |
| `late_entry_family` | `late entries`、`missed entries`、`entering late in trends` | `L2-L4` | 晚入场位置、风险增量、补票回调 |

### 5.4 复合形态与失败结构

| 标准项 | 原文别名/近义词 | 层级 | 量化抓手 |
| --- | --- | --- | --- |
| `measured_move_family` | `measured move`、`AB = CD`、`Leg 1 = Leg 2`、`thin areas and flags` | `L3` | 第一腿高度、区间高度、薄区长度映射 |
| `reversal_family` | `reversal`、`minor reversal`、`major trend reversal`、`MTR`、`higher low MTR`、`lower high MTR` | `L3` | 趋势后横盘、失败续势、关键 swing 失守 |
| `trendline_break_family` | `trendline break`、`trend channel line failed breakout` | `L3` | 趋势线突破、通道失败突破、回测 |
| `failed_pattern_family` | `failed breakout`、`failed reversal`、`failed High/Low 2`、`failed Higher High/Lower Low breakout`、`one-tick failed breakout`、`five-tick failed breakout`、`failed failure` | `L3` | 突破后回归、未达 scalp 目标、反向确认 |
| `double_top_bottom_family` | `double top`、`double bottom`、`micro double top`、`micro double bottom` | `L3` | 两次测试、颈线、第二次测试后的确认 |
| `wedge_family` | `wedge`、`three pushes`、`wedge reversal`、`failed wedge`、`nested wedge` | `L3` | 三推、动能衰减、楔形边界、第三推失败 |
| `triangle_family` | `triangle`、`expanding triangle` | `L3` | 收敛/扩张边界、反转次数、末端突破 |
| `head_shoulders_family` | `head and shoulders`、`failed head and shoulders` | `L3` | 左肩/头/右肩、颈线、失败颈线突破 |
| `rounded_family` | `rounded top`、`rounded bottom`、`V top`、`V bottom` | `L3` | 曲率、转向速度、弧形/急转结构 |
| `climax_family` | `climax`、`buy climax`、`sell climax`、`spike`、`spike and trading range reversal`、`parabola`、`climactic reversal` | `L3` | 连续大实体、斜率突然增大、高潮后区间化 |
| `final_flag_family` | `final flag`、`failed final flag`、`tight trading range final flag`、`huge trend bar final flag` | `L3` | 晚期趋势中的横向/收敛暂停与失败突破 |

### 5.5 执行、风控与管理

| 标准项 | 原文别名/近义词 | 层级 | 量化抓手 |
| --- | --- | --- | --- |
| `trade_style_family` | `scalp`、`scalper's profit`、`swing`、`swinging`、`trading`、`investing` | `L4` | 持仓 bar 数、目标倍数、出场风格 |
| `session_family` | `first hour`、`trading the open`、`middle of day`、`middle of range`、`end of day`、`close of market`、`premarket`、`post-market`、`overnight`、`Globex` | `L4` | 时段过滤器、开盘/午盘/尾盘状态 |
| `order_family` | `stop order`、`limit order`、`market order`、`bracket order`、`OCO`、`order entry system` | `L4` | 入场方式、成交机制、自动保护/止盈 |
| `risk_family` | `protective stop`、`actual risk`、`initial risk`、`slippage`、`black swan` | `L4` | 止损位置、实际滑点、极端事件风险 |
| `probability_family` | `trader's equation`、`probability`、`risk/reward`、`reward at least two times risk` | `L4` | 胜率、赔率、期望值、收益风险比 |
| `management_family` | `scaling in`、`taking profits`、`trade management`、`scale out`、`hold to close` | `L4` | 加仓、减仓、时间止盈、分批止盈 |
| `trap_family` | `trap`、`trapped in a trade`、`trapped out of a trade` | `L4` | 被套/被洗出后的反向流动性事件 |
| `process_family` | `business plan`、`discipline`、`patience`、`overtrading`、`losing because of mistakes`、`good trade goes bad` | `L4` | 执行偏差、规则违背、复盘标签 |

## 6. 对统一接口的建议

### 6.1 不要把所有词都做成“形态接口”

建议只把 `L3` 做成真正的 `PatternSpec`，其余层级作为依赖层：

- `L0` 输出状态标签，例如 `trend`、`range`、`always_in_long`。
- `L1` 输出结构原语，例如 `swing_high`、`inside_bar`、`gap_open`、`trendline_break`。
- `L2` 输出 setup/trigger 事件，例如 `high_2_buy`、`second_entry_short`、`abc_pullback_complete`。
- `L3` 才输出可交易形态，例如 `double_top`、`wedge_bottom`、`final_bull_flag_failure`、`bear_climax_reversal`。
- `L4` 不做形态检测，而做 `TradePlan`、`RiskModel`、`ExecutionPolicy`。

### 6.2 建议的标准对象

| 对象 | 作用 | 最小字段 |
| --- | --- | --- |
| `ContextState` | 记录当前市场大环境 | `state_id`、`direction`、`strength`、`window` |
| `StructurePrimitive` | 记录基础结构点/线/关系 | `primitive_id`、`timestamp`、`price`、`anchors` |
| `SetupEvent` | 记录计数与入场前置条件 | `setup_id`、`direction`、`source_primitives`、`trigger_bar` |
| `PatternEvent` | 记录复合形态 | `pattern_id`、`family`、`direction`、`stage`、`score` |
| `TradePlan` | 记录策略动作 | `entry_rule`、`stop_rule`、`target_rule`、`timeout_rule` |
| `OutcomeLabel` | 记录事后验证标签 | `mfe`、`mae`、`hit_stop`、`hit_target`、`bars_held` |

### 6.3 建议的 `PatternSpec` 字段

| 字段 | 说明 |
| --- | --- |
| `pattern_id` | 统一主键，例如 `wedge_top`、`double_bottom_bull_flag` |
| `family` | 所属家族，例如 `wedge`、`mtr`、`climax` |
| `layer` | 固定为 `L3`，避免和上下文混淆 |
| `direction` | `bullish`、`bearish`、`both` |
| `aliases` | 原始字幕词和书本 index 词 |
| `required_context` | 依赖哪些 `ContextState` |
| `required_primitives` | 依赖哪些 `StructurePrimitive` |
| `setup_logic` | 不看未来 K 线的实时判定条件 |
| `trigger_logic` | 真正触发入场的条件 |
| `invalidation_logic` | 失效条件 |
| `target_logic` | 目标位或测量目标规则 |
| `score_fields` | 强弱评分字段 |
| `realtime_safe` | 是否完全实时可用 |
| `uses_future_bars` | 是否需要事后标签 |
| `source_refs` | 来源视频或索引词条 |

### 6.4 建议的模块边界

```python
def prepare_ohlcv(df): ...
def add_context_features(df): ...
def add_structure_primitives(df): ...
def add_setup_events(df): ...
def detect_pattern(df, pattern_id): ...
def label_pattern_outcomes(df, pattern_id): ...
def build_trade_plan(df, pattern_id): ...
```

建议把 `detect_*` 和 `label_*` 严格分离。凡是依赖未来 K 线确认的，例如 `failed breakout`、`climactic reversal`、`MTR success`，都不应混入实时检测主函数。

## 7. 建议的优先量化顺序

1. `trend / channel / trading_range`：这是所有上层形态的上下文底座。
2. `breakout / failed_breakout`：几乎贯穿整套课程，是最好的统一事件接口。
3. `double_top_bottom`：结构清晰、易做 swing 点版检测。
4. `wedge / three_pushes`：和 `climax`、`MTR`、`final_flag` 共用很多原语。
5. `major_trend_reversal`：适合作为复合事件总线，串联趋势线、双顶双底、楔形。
6. `trading_range / barb_wire / breakout_mode`：后续可承接区间交易和失败突破。
7. `final_flag`：适合在晚期趋势场景里做失败突破研究。
8. `climax / climactic_reversal`：更适合先做标签，再决定是否做实时检测。
9. `measured_move`：可先作为目标位模块，而不是独立开仓形态。
10. `triangle / head_and_shoulders / rounded`：建议放到第二批，因为它们大多是高阶复合图形。

## 8. 注意事项

- 课程里很多词是“场景名”而不是“独立形态”，例如 `Trading Tight Bull Channels` 应该映射回 `channel_family + session/style`。
- 有些词更适合做 `alias` 而不是单独接口，例如 `ii`、`ioi`、`Double Top Twin`、`Exhaustion Bar`。
- 有些词适合做 `target` 或 `risk`，不适合直接做 `pattern`，例如 `Measured Move`、`Actual Risk`、`Trader's Equation`。
- `book index` 比字幕标题更细，补足了 `Barb Wire`、`Dueling Lines`、`Shrinking Stairs`、`M2B/M2S` 等细项；后续若要完全逐句对齐字幕，还可以继续做更细粒度词频抽取。
- 本报告的目标是给后续量化和统一接口提供“母表”。真正编码时，应优先围绕标准项，而不是围绕课程章节名。
