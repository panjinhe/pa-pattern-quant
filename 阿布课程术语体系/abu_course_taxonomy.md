# 阿布课程术语体系文档版

## 1. 文档说明

- 该文件是 `abu_course_taxonomy.json` 的人工阅读版。
- 目标是方便快速浏览术语分层、标准项、课程映射和统一接口字段。
- 来源语料：`英文文档` 下 193 份英文字幕，外加书籍 index 交叉校验。

## 2. 元数据

- 项目：`pa-pattern-quant`
- 生成日期：`2026-03-19`
- 标准化原则：先保留原始英文术语，再把字幕和 index 中的同义词归并到标准项。

## 3. 分层结构

| 层级 ID | 中文名 |
| --- | --- |
| `context` | 市场状态与上下文 |
| `primitive` | 结构原语 |
| `setup` | 计数与入场 |
| `pattern` | 复合形态与失败结构 |
| `execution` | 执行、风控与管理 |

## 4. 术语总表

### 4.1 市场状态与上下文

| 标准 ID | 中文名 | 英文名 | 类型 | 别名 |
| --- | --- | --- | --- | --- |
| `always_in_family` | Always In 状态族 | Always In Family | `state` | always in、always in long、always in short |
| `breakout_family` | 突破族 | Breakout Family | `family` | breakout、breakout pullback、breakout test |
| `buying_selling_pressure` | 买压/卖压 | Buying/Selling Pressure | `concept` | buying pressure、selling pressure |
| `channel_family` | 通道族 | Channel Family | `family` | channel、tight bull channel、tight bear channel、broad bull channel、broad bear channel、micro channel、spike and channel、stairs、shrinking stairs |
| `context_and_bias` | 上下文与顺逆势偏向 | Context and Bias | `concept` | context、with trend、countertrend、fade |
| `market_cycle` | 市场周期 | Market Cycle | `concept` | market cycle、breakout phase、channel phase、trading range phase |
| `price_action` | 价格行为 | Price Action | `concept` | price action |
| `support_resistance_magnet` | 支撑阻力与价格磁铁 | Support/Resistance/Magnet | `family` | support、resistance、price magnet、breakout point、emotional numbers、targets、yesterday high、yesterday low |
| `trading_range_family` | 交易区间族 | Trading Range Family | `family` | trading range、tight trading range、Barb Wire、breakout mode、big up big down |
| `trend_family` | 趋势族 | Trend Family | `family` | trend、bull trend、bear trend、trend resumption、reversal day、trend resumption day |

### 4.2 结构原语

| 标准 ID | 中文名 | 英文名 | 类型 | 别名 |
| --- | --- | --- | --- | --- |
| `bar_classification_family` | K线分类族 | Bar Classification Family | `family` | bull bar、bear bar、trend bar、doji、trading range bar、nontrend bar |
| `bar_timeframe_family` | K线与时间框架族 | Bar and Timeframe Family | `family` | bar、candle、bar chart、candle chart、time frame、tick、pip、point |
| `gap_ema_family` | 缺口与均线族 | Gap and EMA Family | `family` | gap、gap opening、EMA、moving average gap bar、GAP EMA pullback |
| `inside_outside_family` | 内外包族 | Inside/Outside Family | `family` | inside bar、outside bar、ii、iii、ioi |
| `overshoot_family` | 过冲族 | Overshoot Family | `family` | overshoot、undershoot、trend channel line overshoot |
| `signal_setup_entry_family` | setup/信号/入场族 | Signal/Setup/Entry Family | `family` | setup、setup bar、signal bar、entry bar、reversal bar |
| `tail_family` | 影线与极端实体族 | Tail/Wick Family | `family` | tail、wick、shadow、shaved bar、shaved body、exhaustion bar |
| `trendline_family` | 线性结构族 | Trendline Family | `family` | swing point、trendline、micro trendline、trend channel line、micro trend channel line、horizontal line、dueling lines |
| `twin_signal_family` | 双胞胎信号族 | Twin Signal Family | `family` | double bottom twin、double top twin、opposite twins |

### 4.3 计数与入场

| 标准 ID | 中文名 | 英文名 | 类型 | 别名 |
| --- | --- | --- | --- | --- |
| `bar_counting_family` | 数K线族 | Bar Counting Family | `family` | bar counting、counting the legs of a trend |
| `double_flag_pullback_family` | 双测回调族 | Double Flag/Pullback Family | `family` | double top bear flag、double bottom bull flag、double top pullback、double bottom pullback |
| `first_pullback_family` | 首次回调族 | First Pullback Family | `family` | first pullback、first pullback sequence、2HM、11:30 stop run pullback |
| `high_low_counting_family` | H/L 计数族 | High/Low Counting Family | `family` | High 1、High 2、High 3、High 4、Low 1、Low 2、Low 3、Low 4、higher high、lower low |
| `late_entry_family` | 晚入场族 | Late Entry Family | `family` | late entries、missed entries、entering late in trends |
| `leg_pullback_family` | 腿与回调族 | Leg/Pullback Family | `family` | leg、swing、pullback、correction |
| `second_entry_family` | 二次入场族 | Second Entry Family | `family` | second entry、High/Low 2、M2B、M2S |
| `two_legged_family` | 两腿回调族 | Two-Legged Family | `family` | two-legged pullback、ABC correction、ABC pullback、TBTL |

### 4.4 复合形态与失败结构

| 标准 ID | 中文名 | 英文名 | 类型 | 别名 |
| --- | --- | --- | --- | --- |
| `climax_family` | 高潮族 | Climax Family | `family` | climax、buy climax、sell climax、spike、spike and trading range reversal、parabola、climactic reversal |
| `double_top_bottom_family` | 双顶双底族 | Double Top/Bottom Family | `family` | double top、double bottom、micro double top、micro double bottom |
| `failed_pattern_family` | 失败结构族 | Failed Pattern Family | `family` | failed breakout、failed reversal、failed High/Low 2、failed Higher High breakout、failed Lower Low breakout、one-tick failed breakout、five-tick failed breakout、failed failure |
| `final_flag_family` | 最终旗形族 | Final Flag Family | `family` | final flag、failed final flag、tight trading range final flag、huge trend bar final flag |
| `head_shoulders_family` | 头肩形态族 | Head and Shoulders Family | `family` | head and shoulders、failed head and shoulders |
| `measured_move_family` | 测量目标族 | Measured Move Family | `family` | measured move、AB = CD、Leg 1 = Leg 2、thin areas and flags |
| `reversal_family` | 反转族 | Reversal Family | `family` | reversal、minor reversal、major trend reversal、MTR、higher low major trend reversal、lower high major trend reversal |
| `rounded_family` | 圆弧与V形族 | Rounded/V Family | `family` | rounded top、rounded bottom、V top、V bottom |
| `trendline_break_family` | 趋势线破坏族 | Trendline Break Family | `family` | trendline break、trend channel line failed breakout |
| `triangle_family` | 三角形族 | Triangle Family | `family` | triangle、expanding triangle |
| `wedge_family` | 楔形族 | Wedge Family | `family` | wedge、three pushes、wedge reversal、failed wedge、nested wedge |

### 4.5 执行、风控与管理

| 标准 ID | 中文名 | 英文名 | 类型 | 别名 |
| --- | --- | --- | --- | --- |
| `management_family` | 交易管理族 | Management Family | `family` | scaling in、taking profits、trade management、scale out、hold to close |
| `order_family` | 订单族 | Order Family | `family` | stop order、limit order、market order、bracket order、OCO、order entry system |
| `probability_family` | 赔率概率族 | Probability Family | `family` | trader's equation、probability、risk/reward、reward at least two times risk |
| `process_family` | 流程纪律族 | Process Family | `family` | business plan、discipline、patience、overtrading、losing because of mistakes、good trade goes bad |
| `risk_family` | 风险族 | Risk Family | `family` | protective stop、actual risk、initial risk、slippage、black swan |
| `session_family` | 时段族 | Session Family | `family` | first hour、trading the open、middle of day、middle of range、end of day、close of market、premarket、post-market、overnight、Globex |
| `trade_style_family` | 交易风格族 | Trade Style Family | `family` | scalp、scalper's profit、swing、swinging、trading、investing |
| `trap_family` | 陷阱族 | Trap Family | `family` | trap、trapped in a trade、trapped out of a trade |

## 5. 课程主题映射

| 视频 | 英文主题 | 中文主题 | 关联标准项 |
| --- | --- | --- | --- |
| `01` | Terminology | 基本术语 | `price_action、trend_family、trading_range_family、signal_setup_entry_family` |
| `02` | Chart basics and price action | 图表基础与价格行为 | `price_action、context_and_bias、bar_timeframe_family` |
| `03` | Forex Basics | 外汇基础 | `bar_timeframe_family、order_family、risk_family` |
| `04` | My Setup | 我的设置 | `order_family、management_family、process_family` |
| `05` | Program Trading | 交易计划 | `process_family、management_family` |
| `06` | Personality Traits of Successful Traders | 成功交易者特质 | `process_family` |
| `07` | Starting Out | 起步阶段 | `process_family、order_family` |
| `08` | Candles, Setups, and Signal Bars | K线、设置与信号K | `bar_classification_family、signal_setup_entry_family、inside_outside_family` |
| `09` | Pullbacks and Bar Counting | 回调与数K线 | `leg_pullback_family、bar_counting_family、high_low_counting_family` |
| `10` | Buying and selling pressure | 买卖压力 | `buying_selling_pressure、context_and_bias` |
| `11` | Gaps | 缺口 | `gap_ema_family` |
| `12` | Market Cycle | 市场周期 | `market_cycle、trend_family、trading_range_family` |
| `13` | Always In | 单边行情 | `always_in_family、trend_family` |
| `14` | Trends | 趋势 | `trend_family、channel_family、breakout_family` |
| `15` | Breakouts | 突破 | `breakout_family、failed_pattern_family` |
| `16` | Channels | 通道 | `channel_family、trendline_family` |
| `17` | Tight Channels & Micro Channels | 窄通道与微通道 | `channel_family` |
| `18` | Trading Ranges | 交易区间 | `trading_range_family、support_resistance_magnet` |
| `19` | Support and Resistance | 支撑与阻力 | `support_resistance_magnet` |
| `20` | Measured moves | 等距移动 | `measured_move_family` |
| `21` | Reversals | 反转 | `reversal_family、trendline_break_family` |
| `22` | Major Trend Reversals | 趋势反转 | `reversal_family、double_top_bottom_family、wedge_family` |
| `23` | Final Flags | 最终旗形 | `final_flag_family、failed_pattern_family` |
| `24` | Wedges | 楔形 | `wedge_family` |
| `25` | Double Tops and Bottoms | 双顶双底 | `double_top_bottom_family、double_flag_pullback_family` |
| `26` | Triangles | 三角形 | `triangle_family` |
| `27` | Head and Shoulders | 头肩形态 | `head_shoulders_family` |
| `28` | Rounded Tops and Bottoms | 圆弧形态 | `rounded_family` |
| `29` | Climaxes | 高潮 | `climax_family` |
| `30` | Trader’s Equation and Probability | 盈利公式与概率 | `probability_family` |
| `31` | Swing Trading and Scalping | 波段与短线 | `trade_style_family` |
| `32` | Orders | 订单 | `order_family` |
| `33` | Protective Stops | 止损 | `risk_family、trap_family` |
| `34` | Actual Risk | 实际风险 | `risk_family、probability_family` |
| `35` | Scaling in | 分段入场 | `management_family、risk_family` |
| `36` | Trade Management and Taking Profits | 头寸管理与止盈 | `management_family` |
| `37` | How to trade | 如何进行交易 | `order_family、management_family、process_family` |
| `38` | Trading MTR Tops | 顶部趋势反转交易 | `reversal_family、wedge_family、double_top_bottom_family` |
| `39` | Trading MTR Bottoms | 底部趋势反转交易 | `reversal_family、wedge_family、double_top_bottom_family` |
| `40` | Entering Late in Trends | 趋势跟随晚入场 | `late_entry_family、trend_family` |
| `41` | Trading Breakouts | 突破交易 | `breakout_family、failed_pattern_family` |
| `42` | Trading Climactic Reversals | 高潮反转交易 | `climax_family、reversal_family` |
| `43` | Trading Tight Bull Channels | 窄幅多头通道交易 | `channel_family、trend_family` |
| `44` | Trading Tight Bear Channels | 窄幅空头通道交易 | `channel_family、trend_family` |
| `45` | Trading Broad Bull Channels | 宽阔多头通道交易 | `channel_family、trend_family` |
| `46` | Trading Broad Bear Channels | 宽阔空头通道交易 | `channel_family、trend_family` |
| `47` | Trading in Trading Ranges | 区间内交易 | `trading_range_family、failed_pattern_family` |
| `48` | Trading the Open / Middle / End of the Day | 开盘/午盘/尾盘交易 | `session_family` |
| `49` | Swing Trading Examples | 波段交易案例 | `trade_style_family、management_family` |
| `50` | Scalping | 剥头皮交易 | `trade_style_family、order_family` |
| `51` | Losing Because of Mistakes | 因错误而亏损 | `process_family、trap_family` |
| `52` | Losing When Good Trade Goes Bad | 好交易变坏交易 | `management_family、risk_family、process_family` |

## 6. 统一接口字段

### 6.1 PatternSpec 推荐字段

- `pattern_id`
- `family`
- `layer`
- `direction`
- `aliases`
- `required_context`
- `required_primitives`
- `setup_logic`
- `trigger_logic`
- `invalidation_logic`
- `target_logic`
- `score_fields`
- `realtime_safe`
- `uses_future_bars`
- `source_refs`

### 6.2 运行时事件字段

- `symbol`
- `timeframe`
- `timestamp`
- `pattern_id`
- `stage`
- `direction`
- `confidence`
- `entry_price`
- `stop_price`
- `target_price`
- `key_points`

### 6.3 推荐函数边界

- `prepare_ohlcv`
- `add_context_features`
- `add_structure_primitives`
- `add_setup_events`
- `detect_pattern`
- `label_pattern_outcomes`
- `build_trade_plan`

## 7. 使用建议

- `context`、`primitive`、`setup` 不建议直接当作最终交易形态，而应作为上游依赖层。
- 真正适合做统一模式接口的是 `pattern` 层。
- `execution` 层更适合映射为交易计划、风控和执行策略，而不是图形检测。
- 若后续继续扩展术语表，建议优先维护 JSON，再重新生成本 Markdown 文档。
