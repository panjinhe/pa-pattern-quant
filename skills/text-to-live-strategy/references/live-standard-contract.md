# 直播策略标准参考

## 关键路径

- `strategylets/registry.json`：唯一机器可读的策略注册表。
- `阿布课程术语体系/contracts/quant_interface.py`：`QuantSpec / QuantEvent / QuantRun / OpportunityCandidate` 契约来源。
- `strategylets/trend_strategy_common.py`：趋势类策略公共函数与图表辅助。
- `strategylets/range_edge_failed_breakout.py`：区间失败突破母版。
- `strategylets/close_chase_entry.py`：趋势中追进入场样例。
- `strategylets/other_ytb_001_trendline_breakout.py`：历史产出改造为直播策略的样例。
- `产出/标准策略/`：统一产出根目录。

## 命名规则

- `strategy_code`
  - 太妃体系：`PA_taifei_001`
  - 非太妃来源：`other_ytb_001`
- `strategy_name_std`
  - 格式：`<strategy_code>_<策略名>`
  - 例：`PA_taifei_001_看衰区间边线突破`
- `concept_id`
  - 使用英文语义 ID，保持稳定，不带版本和 symbol。
  - 例：`range_edge_failed_breakout`
- `spec_id`
  - 格式：`<concept_id>-v<版本>-<symbol>-<timeframe>`
  - 例：`range_edge_failed_breakout-v1-btc-5m`

## OpportunityCandidate 核心语义

完整字段以 `阿布课程术语体系/contracts/quant_interface.py` 为准；实现时至少保证以下几组信息完整：

- 身份字段：`candidate_id`、`run_id`、`spec_id`、`concept_id`、`symbol`、`timeframe`、`signal_bar_time`
- 交易字段：`side`、`setup_type`、`entry_type`、`entry_zone_low`、`entry_zone_high`、`stop_price`、`take_profit_prices`、`invalidation_price`
- 评分字段：`score`、`score_band`、`candidate_tier`、`hard_gates`、`strengtheners`、`cautions`
- 解释字段：`analysis_summary`、`key_points`、`overlays`、`snapshot_status`、`realtime_safe`、`tags`

当前项目沿用的评分口径：

- 先通过全部 `hard_gates`
- 基础分通常从 `70` 起
- 每个 `strengthener` 加分，每个 `caution` 减分
- `score_band`
  - `A`：`>= 85`
  - `B`：`75-84`
  - `C`：`60-74`
- `candidate_tier`
  - `primary`：`score >= 75`
  - `secondary`：`60 <= score < 75`
  - `< 60` 直接丢弃

## 必须落盘的文件

每个 `run_id` 至少包含：

- `strategy_spec.json`
- `quant_run.json`
- `candidates.jsonl`
- `candidates.parquet`
- `signals.parquet`
- `backtest-summary.json`
- `trades.csv`
- `README.md`
- `plots/example-001.png`
- `plots/equity-curve.png`

## README 必答问题

- 这个策略是什么
- 输入是什么
- 输出是什么
- 为直播提供什么信息
- 成立条件是什么
- 加强条件是什么
- 哪些信号需要谨慎
- 买卖点、止损、目标位如何生成

## 最低验收

- 同一输入重复运行，关键对象 ID 与主要结果稳定。
- 不完整 snapshot 不产出 candidate。
- 同一 bar 不重复产出同策略同方向候选。
- 图里能看到关键结构、入场点和出场点。
- 如果 detect / score 使用了指标判断机会或强弱，图里必须同步展示这些指标：
  - 价格型指标放主图
  - 归一化或评分型指标放副图
- `OpportunityCandidate` 无需 AI 补全即可被直播下游直接消费。
