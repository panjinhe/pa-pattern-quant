---
name: text-to-live-strategy
description: 将 PRD、CSV、字幕、课程笔记、策略说明或历史研究样例转换为可接入直播框架的标准化价格行为策略产物，包括 `strategylets/` 模块、`strategylets/registry.json` 元数据、`QuantSpec / QuantEvent / QuantRun / OpportunityCandidate`、回测结果、README 与案例图。用于当前项目需要把自然语言策略批量标准化、接入 Trading Graph / TA Agent / Narration，或把已有研究样例改造成直播可消费对象时使用。
---

# text-to-live-strategy

用这个 skill 固化当前项目已经验证过的流程：把文字材料整理成 `strategylets/` 标准模块，并同时产出研究轨与直播轨结果，而不是停留在文字总结。

先打开 `references/live-standard-contract.md`，再按下面步骤执行；如果需要底层契约与样例代码，再分别打开：

- `阿布课程术语体系/contracts/quant_interface.py`
- `strategylets/registry.json`
- `strategylets/trend_strategy_common.py`
- 已实现样例：`strategylets/range_edge_failed_breakout.py`、`strategylets/close_chase_entry.py`、`strategylets/inverse1_fail_forward1_success.py`、`strategylets/ema20_gap_touch_fail.py`、`strategylets/other_ytb_001_trendline_breakout.py`

## 默认假设

- 默认使用中文，新增文件使用 UTF-8，git 提交消息使用中文。
- 默认直播目标口径为 `BTCUSDT 5m`；如果当前只有现成的 `ETHUSDT 5m` 数据，就先用它验证结构、回测和图层，并在结果里写明假设。
- 默认交易成本按每边 `2 bps` 处理；除非用户明确指定，不要改成其他值。
- 默认一次只完整实现 `1` 个策略模板；如果输入里有多个策略，其余策略先进入 `registry` 占位。
- 默认优先复用现有 `strategylets` 原语与公共函数，不要从零重写同类结构。

## 先判断是否使用

遇到以下任务时使用这个 skill：

- 把 PRD、CSV、字幕、课程文字、阿布/太妃笔记转成直播可消费的策略对象。
- 把历史 `detect_*.py` / `strategy_*.py` 样例迁移到 `strategylets/` 标准入口。
- 为直播项目批量标准化策略，并统一输出 `OpportunityCandidate`、图表与回测摘要。
- 从一组文字材料中先落一个“标准母版策略”，再给剩余策略补 `registry` 骨架。

仅当用户只是要摘要、翻译、写讲解文案，而不需要代码、策略对象或产物目录时，不使用这个 skill。

## 工作流

### 1. 锁定来源与策略边界

- 读取 PRD、CSV、字幕、课程讲义、历史产出 README；只保留与量化和直播输出有关的部分。
- 合并同一策略的多份来源，并把原始文件路径记入 `source_refs`。
- 如果一次输入覆盖多个策略，先拆成策略清单；默认只把其中 `1` 个做到 `implemented`，其余先写入 `registry` 并标记为 `planned` 或 `templated`。
- 不要把“策略说明”“市场阶段”“买卖点”“过滤条件”“谨慎信号”混成一句话，要分别抽出来。

### 2. 固定命名与注册

- 按 `references/live-standard-contract.md` 的规则生成：
  - `strategy_code`
  - `strategy_name_std`
  - `concept_id`
  - `spec_id`
- 太妃体系默认用 `PA_taifei_001_策略名` 这样的命名。
- 非太妃来源沿用独立命名空间，例如 `other_ytb_001_策略名`。
- 把策略元数据写入 `strategylets/registry.json`，并明确：
  - `status`
  - `module_path`
  - `market_phase`
  - `strategy_positioning`
  - `source_refs`

### 3. 把文字翻译成“直播可用策略语义”

- 不要只产出“买卖点”；要同时产出：
  - 成立条件：`hard_gates`
  - 加强条件：`strengtheners`
  - 谨慎信号：`cautions`
  - 评分与分档：`score`、`score_band`、`candidate_tier`
  - 执行语义：`entry_zone`、`stop_price`、`take_profit_prices`、`invalidation_price`
  - 可解释证据：`analysis_summary`、`key_points`、`overlays`
- 所有“强、弱、明显、接近、失败、确认、测试”都要落成窗口、阈值、比较关系或待验证假设。
- 把实时检测与事后标签分开：凡是使用未来 K 线的信息，只能进入 `label_outcomes` 或结果标签，不能混进实时 `detect`。
- 保留市场阶段语义，例如“区间”“趋势中”“刚发起”“趋势末期”，因为直播后续过滤和讲解都要用到。

### 4. 落成标准策略模块

- 在 `strategylets/` 下新建或改造模块，优先沿用现有实现风格。
- 至少提供以下固定函数：
  - `prepare_ohlcv(df)`
  - `detect(df, params)`
  - `score(df, params)`
  - `build_candidates(df, run_ctx)`
  - `label_outcomes(df, params)`
- 让模块同时产出研究轨对象与直播轨对象，并对齐 `event_id / candidate_id`。
- 优先复用 `strategylets/trend_strategy_common.py` 的公共逻辑，而不是复制整段代码。
- 在图层中显式提供关键点、线段、区域和标签，让前端直接消费，不要让下游自己猜几何关系。

### 5. 产出研究轨与直播轨文件

- 输出根目录固定为：`产出/标准策略/<spec_id>/<run_id>/`
- 每次运行至少落盘：
  - `strategy_spec.json`
  - `quant_run.json`
  - `candidates.jsonl`
  - `candidates.parquet`
  - `signals.parquet`
  - `backtest-summary.json`
  - `trades.csv`
  - `README.md`
  - `plots/`
- `plots/` 至少包含：
  - `example-001.png` 这类案例图
  - `equity-curve.png`
- 案例图必须标出入场点、出场点、止损/目标或失效位，并避免文字严重重叠。
- 如果策略用到 EMA、ATR 比值、均线斜率、突破距离、重叠率、突破深度等指标来判断机会或强弱，案例图必须同步把这些指标画出来：
  - 价格型指标放主图。
  - 归一化或评分型指标放副图。
  - 目标是让直播侧或人工复核能直接看出“为什么这根 bar 会触发”。
- `README.md` 必须能直接回答：
  - 这个策略是什么
  - 输入是什么
  - 输出是什么
  - 为直播提供什么信息
  - 成立条件、加强条件、谨慎信号是什么
  - 买卖点、止损、目标位如何给出

### 6. 运行、验证与汇报

- 至少实际运行一次策略模块，不要只写代码不执行。
- 优先使用模块自带 CLI，例如：

```powershell
.venv\Scripts\python.exe .\strategylets\<module>.py --input <ohlcv.parquet> --output-root .\产出\标准策略 --symbol ETHUSDT --timeframe 5m
```

- 验证以下事项：
  - 同一输入重复运行时，`candidate_id`、分数和主要图层稳定一致。
  - `snapshot_status != complete` 时，不产出可执行 candidate。
  - 同一 `symbol / timeframe / bar / spec / side` 不重复产出 candidate。
  - `primary / secondary / drop` 路由正确。
  - 图中能直观看出为什么会触发该策略。
  - 图中能看到主要决策指标，并能分清哪些指标负责机会判断、哪些指标负责强弱判断。
- 汇报时优先给用户：
  - 当前实现到了哪一步
  - 策略输入是什么
  - 标准输出是什么
  - 为直播链路补上了什么信息
  - 还缺什么才能继续批量化

## 不要这样做

- 不要只返回主观文字总结，不落 `strategylets/`、`registry` 和标准产物。
- 不要把未来函数混进实时检测。
- 不要为了凑完整而编造精确阈值；缺失阈值时明确写成假设与校准项。
- 不要直接复用旧 `产出/` 目录当未来标准入口；历史目录只用作参考样例。
- 不要生成只有研究价值、却没有 `OpportunityCandidate` 的半成品。
- 不要忽略图层可解释性；直播场景需要“为什么做这笔”的证据链，而不只是一个方向标签。
