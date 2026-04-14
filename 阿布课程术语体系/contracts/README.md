# 通用量化接口设计

## 目标

这套接口要同时解决三件事：

1. 让课程里的所有术语、概念、形态都能挂到同一棵标准树上。
2. 让不同检测脚本输出同一种结果结构，便于回测、对比、复用。
3. 让前端不用理解每个形态的内部算法，只消费统一的事件和可视化载荷。

## 核心原则

- 语义身份和量化实现分离。
- 实时检测和事后标签分离。
- 模式检测结果和前端展示结果分离，但两者共用同一事件主键。
- 前端不自己推导几何结构，后端直接返回关键点、线段、区域。

## 建议拆成 5 个对象

### 1. `ConceptSpec`

语义层对象，定义“这是什么概念”。

适合承接：

- `abu_course_taxonomy.json`
- 术语分层
- 别名
- 所属家族
- 是否可量化

它不关心具体怎么检测。

### 2. `QuantSpec`

实现层对象，定义“这个概念当前用哪套规则量化”。

适合承接：

- 某个检测脚本版本
- 参数默认值
- 需要哪些输入列
- 输出哪些特征列
- 是否支持实时检测
- 是否支持回测/可视化

同一个 `ConceptSpec` 可以对应多套 `QuantSpec`。

例子：

- `wedge_family` 是语义对象。
- `wedge-v1-eth-5m`、`wedge-v2-generic` 是具体量化实现。

### 3. `QuantEvent`

事件层对象，定义“一次具体识别结果”。

适合承接：

- 某个品种、某个周期、某段时间里识别出的一个形态
- 方向、阶段、强度分数
- 关键点位
- 入场/止损/目标位
- 事后 outcome 标签
- 前端绘图需要的几何信息

### 4. `QuantRun`

批量运行对象，定义“一次检测任务的整体结果”。

适合承接：

- 本次运行用了哪个 `QuantSpec`
- 运行参数
- 样本区间
- 检测统计
- 回测统计
- 事件列表

### 5. `OpportunityCandidate`

直播规则层对象，定义“这个 setup 现在值不值得进入下游精筛/展示”。

适合承接：

- `opportunity_candidate`
- 买卖方向
- 入场区间
- 止损/目标位
- 成立条件命中
- 加强条件
- 谨慎信号
- 前端图层

## 为什么这样拆

如果只定义一个“大接口”，后面会很快混乱：

- 术语表会和检测参数混在一起。
- 检测输出会和回测输出混在一起。
- 前端会被迫知道“楔形”和“双顶双底”的内部差异。

拆成 `ConceptSpec -> QuantSpec -> QuantEvent -> QuantRun -> OpportunityCandidate` 后，职责就稳定了。

## 和当前仓库的对应关系

### 已有静态语义

- [产出/阿布课程术语体系/abu_course_taxonomy.json](/E:/pa-pattern-quant/产出/阿布课程术语体系/abu_course_taxonomy.json)

它天然适合作为 `ConceptSpec` 的来源。

### 已有量化实现

- [产出/23A 旗形/detect_final_flag.py](/E:/pa-pattern-quant/产出/23A%20旗形/detect_final_flag.py)
- [产出/23A 旗形/strategy_final_flag.py](/E:/pa-pattern-quant/产出/23A%20旗形/strategy_final_flag.py)
- [产出/25A-25B 双顶双底/detect_double_top_bottom.py](/E:/pa-pattern-quant/产出/25A-25B%20双顶双底/detect_double_top_bottom.py)

这些脚本里已经有 `prepare_ohlcv`、`detect_*`、`label_*`、`strategy_*` 的雏形，但目前输出还偏“单项目格式”，还没有统一事件契约。

## 建议的统一流程

```text
taxonomy(语义母表)
  -> quant spec(某个概念的量化实现)
  -> detect(生成事件)
  -> score/build candidate(补候选层口径)
  -> label/backtest(补 outcome 和交易结果)
  -> visualization payload(补前端图层)
  -> frontend render
```

## 前端只需要的最小东西

前端其实只关心 3 类数据：

1. 目录树
   - 有哪些概念
   - 有哪些家族
   - 有哪些可点击的量化实现
2. 列表数据
   - 某个概念识别出了哪些事件
   - 每个事件的时间、方向、评分、是否触发交易
3. 图层数据
   - K 线上要画哪些点、线、区间、箭头

所以前端接口建议按下面分：

- `GET /concepts`
- `GET /quant-specs`
- `GET /runs/:run_id`
- `GET /events/:event_id`
- `GET /events/:event_id/chart`

## 建议的后端输出边界

### 检测脚本只负责

- 标准化数据
- 生成特征
- 生成 `QuantEvent`

### 策略脚本只负责

- 从 `QuantEvent` 生成 `TradePlan`
- 从 `QuantEvent`/评分结果生成 `OpportunityCandidate`
- 运行回测
- 回填 `OutcomeLabel`
- 生成 `QuantRun.summary`

### 绘图脚本只负责

- 把事件转换成 `VisualizationPayload`

## 前端展示建议

前端页建议固定成三栏：

1. 左栏：概念树
   - 先按层级
   - 再按家族
   - 再到具体 `QuantSpec`
2. 中栏：事件表
   - 时间
   - 品种
   - 周期
   - 方向
   - 阶段
   - 分数
   - outcome
3. 右栏：图表
   - candlestick
   - key points
   - segments
   - zones
   - 交易标记

## 设计约束

- 所有事件都必须有稳定主键：`event_id`
- 所有规范都必须有稳定主键：`spec_id`
- 一个事件必须能回溯到：
  - `concept_id`
  - `spec_id`
  - `run_id`
- 任何未来函数字段必须明确标记
- 任何前端绘图元素都要显式返回，不允许前端再猜

## 本目录文件

- [contracts/quant_interface.py](/E:/pa-pattern-quant/contracts/quant_interface.py)：Python 类型
- [contracts/quant_interface.ts](/E:/pa-pattern-quant/contracts/quant_interface.ts)：TypeScript 类型

这两份文件是这套设计的最小契约骨架。
