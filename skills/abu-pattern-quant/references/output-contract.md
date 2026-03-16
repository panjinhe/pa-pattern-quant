# 输出契约

使用这个 skill 时，默认先生成并保存交付物，再在对话中汇报结果。不要只给一段说明而不落盘。

## 1. 产出目录

默认目录结构：

```text
产出/
  <文档名>/
    README.md
    detect_<pattern>.py
    strategy_<pattern>.py
    plot_<pattern>.py
    run-report.md
    plots/
      <pattern>-example-01.png
      <pattern>-example-01.html
```

最低要求：

- 必须有 `README.md`
- 必须有 `detect_<pattern>.py`
- 必须有 `strategy_<pattern>.py`
- 必须有 `run-report.md`
- 必须有至少 1 张案例图 `png`

## 2. README.md 结构

`README.md` 必须按以下顺序组织：

1. `文档摘要`
2. `形态归纳`
3. `量化定义`
4. `交易语义`
5. `交易策略`
6. `代码与文件清单`
7. `运行结果`
8. `图表清单`
9. `验证与风险`

### 文档摘要

- 3 到 6 条要点
- 只保留与量化有关的规则、边界、交易语义
- 不复述课程背景，不摘抄大段原文

### 形态归纳

- 列出识别到的模式族
- 每个模式族都要写：`原文线索`、`归一化模式名`、`为什么归到这里`
- 如果同一段文字可映射到多个模式族，明确写主模式和次模式

### 量化定义

每个模式至少包含以下字段：

- `所需字段`
- `衍生特征`
- `判定逻辑`
- `确认条件`
- `失效条件`
- `待验证假设`

### 交易语义

每个模式默认输出：

- `入场触发`
- `止损位置`
- `目标位/测量目标`
- `失败条件`
- `适用品种或周期假设`
- `建议统计指标`

### 交易策略

每个模式默认补出策略层规则，至少包含：

- `方向`
- `入场规则`
- `出场规则`
- `止损规则`
- `止盈/目标位规则`
- `过滤条件`
- `风险管理假设`
- `建议回测指标`

### 代码与文件清单

至少列出：

- `detect_<pattern>.py` 的职责
- `strategy_<pattern>.py` 的职责
- `plot_<pattern>.py` 的职责
- 图表文件路径
- 运行报告路径

### 运行结果

至少写明：

- 使用的数据文件
- 样本起止时间
- 命中的候选数量或回测样本量
- 是否成功生成图表
- 是否存在运行失败

### 图表清单

至少列出：

- 图表文件路径
- 图表展示的形态类型
- 图表对应的时间区间或案例编号

### 验证与风险

至少覆盖：

- 哪些阈值是待验证假设
- 哪些策略规则是经验假设
- 哪些字段使用了未来 K 线
- 如何切分样本做前向验证与回测

## 3. 代码产物要求

- 检测脚本使用 Python + Polars
- 先写数据准备和列映射，再写特征列，再写模式判定列
- 允许用函数包装重复逻辑，但不要直接接回测引擎
- 任何使用未来 K 线的逻辑都必须显式标注为 `outcome`、`label` 或 `validation`

推荐接口：

```python
import polars as pl

def prepare_ohlcv(df: pl.DataFrame) -> pl.DataFrame:
    ...

def add_pattern_features(df: pl.DataFrame) -> pl.DataFrame:
    ...

def detect_pattern(df: pl.DataFrame) -> pl.DataFrame:
    ...

def label_pattern_outcomes(df: pl.DataFrame) -> pl.DataFrame:
    ...

def build_pattern_strategy(df: pl.DataFrame) -> pl.DataFrame:
    ...
```

## 4. 图表要求

每个案例图至少说明或展示：

- `主图类型`
- `形态区间`
- `关键标记`
- `入场/止损/目标位`
- `需要显示的 hover 字段`

默认以 candlestick 为主图，不要改成抽象说明。

## 5. 运行报告要求

`run-report.md` 至少包含：

- 执行的脚本
- 使用的数据文件
- 执行成功或失败状态
- 关键统计摘要
- 生成文件清单
- 失败原因和下一步建议

## 输出风格

- 默认中文
- 默认 UTF-8
- 表格和列表优先，避免大段叙述
- 如果信息不足，不要伪造；显式写 `待验证假设`
