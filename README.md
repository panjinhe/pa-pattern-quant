# pa-pattern-quant

这是一个围绕 `abu-pattern-quant` skill 搭建的价格行为量化项目。项目的核心目标不是手工写策略说明，而是把阿布价格行为课程讲义、语音转文字稿、中文笔记等文本材料，直接转换成可运行、可回测、可出图、可落盘的量化交付物。

当前项目以 `ETHUSDT 5m` 的 OHLCV 数据为默认样本，使用 `Polars + Plotly + Python` 完成形态检测、策略回测、结果汇总和图表输出。

## Skill 主要功能

`skills/abu-pattern-quant/SKILL.md` 定义了本项目最重要的能力：把单篇课程文档自动转成一套量化研究产物。

它主要做这几件事：

- 从 `.txt`、`.docx`、`.pdf` 等课程材料中抽取可量化的形态语义。
- 把自然语言归一化到稳定的模式族，例如趋势、交易区间、旗形、楔形、三角形、双顶双底、失败突破、主要趋势反转。
- 把“强、弱、明显、接近、失败、测试”等主观词翻译成可测条件、窗口、阈值和待验证假设。
- 生成独立的检测脚本、策略脚本和绘图脚本，而不是把逻辑塞进单个文件。
- 默认运行脚本并产出完整交付目录，而不只是返回一段文字总结。
- 输出信号统计、回测表现、案例图和净值曲线，便于快速判断形态是否具备研究价值。

## 默认交付物

当用这个 skill 处理一篇课程文档时，默认会在 `产出/<文档名>/` 下生成类似结果：

```text
产出/
  <文档名>/
    README.md
    detect_<pattern>.py
    strategy_<pattern>.py
    plot_<pattern>.py
    run-report.md
    backtest-summary.json
    signals_baseline.parquet
    trades_baseline.csv
    equity-curve-baseline.csv
    plots/
      <pattern>-examples.png
      <pattern>-examples.html
      <pattern>-equity-curve.png
      <pattern>-equity-curve.html
```

这些文件分别对应：

- 量化定义与交易语义说明
- 实时检测逻辑
- 策略与回测逻辑
- 案例图和净值图
- 运行结果与回测摘要

## 仓库结构

```text
阿布课程语音转文字/   原始课程文本
skills/               项目内 skill 定义
scripts/              公共脚本与数据准备脚本
data/                 本地样本数据目录
产出/                 每篇文档的量化交付结果
```

重点目录：

- `skills/abu-pattern-quant/`
  这个项目最核心的 skill，包含主说明和参考约定。
- `scripts/build_eth_5m_history.py`
  用于构建默认样本数据 `ETHUSDT-5m-history.parquet`。
- `产出/23A 旗形/`
  已完成的“最终旗形”交付样例。
- `产出/24A 楔形/`
  已完成的“楔形”交付样例。

## 数据说明

默认数据路径是：

`data/binance_um_perp/ETHUSDT/5m/ETHUSDT-5m-history.parquet`

这个历史 parquet 由于体积原因已从 git 跟踪中排除，需要本地自行准备。项目里提供了数据构建脚本：

```powershell
.venv\Scripts\python.exe .\scripts\build_eth_5m_history.py
```

脚本会把 Binance 月度数据与本地分区数据合并，输出项目默认使用的历史样本文件。

## 环境要求

- Python `3.14`
- 依赖见 `pyproject.toml`
- 主要运行库：
  - `polars`
  - `plotly`
  - `kaleido`

如果使用 `uv`，可以按常规方式安装依赖：

```powershell
uv sync
```

## 如何使用这个项目

如果你是在 Codex 里工作，最直接的方式是显式调用 skill，并给出要处理的文档：

```text
[$abu-pattern-quant](C:\Users\Administrator\.codex\skills\abu-pattern-quant\SKILL.md) 阿布课程语音转文字\23A 旗形.txt
```

或：

```text
[$abu-pattern-quant](C:\Users\Administrator\.codex\skills\abu-pattern-quant\SKILL.md) 阿布课程语音转文字\24A 楔形.txt
```

skill 会默认：

- 读取单篇文档
- 归纳形态
- 生成检测/策略/绘图脚本
- 运行脚本
- 产出报告、图表、回测摘要

## 当前项目状态

目前仓库里已经有两篇课程文档的完整交付样例：

- `23A 旗形`
- `24A 楔形`

它们可以作为后续新文档量化交付的结构模板，也可以作为调参、复用脚本和比较不同形态表现的基准样本。

## 适用场景

这个项目适合以下任务：

- 把价格行为课程文档转成量化研究原型
- 快速验证某个形态是否值得继续研究
- 用统一格式沉淀不同形态的检测与策略脚本
- 为后续更严格的样本外验证、参数收紧和组合研究提供母集

不适合直接把课程里的口头经验原封不动当成实盘策略。项目更偏“研究自动化”和“形态结构落盘”，不是现成交易系统。

## 相关文件

- [abu-pattern-quant skill](E:/pa-pattern-quant/skills/abu-pattern-quant/SKILL.md)
- [23A 旗形 README](E:/pa-pattern-quant/产出/23A 旗形/README.md)
- [24A 楔形 README](E:/pa-pattern-quant/产出/24A 楔形/README.md)
