# Polars 与 Plotly 约定

这个文件定义默认数据列、Binance 原始 CSV 映射、Polars 写法和 Plotly 图层习惯。

## 默认数据接口

优先使用以下规范列名：

- `timestamp`
- `open`
- `high`
- `low`
- `close`
- `volume`

可选扩展列：

- `close_time`
- `quote_volume`
- `count`
- `taker_buy_volume`
- `taker_buy_quote_volume`
- `symbol`
- `timeframe`

## Binance 原始 ETH 5m 样本

仓库中的参考样本路径：

- `data/binance_um_perp/ETHUSDT/5m/ETHUSDT-5m-2026-02.csv`

该 CSV 的原始列为：

- `open_time, open, high, low, close, volume, close_time, quote_volume, count, taker_buy_volume, taker_buy_quote_volume, ignore`

默认映射规则：

```python
import polars as pl

def prepare_ohlcv(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.rename({"open_time": "timestamp"})
        .with_columns(
            pl.from_epoch("timestamp", time_unit="ms").alias("timestamp"),
            pl.from_epoch("close_time", time_unit="ms").alias("close_time"),
            pl.col(["open", "high", "low", "close", "volume", "quote_volume",
                    "taker_buy_volume", "taker_buy_quote_volume"]).cast(pl.Float64),
            pl.col("count").cast(pl.Int64),
        )
        .drop("ignore")
        .sort("timestamp")
    )
```

## Polars 实现习惯

- 优先使用 `with_columns` 批量生成特征列。
- 所有阈值用具名常量，不把 magic number 散落在表达式里。
- 所有模式判定列命名成布尔列，如 `is_double_top`、`is_triangle_breakout`。
- 优先先构造基础特征，再构造模式列；不要把全部逻辑揉成一条长表达式。

推荐基础特征：

- `bar_range = high - low`
- `body_size = abs(close - open)`
- `upper_wick = high - max(open, close)`
- `lower_wick = min(open, close) - low`
- `body_ratio = body_size / bar_range`
- `true_range`
- `atr_n`
- `rolling_high_n`
- `rolling_low_n`
- `swing_high`
- `swing_low`

推荐把结构参数抽成常量：

```python
ATR_WINDOW = 20
SWING_LOOKBACK = 3
DOUBLE_TOP_TOLERANCE_ATR = 0.3
BREAKOUT_CONFIRM_BARS = 2
```

## Plotly 绘图习惯

- 优先使用 `plotly.graph_objects`。
- 主图默认 `go.Candlestick`。
- 支撑阻力、三角形边界、通道线、颈线使用 `fig.add_shape(...)`。
- swing 点、突破点、入场止损目标使用 `go.Scatter(mode="markers+text")`。
- 模式区间高亮使用矩形 `shape` 或半透明带状区域。
- hover 中至少展示：时间、OHLC、volume、模式标签、关键特征值。

推荐颜色语义：

- 多头结构：绿色系
- 空头结构：红色系
- 中性边界/区间：蓝色或灰色
- 待确认/假设：橙色虚线

## 交易语义字段约定

输出时默认使用以下字段名：

- `entry_trigger`
- `stop_rule`
- `target_rule`
- `invalidation_rule`
- `market_scope`
- `timeframe_scope`
- `evaluation_metrics`

如果用户没有给市场和周期，就写成假设而不是省略。

## 验证建议

- 先在样本上画出 `swing_high` / `swing_low`，确认结构识别方向正确。
- 再叠加模式边界与入场止损目标，确认没有“指标看起来对、图上却不对”的情况。
- 对突破类模式，额外检查突破后 `n` 根内是否有回归边界的失败突破情况。
