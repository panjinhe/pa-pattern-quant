from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal, TypeAlias


ConceptLayer: TypeAlias = Literal[
    "context",
    "primitive",
    "setup",
    "pattern",
    "execution",
]

ConceptKind: TypeAlias = Literal["concept", "family", "state", "signal", "pattern", "risk"]
Direction: TypeAlias = Literal["bullish", "bearish", "both", "neutral"]
EventStage: TypeAlias = Literal[
    "context",
    "candidate",
    "setup",
    "trigger",
    "confirmed",
    "invalidated",
    "completed",
]
OverlayKind: TypeAlias = Literal["marker", "segment", "zone", "label"]


@dataclass(frozen=True)
class ConceptSpec:
    """静态语义定义：这个概念是什么。"""

    concept_id: str
    name_zh: str
    name_en: str
    layer: ConceptLayer
    kind: ConceptKind
    family: str
    aliases: list[str] = field(default_factory=list)
    quantifiable: bool = True
    notes: str | None = None


@dataclass(frozen=True)
class DetectorBinding:
    """量化实现绑定：这个概念由哪个脚本/函数检测。"""

    module_path: str
    detect_function: str
    label_function: str | None = None
    strategy_function: str | None = None
    plot_function: str | None = None


@dataclass(frozen=True)
class QuantSpec:
    """具体量化实现：这个概念当前怎么被量化。"""

    spec_id: str
    concept_id: str
    version: str
    title: str
    input_columns: list[str]
    feature_columns: list[str]
    event_columns: list[str]
    required_context_ids: list[str] = field(default_factory=list)
    required_primitive_ids: list[str] = field(default_factory=list)
    params: dict[str, Any] = field(default_factory=dict)
    detector: DetectorBinding | None = None
    realtime_safe: bool = True
    uses_future_bars: bool = False
    supports_backtest: bool = True
    supports_visualization: bool = True


@dataclass(frozen=True)
class KeyPoint:
    """前端直接使用的关键点。"""

    point_id: str
    role: str
    timestamp: str
    price: float
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Overlay:
    """图层对象，前端不再自己猜几何。"""

    overlay_id: str
    kind: OverlayKind
    role: str
    points: list[KeyPoint] = field(default_factory=list)
    style: dict[str, Any] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TradePlan:
    """交易计划，适用于 pattern/execution 层事件。"""

    direction: Direction
    entry_trigger: str
    entry_price: float | None = None
    stop_price: float | None = None
    target_prices: list[float] = field(default_factory=list)
    timeout_bars: int | None = None
    invalidation_rule: str | None = None
    tags: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class OutcomeLabel:
    """事后标签，必须和实时检测分开。"""

    lookahead_bars: int
    outcome_class: str
    hit_target: bool | None = None
    hit_stop: bool | None = None
    mfe: float | None = None
    mae: float | None = None
    pnl_r: float | None = None
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class QuantEvent:
    """一次具体识别结果。"""

    event_id: str
    run_id: str
    concept_id: str
    spec_id: str
    symbol: str
    timeframe: str
    direction: Direction
    stage: EventStage
    detected_at: str
    start_time: str | None = None
    end_time: str | None = None
    confidence: float | None = None
    score: float | None = None
    family: str | None = None
    features: dict[str, Any] = field(default_factory=dict)
    key_points: list[KeyPoint] = field(default_factory=list)
    overlays: list[Overlay] = field(default_factory=list)
    trade_plan: TradePlan | None = None
    outcome: OutcomeLabel | None = None
    tags: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RunSummary:
    """一次量化运行的摘要。"""

    rows: int
    event_count: int
    signal_count: int | None = None
    trade_count: int | None = None
    win_rate: float | None = None
    avg_r: float | None = None
    total_r: float | None = None
    total_net_pnl: float | None = None
    max_drawdown: float | None = None
    profit_factor: float | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class QuantRun:
    """一次完整批处理：前端列表页通常直接消费这个对象。"""

    run_id: str
    spec_id: str
    concept_id: str
    symbol: str
    timeframe: str
    data_source: str
    started_at: str
    ended_at: str | None = None
    sample_start: str | None = None
    sample_end: str | None = None
    params: dict[str, Any] = field(default_factory=dict)
    summary: RunSummary | None = None
    events: list[QuantEvent] = field(default_factory=list)
    artifacts: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
