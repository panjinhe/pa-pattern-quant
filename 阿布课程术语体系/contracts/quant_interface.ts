export type ConceptLayer =
  | "context"
  | "primitive"
  | "setup"
  | "pattern"
  | "execution";

export type ConceptKind =
  | "concept"
  | "family"
  | "state"
  | "signal"
  | "pattern"
  | "risk";

export type Direction = "bullish" | "bearish" | "both" | "neutral";

export type EventStage =
  | "context"
  | "candidate"
  | "setup"
  | "trigger"
  | "confirmed"
  | "invalidated"
  | "completed";

export type OverlayKind = "marker" | "segment" | "zone" | "label";

export interface ConceptSpec {
  concept_id: string;
  name_zh: string;
  name_en: string;
  layer: ConceptLayer;
  kind: ConceptKind;
  family: string;
  aliases: string[];
  quantifiable: boolean;
  notes?: string | null;
}

export interface DetectorBinding {
  module_path: string;
  detect_function: string;
  label_function?: string | null;
  strategy_function?: string | null;
  plot_function?: string | null;
}

export interface QuantSpec {
  spec_id: string;
  concept_id: string;
  version: string;
  title: string;
  input_columns: string[];
  feature_columns: string[];
  event_columns: string[];
  required_context_ids: string[];
  required_primitive_ids: string[];
  params: Record<string, unknown>;
  detector?: DetectorBinding | null;
  realtime_safe: boolean;
  uses_future_bars: boolean;
  supports_backtest: boolean;
  supports_visualization: boolean;
}

export interface KeyPoint {
  point_id: string;
  role: string;
  timestamp: string;
  price: number;
  meta: Record<string, unknown>;
}

export interface Overlay {
  overlay_id: string;
  kind: OverlayKind;
  role: string;
  points: KeyPoint[];
  style: Record<string, unknown>;
  meta: Record<string, unknown>;
}

export interface TradePlan {
  direction: Direction;
  entry_trigger: string;
  entry_price?: number | null;
  stop_price?: number | null;
  target_prices: number[];
  timeout_bars?: number | null;
  invalidation_rule?: string | null;
  tags: string[];
}

export interface OutcomeLabel {
  lookahead_bars: number;
  outcome_class: string;
  hit_target?: boolean | null;
  hit_stop?: boolean | null;
  mfe?: number | null;
  mae?: number | null;
  pnl_r?: number | null;
  meta: Record<string, unknown>;
}

export interface QuantEvent {
  event_id: string;
  run_id: string;
  concept_id: string;
  spec_id: string;
  symbol: string;
  timeframe: string;
  direction: Direction;
  stage: EventStage;
  detected_at: string;
  start_time?: string | null;
  end_time?: string | null;
  confidence?: number | null;
  score?: number | null;
  family?: string | null;
  features: Record<string, unknown>;
  key_points: KeyPoint[];
  overlays: Overlay[];
  trade_plan?: TradePlan | null;
  outcome?: OutcomeLabel | null;
  tags: string[];
  meta: Record<string, unknown>;
}

export interface RunSummary {
  rows: number;
  event_count: number;
  signal_count?: number | null;
  trade_count?: number | null;
  win_rate?: number | null;
  avg_r?: number | null;
  total_r?: number | null;
  total_net_pnl?: number | null;
  max_drawdown?: number | null;
  profit_factor?: number | null;
  extra: Record<string, unknown>;
}

export interface QuantRun {
  run_id: string;
  spec_id: string;
  concept_id: string;
  symbol: string;
  timeframe: string;
  data_source: string;
  started_at: string;
  ended_at?: string | null;
  sample_start?: string | null;
  sample_end?: string | null;
  params: Record<string, unknown>;
  summary?: RunSummary | null;
  events: QuantEvent[];
  artifacts: Record<string, string>;
}
