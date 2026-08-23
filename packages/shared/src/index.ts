export type QueryMode = "sql" | "promql";

export type ServiceStatus = "healthy" | "degraded" | "provisioning";

export interface Service {
  id: string;
  name: string;
  region: string;
  status: ServiceStatus;
  version: string;
  databases: number;
  metrics: number;
}

export interface SchemaColumn {
  name: string;
  type: string;
  description: string;
}

export interface SchemaTable {
  database: string;
  name: string;
  engine: string;
  columns: SchemaColumn[];
}

export interface MetricDefinition {
  name: string;
  type: "counter" | "histogram" | "gauge";
  description: string;
  labels: string[];
}

export interface QueryContext {
  serviceId: string;
  tables?: string[];
  metrics?: string[];
  savedQuery?: string;
}

export interface GenerationRequest {
  mode: QueryMode;
  serviceId: string;
  prompt: string;
  context?: QueryContext;
}

export interface QueryDraft {
  mode: QueryMode;
  query: string;
  explanation: string;
  assumptions: string[];
  referencedContext: string[];
  warnings: string[];
}

export interface QueryRequest {
  mode: QueryMode;
  serviceId: string;
  query: string;
}

export type QueryRunState = "queued" | "running" | "success" | "error" | "cancelled";

export interface QueryRun {
  id: string;
  mode: QueryMode;
  serviceId: string;
  query: string;
  state: QueryRunState;
  startedAt: string;
  finishedAt?: string;
  durationMs?: number;
  error?: string;
}

export interface SqlResult {
  mode: "sql";
  columns: string[];
  rows: Array<Record<string, string | number | boolean | null>>;
}

export interface TimeSeriesPoint {
  timestamp: number;
  value: number;
}

export interface TimeSeries {
  metric: string;
  labels: Record<string, string>;
  points: TimeSeriesPoint[];
}

export interface PromqlResult {
  mode: "promql";
  series: TimeSeries[];
}

export type QueryResult = SqlResult | PromqlResult;

export type QueryEvent =
  | { type: "status"; run: QueryRun; message: string }
  | { type: "data"; result: QueryResult }
  | { type: "complete"; run: QueryRun }
  | { type: "error"; run: QueryRun; message: string };

export interface QueryHistoryItem extends QueryRun {
  resultSummary?: string;
}
