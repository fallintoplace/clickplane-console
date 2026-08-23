import { createClient, type ClickHouseClient, type ResponseJSON } from "@clickhouse/client";
import type {
  MetricDefinition,
  PromqlResult,
  QueryMode,
  QueryResult,
  SchemaTable,
  SqlResult,
  TimeSeries,
} from "@clickplane/shared";
import { metrics, schemas } from "./fixtures.js";

export interface QueryExecutionRequest {
  mode: QueryMode;
  serviceId: string;
  query: string;
  queryId: string;
  signal: AbortSignal;
}

export interface QueryExecution {
  result: QueryResult;
  rowsScanned: number;
  bytesRead: number;
  rowsReturned?: number;
  seriesReturned?: number;
}

export interface QueryExecutor {
  readonly kind: "fixture" | "clickhouse";
  getSchema(serviceId: string): Promise<SchemaTable[]>;
  getMetrics(serviceId: string): MetricDefinition[];
  execute(request: QueryExecutionRequest): Promise<QueryExecution>;
  cancel(queryId: string): Promise<void>;
  close(): Promise<void>;
}

const READ_ONLY_SETTINGS = {
  readonly: "1",
  max_execution_time: 30,
  max_result_rows: "500",
  max_result_bytes: "50000000",
  result_overflow_mode: "break",
} as const;

function toNumber(value: unknown): number {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function toStringValue(value: unknown): string {
  return typeof value === "string" ? value : String(value ?? "");
}

function normalizeCell(value: unknown): string | number | boolean | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") return value;
  const serialized = JSON.stringify(value);
  return serialized === undefined ? String(value) : serialized;
}

function buildRequestResult(): SqlResult {
  return {
    mode: "sql",
    columns: ["service", "route", "requests", "average_duration_ms"],
    rows: [
      { service: "checkout-api", route: "/checkout", requests: 18_420, average_duration_ms: 148.2 },
      { service: "checkout-api", route: "/payment", requests: 11_280, average_duration_ms: 231.7 },
      { service: "catalog-api", route: "/products", requests: 9_840, average_duration_ms: 87.4 },
      { service: "identity-api", route: "/session", requests: 7_540, average_duration_ms: 64.9 },
    ],
  };
}

function buildErrorsResult(): SqlResult {
  return {
    mode: "sql",
    columns: ["service", "error_type", "errors"],
    rows: [
      { service: "checkout-api", error_type: "payment_timeout", errors: 184 },
      { service: "identity-api", error_type: "invalid_session", errors: 96 },
      { service: "catalog-api", error_type: "upstream_timeout", errors: 61 },
    ],
  };
}

function buildRevenueResult(): SqlResult {
  return {
    mode: "sql",
    columns: ["region", "revenue"],
    rows: [
      { region: "DACH", revenue: 184_210.4 },
      { region: "Nordics", revenue: 133_904.8 },
      { region: "Benelux", revenue: 98_311.2 },
    ],
  };
}

function buildSqlResult(query: string): SqlResult {
  const normalized = query.toLowerCase();
  if (normalized.includes("analytics.errors") || /\berror_type\b/.test(normalized)) return buildErrorsResult();
  if (normalized.includes("commerce.orders") || /\brevenue\b/.test(normalized)) return buildRevenueResult();
  return buildRequestResult();
}

function buildPromqlResult(query: string): PromqlResult {
  const now = Math.floor(Date.now() / 300_000) * 300;
  const normalized = query.toLowerCase();
  const isLatency = normalized.includes("histogram_quantile") || normalized.includes("duration_seconds_bucket");
  const isErrors = normalized.includes("http_request_errors_total");
  const metric = isLatency ? "http_request_duration_seconds" : isErrors ? "http_request_errors_total" : "http_requests_total";
  const unit = isLatency ? "seconds" : "per_second";
  const servicesToPlot = [
    {
      name: "checkout-api",
      base: isLatency ? 0.19 : isErrors ? 0.028 : 14,
      slope: isLatency ? 0.004 : isErrors ? 0.0018 : 0.45,
    },
    {
      name: "catalog-api",
      base: isLatency ? 0.11 : isErrors ? 0.012 : 9,
      slope: isLatency ? 0.002 : isErrors ? 0.0009 : 0.22,
    },
    {
      name: "identity-api",
      base: isLatency ? 0.08 : isErrors ? 0.007 : 6,
      slope: isLatency ? -0.001 : isErrors ? -0.0004 : -0.12,
    },
  ];

  const series: TimeSeries[] = servicesToPlot.map(({ name, base, slope }) => ({
    metric,
    labels: { service: name },
    points: Array.from({ length: 18 }, (_, index) => ({
      timestamp: now - (17 - index) * 300,
      value: Math.max(0.001, base + slope * index + Math.sin(index * 1.4) * base * 0.32),
    })),
  }));

  return { mode: "promql", unit, series };
}

export function buildResult(mode: QueryMode, query: string): QueryResult {
  return mode === "sql" ? buildSqlResult(query) : buildPromqlResult(query);
}

export class FixtureQueryExecutor implements QueryExecutor {
  readonly kind = "fixture" as const;

  async getSchema(serviceId: string): Promise<SchemaTable[]> {
    return schemas[serviceId] ?? [];
  }

  getMetrics(serviceId: string): MetricDefinition[] {
    return metrics[serviceId] ?? [];
  }

  async execute(request: QueryExecutionRequest): Promise<QueryExecution> {
    if (/\bfail\b|\bsyntax_error\b/i.test(request.query)) {
      throw new Error("The query engine rejected this expression.");
    }
    const result = buildResult(request.mode, request.query);
    return {
      result,
      rowsScanned: result.mode === "sql" ? 1_840_000 : 0,
      bytesRead: result.mode === "sql" ? 38_200_000 : 0,
      ...(result.mode === "sql" ? { rowsReturned: result.rows.length } : { seriesReturned: result.series.length }),
    };
  }

  async cancel(_queryId: string): Promise<void> {
    // Fixture execution is controlled by the API job lifecycle.
  }

  async close(): Promise<void> {
    // No external connection to close.
  }
}

interface ClickHouseExecutorOptions {
  url: string;
  username: string;
  password: string;
  database: string;
  enableServerCancellation: boolean;
}

interface SchemaTableRow {
  database?: unknown;
  name?: unknown;
  engine?: unknown;
}

interface SchemaColumnRow {
  database?: unknown;
  table?: unknown;
  name?: unknown;
  type?: unknown;
  comment?: unknown;
}

export class ClickHouseQueryExecutor implements QueryExecutor {
  readonly kind = "clickhouse" as const;
  private readonly client: ClickHouseClient;
  private readonly controlClient: ClickHouseClient;
  private readonly enableServerCancellation: boolean;
  private readonly activeRequests = new Map<string, AbortController>();

  constructor(options: ClickHouseExecutorOptions) {
    this.client = createClient({
      url: options.url,
      username: options.username,
      password: options.password,
      database: options.database,
      application: "clickplane-console",
      request_timeout: 35_000,
      clickhouse_settings: READ_ONLY_SETTINGS,
    });
    this.controlClient = createClient({
      url: options.url,
      username: options.username,
      password: options.password,
      database: options.database,
      application: "clickplane-console-control",
      request_timeout: 5_000,
    });
    this.enableServerCancellation = options.enableServerCancellation;
  }

  async getSchema(_serviceId: string): Promise<SchemaTable[]> {
    const [tables, columns] = await Promise.all([
      this.queryJson<SchemaTableRow>(`
        SELECT database, name, engine
        FROM system.tables
        WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
        ORDER BY database, name
      `),
      this.queryJson<SchemaColumnRow>(`
        SELECT database, table, name, type, comment
        FROM system.columns
        WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
        ORDER BY database, table, position
      `),
    ]);

    const columnsByTable = new Map<string, SchemaColumnRow[]>();
    for (const column of columns.data) {
      const key = `${toStringValue(column.database)}.${toStringValue(column.table)}`;
      const current = columnsByTable.get(key) ?? [];
      current.push(column);
      columnsByTable.set(key, current);
    }

    return tables.data.map((table) => {
      const database = toStringValue(table.database);
      const name = toStringValue(table.name);
      const key = `${database}.${name}`;
      return {
        database,
        name,
        engine: toStringValue(table.engine),
        columns: (columnsByTable.get(key) ?? []).map((column) => ({
          name: toStringValue(column.name),
          type: toStringValue(column.type),
          description: toStringValue(column.comment),
        })),
      };
    });
  }

  getMetrics(serviceId: string): MetricDefinition[] {
    return metrics[serviceId] ?? [];
  }

  async execute(request: QueryExecutionRequest): Promise<QueryExecution> {
    if (request.mode === "promql") {
      const result = buildResult(request.mode, request.query);
      return { result, rowsScanned: 0, bytesRead: 0, seriesReturned: result.mode === "promql" ? result.series.length : undefined };
    }

    const controller = new AbortController();
    const abort = (): void => controller.abort();
    request.signal.addEventListener("abort", abort, { once: true });
    this.activeRequests.set(request.queryId, controller);

    try {
      const response = await this.client.query({
        query: request.query,
        format: "JSON",
        query_id: request.queryId,
        abort_signal: controller.signal,
      });
      const payload = await response.json<Record<string, unknown>>();
      const columns = payload.meta?.map((column) => column.name) ?? Object.keys(payload.data[0] ?? {});
      const rows = payload.data.map((row) => Object.fromEntries(columns.map((column) => [column, normalizeCell(row[column])]))) as SqlResult["rows"];
      return {
        result: { mode: "sql", columns, rows },
        rowsScanned: toNumber(payload.statistics?.rows_read),
        bytesRead: toNumber(payload.statistics?.bytes_read),
        rowsReturned: rows.length,
      };
    } finally {
      request.signal.removeEventListener("abort", abort);
      this.activeRequests.delete(request.queryId);
    }
  }

  async cancel(queryId: string): Promise<void> {
    this.activeRequests.get(queryId)?.abort();
    if (!this.enableServerCancellation) return;

    try {
      await this.controlClient.command({
        query: `KILL QUERY WHERE query_id = '${queryId}' SYNC`,
      });
    } catch {
      // Aborting the HTTP request is still useful when the configured user cannot kill queries.
    }
  }

  async close(): Promise<void> {
    for (const controller of this.activeRequests.values()) controller.abort();
    await Promise.all([this.client.close(), this.controlClient.close()]);
  }

  private async queryJson<T>(query: string): Promise<ResponseJSON<T>> {
    const response = await this.client.query({ query, format: "JSON" });
    return response.json<T>();
  }
}

export function createQueryExecutor(env: NodeJS.ProcessEnv = process.env): QueryExecutor {
  const requestedMode = env.CLICKPLANE_EXECUTOR ?? "fixture";
  if (requestedMode === "fixture") return new FixtureQueryExecutor();
  if (requestedMode !== "clickhouse") {
    throw new Error(`Unsupported CLICKPLANE_EXECUTOR value: ${requestedMode}`);
  }

  const url = env.CLICKHOUSE_URL;
  if (!url) throw new Error("CLICKHOUSE_URL is required when CLICKPLANE_EXECUTOR=clickhouse");

  return new ClickHouseQueryExecutor({
    url,
    username: env.CLICKHOUSE_USERNAME ?? "default",
    password: env.CLICKHOUSE_PASSWORD ?? "",
    database: env.CLICKHOUSE_DATABASE ?? "default",
    enableServerCancellation: env.CLICKHOUSE_ENABLE_KILL_QUERY !== "false",
  });
}
