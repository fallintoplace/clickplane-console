import Fastify, { type FastifyInstance, type FastifyReply } from "fastify";
import cors from "@fastify/cors";
import { randomUUID } from "node:crypto";
import { z } from "zod";
import type {
  GenerationRequest,
  MetricDefinition,
  PromqlResult,
  QueryDraft,
  QueryEvent,
  QueryHistoryItem,
  QueryMode,
  QueryRequest,
  QueryResult,
  QueryRun,
  QueryRunState,
  SchemaTable,
  Service,
  SqlResult,
  TimeSeries,
} from "@clickplane/shared";
import { metrics, schemas, services } from "./fixtures.js";

const generationRequestSchema = z.object({
  mode: z.enum(["sql", "promql"]),
  serviceId: z.string().min(1),
  prompt: z.string().trim().min(3).max(500),
  context: z
    .object({
      tables: z.array(z.string()).optional(),
      metrics: z.array(z.string()).optional(),
      savedQuery: z.string().optional(),
    })
    .optional(),
});

const queryRequestSchema = z.object({
  mode: z.enum(["sql", "promql"]),
  serviceId: z.string().min(1),
  query: z.string().trim().min(1).max(10_000),
});

interface QueryJob {
  run: QueryRun;
  events: QueryEvent[];
  subscribers: Set<(event: QueryEvent) => void>;
  timers: NodeJS.Timeout[];
  evictionTimer?: NodeJS.Timeout;
}

const JOB_TTL_MS = 5 * 60_000;
const jobs = new Map<string, QueryJob>();
const history: QueryHistoryItem[] = [];

function findService(serviceId: string): Service | undefined {
  return services.find((service) => service.id === serviceId);
}

function getSchema(serviceId: string): SchemaTable[] {
  return schemas[serviceId] ?? [];
}

function getMetrics(serviceId: string): MetricDefinition[] {
  return metrics[serviceId] ?? [];
}

function getSelectedTables(serviceId: string, requestedTables?: string[]): SchemaTable[] {
  const availableTables = getSchema(serviceId);
  if (!requestedTables?.length) return availableTables;

  const requested = new Set(requestedTables);
  return availableTables.filter((table) => requested.has(`${table.database}.${table.name}`) || requested.has(table.name));
}

function getSelectedMetrics(serviceId: string, requestedMetrics?: string[]): MetricDefinition[] {
  const availableMetrics = getMetrics(serviceId);
  if (!requestedMetrics?.length) return availableMetrics;

  const requested = new Set(requestedMetrics);
  return availableMetrics.filter((metric) => requested.has(metric.name));
}

function addHistory(run: QueryRun, resultSummary?: string): void {
  const item: QueryHistoryItem = { ...run, resultSummary };
  const existingIndex = history.findIndex((entry) => entry.id === run.id);
  if (existingIndex >= 0) {
    history.splice(existingIndex, 1);
  }
  history.unshift(item);
  history.splice(20);
}

function validateQuery(mode: QueryMode, query: string): string[] {
  const errors: string[] = [];
  const normalized = query.trim().toLowerCase();

  if (mode === "sql") {
    if (!/^(select|with|explain)\b/.test(normalized)) {
      errors.push("Only SELECT, WITH, and EXPLAIN queries are allowed.");
    }
    if (/\b(insert|update|delete|drop|alter|truncate|create|optimize|system)\b/.test(normalized)) {
      errors.push("Write and administrative SQL statements are blocked in the console.");
    }
    if (query.includes(";")) {
      errors.push("Submit one SQL statement at a time.");
    }
  }

  if (mode === "promql") {
    if (query.includes(";")) {
      errors.push("Submit one PromQL expression at a time.");
    }
    if (query.length > 2_000) {
      errors.push("PromQL expressions must be shorter than 2,000 characters.");
    }
  }

  return errors;
}

function buildSqlDraft(prompt: string, serviceId: string, requestedTables?: string[]): QueryDraft {
  const normalized = prompt.toLowerCase();
  const tables = getSelectedTables(serviceId, requestedTables);
  const hasErrorsTable = tables.some((table) => table.name === "errors");
  const hasOrdersTable = tables.some((table) => table.name === "orders");

  if (/revenue|order|customer/.test(normalized) && hasOrdersTable) {
    return {
      mode: "sql",
      query: `SELECT\n  region,\n  sum(revenue) AS revenue\nFROM commerce.orders\nWHERE created_at >= now() - INTERVAL 1 DAY\nGROUP BY region\nORDER BY revenue DESC\nLIMIT 10`,
      explanation: "Aggregates yesterday's order revenue by region and returns the ten highest-revenue regions.",
      assumptions: ["Revenue is stored in commerce.orders.revenue.", "Yesterday means the last 24 hours."],
      referencedContext: ["commerce.orders", "created_at", "region", "revenue"],
      warnings: ["Review the time window before running this query."],
    };
  }

  if (/error|failure|failed|exception/.test(normalized) && hasErrorsTable) {
    return {
      mode: "sql",
      query: `SELECT\n  service,\n  error_type,\n  count() AS errors\nFROM analytics.errors\nWHERE timestamp >= now() - INTERVAL 1 HOUR\nGROUP BY service, error_type\nORDER BY errors DESC\nLIMIT 20`,
      explanation: "Counts recent errors by service and error type so the largest failure sources are visible first.",
      assumptions: ["The last hour is the intended analysis window.", "analytics.errors contains one row per observed error."],
      referencedContext: ["analytics.errors", "timestamp", "service", "error_type"],
      warnings: ["The result is capped at 20 grouped rows."],
    };
  }

  return {
    mode: "sql",
    query: `SELECT\n  service,\n  route,\n  count() AS requests,\n  avg(duration_ms) AS average_duration_ms\nFROM analytics.http_requests\nWHERE timestamp >= now() - INTERVAL 1 HOUR\nGROUP BY service, route\nORDER BY requests DESC\nLIMIT 100`,
    explanation: "Summarizes recent request volume and average duration by service and route.",
    assumptions: ["The last hour is the intended time window.", "analytics.http_requests is the request event table."],
    referencedContext: ["analytics.http_requests", "timestamp", "service", "route", "duration_ms"],
    warnings: ["The result is capped at 100 grouped rows."],
  };
}

function buildPromqlDraft(prompt: string, serviceId: string, requestedMetrics?: string[]): QueryDraft {
  const normalized = prompt.toLowerCase();
  const availableMetrics = getSelectedMetrics(serviceId, requestedMetrics);
  const metricNames = new Set(availableMetrics.map((metric) => metric.name));

  if (/latency|slow|p95|duration/.test(normalized) && metricNames.has("http_request_duration_seconds")) {
    return {
      mode: "promql",
      query: "histogram_quantile(0.95, sum by (le, service) (rate(http_request_duration_seconds_bucket[5m])))",
      explanation: "Calculates the 95th percentile request latency for each service over rolling five-minute windows.",
      assumptions: ["Latency is represented by the HTTP duration histogram.", "The last five minutes is an appropriate rate window."],
      referencedContext: ["http_request_duration_seconds", "service", "le"],
      warnings: ["High-cardinality label combinations can increase query cost."],
    };
  }

  if (/error|failure|failed|exception/.test(normalized) && metricNames.has("http_request_errors_total")) {
    return {
      mode: "promql",
      query: "sum by (service) (rate(http_request_errors_total[5m]))",
      explanation: "Shows the per-service error rate over rolling five-minute windows.",
      assumptions: ["http_request_errors_total is a monotonically increasing counter.", "The last five minutes is an appropriate rate window."],
      referencedContext: ["http_request_errors_total", "service"],
      warnings: ["The graph uses the selected time range when executed."],
    };
  }

  return {
    mode: "promql",
    query: "sum by (service) (rate(http_requests_total[5m]))",
    explanation: "Shows request throughput by service over rolling five-minute windows.",
    assumptions: ["http_requests_total is the request counter.", "The last five minutes is an appropriate rate window."],
    referencedContext: ["http_requests_total", "service"],
    warnings: ["The graph uses a capped number of returned series."],
  };
}

function generateDraft(request: GenerationRequest): QueryDraft {
  return request.mode === "sql"
    ? buildSqlDraft(request.prompt, request.serviceId, request.context?.tables)
    : buildPromqlDraft(request.prompt, request.serviceId, request.context?.metrics);
}

function buildRequestResult(): SqlResult {
  return {
    mode: "sql",
    columns: ["service", "route", "requests", "average_duration_ms"],
    rows: [
      { service: "checkout-api", route: "/checkout", requests: 18420, average_duration_ms: 148.2 },
      { service: "checkout-api", route: "/payment", requests: 11280, average_duration_ms: 231.7 },
      { service: "catalog-api", route: "/products", requests: 9840, average_duration_ms: 87.4 },
      { service: "identity-api", route: "/session", requests: 7540, average_duration_ms: 64.9 },
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

function emit(job: QueryJob, event: QueryEvent): void {
  job.events.push(event);
  for (const subscriber of job.subscribers) {
    try {
      subscriber(event);
    } catch {
      job.subscribers.delete(subscriber);
    }
  }
}

function isTerminal(state: QueryRunState): boolean {
  return state === "success" || state === "error" || state === "cancelled";
}

function transitionJob(job: QueryJob, state: QueryRunState, message: string): void {
  job.run = {
    ...job.run,
    state,
    ...(state === "running" ? { startedAt: new Date().toISOString() } : {}),
  };
  emit(job, { type: "status", run: job.run, message });
}

function scheduleEviction(job: QueryJob): void {
  const timer = setTimeout(() => {
    if (jobs.get(job.run.id) === job) jobs.delete(job.run.id);
  }, JOB_TTL_MS);
  timer.unref();
  job.evictionTimer = timer;
}

function finishJob(job: QueryJob, state: "success" | "error" | "cancelled", message: string, resultSummary: string, error?: string): void {
  if (isTerminal(job.run.state)) return;

  for (const timer of job.timers) clearTimeout(timer);
  job.timers = [];

  const finishedAt = new Date().toISOString();
  job.run = {
    ...job.run,
    state,
    finishedAt,
    durationMs: Math.max(0, Date.parse(finishedAt) - Date.parse(job.run.startedAt)),
    ...(error ? { error } : {}),
  };
  emit(job, { type: "status", run: job.run, message });
  if (state === "error" && error) {
    emit(job, { type: "execution-error", run: job.run, message: error });
  }
  emit(job, { type: "complete", run: job.run });
  addHistory(job.run, resultSummary);
  scheduleEviction(job);
}

function startJob(job: QueryJob): void {
  emit(job, { type: "status", run: job.run, message: "Queued for execution" });

  job.timers.push(
    setTimeout(() => {
      if (job.run.state !== "queued") return;
      transitionJob(job, "running", "Query is running");
    }, 160),
  );

  job.timers.push(
    setTimeout(() => {
      if (job.run.state !== "running") return;

      if (/\bfail\b|\bsyntax_error\b/i.test(job.run.query)) {
        const errorMessage = "The query engine rejected this expression.";
        finishJob(job, "error", "Query failed", "Query failed", errorMessage);
        return;
      }

      const result = buildResult(job.run.mode, job.run.query);
      emit(job, { type: "data", result });
      finishJob(job, "success", "Query completed", job.run.mode === "sql" ? "Rows returned" : "Series returned");
    },
  1_050),
  );
}

function cancelJob(job: QueryJob): void {
  finishJob(job, "cancelled", "Query cancelled", "Cancelled by user");
}

function sendSse(reply: FastifyReply, event: QueryEvent): void {
  reply.raw.write(`event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`);
}

export function buildApp(): FastifyInstance {
  const app = Fastify({ logger: false });

  app.register(cors, { origin: true });
  app.addHook("onClose", async () => {
    for (const job of jobs.values()) {
      for (const timer of job.timers) clearTimeout(timer);
      if (job.evictionTimer) clearTimeout(job.evictionTimer);
      job.subscribers.clear();
    }
    jobs.clear();
    history.splice(0);
  });

  app.get("/api/health", async () => ({ status: "ok", service: "clickplane-api" }));

  app.get("/api/services", async () => ({ services }));

  app.get<{ Params: { serviceId: string } }>("/api/services/:serviceId/schema", async (request, reply) => {
    if (!findService(request.params.serviceId)) {
      return reply.code(404).send({ error: "Service not found" });
    }
    return { tables: getSchema(request.params.serviceId) };
  });

  app.get<{ Params: { serviceId: string } }>("/api/services/:serviceId/metrics", async (request, reply) => {
    if (!findService(request.params.serviceId)) {
      return reply.code(404).send({ error: "Service not found" });
    }
    return { metrics: getMetrics(request.params.serviceId) };
  });

  app.post("/api/query-drafts", async (request, reply) => {
    const parsed = generationRequestSchema.safeParse(request.body);
    if (!parsed.success) {
      return reply.code(400).send({ error: "Invalid generation request", details: parsed.error.flatten() });
    }
    if (!findService(parsed.data.serviceId)) {
      return reply.code(404).send({ error: "Service not found" });
    }
    return { draft: generateDraft(parsed.data) };
  });

  app.post("/api/queries", async (request, reply) => {
    const parsed = queryRequestSchema.safeParse(request.body);
    if (!parsed.success) {
      return reply.code(400).send({ error: "Invalid query request", details: parsed.error.flatten() });
    }
    if (!findService(parsed.data.serviceId)) {
      return reply.code(404).send({ error: "Service not found" });
    }
    const validationErrors = validateQuery(parsed.data.mode, parsed.data.query);
    if (validationErrors.length > 0) {
      return reply.code(422).send({ error: "Query validation failed", details: validationErrors });
    }

    const run: QueryRun = {
      id: randomUUID(),
      mode: parsed.data.mode,
      serviceId: parsed.data.serviceId,
      query: parsed.data.query,
      state: "queued",
      startedAt: new Date().toISOString(),
    };
    const job: QueryJob = { run, events: [], subscribers: new Set(), timers: [] };
    jobs.set(run.id, job);
    startJob(job);
    return reply.code(202).send({ run });
  });

  app.get<{ Params: { queryId: string } }>("/api/queries/:queryId/events", async (request, reply) => {
    const job = jobs.get(request.params.queryId);
    if (!job) {
      return reply.code(404).send({ error: "Query not found" });
    }

    reply.hijack();
    reply.raw.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    });

    const subscriber = (event: QueryEvent) => sendSse(reply, event);
    for (const event of job.events) sendSse(reply, event);

    if (["success", "error", "cancelled"].includes(job.run.state)) {
      reply.raw.end();
      return;
    }

    job.subscribers.add(subscriber);
    request.raw.on("close", () => {
      job.subscribers.delete(subscriber);
    });
  });

  app.post<{ Params: { queryId: string } }>("/api/queries/:queryId/cancel", async (request, reply) => {
    const job = jobs.get(request.params.queryId);
    if (!job) {
      return reply.code(404).send({ error: "Query not found" });
    }
    cancelJob(job);
    return { run: job.run };
  });

  app.get("/api/query-history", async () => ({ history }));

  return app;
}
