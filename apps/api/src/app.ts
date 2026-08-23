import Fastify, { type FastifyInstance, type FastifyReply } from "fastify";
import cors from "@fastify/cors";
import { randomUUID } from "node:crypto";
import { z } from "zod";
import type {
  GenerationRequest,
  MetricDefinition,
  QueryDraft,
  QueryEvent,
  QueryHistoryItem,
  QueryMode,
  QueryProgress,
  QueryRequest,
  QueryRun,
  QueryRunState,
  SchemaTable,
  Service,
} from "@clickplane/shared";
import { metrics, schemas, services } from "./fixtures.js";
import { createQueryExecutor, type QueryExecutor } from "./query-executor.js";

export { buildResult } from "./query-executor.js";

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
  abortController?: AbortController;
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

function getSelectedTables(serviceId: string, requestedTables?: string[], availableTables = getSchema(serviceId)): SchemaTable[] {
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

function buildSqlDraft(prompt: string, serviceId: string, requestedTables?: string[], availableTables = getSchema(serviceId)): QueryDraft {
  const normalized = prompt.toLowerCase();
  const tables = getSelectedTables(serviceId, requestedTables, availableTables);
  const errorsTable = tables.find((table) => table.name === "errors");
  const ordersTable = tables.find((table) => table.name === "orders");
  const requestTable = tables.find((table) => table.name === "http_requests") ?? tables[0];
  const errorsReference = errorsTable ? `${errorsTable.database}.${errorsTable.name}` : "analytics.errors";
  const ordersReference = ordersTable ? `${ordersTable.database}.${ordersTable.name}` : "commerce.orders";
  const requestReference = requestTable ? `${requestTable.database}.${requestTable.name}` : "analytics.http_requests";

  if (/revenue|order|customer/.test(normalized) && ordersTable) {
    return {
      mode: "sql",
      query: `SELECT\n  region,\n  sum(revenue) AS revenue\nFROM ${ordersReference}\nWHERE created_at >= now() - INTERVAL 1 DAY\nGROUP BY region\nORDER BY revenue DESC\nLIMIT 10`,
      explanation: "Aggregates yesterday's order revenue by region and returns the ten highest-revenue regions.",
      assumptions: [`Revenue is stored in ${ordersReference}.revenue.`, "Yesterday means the last 24 hours."],
      referencedContext: [ordersReference, "created_at", "region", "revenue"],
      warnings: ["Review the time window before running this query."],
    };
  }

  if (/error|failure|failed|exception/.test(normalized) && errorsTable) {
    return {
      mode: "sql",
      query: `SELECT\n  service,\n  error_type,\n  count() AS errors\nFROM ${errorsReference}\nWHERE timestamp >= now() - INTERVAL 1 HOUR\nGROUP BY service, error_type\nORDER BY errors DESC\nLIMIT 20`,
      explanation: "Counts recent errors by service and error type so the largest failure sources are visible first.",
      assumptions: ["The last hour is the intended analysis window.", `${errorsReference} contains one row per observed error.`],
      referencedContext: [errorsReference, "timestamp", "service", "error_type"],
      warnings: ["The result is capped at 20 grouped rows."],
    };
  }

  return {
    mode: "sql",
    query: `SELECT\n  service,\n  route,\n  count() AS requests,\n  avg(duration_ms) AS average_duration_ms\nFROM ${requestReference}\nWHERE timestamp >= now() - INTERVAL 1 HOUR\nGROUP BY service, route\nORDER BY requests DESC\nLIMIT 100`,
    explanation: "Summarizes recent request volume and average duration by service and route.",
    assumptions: ["The last hour is the intended time window.", `${requestReference} is the request event table.`],
    referencedContext: [requestReference, "timestamp", "service", "route", "duration_ms"],
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

function generateDraft(request: GenerationRequest, availableTables = getSchema(request.serviceId)): QueryDraft {
  return request.mode === "sql"
    ? buildSqlDraft(request.prompt, request.serviceId, request.context?.tables, availableTables)
    : buildPromqlDraft(request.prompt, request.serviceId, request.context?.metrics);
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

function progressForStage(stage: QueryProgress["stage"], startedAt: string, details: Partial<QueryProgress> = {}): QueryProgress {
  return {
    stage,
    elapsedMs: Math.max(0, Date.now() - Date.parse(startedAt)),
    rowsScanned: details.rowsScanned ?? 0,
    bytesRead: details.bytesRead ?? 0,
    ...(details.rowsReturned === undefined ? {} : { rowsReturned: details.rowsReturned }),
    ...(details.seriesReturned === undefined ? {} : { seriesReturned: details.seriesReturned }),
  };
}

function transitionJob(
  job: QueryJob,
  state: QueryRunState,
  message: string,
  stage: QueryProgress["stage"],
  details: Partial<QueryProgress> = {},
): void {
  const startedAt = state === "running" && job.run.state === "queued" ? new Date().toISOString() : job.run.startedAt;
  job.run = {
    ...job.run,
    state,
    startedAt,
    progress: progressForStage(stage, startedAt, { ...job.run.progress, ...details }),
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

function finishJob(
  job: QueryJob,
  state: "success" | "error" | "cancelled",
  message: string,
  resultSummary: string,
  error?: string,
  resultDetails: Partial<QueryProgress> = {},
): void {
  if (isTerminal(job.run.state)) return;

  for (const timer of job.timers) clearTimeout(timer);
  job.timers = [];

  const finishedAt = new Date().toISOString();
  job.run = {
    ...job.run,
    state,
    finishedAt,
    durationMs: Math.max(0, Date.parse(finishedAt) - Date.parse(job.run.startedAt)),
    progress: progressForStage("complete", job.run.startedAt, {
      ...job.run.progress,
      ...resultDetails,
      elapsedMs: Math.max(0, Date.parse(finishedAt) - Date.parse(job.run.startedAt)),
    }),
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

function wait(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function executeJob(job: QueryJob, executor: QueryExecutor): Promise<void> {
  if (isTerminal(job.run.state)) return;
  const abortController = new AbortController();
  job.abortController = abortController;
  const executionPromise = executor.execute({
    mode: job.run.mode,
    serviceId: job.run.serviceId,
    query: job.run.query,
    queryId: job.run.id,
    signal: abortController.signal,
  });
  void executionPromise.catch(() => undefined);

  try {
    await wait(400);
    if (job.run.state !== "running") {
      await executionPromise.catch(() => undefined);
      return;
    }
    transitionJob(job, "running", "Streaming result rows", "streaming");

    const execution = await executionPromise;
    if (isTerminal(job.run.state)) return;
    emit(job, { type: "data", result: execution.result });
    finishJob(
      job,
      "success",
      "Query completed",
      job.run.mode === "sql" ? "Rows returned" : "Series returned",
      undefined,
      {
        rowsScanned: execution.rowsScanned,
        bytesRead: execution.bytesRead,
        ...(execution.rowsReturned === undefined ? {} : { rowsReturned: execution.rowsReturned }),
        ...(execution.seriesReturned === undefined ? {} : { seriesReturned: execution.seriesReturned }),
      },
    );
  } catch (error: unknown) {
    if (isTerminal(job.run.state) || abortController.signal.aborted) return;
    const errorMessage = error instanceof Error ? error.message : "The query engine rejected this expression.";
    finishJob(job, "error", "Query failed", "Query failed", errorMessage);
  } finally {
    if (job.abortController === abortController) job.abortController = undefined;
  }
}

function startJob(job: QueryJob, executor: QueryExecutor): void {
  emit(job, { type: "status", run: job.run, message: "Queued for execution" });

  job.timers.push(
    setTimeout(() => {
      if (job.run.state !== "queued") return;
      transitionJob(job, "running", "Planning query", "planning");
    }, 160),
  );

  job.timers.push(
    setTimeout(() => {
      if (job.run.state !== "running") return;
      transitionJob(job, "running", "Executing on the selected service", "executing");
      void executeJob(job, executor);
    }, 360),
  );
}

function cancelJob(job: QueryJob, executor: QueryExecutor): void {
  job.abortController?.abort();
  void executor.cancel(job.run.id);
  finishJob(job, "cancelled", "Query cancelled", "Cancelled by user");
}

function sendSse(reply: FastifyReply, event: QueryEvent): void {
  reply.raw.write(`event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`);
}

export interface BuildAppOptions {
  executor?: QueryExecutor;
}

export function buildApp(options: BuildAppOptions = {}): FastifyInstance {
  const app = Fastify({ logger: false });
  const executor = options.executor ?? createQueryExecutor();

  app.register(cors, { origin: true });
  app.addHook("onClose", async () => {
    for (const job of jobs.values()) {
      for (const timer of job.timers) clearTimeout(timer);
      job.abortController?.abort();
      if (job.evictionTimer) clearTimeout(job.evictionTimer);
      job.subscribers.clear();
    }
    jobs.clear();
    history.splice(0);
    await executor.close();
  });

  app.get("/api/health", async () => ({ status: "ok", service: "clickplane-api", executor: executor.kind }));

  app.get("/api/services", async () => ({ services }));

  app.get<{ Params: { serviceId: string } }>("/api/services/:serviceId/schema", async (request, reply) => {
    if (!findService(request.params.serviceId)) {
      return reply.code(404).send({ error: "Service not found" });
    }
    try {
      return { tables: await executor.getSchema(request.params.serviceId) };
    } catch (error: unknown) {
      return reply.code(502).send({ error: error instanceof Error ? error.message : "Could not load ClickHouse schema" });
    }
  });

  app.get<{ Params: { serviceId: string } }>("/api/services/:serviceId/metrics", async (request, reply) => {
    if (!findService(request.params.serviceId)) {
      return reply.code(404).send({ error: "Service not found" });
    }
    return { metrics: executor.getMetrics(request.params.serviceId) };
  });

  app.post("/api/query-drafts", async (request, reply) => {
    const parsed = generationRequestSchema.safeParse(request.body);
    if (!parsed.success) {
      return reply.code(400).send({ error: "Invalid generation request", details: parsed.error.flatten() });
    }
    if (!findService(parsed.data.serviceId)) {
      return reply.code(404).send({ error: "Service not found" });
    }
    try {
      const availableTables = parsed.data.mode === "sql" ? await executor.getSchema(parsed.data.serviceId) : undefined;
      return { draft: generateDraft(parsed.data, availableTables ?? getSchema(parsed.data.serviceId)) };
    } catch (error: unknown) {
      return reply.code(502).send({ error: error instanceof Error ? error.message : "Could not load query context" });
    }
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
      progress: { stage: "queued", elapsedMs: 0, rowsScanned: 0, bytesRead: 0 },
    };
    const job: QueryJob = { run, events: [], subscribers: new Set(), timers: [] };
    jobs.set(run.id, job);
    startJob(job, executor);
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
    cancelJob(job, executor);
    return { run: job.run };
  });

  app.get("/api/query-history", async () => ({ history }));

  return app;
}
