import { useEffect, useMemo, useRef, useState } from "react";
import type {
  MetricDefinition,
  PromqlResult,
  QueryDraft,
  QueryEvent,
  QueryHistoryItem,
  QueryMode,
  QueryResult,
  QueryRun,
  SchemaTable,
  Service,
  SqlResult,
  TimeSeries,
} from "@clickplane/shared";

const SQL_START = `SELECT
  service,
  route,
  count() AS requests,
  avg(duration_ms) AS average_duration_ms
FROM analytics.http_requests
WHERE timestamp >= now() - INTERVAL 1 HOUR
GROUP BY service, route
ORDER BY requests DESC
LIMIT 100`;

const PROMQL_START = "sum by (service) (rate(http_requests_total[5m]))";

type Notice = { tone: "info" | "success" | "error"; text: string };

interface ActiveExecution {
  id: string;
  mode: QueryMode;
  serviceId: string;
  stream: EventSource;
}

async function readJson<T>(response: Response): Promise<T> {
  const body = (await response.json()) as T & { error?: string; details?: string[] };
  if (!response.ok) {
    throw new Error(body.error ?? "The request failed");
  }
  return body;
}

function App() {
  const [services, setServices] = useState<Service[]>([]);
  const [selectedServiceId, setSelectedServiceId] = useState("");
  const [queryServiceId, setQueryServiceId] = useState("");
  const [schema, setSchema] = useState<SchemaTable[]>([]);
  const [metrics, setMetrics] = useState<MetricDefinition[]>([]);
  const [history, setHistory] = useState<QueryHistoryItem[]>([]);
  const [mode, setMode] = useState<QueryMode>("sql");
  const [prompt, setPrompt] = useState("Show me the biggest errors from the last hour");
  const [query, setQuery] = useState(SQL_START);
  const [draft, setDraft] = useState<QueryDraft | null>(null);
  const [run, setRun] = useState<QueryRun | null>(null);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [isGenerating, setIsGenerating] = useState(false);
  const [notice, setNotice] = useState<Notice>({ tone: "info", text: "Ready for a query." });
  const streamRef = useRef<EventSource | null>(null);
  const activeExecutionRef = useRef<ActiveExecution | null>(null);
  const pendingHistoryQueryRef = useRef<string | null>(null);
  const pendingHistoryServiceRef = useRef<string | null>(null);
  const serviceContextRequestRef = useRef(0);

  const selectedService = services.find((service) => service.id === selectedServiceId);
  const isQueryRunning = run?.state === "queued" || run?.state === "running";
  const isQueryStale = Boolean(queryServiceId && selectedServiceId && queryServiceId !== selectedServiceId);

  useEffect(() => {
    void Promise.all([
      fetch("/api/services").then((response) => readJson<{ services: Service[] }>(response)),
      fetch("/api/query-history").then((response) => readJson<{ history: QueryHistoryItem[] }>(response)),
    ])
      .then(([serviceResponse, historyResponse]) => {
        setServices(serviceResponse.services);
        const firstServiceId = serviceResponse.services[0]?.id ?? "";
        setSelectedServiceId(firstServiceId);
        setQueryServiceId(firstServiceId);
        setHistory(historyResponse.history);
      })
      .catch((error: unknown) => {
        setNotice({ tone: "error", text: error instanceof Error ? error.message : "Could not load the console." });
      });

    return () => {
      streamRef.current?.close();
      streamRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (!selectedServiceId) return;
    const controller = new AbortController();
    const requestId = ++serviceContextRequestRef.current;
    const requestedServiceId = selectedServiceId;

    void Promise.all([
      fetch(`/api/services/${requestedServiceId}/schema`, { signal: controller.signal }).then((response) => readJson<{ tables: SchemaTable[] }>(response)),
      fetch(`/api/services/${requestedServiceId}/metrics`, { signal: controller.signal }).then((response) => readJson<{ metrics: MetricDefinition[] }>(response)),
    ])
      .then(([schemaResponse, metricsResponse]) => {
        if (controller.signal.aborted || requestId !== serviceContextRequestRef.current || requestedServiceId !== selectedServiceId) return;
        setSchema(schemaResponse.tables);
        setMetrics(metricsResponse.metrics);
      })
      .catch((error: unknown) => {
        if (controller.signal.aborted || requestId !== serviceContextRequestRef.current) return;
        setNotice({ tone: "error", text: error instanceof Error ? error.message : "Could not load service context." });
      });

    return () => controller.abort();
  }, [selectedServiceId]);

  useEffect(() => {
    const historyQuery = pendingHistoryQueryRef.current;
    const historyServiceId = pendingHistoryServiceRef.current;
    pendingHistoryQueryRef.current = null;
    pendingHistoryServiceRef.current = null;
    setQuery(historyQuery ?? (mode === "sql" ? SQL_START : PROMQL_START));
    setQueryServiceId(historyServiceId ?? selectedServiceId);
    setDraft(null);
    setResult(null);
    setRun(null);
    setPrompt(mode === "sql" ? "Show me the biggest errors from the last hour" : "Show request errors by service");
    void invalidateActiveExecution();
  }, [mode]);

  const contextCount = mode === "sql" ? schema.length : metrics.length;
  const currentContext = useMemo(() => {
    if (mode === "sql") return schema.map((table) => `${table.database}.${table.name}`);
    return metrics.map((metric) => metric.name);
  }, [metrics, mode, schema]);

  function ownsExecution(queryId: string): boolean {
    return activeExecutionRef.current?.id === queryId;
  }

  function releaseExecution(queryId: string): void {
    const active = activeExecutionRef.current;
    if (!active || active.id !== queryId) return;
    active.stream.close();
    activeExecutionRef.current = null;
    if (streamRef.current === active.stream) streamRef.current = null;
  }

  async function invalidateActiveExecution(): Promise<void> {
    const active = activeExecutionRef.current;
    activeExecutionRef.current = null;
    streamRef.current?.close();
    streamRef.current = null;
    if (!active) return;

    try {
      await fetch(`/api/queries/${active.id}/cancel`, { method: "POST" });
    } catch {
      // The UI has already detached from this execution. Cancellation is best effort.
    }
  }

  function selectService(serviceId: string): void {
    if (serviceId === selectedServiceId) return;
    void invalidateActiveExecution();
    setSelectedServiceId(serviceId);
    setRun(null);
    setResult(null);
    setNotice({ tone: "info", text: "Service changed. Review the query target before running." });
  }

  async function generateQuery(): Promise<void> {
    if (!selectedServiceId || !prompt.trim()) return;
    const requestServiceId = selectedServiceId;
    const requestMode = mode;
    await invalidateActiveExecution();
    setIsGenerating(true);
    setNotice({ tone: "info", text: "Resolving service context and drafting a query..." });
    try {
      const response = await fetch("/api/query-drafts", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          mode: requestMode,
          serviceId: requestServiceId,
          prompt,
          context: {
            ...(requestMode === "sql" ? { tables: currentContext } : { metrics: currentContext }),
          },
        }),
      });
      const body = await readJson<{ draft: QueryDraft }>(response);
      if (selectedServiceId !== requestServiceId || mode !== requestMode) return;
      setDraft(body.draft);
      setQuery(body.draft.query);
      setQueryServiceId(requestServiceId);
      setResult(null);
      setRun(null);
      setNotice({ tone: "success", text: "Draft ready. Review it before running." });
    } catch (error: unknown) {
      setNotice({ tone: "error", text: error instanceof Error ? error.message : "Could not generate a query." });
    } finally {
      setIsGenerating(false);
    }
  }

  async function loadHistory(): Promise<void> {
    const response = await fetch("/api/query-history");
    const body = await readJson<{ history: QueryHistoryItem[] }>(response);
    setHistory(body.history);
  }

  function subscribeToQuery(query: QueryRun): void {
    streamRef.current?.close();
    const stream = new EventSource(`/api/queries/${query.id}/events`);
    activeExecutionRef.current = {
      id: query.id,
      mode: query.mode,
      serviceId: query.serviceId,
      stream,
    };
    streamRef.current = stream;

    const handleStatus = (event: Event) => {
      if (!ownsExecution(query.id)) return;
      const payload = JSON.parse((event as MessageEvent<string>).data) as Extract<QueryEvent, { type: "status" }>;
      setRun(payload.run);
      setNotice({ tone: "info", text: payload.message });
    };

    const handleData = (event: Event) => {
      if (!ownsExecution(query.id)) return;
      const payload = JSON.parse((event as MessageEvent<string>).data) as Extract<QueryEvent, { type: "data" }>;
      setResult(payload.result);
    };

    const handleComplete = (event: Event) => {
      if (!ownsExecution(query.id)) return;
      const payload = JSON.parse((event as MessageEvent<string>).data) as Extract<QueryEvent, { type: "complete" }>;
      setRun(payload.run);
      setNotice({ tone: payload.run.state === "success" ? "success" : "info", text: payload.run.state === "success" ? "Query completed." : "Query finished." });
      releaseExecution(query.id);
      void loadHistory();
    };

    const handleExecutionError = (event: Event) => {
      if (!ownsExecution(query.id)) return;
      const data = (event as MessageEvent<string>).data;
      if (!data) {
        setNotice({ tone: "error", text: "The query stream disconnected." });
        return;
      }
      const payload = JSON.parse(data) as Extract<QueryEvent, { type: "execution-error" }>;
      setRun(payload.run);
      setNotice({ tone: "error", text: payload.message });
      releaseExecution(query.id);
      void loadHistory();
    };

    stream.addEventListener("status", handleStatus);
    stream.addEventListener("data", handleData);
    stream.addEventListener("complete", handleComplete);
    stream.addEventListener("execution-error", handleExecutionError);
    stream.onerror = () => {
      if (!ownsExecution(query.id)) return;
      setNotice({ tone: "error", text: "The query stream disconnected." });
      releaseExecution(query.id);
    };
  }

  async function runQuery(): Promise<void> {
    if (!selectedServiceId || !query.trim() || isQueryRunning || isQueryStale) return;
    const requestServiceId = selectedServiceId;
    const requestMode = mode;
    await invalidateActiveExecution();
    setNotice({ tone: "info", text: "Submitting query..." });
    setResult(null);
    try {
      const response = await fetch("/api/queries", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode: requestMode, serviceId: requestServiceId, query }),
      });
      const body = await readJson<{ run: QueryRun }>(response);
      if (selectedServiceId !== requestServiceId || mode !== requestMode || isQueryStale) {
        await fetch(`/api/queries/${body.run.id}/cancel`, { method: "POST" });
        return;
      }
      setRun(body.run);
      subscribeToQuery(body.run);
    } catch (error: unknown) {
      setNotice({ tone: "error", text: error instanceof Error ? error.message : "Could not run the query." });
    }
  }

  async function cancelQuery(): Promise<void> {
    if (!run || !isQueryRunning) return;
    await fetch(`/api/queries/${run.id}/cancel`, { method: "POST" });
    setNotice({ tone: "info", text: "Cancellation requested..." });
  }

  function selectHistoryItem(item: QueryHistoryItem): void {
    selectService(item.serviceId);
    if (item.mode === mode) {
      pendingHistoryQueryRef.current = null;
      pendingHistoryServiceRef.current = null;
      setQuery(item.query);
      setQueryServiceId(item.serviceId);
    } else {
      pendingHistoryQueryRef.current = item.query;
      pendingHistoryServiceRef.current = item.serviceId;
      setMode(item.mode);
    }
    setResult(null);
    setDraft(null);
    setNotice({ tone: "info", text: "Loaded a query from history." });
  }

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand-lockup">
          <div className="brand-mark">CP</div>
          <div>
            <div className="brand-name">ClickPlane Console</div>
            <div className="brand-subtitle">control plane / query workspace</div>
          </div>
        </div>
        <div className="topbar-actions">
          <span className="environment-pill"><span className="status-dot healthy" /> local workspace</span>
          <button className="avatar-button" aria-label="Open user menu">HV</button>
        </div>
      </header>

      <div className="workspace-grid">
        <aside className="sidebar">
          <div className="workspace-heading">
            <span className="eyebrow">Organization</span>
            <strong>northstar-labs</strong>
            <span className="workspace-role">Owner · 3 services</span>
          </div>

          <div className="sidebar-section">
            <div className="section-label">Services</div>
            <div className="service-list">
              {services.map((service) => (
                <button
                  className={`service-item ${service.id === selectedServiceId ? "selected" : ""}`}
                  key={service.id}
                  onClick={() => selectService(service.id)}
                >
                  <span className={`status-dot ${service.status}`} />
                  <span className="service-item-copy">
                    <strong>{service.name}</strong>
                    <small>{service.region}</small>
                  </span>
                  <span className="service-chevron">›</span>
                </button>
              ))}
            </div>
          </div>

          <div className="sidebar-section context-section">
            <div className="section-label">{mode === "sql" ? "Schema" : "Metrics"}</div>
            {mode === "sql" ? (
              <div className="tree-list">
                {schema.map((table) => (
                  <details key={`${table.database}.${table.name}`} open>
                    <summary><span className="tree-icon">▸</span>{table.database}.{table.name}</summary>
                    <div className="column-list">
                      {table.columns.slice(0, 6).map((column) => (
                        <div className="column-item" key={column.name}>
                          <span>{column.name}</span><code>{column.type}</code>
                        </div>
                      ))}
                    </div>
                  </details>
                ))}
              </div>
            ) : (
              <div className="metric-list">
                {metrics.map((metric) => (
                  <div className="metric-item" key={metric.name}>
                    <span className="metric-glyph">∿</span>
                    <span><strong>{metric.name}</strong><small>{metric.type} · {metric.labels.length} labels</small></span>
                  </div>
                ))}
              </div>
            )}
          </div>

          <div className="sidebar-footer">
            <div className="security-note"><span>⌁</span><span><strong>Read-only sandbox</strong><small>Queries are validated before execution.</small></span></div>
          </div>
        </aside>

        <main className="main-content">
          <div className="content-heading">
            <div>
              <span className="eyebrow">Console</span>
              <h1>{selectedService?.name ?? "Loading service..."}</h1>
              <p><span className={`status-dot ${selectedService?.status ?? "provisioning"}`} /> {selectedService?.status ?? "connecting"} · {selectedService?.region ?? ""} · ClickHouse {selectedService?.version ?? ""}</p>
            </div>
            <div className="heading-actions">
              <button className="secondary-button">Service details</button>
              <button className="primary-button compact">＋ New tab</button>
            </div>
          </div>

          <div className="mode-tabs" role="tablist" aria-label="Query mode">
            <button className={mode === "sql" ? "active" : ""} onClick={() => setMode("sql")} role="tab" aria-selected={mode === "sql"}>⌘ SQL Console</button>
            <button className={mode === "promql" ? "active" : ""} onClick={() => setMode("promql")} role="tab" aria-selected={mode === "promql"}>◌ Metrics Explorer</button>
            <span className="tab-spacer" />
            <span className="context-badge">{contextCount} {mode === "sql" ? "tables" : "metrics"} in context</span>
          </div>

          <section className="natural-language-card">
            <div className="card-kicker"><span className="sparkle">✦</span> Natural language query</div>
            <div className="prompt-row">
              <textarea
                aria-label="Natural language query prompt"
                value={prompt}
                onChange={(event) => setPrompt(event.target.value)}
                onKeyDown={(event) => {
                  if ((event.metaKey || event.ctrlKey) && event.key === "Enter") void generateQuery();
                }}
                placeholder={mode === "sql" ? "Ask for an analysis of your data..." : "Ask about your metrics..."}
                rows={2}
              />
              <button className="generate-button" disabled={isGenerating || !selectedServiceId} onClick={() => void generateQuery()}>
                {isGenerating ? <><span className="spinner" /> Drafting</> : <>Generate {mode === "sql" ? "SQL" : "PromQL"} <span>⌘↵</span></>}
              </button>
            </div>
            <div className="prompt-hint">The draft uses the selected service context. Always review generated queries before running them.</div>
          </section>

          <section className="editor-card">
            <div className="panel-toolbar">
              <div className="panel-title"><span className="file-dot" /> {mode === "sql" ? "query.sql" : "query.promql"} <span className="unsaved-label">local draft</span></div>
              <div className="toolbar-actions">
                <button className="toolbar-button">Format</button>
                <button className="toolbar-button">Save query</button>
                {isQueryRunning ? (
                  <button className="cancel-button" onClick={() => void cancelQuery()}>Cancel query</button>
                ) : (
                  <button className="run-button" onClick={() => void runQuery()} disabled={!selectedServiceId || !query.trim() || isQueryStale}><span>▶</span> Run query</button>
                )}
              </div>
            </div>
            <textarea className="query-editor" aria-label={`${mode} editor`} value={query} onChange={(event) => setQuery(event.target.value)} spellCheck={false} />
            {isQueryStale && (
              <div className="stale-query-banner">
                <span><strong>Query target changed.</strong> This draft was created for {services.find((service) => service.id === queryServiceId)?.name ?? queryServiceId}, but the console is now on {selectedService?.name ?? selectedServiceId}.</span>
                <button onClick={() => { setQueryServiceId(selectedServiceId); setDraft(null); setNotice({ tone: "info", text: "Query rebound to the current service. Review it before running." }); }}>Use current service</button>
              </div>
            )}
            {draft && (
              <div className="draft-inspector">
                <div className="draft-summary"><span className="check-icon">✓</span><span><strong>Generated draft</strong><small>{draft.explanation}</small></span></div>
                <div className="draft-details">
                  <span>Context: {draft.referencedContext.slice(0, 3).join(", ")}</span>
                  <span>Assumptions: {draft.assumptions.length}</span>
                  <span>Warnings: {draft.warnings.length}</span>
                </div>
              </div>
            )}
          </section>

          <section className="results-card">
            <div className="panel-toolbar results-toolbar">
              <div className="panel-title"><span className="results-icon">▤</span> Results {run && <span className={`run-status ${run.state}`}>{run.state}</span>}</div>
              <div className="results-meta">{run?.durationMs ? `${run.durationMs} ms` : "Awaiting query"} <span>·</span> {notice.text}</div>
            </div>
            {result ? <ResultPanel result={result} /> : <EmptyResults isRunning={isQueryRunning} />}
          </section>

          {notice.tone === "error" && <div className="error-banner">! {notice.text}</div>}
        </main>

        <aside className="right-rail">
          <div className="right-rail-heading"><span className="eyebrow">Workspace context</span><button className="icon-button" aria-label="Close context panel">×</button></div>
          <div className="context-card">
            <div className="context-card-heading"><span className="sparkle">✦</span><strong>Assistant context</strong></div>
            <p>This request can use the selected service and its {mode === "sql" ? "schema" : "metric catalog"}.</p>
            <div className="context-chips">{currentContext.slice(0, 4).map((item) => <span key={item}>@{item}</span>)}</div>
          </div>
          <div className="right-rail-section">
            <div className="section-label">Query history</div>
            {history.length === 0 ? <div className="muted-copy">Your completed queries will appear here.</div> : history.slice(0, 6).map((item) => (
              <button className="history-item" key={item.id} onClick={() => selectHistoryItem(item)}>
                <span className={`history-mode ${item.mode}`}>{item.mode === "sql" ? "SQL" : "PromQL"}</span>
                <span><strong>{item.query.split("\n")[0].slice(0, 34)}</strong><small>{item.state} · {item.resultSummary ?? "query"}</small></span>
              </button>
            ))}
          </div>
          <div className="right-rail-section interview-note">
            <div className="section-label">Control-plane note</div>
            <p>Desired query state is kept separate from execution state, so the UI can recover after cancellation or a lost stream.</p>
          </div>
        </aside>
      </div>
    </div>
  );
}

function EmptyResults({ isRunning }: { isRunning: boolean }) {
  return (
    <div className="empty-results">
      <div className={`empty-icon ${isRunning ? "is-running" : ""}`}>{isRunning ? <span className="spinner large" /> : "⌁"}</div>
      <strong>{isRunning ? "Query is running" : "No results yet"}</strong>
      <span>{isRunning ? "Results will stream into this panel when they are ready." : "Generate or edit a query, then run it to inspect the result."}</span>
    </div>
  );
}

function ResultPanel({ result }: { result: QueryResult }) {
  if (result.mode === "sql") return <SqlResultTable result={result} />;
  return <PromqlResultView result={result} />;
}

function SqlResultTable({ result }: { result: SqlResult }) {
  return (
    <div className="table-wrap">
      <table>
        <thead><tr>{result.columns.map((column) => <th key={column}>{column}</th>)}</tr></thead>
        <tbody>{result.rows.map((row, rowIndex) => <tr key={rowIndex}>{result.columns.map((column) => <td key={column}>{formatCell(row[column])}</td>)}</tr>)}</tbody>
      </table>
      <div className="table-footer">Showing {result.rows.length} rows <span>·</span> 500 row display limit</div>
    </div>
  );
}

function PromqlResultView({ result }: { result: PromqlResult }) {
  return (
    <div className="timeseries-wrap">
      <TimeSeriesChart series={result.series} />
      <div className="series-table">
        {result.series.map((series) => (
          <div className="series-row" key={series.labels.service}>
            <span className="series-color" />
            <code>{series.metric}{formatLabels(series.labels)}</code>
            <span className="series-latest">{formatSeriesValue(series.points.at(-1)?.value ?? 0, result.unit)}</span>
          </div>
        ))}
      </div>
      <div className="table-footer">Showing {result.series.length} series <span>·</span> cardinality limit active</div>
    </div>
  );
}

function TimeSeriesChart({ series }: { series: TimeSeries[] }) {
  const width = 760;
  const height = 220;
  const padding = { top: 18, right: 20, bottom: 28, left: 44 };
  const points = series.flatMap((item) => item.points);
  const min = Math.min(...points.map((point) => point.value));
  const max = Math.max(...points.map((point) => point.value));
  const start = points[0]?.timestamp ?? 0;
  const end = points.at(-1)?.timestamp ?? 1;
  const valueRange = max - min || 1;
  const timeRange = end - start || 1;

  const pointToSvg = (timestamp: number, value: number): string => {
    const x = padding.left + ((timestamp - start) / timeRange) * (width - padding.left - padding.right);
    const y = height - padding.bottom - ((value - min) / valueRange) * (height - padding.top - padding.bottom);
    return `${x.toFixed(2)},${y.toFixed(2)}`;
  };

  return (
    <div className="chart-wrap">
      <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="PromQL time-series chart">
        {[0, 1, 2, 3].map((line) => {
          const y = padding.top + (line / 3) * (height - padding.top - padding.bottom);
          return <line key={line} x1={padding.left} x2={width - padding.right} y1={y} y2={y} className="chart-gridline" />;
        })}
        {series.map((item, index) => <polyline key={item.labels.service} points={item.points.map((point) => pointToSvg(point.timestamp, point.value)).join(" ")} className={`chart-line line-${index % 3}`} />)}
        <text x={padding.left} y={height - 7} className="chart-label">30 min ago</text>
        <text x={width - padding.right} y={height - 7} textAnchor="end" className="chart-label">now</text>
        <text x={padding.left - 8} y={padding.top + 4} textAnchor="end" className="chart-label">{max.toFixed(3)}</text>
        <text x={padding.left - 8} y={height - padding.bottom + 4} textAnchor="end" className="chart-label">{min.toFixed(3)}</text>
      </svg>
    </div>
  );
}

function formatCell(value: string | number | boolean | null): string {
  if (value === null) return "null";
  if (typeof value === "number") return value.toLocaleString();
  return String(value);
}

function formatLabels(labels: Record<string, string>): string {
  return `{${Object.entries(labels).map(([key, value]) => `${key}="${value}"`).join(", ")}}`;
}

function formatSeriesValue(value: number, unit: PromqlResult["unit"]): string {
  if (unit === "seconds") return `${(value * 1_000).toFixed(1)} ms`;
  if (unit === "ratio") return value.toFixed(4);
  return `${value.toFixed(3)} /s`;
}

export default App;
