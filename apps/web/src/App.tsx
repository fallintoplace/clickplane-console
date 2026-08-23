import { useEffect, useMemo, useRef, useState, type MouseEvent } from "react";
import type {
  MetricDefinition,
  PromqlResult,
  QueryDraft,
  QueryEvent,
  QueryHistoryItem,
  QueryMode,
  QueryProgress,
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
const INCIDENT_PROMPT = "What caused the checkout error spike? Find the biggest errors in the last hour.";

const QUERY_STAGES: Array<{ id: QueryProgress["stage"]; label: string }> = [
  { id: "queued", label: "Queued" },
  { id: "planning", label: "Planning" },
  { id: "executing", label: "Executing" },
  { id: "streaming", label: "Streaming" },
  { id: "complete", label: "Complete" },
];

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
  const [highlightedContext, setHighlightedContext] = useState("");
  const [notice, setNotice] = useState<Notice>({ tone: "info", text: "Ready for a query." });
  const streamRef = useRef<EventSource | null>(null);
  const activeExecutionRef = useRef<ActiveExecution | null>(null);
  const pendingHistoryQueryRef = useRef<string | null>(null);
  const pendingHistoryServiceRef = useRef<string | null>(null);
  const pendingPromptRef = useRef<string | null>(null);
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
    setHighlightedContext("");
    setPrompt(pendingPromptRef.current ?? (mode === "sql" ? "Show me the biggest errors from the last hour" : "Show request errors by service"));
    pendingPromptRef.current = null;
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
    setSchema([]);
    setMetrics([]);
    setRun(null);
    setResult(null);
    setHighlightedContext("");
    setNotice({ tone: "info", text: "Service changed. Review the query target before running." });
  }

  function investigateIncident(): void {
    if (!selectedService?.incident) return;
    if (mode === "sql") {
      setPrompt(INCIDENT_PROMPT);
      setDraft(null);
      setRun(null);
      setResult(null);
    } else {
      pendingPromptRef.current = INCIDENT_PROMPT;
      setMode("sql");
    }
    setNotice({ tone: "info", text: "Incident prompt ready. Generate SQL to start the investigation." });
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
          <span className="avatar-button" aria-label="Current user">HV</span>
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
                    <small>{service.region} · v{service.version}</small>
                  </span>
                  {service.incident && <span className="incident-marker" title="Active incident">!</span>}
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
                  <details
                    className={contextMatches(`${table.database}.${table.name}`, highlightedContext) ? "context-highlight" : ""}
                    key={`${table.database}.${table.name}`}
                    open
                  >
                    <summary onClick={() => setHighlightedContext(`${table.database}.${table.name}`)}><span className="tree-icon">▸</span>{table.database}.{table.name}</summary>
                    <div className="column-list">
                      {table.columns.slice(0, 6).map((column) => (
                        <button
                          className={`column-item ${contextMatches(`${table.database}.${table.name}.${column.name}`, highlightedContext) || contextMatches(column.name, highlightedContext) ? "context-highlight" : ""}`}
                          key={column.name}
                          onClick={() => setHighlightedContext(`${table.database}.${table.name}.${column.name}`)}
                          type="button"
                        >
                          <span>{column.name}</span><code>{column.type}</code>
                        </button>
                      ))}
                    </div>
                  </details>
                ))}
              </div>
            ) : (
              <div className="metric-list">
                {metrics.map((metric) => (
                  <button
                    className={`metric-item ${contextMatches(metric.name, highlightedContext) ? "context-highlight" : ""}`}
                    key={metric.name}
                    onClick={() => setHighlightedContext(metric.name)}
                    type="button"
                  >
                    <span className="metric-glyph">∿</span>
                    <span><strong>{metric.name}</strong><small>{metric.type} · {metric.labels.length} labels</small></span>
                  </button>
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
          </div>

          {selectedService?.incident && (
            <section className="incident-banner">
              <div className="incident-symbol">!</div>
              <div className="incident-copy">
                <div className="incident-heading"><strong>{selectedService.incident.changePercent}% error-rate increase</strong><span>started {selectedService.incident.startedMinutesAgo}m ago</span></div>
                <p>{selectedService.incident.title} over the last {selectedService.incident.windowMinutes} minutes.</p>
              </div>
              <button className="incident-action" onClick={investigateIncident}>Investigate incident <span>→</span></button>
            </section>
          )}

          <div className="mode-tabs" role="tablist" aria-label="Query mode">
            <button className={mode === "sql" ? "active" : ""} onClick={() => setMode("sql")} role="tab" aria-selected={mode === "sql"}>⌘ SQL Console</button>
            <button className={mode === "promql" ? "active" : ""} onClick={() => setMode("promql")} role="tab" aria-selected={mode === "promql"}>◌ Metrics Explorer</button>
            <span className="tab-spacer" />
            <span className="context-badge">{contextCount} {mode === "sql" ? "tables" : "metrics"} in context</span>
          </div>

          <section className="natural-language-card">
            <div className="assistant-heading">
              <div className="card-kicker"><span className="sparkle">✦</span> ClickPlane Assistant</div>
              <span className="read-only-badge">✓ read-only</span>
            </div>
            <p className="assistant-intro">Ask about <strong>{selectedService?.name ?? "this service"}</strong>. I’ll use the selected {mode === "sql" ? "schema" : "metric catalog"} to draft a query you can review.</p>
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
              <button className="generate-button" disabled={isGenerating || !selectedServiceId || contextCount === 0} onClick={() => void generateQuery()}>
                {isGenerating ? <><span className="spinner" /> Drafting</> : contextCount === 0 ? <>Loading context <span>…</span></> : <>Generate {mode === "sql" ? "SQL" : "PromQL"} <span>⌘↵</span></>}
              </button>
            </div>
            <div className="prompt-hint">Tip: describe the symptom, time window, and grouping you want. Press ⌘↵ to generate.</div>
            {draft && (
              <div className="assistant-review">
                <div className="assistant-response">
                  <span className="check-icon">✓</span>
                  <div><strong>Draft ready for review</strong><p>{draft.explanation}</p></div>
                </div>
                <div className="assistant-review-grid">
                  <div>
                    <span className="assistant-label">Context used</span>
                    <div className="context-chips">
                      {draft.referencedContext.map((item) => (
                        <button className={`context-chip ${contextMatches(item, highlightedContext) ? "active" : ""}`} key={item} onClick={() => setHighlightedContext(item)} type="button">@{item}</button>
                      ))}
                    </div>
                  </div>
                  <div>
                    <span className="assistant-label">Assumptions</span>
                    <ul className="assistant-list">{draft.assumptions.map((item) => <li key={item}>{item}</li>)}</ul>
                  </div>
                  {draft.warnings.length > 0 && (
                    <div className="assistant-warning">
                      <span className="assistant-label">Review before running</span>
                      <ul className="assistant-list">{draft.warnings.map((item) => <li key={item}>{item}</li>)}</ul>
                    </div>
                  )}
                </div>
              </div>
            )}
          </section>

          <section className="editor-card">
            <div className="panel-toolbar">
              <div className="panel-title"><span className="file-dot" /> {mode === "sql" ? "query.sql" : "query.promql"} <span className="unsaved-label">local draft</span></div>
              <div className="toolbar-actions">
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
          </section>

          <section className="results-card">
            <div className="panel-toolbar results-toolbar">
              <div className="panel-title"><span className="results-icon">▤</span> Results {run && <span className={`run-status ${run.state}`}>{run.state}</span>}</div>
              <div className="results-meta">{run?.durationMs ? `${run.durationMs} ms` : "Awaiting query"} <span>·</span> {notice.text}</div>
            </div>
            {run && <QueryExecutionStrip run={run} serviceName={selectedService?.name} />}
            {result ? <ResultPanel result={result} /> : <EmptyResults isRunning={isQueryRunning} />}
          </section>

          {notice.tone === "error" && <div className="error-banner">! {notice.text}</div>}
        </main>

        <aside className="right-rail">
          <div className="right-rail-heading"><span className="eyebrow">Investigation state</span></div>
          <div className="context-card">
            <div className="context-card-heading"><span className="sparkle">✦</span><strong>{selectedService?.name ?? "Service context"}</strong><span className={`rail-status ${selectedService?.status ?? "provisioning"}`}>{selectedService?.status ?? "loading"}</span></div>
            <div className="rail-service-meta">{selectedService?.region ?? ""} · ClickHouse {selectedService?.version ?? ""} · {mode === "sql" ? `${schema.length} tables` : `${metrics.length} metrics`}</div>
            {selectedService?.incident && (
              <div className="rail-incident"><strong>{selectedService.incident.changePercent}% error-rate increase</strong><span>Active for {selectedService.incident.windowMinutes}m</span><button onClick={investigateIncident} type="button">Open incident prompt →</button></div>
            )}
            {draft ? (
              <div className="rail-block"><span className="assistant-label">AI interpretation</span><p>{draft.explanation}</p><div className="context-chips">{draft.referencedContext.slice(0, 4).map((item) => <button className={`context-chip ${contextMatches(item, highlightedContext) ? "active" : ""}`} key={item} onClick={() => setHighlightedContext(item)} type="button">@{item}</button>)}</div></div>
            ) : (
              <div className="rail-block"><span className="assistant-label">Available context</span><p>Select a table or metric, or ask the assistant to ground a draft in this catalog.</p><div className="context-chips">{currentContext.slice(0, 4).map((item) => <button className={`context-chip ${contextMatches(item, highlightedContext) ? "active" : ""}`} key={item} onClick={() => setHighlightedContext(item)} type="button">@{item}</button>)}</div></div>
            )}
            {run && <div className="rail-block"><span className="assistant-label">Execution</span><p><code>{run.id.slice(0, 8)}</code> · {run.progress?.stage ?? run.state}</p><div className="rail-progress"><span style={{ width: `${run.progress ? ((QUERY_STAGES.findIndex((stage) => stage.id === run.progress?.stage) + 1) / QUERY_STAGES.length) * 100 : 10}%` }} /></div></div>}
            {result && <div className="rail-block"><span className="assistant-label">Latest result</span><p>{summarizeResult(result)}</p></div>}
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
            <div className="section-label">Safe workflow</div>
            <p>Review the generated query, confirm the target, then run it in the read-only sandbox.</p>
          </div>
        </aside>
      </div>
    </div>
  );
}

function QueryExecutionStrip({ run, serviceName }: { run: QueryRun; serviceName?: string }) {
  const progress = run.progress;
  const stage = progress?.stage ?? (run.state === "queued" ? "queued" : "executing");
  const stageIndex = Math.max(0, QUERY_STAGES.findIndex((item) => item.id === stage));
  const returned = run.mode === "sql" ? progress?.rowsReturned : progress?.seriesReturned;

  return (
    <div className="execution-strip">
      <div className="execution-heading">
        <div><span className="assistant-label">Execution telemetry</span><strong>{run.state === "running" ? `Running on ${serviceName ?? "selected service"}` : run.state}</strong></div>
        <code>{run.id.slice(0, 8)}</code>
      </div>
      <div className="execution-stages" aria-label="Query execution progress">
        {QUERY_STAGES.map((item, index) => {
          const isDone = index <= stageIndex && run.state !== "running" && run.state !== "queued" || index < stageIndex;
          const isActive = index === stageIndex && !isDone;
          return <span className={`execution-stage ${isDone ? "done" : ""} ${isActive ? "active" : ""} ${run.state === "error" && index === stageIndex ? "error" : ""}`} key={item.id}><span className="execution-stage-dot" />{item.label}</span>;
        })}
      </div>
      <div className="execution-stats">
        <div><span>Elapsed</span><strong>{formatDuration(progress?.elapsedMs ?? run.durationMs)}</strong></div>
        <div><span>Rows scanned</span><strong>{formatCount(progress?.rowsScanned)}</strong></div>
        <div><span>Bytes read</span><strong>{formatBytes(progress?.bytesRead)}</strong></div>
        <div><span>{run.mode === "sql" ? "Rows returned" : "Series returned"}</span><strong>{formatCount(returned)}</strong></div>
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
  const [hiddenSeries, setHiddenSeries] = useState<Set<string>>(new Set());
  const chartSeries = result.series.map((series, colorIndex) => ({ ...series, colorIndex }));
  const visibleSeries = chartSeries.filter((series) => !hiddenSeries.has(seriesKey(series)));

  useEffect(() => {
    setHiddenSeries(new Set());
  }, [result]);

  function toggleSeries(series: TimeSeries): void {
    const key = seriesKey(series);
    setHiddenSeries((current) => {
      const next = new Set(current);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }

  return (
    <div className="timeseries-wrap">
      <TimeSeriesChart series={visibleSeries} unit={result.unit} />
      <div className="series-table">
        {result.series.map((series, index) => {
          const hidden = hiddenSeries.has(seriesKey(series));
          return <button className={`series-row ${hidden ? "is-muted" : ""}`} key={seriesKey(series)} onClick={() => toggleSeries(series)} type="button" aria-pressed={!hidden}>
            <span className={`series-color line-color-${index % 3}`} />
            <code>{series.metric}{formatLabels(series.labels)}</code>
            <span className="series-latest">{formatSeriesValue(series.points.at(-1)?.value ?? 0, result.unit)}</span>
          </button>;
        })}
      </div>
      <div className="table-footer">Showing {visibleSeries.length} of {result.series.length} series <span>·</span> click a legend row to hide or show</div>
    </div>
  );
}

function TimeSeriesChart({ series, unit }: { series: Array<TimeSeries & { colorIndex?: number }>; unit: PromqlResult["unit"] }) {
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);
  const width = 760;
  const height = 220;
  const padding = { top: 18, right: 20, bottom: 28, left: 44 };
  const points = series.flatMap((item) => item.points);
  if (series.length === 0 || points.length === 0) {
    return <div className="chart-empty">All series are hidden. Select a legend row to show one.</div>;
  }
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

  const hoveredPoint = hoverIndex === null ? undefined : series[0]?.points[hoverIndex];
  const hoverX = hoveredPoint ? padding.left + ((hoveredPoint.timestamp - start) / timeRange) * (width - padding.left - padding.right) : 0;
  const handleMouseMove = (event: MouseEvent<HTMLDivElement>): void => {
    const rect = event.currentTarget.getBoundingClientRect();
    const ratio = Math.min(1, Math.max(0, (event.clientX - rect.left) / rect.width));
    setHoverIndex(Math.round(ratio * ((series[0]?.points.length ?? 1) - 1)));
  };

  return (
    <div className="chart-wrap chart-frame" onMouseLeave={() => setHoverIndex(null)} onMouseMove={handleMouseMove}>
      <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="PromQL time-series chart">
        {[0, 1, 2, 3].map((line) => {
          const y = padding.top + (line / 3) * (height - padding.top - padding.bottom);
          return <line key={line} x1={padding.left} x2={width - padding.right} y1={y} y2={y} className="chart-gridline" />;
        })}
        {series.map((item, index) => <polyline key={seriesKey(item)} points={item.points.map((point) => pointToSvg(point.timestamp, point.value)).join(" ")} className={`chart-line line-${item.colorIndex ?? index % 3}`} />)}
        {hoveredPoint && <line x1={hoverX} x2={hoverX} y1={padding.top} y2={height - padding.bottom} className="chart-crosshair" />}
        {hoveredPoint && series.map((item, index) => {
          const point = item.points[hoverIndex ?? 0];
          if (!point) return null;
          const [x, y] = pointToSvg(point.timestamp, point.value).split(",");
          return <circle className={`chart-point line-${item.colorIndex ?? index % 3}`} cx={x} cy={y} key={seriesKey(item)} r="3.5" />;
        })}
        <text x={padding.left} y={height - 7} className="chart-label">30 min ago</text>
        <text x={width - padding.right} y={height - 7} textAnchor="end" className="chart-label">now</text>
        <text x={padding.left - 8} y={padding.top + 4} textAnchor="end" className="chart-label">{max.toFixed(3)}</text>
        <text x={padding.left - 8} y={height - padding.bottom + 4} textAnchor="end" className="chart-label">{min.toFixed(3)}</text>
      </svg>
      {hoveredPoint && (
        <div className="chart-tooltip" style={{ left: `${(hoverX / width) * 100}%` }}>
          <strong>{formatTimestamp(hoveredPoint.timestamp)}</strong>
          {series.map((item, index) => <div key={seriesKey(item)}><span className={`tooltip-swatch line-color-${item.colorIndex ?? index % 3}`} />{item.labels.service ?? item.metric}<b>{formatSeriesValue(item.points[hoverIndex ?? 0]?.value ?? 0, unit)}</b></div>)}
        </div>
      )}
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

function seriesKey(series: TimeSeries): string {
  return `${series.metric}${formatLabels(series.labels)}`;
}

function contextMatches(value: string, selected: string): boolean {
  if (!selected) return false;
  return selected === value || selected.startsWith(`${value}.`) || value.startsWith(`${selected}.`) || selected.endsWith(`.${value}`);
}

function summarizeResult(result: QueryResult): string {
  if (result.mode === "sql") {
    const errorRows = result.rows.filter((row) => typeof row.error_type === "string" && typeof row.errors === "number");
    if (errorRows.length > 0) {
      const leading = errorRows.reduce((current, row) => (Number(row.errors) > Number(current.errors) ? row : current));
      return `${String(leading.error_type)} is the largest error source with ${formatCount(Number(leading.errors))} events.`;
    }
    return result.rows.length > 0 ? `Returned ${formatCount(result.rows.length)} rows for review.` : "No rows returned.";
  }

  const leading = result.series.reduce<TimeSeries | undefined>((current, series) => {
    if (!current) return series;
    return (series.points.at(-1)?.value ?? 0) > (current.points.at(-1)?.value ?? 0) ? series : current;
  }, undefined);
  if (!leading) return "No series returned.";
  return `${leading.labels.service ?? leading.metric} is the highest series at ${formatSeriesValue(leading.points.at(-1)?.value ?? 0, result.unit)}.`;
}

function formatDuration(milliseconds?: number): string {
  if (milliseconds === undefined) return "—";
  return milliseconds >= 1_000 ? `${(milliseconds / 1_000).toFixed(1)} s` : `${Math.max(0, Math.round(milliseconds))} ms`;
}

function formatCount(value?: number): string {
  if (value === undefined) return "—";
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}k`;
  return value.toLocaleString();
}

function formatBytes(value?: number): string {
  if (value === undefined) return "—";
  if (value >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)} GB`;
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)} MB`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)} KB`;
  return `${value} B`;
}

function formatTimestamp(timestamp: number): string {
  return new Date(timestamp * 1_000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

export default App;
