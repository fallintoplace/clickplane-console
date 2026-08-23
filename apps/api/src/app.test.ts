import { afterEach, describe, expect, it } from "vitest";
import type { FastifyInstance } from "fastify";
import { buildApp, buildResult } from "./app.js";

function wait(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

describe("ClickPlane API", () => {
  let app: FastifyInstance | undefined;

  afterEach(async () => {
    await app?.close();
    app = undefined;
  });

  it("returns service context for the console", async () => {
    app = buildApp();
    const response = await app.inject({ method: "GET", url: "/api/services" });

    expect(response.statusCode).toBe(200);
    expect(response.json().services).toHaveLength(2);
  });

  it("generates a reviewable SQL draft from natural language", async () => {
    app = buildApp();
    const response = await app.inject({
      method: "POST",
      url: "/api/query-drafts",
      payload: {
        mode: "sql",
        serviceId: "checkout-eu",
        prompt: "Show me the biggest errors from the last hour",
      },
    });

    expect(response.statusCode).toBe(200);
    const draft = response.json().draft;
    expect(draft.mode).toBe("sql");
    expect(draft.query).toMatch(/^SELECT/);
    expect(draft.referencedContext).toContain("analytics.errors");
  });

  it("generates a PromQL draft with metric context", async () => {
    app = buildApp();
    const response = await app.inject({
      method: "POST",
      url: "/api/query-drafts",
      payload: {
        mode: "promql",
        serviceId: "checkout-eu",
        prompt: "Show request latency by service",
      },
    });

    expect(response.statusCode).toBe(200);
    const draft = response.json().draft;
    expect(draft.mode).toBe("promql");
    expect(draft.query).toContain("histogram_quantile");
    expect(draft.referencedContext).toContain("http_request_duration_seconds");
  });

  it("keeps generated fixture results aligned with the submitted query", () => {
    const errorResult = buildResult("sql", "SELECT error_type FROM analytics.errors");
    expect(errorResult).toMatchObject({ mode: "sql", columns: ["service", "error_type", "errors"] });

    const latencyResult = buildResult("promql", "histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))");
    expect(latencyResult).toMatchObject({ mode: "promql", unit: "seconds" });
    expect(latencyResult.mode === "promql" && latencyResult.series[0]?.metric).toBe("http_request_duration_seconds");
  });

  it("uses only server-owned context for draft generation", async () => {
    app = buildApp();
    const response = await app.inject({
      method: "POST",
      url: "/api/query-drafts",
      payload: {
        mode: "sql",
        serviceId: "analytics-us",
        prompt: "Show the biggest errors",
        context: { tables: ["analytics.errors"] },
      },
    });

    expect(response.statusCode).toBe(200);
    expect(response.json().draft.query).toContain("analytics.http_requests");
    expect(response.json().draft.query).not.toContain("analytics.errors");
  });

  it("blocks unsafe SQL before execution", async () => {
    app = buildApp();
    const response = await app.inject({
      method: "POST",
      url: "/api/queries",
      payload: {
        mode: "sql",
        serviceId: "checkout-eu",
        query: "DROP TABLE analytics.errors",
      },
    });

    expect(response.statusCode).toBe(422);
    expect(response.json().details).toContain("Only SELECT, WITH, and EXPLAIN queries are allowed.");
  });

  it("rejects a query for an unknown service", async () => {
    app = buildApp();
    const response = await app.inject({
      method: "POST",
      url: "/api/queries",
      payload: {
        mode: "promql",
        serviceId: "unknown",
        query: "up",
      },
    });

    expect(response.statusCode).toBe(404);
  });

  it("cancels a queued query before execution", async () => {
    app = buildApp();
    const createResponse = await app.inject({
      method: "POST",
      url: "/api/queries",
      payload: {
        mode: "promql",
        serviceId: "checkout-eu",
        query: "sum by (service) (rate(http_requests_total[5m]))",
      },
    });
    const queryId = createResponse.json().run.id as string;

    const cancelResponse = await app.inject({
      method: "POST",
      url: `/api/queries/${queryId}/cancel`,
    });

    expect(cancelResponse.statusCode).toBe(200);
    expect(cancelResponse.json().run.state).toBe("cancelled");
  });

  it("emits a terminal execution-error event with timing metadata", async () => {
    app = buildApp();
    const createResponse = await app.inject({
      method: "POST",
      url: "/api/queries",
      payload: {
        mode: "sql",
        serviceId: "checkout-eu",
        query: "SELECT syntax_error FROM analytics.errors",
      },
    });
    const queryId = createResponse.json().run.id as string;
    await wait(1_250);

    const eventsResponse = await app.inject({
      method: "GET",
      url: `/api/queries/${queryId}/events`,
    });

    expect(eventsResponse.statusCode).toBe(200);
    expect(eventsResponse.body).toContain("event: execution-error");
    expect(eventsResponse.body).toContain("finishedAt");
    expect(eventsResponse.body).toContain("event: complete");
  });
});
