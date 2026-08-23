import { afterEach, describe, expect, it } from "vitest";
import type { FastifyInstance } from "fastify";
import { buildApp } from "./app.js";

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
});
