# ClickPlane Console

An independent ClickHouse-inspired Cloud Console practice project.

The project is intentionally shaped around the Control Plane full-stack interview surface:

- service and workspace context
- a SQL-first query console
- natural-language to reviewable SQL and PromQL drafts
- safe read-only execution
- cancellation and streamed query events
- PromQL/time-series exploration
- authorization and failure-handling discussion points
- query-aware fixture results that match the generated SQL or PromQL

It is not affiliated with ClickHouse.

## Run locally

Requirements: Node 22+ and Yarn.

```bash
yarn install
yarn dev
```

Then open [http://localhost:5173](http://localhost:5173).

The API runs at [http://localhost:3001](http://localhost:3001).

## Verification

```bash
yarn test
yarn typecheck
yarn build
```

## Architecture

```text
apps/web          React console and query workspace
apps/api          Node/Fastify control-plane API
packages/shared   Shared query, service, schema, metric, and result types
```

The first version uses deterministic local fixtures behind the API. SQL and PromQL are represented
as separate query modes with a shared lifecycle. A real ClickHouse or Prometheus adapter can be
added without changing the core console flow.

The fixture executor derives its result shape from the submitted query, so an error query returns
error columns and a latency query returns latency series. It is deliberately not a SQL parser.
It is a deterministic seam for practicing the frontend and control-plane behavior before wiring a
real backend.

Completed queries remain reconnectable for five minutes, then are evicted from the in-memory job
store. Edited queries are preserved when the selected service changes, but the console marks them
stale and requires an explicit rebind before execution.

## Golden path

1. Select `checkout-production`.
2. Ask the console to show the biggest errors from the last hour.
3. Review the generated SQL.
4. Run it and inspect the table result.
5. Switch to Metrics Explorer.
6. Generate a PromQL error-rate query.
7. Inspect the streamed time-series result.
