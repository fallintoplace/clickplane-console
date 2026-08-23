# ClickPlane Console

An experimental ClickHouse-inspired Cloud Console for AI-assisted analytics, service context,
safe query execution, and real-time query workflows.

- service and workspace context
- a SQL-first query console
- natural-language to reviewable SQL and PromQL drafts
- safe read-only execution
- cancellation and streamed query events
- PromQL/time-series exploration
- authorization and failure-handling discussion points

It is not affiliated with ClickHouse.

## Run locally

Requirements: Node 22+ and Yarn.

```bash
yarn install
yarn dev
```

Then open [http://localhost:5173](http://localhost:5173).

The API runs at [http://localhost:3001](http://localhost:3001).

The default mode uses deterministic fixtures, so the console works without external services.

## Run against local ClickHouse

Docker and Docker Compose are required for the local database.

```bash
yarn clickhouse:up
yarn dev:clickhouse
```

Then open [http://localhost:5173](http://localhost:5173). The Compose seed creates request, error,
and order tables under the `analytics` and `commerce` databases. The API discovers the live schema
and sends SQL to ClickHouse with read-only settings, a 30-second execution limit, and a 500-row
result limit.

For ClickHouse Cloud or another endpoint, set `CLICKPLANE_EXECUTOR=clickhouse` together with
`CLICKHOUSE_URL`, `CLICKHOUSE_USERNAME`, `CLICKHOUSE_PASSWORD`, and optionally
`CLICKHOUSE_DATABASE` before starting the API.

## Verification

```bash
yarn test
yarn typecheck
yarn build
```

## Architecture

```text
apps/web          React console and query workspace
apps/api          Node/Fastify control-plane API and query executors
packages/shared   Shared query, service, schema, metric, and result types
```

SQL and PromQL are represented as separate query modes with a shared lifecycle. The API has a
fixture executor for offline demos and a ClickHouse executor behind the same interface. PromQL
continues to use deterministic local results while the SQL path can run against a real ClickHouse
endpoint.

The fixture executor derives its result shape from the submitted query, so an error query returns
error columns and a latency query returns latency series. It is deliberately not a SQL parser and
remains useful for testing the frontend and control-plane behavior without a database.

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
