import type { MetricDefinition, SchemaTable, Service } from "@clickplane/shared";

export const services: Service[] = [
  {
    id: "checkout-eu",
    name: "checkout-production",
    region: "eu-central-1",
    status: "healthy",
    version: "24.11.3",
    databases: 4,
    metrics: 38,
  },
  {
    id: "analytics-us",
    name: "analytics-staging",
    region: "us-east-1",
    status: "degraded",
    version: "24.10.2",
    databases: 2,
    metrics: 24,
  },
];

const commonRequestColumns = [
  { name: "timestamp", type: "DateTime64(3)", description: "Request event time" },
  { name: "service", type: "LowCardinality(String)", description: "Logical service name" },
  { name: "route", type: "String", description: "HTTP route template" },
  { name: "status_code", type: "UInt16", description: "HTTP response status" },
  { name: "duration_ms", type: "Float32", description: "Request duration in milliseconds" },
];

export const schemas: Record<string, SchemaTable[]> = {
  "checkout-eu": [
    {
      database: "analytics",
      name: "http_requests",
      engine: "MergeTree",
      columns: commonRequestColumns,
    },
    {
      database: "analytics",
      name: "errors",
      engine: "MergeTree",
      columns: [
        ...commonRequestColumns,
        { name: "error_type", type: "LowCardinality(String)", description: "Normalized error category" },
        { name: "message", type: "String", description: "Error message" },
      ],
    },
    {
      database: "commerce",
      name: "orders",
      engine: "MergeTree",
      columns: [
        { name: "created_at", type: "DateTime", description: "Order creation time" },
        { name: "customer_id", type: "UUID", description: "Customer identifier" },
        { name: "region", type: "LowCardinality(String)", description: "Customer region" },
        { name: "revenue", type: "Decimal(18, 2)", description: "Order revenue" },
        { name: "status", type: "LowCardinality(String)", description: "Order status" },
      ],
    },
  ],
  "analytics-us": [
    {
      database: "analytics",
      name: "http_requests",
      engine: "MergeTree",
      columns: commonRequestColumns,
    },
    {
      database: "analytics",
      name: "deployments",
      engine: "MergeTree",
      columns: [
        { name: "deployed_at", type: "DateTime", description: "Deployment time" },
        { name: "service", type: "String", description: "Deployed service" },
        { name: "version", type: "String", description: "Application version" },
        { name: "environment", type: "String", description: "Deployment environment" },
      ],
    },
  ],
};

const serviceMetrics: MetricDefinition[] = [
  {
    name: "http_requests_total",
    type: "counter",
    description: "Total number of HTTP requests",
    labels: ["service", "route", "method", "status"],
  },
  {
    name: "http_request_errors_total",
    type: "counter",
    description: "Total number of failed HTTP requests",
    labels: ["service", "route", "error_type"],
  },
  {
    name: "http_request_duration_seconds",
    type: "histogram",
    description: "HTTP request duration distribution",
    labels: ["service", "route", "le"],
  },
  {
    name: "active_sessions",
    type: "gauge",
    description: "Currently active user sessions",
    labels: ["service", "region"],
  },
];

export const metrics: Record<string, MetricDefinition[]> = {
  "checkout-eu": serviceMetrics,
  "analytics-us": serviceMetrics.filter((metric) => metric.name !== "active_sessions"),
};
