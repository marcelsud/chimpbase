# Telemetry

Chimpbase provides structured logging, metrics, and distributed tracing out of the box.

## Logging

```ts
ctx.log.debug("processing batch", { batchId: 42 });
ctx.log.info("order created", { orderId: 123, total: 99.99 });
ctx.log.warn("rate limit approaching", { current: 90, max: 100 });
ctx.log.error("payment failed", { orderId: 123, error: "card declined" });
```

All log methods accept an optional attributes object with structured key-value data.

## Metrics

```ts
ctx.metric("orders.created", 1, {
  projectSlug: "storefront",
  region: "us-east",
});

ctx.metric("order.total", 99.99, {
  currency: "USD",
});
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `string` | Metric name |
| `value` | `number` | Metric value |
| `labels` | `Record<string, string \| number \| boolean \| null>` | Optional labels |

## Tracing

Wrap operations in trace spans for distributed tracing:

```ts
const result = await ctx.trace("process.payment", async (span) => {
  span.setAttribute("order.id", orderId);
  span.setAttribute("amount", total);

  const charge = await chargeCard(total);
  span.setAttribute("charge.id", charge.id);

  return charge;
}, {
  provider: "stripe",
});
```

Traces can be nested — inner traces appear as child spans.

## Telemetry Persistence

By default, telemetry is buffered in memory; `ctx.log.*()` does not print to stdout. Use `drainTelemetryRecords()` on the host to inspect it in tests, configure a sink to export it, or enable persistence to store it in streams:

```ts
export default {
  telemetry: {
    minLevel: "info",  // "debug" | "info" | "warn" | "error"
    persist: {
      log: true,       // persist logs to stream
      metric: true,    // persist metrics to stream
      trace: true,     // persist traces to stream
    },
  },
} satisfies ChimpbaseAppDefinitionInput;
```

## Per-Handler Control

Override telemetry persistence at the handler level. These settings do not suppress in-memory records or export to configured sinks:

```ts
// Disable persistence for a noisy action
action("pollStatus", handler, { telemetry: false });

// Enable only logging for a specific cron
cron("cleanup", "0 * * * *", handler, {
  telemetry: { log: true, metric: false, trace: false },
});
```

## OpenTelemetry Export

Chimpbase can export telemetry to any OpenTelemetry-compatible backend (Jaeger, Grafana, Datadog, etc.) via `chimpbase/otel`.

### Install

```bash
bun add chimpbase
```

### Setup

Pass a sink to `createChimpbase`:

```ts
import { createChimpbase } from "chimpbase/runtime/bun";
import { createOtelSink } from "chimpbase/otel";

const chimpbase = await createChimpbase({
  app: myApp,
  sinks: [createOtelSink()],
});
```

Zero-config: `createOtelSink()` reads `OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_SERVICE_NAME` from environment variables.

### Options

```ts
createOtelSink({
  serviceName: "my-service",       // defaults to OTEL_SERVICE_NAME or "chimpbase-app"
  endpoint: "http://localhost:4318", // OTLP HTTP endpoint
  traceExporter: customExporter,   // custom SpanExporter
  spanProcessor: customProcessor,  // custom SpanProcessor (e.g. SimpleSpanProcessor for tests)
  metricExporter: customExporter,  // custom metric exporter
  logExporter: customExporter,     // custom log exporter
});
```

### What Gets Exported

- **Handler spans**: Every action, worker, cron, and route execution creates a root span (e.g. `action:createUser`, `worker:email.sync`, `cron:cleanup`)
- **Trace spans**: `ctx.trace()` calls become child spans of the handler span, with proper parent-child hierarchy for nested traces
- **Logs**: `ctx.log.*()` calls emit OTel LogRecords with severity level
- **Metrics**: `ctx.metric()` calls record OTel counter metrics

All spans and logs include `chimpbase.scope.kind` and `chimpbase.scope.name` attributes.

### Coexistence

OTel export works alongside the existing telemetry features:

- `drainTelemetryRecords()` continues to work for tests
- Stream persistence (`telemetry.persist`) continues to write to internal streams
- Sinks receive telemetry in parallel, not instead of existing mechanisms

The in-memory history keeps the latest 10,000 records after each handler execution. Stream persistence and sinks still receive all records.

### Custom Sinks

You can implement your own sink by implementing the `ChimpbaseTelemetrySink` interface from `chimpbase`:

```ts
import type { ChimpbaseTelemetrySink } from "chimpbase";

const mySink: ChimpbaseTelemetrySink = {
  onLog(scope, level, message, attributes) { /* ... */ },
  onMetric(scope, name, value, labels) { /* ... */ },
  startSpan(scope, name, attributes) {
    return {
      setAttribute(key, value) { /* ... */ },
      end(status, errorMessage) { /* ... */ },
    };
  },
  startHandlerSpan(scope) {
    return {
      setAttribute(key, value) { /* ... */ },
      end(status, errorMessage) { /* ... */ },
      runInContext(fn) { return fn(); },
    };
  },
  async shutdown() { /* ... */ },
};
```

The `runInContext` method is optional but enables proper parent-child span propagation when present.

## Secrets

Access secrets from environment variables, `.env` files, or mounted secret directories:

```ts
const apiKey = ctx.secret("STRIPE_API_KEY");
const sender = ctx.secret("EMAIL_SENDER") ?? "noreply@example.com";
```

Secrets are loaded from environment variables, `.env` files, and mounted secret directories (e.g., `/run/secrets`).
