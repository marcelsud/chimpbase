# Deno advanced example

Order fulfilment with workflows, API-key authentication, webhooks, REST collections, and telemetry. Requires Deno 2+. Start with the [basic example](../basic/README.md) if you are learning Chimpbase.

## Run

```bash
cd examples/deno/advanced
export CHIMPBASE_BOOTSTRAP_API_KEY=dev-key
deno task dev
```

The server listens on port 3000. `/health` is public; other routes require `X-API-Key: dev-key`.

```bash
curl http://localhost:3000/orders -H "X-API-Key: dev-key"
```

SQLite is the default. Set `DATABASE_URL` to use an existing PostgreSQL database. Set `OTEL_EXPORTER_OTLP_ENDPOINT` to send telemetry to a collector.

## Docker

From the same example directory:

```bash
docker compose up --build
```

Compose starts PostgreSQL, an OTel collector, and three app replicas on ports 3000–3002. It sets the API key to `dev-bootstrap-key`. Jobs may retry; external effects must be safe to repeat.

## Tests

```bash
deno task test
```

These use in-memory storage. See the [workflow](../../../docs/advanced/workflows.md), [authentication](../../../docs/advanced/auth.md), and [webhook](../../../docs/advanced/webhooks.md) references for the APIs. Shared order handlers live in `examples/shared/orders`; runtime setup lives in `app.ts`.
