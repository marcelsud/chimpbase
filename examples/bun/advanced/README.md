# Bun advanced example

Order fulfilment with workflows, API-key authentication, webhooks, REST collections, telemetry, and attachments. Start with the [basic example](../basic/README.md) if you are learning Chimpbase.

## Run

From the repository root:

```bash
bun install
export CHIMPBASE_BOOTSTRAP_API_KEY=dev-key
bun run dev:bun:advanced
```

The server listens on port 3000. `/health` is public; other routes require `X-API-Key: dev-key`.

```bash
curl http://localhost:3000/orders -H "X-API-Key: dev-key"
```

SQLite is the default. Set `DATABASE_URL` to use an existing PostgreSQL database. Set `OTEL_EXPORTER_OTLP_ENDPOINT` to send telemetry to a collector.

Attachments use memory storage by default. Set `ATTACHMENTS_ROOT` to persist files and `BLOBS_SIGNING_SECRET` to a private signing key when deploying. The optional backup cron needs `ATTACHMENTS_BACKUP_ROOT` and `rsync` installed.

## Docker

```bash
cd examples/bun/advanced
docker compose up --build
```

Compose starts PostgreSQL, an OTel collector, and three app replicas on ports 3000–3002. It sets the API key to `dev-bootstrap-key` and stores attachments in a shared volume. Jobs may retry; external effects must be safe to repeat.

## Tests

From the repository root:

```bash
bun run --cwd examples/bun/advanced test:app
bun run --cwd examples/bun/advanced test:e2e
```

These use in-memory storage. See the [workflow](../../../docs/advanced/workflows.md), [authentication](../../../docs/advanced/auth.md), and [webhook](../../../docs/advanced/webhooks.md), plus the [blob](../../../docs/advanced/blobs.md) references for the APIs. Shared order handlers live in `examples/shared/orders`; runtime setup lives in `app.ts`.
