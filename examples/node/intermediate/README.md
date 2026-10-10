# Node intermediate example

Extends the [basic orders app](../basic/README.md) with event subscriptions, queued completion notifications, cron, and telemetry. Requires Node 22+. Shared handlers live in `examples/shared/orders`.

## Run

From the repository root:

```bash
bun install
bun run dev:node:intermediate
```

SQLite is the default. Set `DATABASE_URL` to use an existing PostgreSQL database. The server listens on port 3000.

## Tests

```bash
bun run --cwd examples/node/intermediate test:app
bun run --cwd examples/node/intermediate test:e2e
```

Tests use Node's test runner, in-memory storage and synchronous subscription dispatch. See [subscriptions](../../../docs/subscriptions.md), [workers](../../../docs/workers.md), and [cron](../../../docs/cron.md) for API details. The [advanced example](../advanced/README.md) adds workflows, authentication, plugins, and Docker.
