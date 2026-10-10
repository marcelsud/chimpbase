# Deno intermediate example

Extends the [basic orders app](../basic/README.md) with event subscriptions, queued completion notifications, cron, and telemetry. Requires Deno 2+. Shared handlers live in `examples/shared/orders`.

## Run

```bash
cd examples/deno/intermediate
deno task dev
```

SQLite is the default. Set `DATABASE_URL` to use an existing PostgreSQL database. The server listens on port 3000.

## Tests

From the same example directory:

```bash
deno task test
```

Tests use in-memory storage and synchronous subscription dispatch. See [subscriptions](../../../docs/subscriptions.md), [workers](../../../docs/workers.md), and [cron](../../../docs/cron.md) for API details. The [advanced example](../advanced/README.md) adds workflows, authentication, plugins, and Docker.
