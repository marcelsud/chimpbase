# Node basic example

A runnable orders app with actions and HTTP routes over SQLite. Requires Node 22+; `tsx` loads the repository's TypeScript source.

## Run

From the repository root:

```bash
bun install
bun run dev:node:basic
```

The server listens on port 3000; set `PORT` to change it.

```bash
curl -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{"customer":"alice@example.com","amount":4200}'
curl http://localhost:3000/orders
curl http://localhost:3000/health
```

`chimpbase.app.ts` defines actions and routes, `chimpbase.migrations.ts` defines the table, and `app.ts` starts the runtime.

## Tests

```bash
bun run --cwd examples/node/basic test
```

This runs `tests/app.nodetest.ts` with Node's test runner and in-memory storage. For public package setup, see [Getting Started](../../../docs/getting-started.md). Continue with [background work](../intermediate/README.md) when you need queues or subscriptions.
