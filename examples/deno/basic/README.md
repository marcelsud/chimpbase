# Deno basic example

A runnable orders app with actions and HTTP routes over SQLite. Requires Deno 2+. The local `deno.json` imports repository source; this example is outside the Bun workspace.

## Run

```bash
cd examples/deno/basic
deno task dev
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

From the same example directory:

```bash
deno task test
```

Tests use in-memory storage. For public package setup, see [Getting Started](../../../docs/getting-started.md). Continue with [background work](../intermediate/README.md) when you need queues or subscriptions.
