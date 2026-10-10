# Configuration

`chimpbase.app.ts` describes your application. The CLI loads it; environment variables select the server and storage settings.

## App definition

The [Getting Started](/getting-started) app only needs `project` and `registrations`. Other application settings are optional:

| Field | Purpose |
|-------|---------|
| `project.name` | Project identifier; defaults to `chimpbase-app` |
| `registrations` | Actions, routes, subscriptions, workers and cron jobs |
| `migrations` | Application SQL migrations; see [database access](/database) |
| `worker.maxAttempts` | Maximum job attempts; defaults to `5` |
| `worker.retryDelayMs` | Retry delay; defaults to `1000` |

For telemetry, workflows, modules or a custom HTTP framework, see [advanced guides](/advanced/).

## Storage

Without configuration, the CLI uses SQLite at `data/{project-name}.db`. SQLite runs in one process and is suitable for local development or a single-runtime deployment.

For PostgreSQL, export a connection URL before starting the CLI:

```bash
export DATABASE_URL=postgresql://localhost/mydb
bunx chimpbase dev
```

A nonempty `DATABASE_URL` or `CHIMPBASE_DATABASE_URL` selects PostgreSQL automatically. Use PostgreSQL when multiple processes need to share queues or other coordination state.

For temporary data in tests:

```bash
CHIMPBASE_STORAGE_ENGINE=memory bunx chimpbase dev
```

Memory data is lost when the process stops.

## Environment variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `CHIMPBASE_SERVER_PORT` or `PORT` | HTTP port | `3000` |
| `CHIMPBASE_STORAGE_ENGINE` | `sqlite`, `postgres` or `memory` | Auto-detect: `postgres` with a database URL, otherwise `sqlite` |
| `CHIMPBASE_DATABASE_URL` or `DATABASE_URL` | PostgreSQL connection URL | Unset |
| `CHIMPBASE_STORAGE_PATH` | SQLite file path | `data/{project-name}.db` |
| `CHIMPBASE_WORKER_CONCURRENCY` | PostgreSQL worker concurrency | `1` |
| `CHIMPBASE_WORKER_POLL_INTERVAL_MS` | Worker poll interval | `250` |
| `CHIMPBASE_WORKER_LEASE_MS` | Worker lease duration | `30000` |
| `CHIMPBASE_ENV_FILE` | File loaded for `ctx.secret(...)` | `.env` |
| `CHIMPBASE_SECRETS_DIR` | Directory loaded for `ctx.secret(...)` | `/run/secrets` |

`CHIMPBASE_DATABASE_URL` takes precedence over `DATABASE_URL`; `CHIMPBASE_SERVER_PORT` takes precedence over `PORT`. `memory` explicitly selects memory storage. A database URL takes precedence over `CHIMPBASE_STORAGE_ENGINE=sqlite`; use a programmatic `storage.engine` override to force SQLite in that case.

The secret loader combines `.env`, process environment and mounted secret files, in that order; later values win. It does not copy `.env` values into the process environment. Export runtime configuration variables in your shell or provide them through your process manager.

## CLI

Run these commands from the directory containing `chimpbase.app.ts`. Use `--project-dir PATH` for another directory.

```bash
# HTTP server and background worker
bunx chimpbase dev

# HTTP server only
bunx chimpbase dev --serve

# Background worker only
bunx chimpbase dev --worker

# Call an action; --args is JSON
bunx chimpbase dev --action createNote --args '{"body":"From the CLI"}'
```

The schema commands create or check `db/schema.snapshot.json` and `db/schema.generated.ts` from application migrations. They use a temporary PostgreSQL database through Docker; they do not generate migration SQL.

```bash
bunx chimpbase schema generate
bunx chimpbase schema check
```

Workflow contract and module commands are documented in [advanced guides](/advanced/).

## Runtime hosts

All hosts use the same `chimpbase` package and portable `chimpbase/runtime` API:

| Host | Host import | Start command |
|------|-------------|---------------|
| Bun | `chimpbase/runtime/bun` | `bunx chimpbase dev` |
| Node | `chimpbase/runtime/node` | `npx chimpbase-node dev` |
| Deno | `npm:chimpbase/runtime/deno` | `deno run -A npm:chimpbase/runtime/deno/cli dev` |

For Node, install with `npm install chimpbase` and use a Node release that supports loading `.ts` files and `node:sqlite`. For Deno, install with `deno add npm:chimpbase`; the CLI needs permissions to open the database, load project files and serve HTTP.

For a custom entry point or separate HTTP and worker processes, see [app composition](/advanced/app-composition).
