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

## Install from source

The public `chimpbase` package is not published to npm yet. Until it is available, build a package from the repository and install it in a separate project. Packaging requires Bun 1.3 and Node 24:

```bash
git clone https://github.com/marcelsud/chimpbase.git
cd chimpbase
bun install --frozen-lockfile
bun run build:publish:packages
npm pack --ignore-scripts

cd ..
mkdir my-app
cd my-app
npm init -y
npm pkg set type=module
npm install ../chimpbase/chimpbase-0.8.0.tgz
```

Save the [Getting Started app](/getting-started#create-the-app) as `chimpbase.app.ts` in `my-app`, then use the [start command](#runtime-hosts) for your chosen runtime. This local package works with Bun and Node; its imports are the same as the public package. The tarball name follows the version in the repository's root `package.json`.

## Storage

Without configuration, the CLI uses SQLite at `data/{project-name}.db`. SQLite runs in one process and is suitable for local development or a single-runtime deployment.

For PostgreSQL, first create a database. With Docker installed and running, this starts a local development database on port 5439:

```bash
docker run -d --name chimpbase-postgres \
  -p 127.0.0.1:5439:5432 \
  -e POSTGRES_USER=chimpbase \
  -e POSTGRES_PASSWORD=chimpbase \
  -e POSTGRES_DB=my_app \
  postgres:17-alpine

docker exec chimpbase-postgres pg_isready -U chimpbase -d my_app
```

Wait until `pg_isready` reports `accepting connections`, then export the connection URL in the terminal where you start the CLI:

```bash
export DATABASE_URL=postgresql://chimpbase:chimpbase@127.0.0.1:5439/my_app
bunx chimpbase dev
```

A nonempty `DATABASE_URL` or `CHIMPBASE_DATABASE_URL` selects PostgreSQL automatically. For Node, use the matching [start command](#runtime-hosts) after exporting the same URL. The Getting Started app uses collections, so it runs on PostgreSQL without application SQL migrations; Chimpbase creates its internal tables on startup. Existing SQLite notes remain in the SQLite file and are not copied to PostgreSQL.

Use your own connection URL if you already have a database. These container credentials are for local development. Stop the database with `docker stop chimpbase-postgres`; restart it with `docker start chimpbase-postgres`. Use PostgreSQL when multiple processes need to share queues or other coordination state.

PostgreSQL connection acquisition times out after 5 seconds. SQL queries and idle transactions have 30-second deadlines on the client and server, so a network failure cannot leave database locks held indefinitely. Set `storage.connectionTimeoutMs` and `storage.queryTimeoutMs` when creating the runtime to change these deadlines, including for long migrations, queries, or external waits inside a transaction. Both values must be positive finite integers:

```ts
import { createChimpbase } from "chimpbase/runtime/bun";

const chimpbase = await createChimpbase({
  storage: {
    engine: "postgres",
    url: process.env.DATABASE_URL,
    connectionTimeoutMs: 5_000,
    queryTimeoutMs: 120_000,
  },
});
```

Each PostgreSQL host admits at most 10 concurrent ordinary requests and 10 authenticated mesh RPC requests. Requests beyond these limits receive HTTP 503 before execution; direct `executeAction` calls reject. The pools reserve up to 10 connections for ordinary work and workers, 10 for RPC callbacks, and 2 for detached lifecycle work such as mesh heartbeats. The latter pools open lazily, so budget up to 22 database connections per host. Deeper call chains can exhaust the RPC pool; their acquisition and RPC deadlines remain finite. Failed queries discard the affected transaction connection and roll back its writes; the next transaction reconnects. If the connection fails while awaiting a commit reply, the commit outcome can be unknown, so retries of external effects still need idempotency.

For temporary data in tests:

```bash
CHIMPBASE_STORAGE_ENGINE=memory bunx chimpbase dev
```

For Node, put `CHIMPBASE_STORAGE_ENGINE=memory` before the matching start command instead. Memory data is lost when the process stops. These shell commands use Bash/Zsh syntax.

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

Worker concurrency and leases must be positive safe integers. The poll interval must be an integer from `1` to `2147483647`. Explicitly empty, malformed, fractional or nonfinite worker values fail at startup. App retry settings require positive safe integer `maxAttempts` and nonnegative safe integer `retryDelayMs`. Completed jobs are removed daily after 30 days; configure `workerRuntime.retention` in a custom entry point to change or disable cleanup. See [Workers & Queues](/workers#configuration).

The secret loader combines `.env`, process environment and mounted secret files, in that order; later values win. It does not copy `.env` values into the process environment. Export runtime configuration variables in your shell or provide them through your process manager.

## CLI

Run these commands from the directory containing `chimpbase.app.ts`. Use `--project-dir PATH` for another directory.

```bash
# HTTP server and background worker
bunx chimpbase dev

# Restart on source edits
bunx chimpbase dev --watch

# HTTP server only
bunx chimpbase dev --serve

# Background worker only
bunx chimpbase dev --worker

# Call an action; --args is JSON
bunx chimpbase dev --action createNote --args '{"body":"From the CLI"}'

# Inspect durable background work and retry a terminal failed job
bunx chimpbase jobs list --status failed
bunx chimpbase jobs inspect 42
bunx chimpbase jobs retry 42

# Show commands and options without starting the app
bunx chimpbase --help
```

`dev` starts once by default. Add `--watch` for the edit–run loop; the CLI uses the runtime's native process watcher and imports the original app file, including from read-only source directories. Changes to `chimpbase.app.ts` or imported source files restart the process and reload actions. SQLite data survives a restart; memory storage resets. Restart manually after editing configuration or secret files. The CLI stops HTTP, the worker, storage and telemetry sinks when it receives `SIGINT` or `SIGTERM`.

Bun 1.3 performs a hard restart on source edits and skips application `onStop` hooks during that restart. Stopping the CLI with `Ctrl+C` or `SIGTERM` runs cleanup. When cleanup hooks must run between changes, stop and start the CLI manually instead of using watch mode.

The same option works on every host:

```bash
npx chimpbase-node dev --watch
```

Watch mode applies to long-running HTTP and worker modes. `--action` is a one-shot command and cannot be combined with `--watch`, `--serve` or `--worker`. Unknown flags, missing values and extra positional arguments fail before the app starts. Use `--args` only with `--action`.

The schema commands create or check `db/schema.snapshot.json` and `db/schema.generated.ts` from application migrations. They use a temporary PostgreSQL database through Docker; they do not generate migration SQL.

```bash
bunx chimpbase schema generate
bunx chimpbase schema check
```

Workflow contract and module commands are documented in [advanced guides](/advanced/).

## Runtime hosts

Both hosts use the same `chimpbase` package and portable `chimpbase/runtime` API. The Getting Started app runs unchanged on each host. Use Bun 1.3 or Node 24; these are the runtime versions used by the repository checks.

Until the npm package is published, [install from source](#install-from-source), then skip the install commands below and use the matching start command.

| Host | Host import for a custom entry point |
|------|--------------------------------------|
| Bun | `chimpbase/runtime/bun` |
| Node | `chimpbase/runtime/node` |

For Bun, follow [Getting Started](/getting-started). For Node, use the setup below in a new project directory, then save the same [app definition](/getting-started#create-the-app) as `chimpbase.app.ts`. Start the CLI from that directory and verify it with the same HTTP requests in [Run it](/getting-started#run-it).

### Node

Node 24 loads the app's TypeScript directly and includes `node:sqlite`. Install with npm and mark the project as an ES module:

```bash
mkdir my-app
cd my-app
npm init -y
npm pkg set type=module
npm install chimpbase
```

After saving `chimpbase.app.ts`, start the Node CLI:

```sh chimpbase-run:node
npx chimpbase-node dev
```

For a custom entry point or separate HTTP and worker processes, see [app composition](/advanced/app-composition).
