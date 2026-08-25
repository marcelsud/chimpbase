# Changelog

## Unreleased

### Changed

- consolidated the public API into one `chimpbase` package with portable root and runtime exports, direct feature subpaths, and explicit `runtime/bun`, `runtime/node`, and `runtime/deno` adapter subpaths
- made every `packages/*` workspace private; scoped workspace names remain internal development aliases and build output rewrites them to public `chimpbase` subpaths

### Fixed

- awaited server, telemetry sink, and storage cleanup during runtime shutdown

## 0.7.0 - 2026-07-11

Stabilizes the multi-runtime release and makes durable job dispatch consistent with the other context primitives.

### Added

- added `ctx.enqueue(name, payload, options?)` as the preferred durable job API; `ctx.queue.enqueue(...)` remains available as a deprecated compatibility alias
- added PostgreSQL durability coverage for host replacement, retries and DLQ delivery, concurrent claims, expired lease recovery, workflow resumption, and multi-replica cron scheduling
- added CI builds and PostgreSQL-backed `/health` smoke tests for the Bun, Node, and Deno advanced images
- added the Chimpbase Cloud and `chimpctl` documentation pages

### Changed

- upgraded the repository toolchain and CI to Bun 1.3.14
- aligned examples, plugins, tests, and documentation on `ctx.enqueue(...)`
- made Node runtime detection safe when Node is unavailable and validated the Node 24 SQLite host path
- made the Deno advanced image cache application and test dependencies during its build
- extracted `tools/swarm` to a sibling repo (now `@chimpbase/deployer`); removed `"tools/*"` from chimpbase workspaces and `tools/**/*.ts` from `tsconfig.json`. Cross-repo wiring now lives in a parent workspace at the shared root.

### Fixed

- fixed Bun and Node advanced Docker builds so frozen installs receive a complete workspace view
- fixed the VitePress production build by declaring its Vue server-renderer dependency explicitly
- fixed the PostgreSQL CI job to execute the real LISTEN/NOTIFY, mesh, schema, Deno, and durability suites
- avoided polling the event bus after transactions that committed no events

## 0.6.0 - 2026-04-18

Adds services and distributed registry via the new `@chimpbase/mesh` plugin.

### Added

- added `@chimpbase/mesh` with `service()` sugar (versioning, settings, methods, mixins, lifecycle hooks) and `chimpbaseMesh()` plugin
- added `ctx.mesh.call(name, args, options)` with strategies (`local-first`, `round-robin`, `random`, `cpu`), retry/timeout/fallback, and user middleware
- added `ctx.mesh.emit(event, payload, { balanced })` — broadcast via pubsub, balanced via queue worker (exactly-once across the cluster)
- added distributed registry table `_chimpbase_mesh_nodes` populated on start, refreshed by heartbeat `setInterval`, and cleaned by `__chimpbase.mesh.gc` cron
- added peer discovery via `PostgresListenEventBus` (announce / leave / heartbeat packets)
- added HTTP RPC transport at `/__chimpbase/mesh/rpc` with `x-chimpbase-mesh-token` timing-safe authentication
- added context-extension hook in `@chimpbase/runtime`: `contextExtension(key, { context?, routeEnv? })` + `registerContextExtension` on the registration target so plugins can attach clients to `ChimpbaseContext`
- added integration test harness `tests/mesh.integration.test.ts` gated by `CHIMPBASE_TEST_PG_URL`
- added mesh documentation at `docs/mesh.md`

## 0.4.0 - 2026-04-11

Adds telemetry sink interface and OpenTelemetry integration.

### Added

- added `ChimpbaseTelemetrySink` interface to `@chimpbase/runtime` for exporting telemetry to external systems
- added `sinks` option to `createChimpbase()` for plugging in one or more telemetry sinks
- added automatic handler spans for action, worker, cron, and route execution
- added `runInContext` on sink spans for proper parent-child span propagation
- added `@chimpbase/otel` package with `createOtelSink()` factory for OpenTelemetry export
- `createOtelSink()` supports zero-config via `OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_SERVICE_NAME` env vars
- added telemetry sink and OpenTelemetry integration test coverage
- added OpenTelemetry documentation to the telemetry docs page

## 0.1.4 - 2026-03-11

Adds optional telemetry stream persistence for `ctx.log`, `ctx.metric` and `ctx.trace`.

### Added

- added opt-in telemetry persistence via `telemetry.persist` in `createChimpbase` config
- telemetry records are buffered during handler execution and batch-appended to dedicated streams (`_chimpbase.logs`, `_chimpbase.metrics`, `_chimpbase.traces`)
- added `minLevel` filter to skip debug/info logs when persisting (e.g. only persist `warn` and `error`)
- added per-handler telemetry override via `{ telemetry }` option on `action()`, `worker()`, `subscription()` and `cron()` factories
- added automatic cleanup cron (`__chimpbase.telemetry.cleanup`) with configurable retention period and schedule
- added telemetry persistence test coverage

## 0.1.3 - 2026-03-11

Adds the first durable cron primitive to the alpha runtime surface.

### Added

- added `cron(...)`, `defineCron(...)` and `@Cron(...)` to `@chimpbase/runtime`
- added durable cron schedule orchestration to the core engine and Bun host
- added SQLite and Postgres persistence for cron schedules and scheduled runs
- added runtime and integration coverage proving the next fire time advances before handler retries
- added cron documentation and a concrete backlog snapshot example in `examples/bun/todo-ts`

## 0.1.2 - 2026-03-11

Big-bang alpha naming cleanup across the durable primitives.

### Changed

- renamed durable queue handlers from `queue(...)` / `@Queue(...)` to `worker(...)` / `@Worker(...)`
- renamed job dispatch from `ctx.queue.send(...)` to `ctx.queue.enqueue(...)`
- renamed durable stream writes from `ctx.stream.publish(...)` to `ctx.stream.append(...)`
- updated examples, docs and tests to use the new queue and stream vocabulary consistently

## 0.1.1 - 2026-03-11

Refines the alpha API surface without changing the overall release model.

### Changed

- renamed the ephemeral event API from `emit`/`listener` to `ctx.pubsub.publish(...)` and `subscription(...)`
- renamed the decorator API from `@Listener(...)` to `@Subscription(...)`
- updated examples, docs and tests to use the new pub/sub naming consistently

## 0.1.0 - 2026-03-11

Initial public alpha release of the Chimpbase monorepo.

### Added

- `@chimpbase/bun` as the Bun host package
- `@chimpbase/runtime` with actions, listeners, queues and durable workflow primitives
- `@chimpbase/core` with the execution engine and host contracts
- Postgres-first runtime support with SQLite support for local development and tests
- durable workflows with versioned workflow contracts
- `ctx.db.kysely()` backed by Kysely while keeping `ctx.db.query()` available
- schema generation and schema check commands for Postgres migrations
- Bun examples under `examples/bun/*`

### Release model

- packages are published from the monorepo
- package dependencies use Bun workspaces and are rewritten on publish
- `0.1.0` intentionally ships TypeScript source instead of a prebuilt `dist/` directory
- release verification runs through `bun run release:check`
