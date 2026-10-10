# State & Storage

Beyond your application database, Chimpbase provides built-in storage primitives for operational state. See the dedicated pages for full API details: [KV Store](/advanced/kv), [Collections](/collections), [Streams](/advanced/streams).

## Key-Value Store

Simple key-value storage for configuration, flags, and lightweight state:

```ts
await ctx.kv.set("config:feature-flags", { darkMode: true });
const flags = await ctx.kv.get("config:feature-flags");
await ctx.kv.delete("config:feature-flags");
const keys = await ctx.kv.list({ prefix: "config:" });
```

Stored in the `_chimpbase_kv` table.

## Collections

Schemaless JSON document storage for data that doesn't need a rigid schema:

```ts
const id = await ctx.collection.insert("notes", { title: "Meeting notes", content: "..." });
const notes = await ctx.collection.find("notes", { title: "Meeting notes" });
const note = await ctx.collection.findOne("notes", { id: noteId });
await ctx.collection.update("notes", { id: noteId }, { content: "updated" });
await ctx.collection.delete("notes", { id: noteId });
```

Stored in the `_chimpbase_collections` table.

## Streams

Append-only event streams for audit logs, activity feeds, and event sourcing patterns:

```ts
await ctx.stream.append("audit.todos", "todo.created", {
  todoId: todo.id,
  createdBy: input.userId,
});

const events = await ctx.stream.read("audit.todos");
```

Stored in the `_chimpbase_stream_events` table.

## Storage Engines

Chimpbase supports three storage engines. PostgreSQL is the recommended default for production. See [Configuration](/configuration) for setup details.

| Engine | Use case | Coordination |
|--------|----------|-------------|
| PostgreSQL | Production | Multi-process, distributed |
| SQLite | Local development, tests | Single-process |
| Memory | Unit tests | Ephemeral |

## Internal Tables

Chimpbase manages its own tables, prefixed with `_chimpbase_`:

| Table | Purpose |
|-------|---------|
| `_chimpbase_events` | Pub/sub event bus |
| `_chimpbase_kv` | Key-value store |
| `_chimpbase_collections` | JSON document storage |
| `_chimpbase_stream_events` | Append-only event streams and persisted telemetry (logs, metrics, traces) |
| `_chimpbase_cron_schedules` | Durable cron metadata |
| `_chimpbase_workflows` | Workflow state |
| `_chimpbase_queue_jobs` | Job queue with retry tracking |

These tables are created and migrated automatically. Your application tables live alongside them in the same database.

## Schema Management

The CLI generates a JSON schema snapshot and TypeScript types from your declared migrations. It does not generate migration SQL; define application migrations in your app first (see [Configuration](/configuration)).

```bash
# Generate schema snapshot and types
bunx chimpbase schema generate

# Check generated artifacts are up to date
bunx chimpbase schema check
```

Schema generation replays PostgreSQL migrations in a temporary Docker container; Docker must be running.
