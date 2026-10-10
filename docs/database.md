# Database

Every handler has access to the database via `ctx.db`. Chimpbase supports PostgreSQL, SQLite, and in-memory storage.

## Raw SQL

```ts
import { v } from "@chimpbase/runtime";

const users = await ctx.db.query(
  "SELECT id, email FROM users WHERE status = ?1",
  ["active"],
  v.object({ id: v.number(), email: v.string() }),
);
```

### Parameterized queries

Use positional parameters (`?1`, `?2`, etc. for SQLite; `$1`, `$2` for PostgreSQL):

```ts
// SQLite
await ctx.db.query(
  "INSERT INTO orders (customer_id, total) VALUES (?1, ?2) RETURNING *",
  [customerId, total],
);

// PostgreSQL
await ctx.db.query(
  "INSERT INTO orders (customer_id, total) VALUES ($1, $2) RETURNING *",
  [customerId, total],
);
```

### Return type

`ctx.db.query(sql, params, validator)` validates every returned row at the database boundary and returns the validator's inferred type. Invalid rows throw with their exact row and field path.

### Engine adapters

Custom `ChimpbaseEngineAdapter` implementations must implement `persistEvents(events)` separately
from `commitTransaction()`. Move event insertion, ID assignment, and durable subscription job
insertion out of the old `commitTransaction(events)` method into `persistEvents(events)`, which
runs inside the open transaction and may be called for multiple batches of cascading events.
`commitTransaction()` now only commits that transaction. This lets synchronous subscriptions
use persisted event IDs while their writes and idempotency markers still roll back with the publisher.

## Kysely (type-safe queries)

For type-safe query building, use the [Kysely](https://kysely.dev/) integration:

```ts
interface Database {
  users: {
    id: number;
    email: string;
    name: string;
    created_at: string;
  };
  orders: {
    id: number;
    user_id: number;
    total: number;
    status: string;
  };
}

const db = ctx.db.kysely<Database>();

const activeUsers = await db
  .selectFrom("users")
  .where("email", "like", "%@example.com")
  .selectAll()
  .execute();

const order = await db
  .insertInto("orders")
  .values({ user_id: 1, total: 99.99, status: "pending" })
  .returningAll()
  .executeTakeFirst();
```

## Migrations

Define migrations in your app:

```ts
export default {
  migrations: {
    sqlite: [
      {
        name: "001_init",
        sql: `
          CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
          );
        `,
      },
    ],
    postgres: [
      {
        name: "001_init",
        sql: `
          CREATE TABLE users (
            id BIGSERIAL PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
          );
        `,
      },
    ],
  },
};
```

Or load from SQL files using a `chimpbase.migrations.ts` file.

Named migrations run automatically on startup and are recorded in `_chimpbase_migrations`.
Each name runs once; add a new name for subsequent changes. Pending migrations and their
history records run in one transaction, so a failed batch is rolled back and can be retried.
PostgreSQL startup also locks migration execution across hosts.

Databases created before migration history was introduced need their already applied names
recorded before restarting with non-idempotent migrations. Anonymous `migrationsSql` statements
still run on every startup.

## Storage Configuration

Custom storage adapters implementing `ChimpbaseEngineAdapter` must provide
`kvSetIfAbsent(key, value, ttlMs?)`: atomically write an absent or expired key and
return `true`, or preserve an existing live key and return `false`. The write must
use the adapter's current transaction so rollback also releases the reservation.
Built-in adapters provide this operation for `ctx.kv.setIfAbsent` and inbound webhook deduplication.

Configure storage in your app definition:

```ts
const chimpbase = await createChimpbase({
  storage: {
    engine: "postgres",                       // "postgres" | "sqlite" | "memory"
    url: "postgresql://localhost/mydb",        // for postgres
    // path: "./data/app.db",                 // for sqlite
  },
});
```

Or via environment variables:

```
CHIMPBASE_STORAGE_ENGINE=postgres
CHIMPBASE_DATABASE_URL=postgresql://localhost/mydb
```
