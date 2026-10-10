# Database

Every handler has access to the database via `ctx.db`. Chimpbase supports PostgreSQL, SQLite, and in-memory storage.

## Raw SQL

```ts
import { v } from "chimpbase/runtime";

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

Storage is selected by the host, separately from your app definition. See [Configuration](/configuration) for SQLite, PostgreSQL, and memory settings.

For engine integrations, see [Custom storage adapters](/advanced/storage-adapters).
