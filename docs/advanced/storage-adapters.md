# Custom Storage Adapters

The built-in hosts already support PostgreSQL, SQLite, and memory. Write a custom adapter only when you need a different storage integration.

An adapter implements `ChimpbaseEngineAdapter` from `chimpbase/core`. Read that interface for the complete contract.

Kysely integrations implement `ChimpbaseKyselyExecutor.streamQuery` alongside `executeQuery`;
SQLite statement wrappers implement `ChimpbaseSqliteStatement.iterate` alongside `all` and `run`.
An adapter's `createKysely(schema, executor)` must honor the optional executor override so
retained lifecycle queries and streams use the adapter selected when they execute.

## Events and Transactions

`persistEvents(events)` runs inside the current transaction. It inserts events, assigns IDs, and creates durable subscription jobs. Synchronous subscriptions can publish more events, so this method may run for several batches before commit.

`commitTransaction()` only commits the transaction. Event persistence, subscription writes, and idempotency markers must roll back together when the operation fails.

## Atomic Key Reservations

`kvSetIfAbsent(key, value, ttlMs?)` must atomically write an absent or expired key and return `true`. An existing live key keeps its value and expiration, and the method returns `false`.

The reservation participates in the current transaction: rollback releases it. The runtime uses this for `ctx.kv.setIfAbsent` and inbound webhook deduplication across hosts.

See [Database](/database) for application queries and migrations, and [Configuration](/configuration) for the built-in hosts.
