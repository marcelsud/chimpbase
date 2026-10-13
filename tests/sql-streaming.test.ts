import { describe, expect, spyOn, test } from "bun:test";
import { Database } from "bun:sqlite";
import { sql, type QueryResult } from "kysely";
import { Client } from "pg";
import { EventEmitter } from "node:events";

import { createDefaultChimpbasePlatformShim } from "../packages/core/host.ts";
import { normalizeProjectConfig } from "../packages/core/index.ts";
import { createSqliteEngineAdapter as createSharedSqliteEngineAdapter } from "../packages/core/sqlite-storage.ts";
import { createSqliteEngineAdapter } from "../packages/bun/src/sqlite_adapter.ts";
import { createPostgresEngineAdapter, openPostgresPool } from "../packages/postgres/src/index.ts";
import { isArrayValue } from "../packages/runtime/index.ts";

const platform = createDefaultChimpbasePlatformShim();

async function nextRow<TRow>(stream: AsyncIterator<TRow, unknown>): Promise<TRow> {
  const result = await stream.next();
  if (result.done) throw new Error("stream ended before the expected row");
  return result.value;
}

for (const engine of ["sqlite", "postgres"] as const) {
  test(`${engine} Kysely validates rows without dropping columns or copying records`, async () => {
    let rows: unknown = [];
    let fetched = false;
    const client = Object.assign(new EventEmitter(), {
      async query(statement: string) {
        if (statement.startsWith("DO ")) fetched = false;
        const result = statement.startsWith("FETCH ") ? (fetched ? [] : rows) : statement.startsWith("select ") ? rows : [];
        if (statement.startsWith("FETCH ")) fetched = true;
        return { rows: result, rowCount: isArrayValue(result) ? result.length : 0 };
      },
      release() {},
    });
    const adapter = engine === "sqlite"
      ? createSharedSqliteEngineAdapter({
          exec() {},
          query() {
            return {
              reader: true,
              all() { return rows; },
              *iterate() { if (isArrayValue(rows)) yield* rows; },
              run() { throw new Error("unexpected write"); },
            };
          },
        } as unknown as Parameters<typeof createSharedSqliteEngineAdapter>[0], platform)
      : createPostgresEngineAdapter({ query: client.query, async connect() { return client; } } as unknown as Parameters<typeof createPostgresEngineAdapter>[0], platform);
    type Row = { id: number; extra: string; nullable: null; data: { nested: number[] }; timestamp: Date; bytes: Buffer };
    const db = adapter.createKysely<{ items: Row }>();
    try {
      const original: Row[] = [{ id: 1, extra: "ação/😀", nullable: null, data: { nested: [1, 2] },
        timestamp: new Date("2020-01-01T00:00:00Z"), bytes: Buffer.from([0, 255]) }];
      for (const valid of [[], original]) {
        rows = valid;
        expect(await db.selectFrom("items").selectAll().execute()).toBe(valid);
        const streamed: Row[] = [];
        for await (const row of db.selectFrom("items").selectAll().stream(1)) streamed.push(row);
        expect(streamed).toEqual(valid);
        if (valid.length > 0) expect(streamed[0]).toBe(valid[0]);
      }
      for (const invalid of [undefined, null, 42, {}, [null], ["value"], [1], [[]]]) {
        rows = invalid;
        await expect(db.selectFrom("items").selectAll().execute()).rejects.toThrow(/^Kysely query rows must be database records$/);
        if (isArrayValue(invalid)) {
          await expect(db.selectFrom("items").selectAll().stream(1).next()).rejects.toThrow(/^Kysely query rows must be database records$/);
        }
      }
    } finally { await db.destroy(); }
  });
}

test("SQLite streaming reads only the requested chunk and closes an abandoned iterator", async () => {
  let reads = 0;
  let closed = false;
  const adapter = createSharedSqliteEngineAdapter({
    exec() {},
    query() {
      return {
        reader: true,
        all() { throw new Error("streaming must not materialize all rows"); },
        run() { throw new Error("unexpected write"); },
        *iterate() {
          try {
            for (let id = 1; id <= 10_000; id++) { reads++; yield { id }; }
          } finally { closed = true; }
        },
      };
    },
  }, platform);
  const db = adapter.createKysely<{ items: { id: number } }>();
  const stream = db.selectFrom("items").selectAll().stream(2);
  expect(await nextRow(stream)).toEqual({ id: 1 });
  expect(reads).toBe(2);
  await stream.return?.();
  expect(reads).toBe(2);
  expect(closed).toBe(true);
  await db.destroy();
});

test("SQLite rejects invalid stream sizes before running SQL and preserves zero-sized streams", async () => {
  const native = new Database(":memory:");
  const query = spyOn(native, "query");
  const db = createSqliteEngineAdapter(native, platform).createKysely<{ items: { id: number } }>();
  try {
    await db.schema.createTable("items").addColumn("id", "integer").execute();
    await db.insertInto("items").values([{ id: 1 }, { id: 2 }, { id: 3 }]).execute();
    for (const size of [NaN, Infinity, -Infinity, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
      query.mockClear();
      await expect(db.selectFrom("items").selectAll().stream(size).next()).rejects.toThrow("finite safe integer");
      expect(query).not.toHaveBeenCalled();
    }
    for (const size of [0, -1, 1, 2, 100]) {
      const rows: { id: number }[] = [];
      for await (const row of db.selectFrom("items").selectAll().orderBy("id").stream(size)) rows.push(row);
      expect(rows).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
    }
    for await (const row of db.selectFrom("items").selectAll().stream(1)) { expect(row.id).toBe(1); break; }
    expect(await db.selectFrom("items").selectAll().execute()).toHaveLength(3);
    const interleaved: { id: number }[] = [];
    for await (const row of db.selectFrom("items").selectAll().orderBy("id").stream(1)) {
      interleaved.push(row);
      expect(await db.selectFrom("items").selectAll().orderBy("id").execute()).toHaveLength(3);
      if (interleaved.length > 3) throw new Error("cached query restarted the active stream");
    }
    expect(interleaved).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
    const inserted: { id: number }[] = [];
    for await (const row of db.insertInto("items").values({ id: 4 }).returningAll().stream(1)) inserted.push(row);
    expect(inserted).toEqual([{ id: 4 }]);
  } finally { query.mockRestore(); await db.destroy(); native.close(); }
});

const PG_URL = process.env.CHIMPBASE_TEST_PG_URL;
const describeIfPg = PG_URL ? describe : describe.skip;

test("PostgreSQL stream ownership checks run before checking out a connection", async () => {
  let checkouts = 0;
  const pool = {
    async connect() { checkouts++; throw new Error("unexpected connection checkout"); },
  } as unknown as Parameters<typeof createPostgresEngineAdapter>[0];
  const db = createPostgresEngineAdapter(pool, platform).createKysely<{ items: { id: number } }>("chimpbase_left");
  try {
    await expect(db.withSchema("chimpbase_right").selectFrom("items").selectAll().stream(1).next()).rejects.toThrow("cannot access schema chimpbase_right");
    await expect(db.selectFrom("items").selectAll().stream(NaN).next()).rejects.toThrow("finite safe integer");
    expect(checkouts).toBe(0);
  } finally { await db.destroy(); }
});

describeIfPg("PostgreSQL cursor streaming", () => {
  const createPool = (queryTimeoutMs = 2000) => openPostgresPool(normalizeProjectConfig({
    storage: { engine: "postgres", url: PG_URL, queryTimeoutMs },
  }), 1);

  test("bounds cursor reads, closes on early exit, and clears completed cursor deadlines", async () => {
    const pool = createPool(200);
    const db = createPostgresEngineAdapter(pool, platform).createKysely();
    const read = spyOn(Client.prototype, "query");
    const fetches = () => read.mock.calls.filter(([query]) => typeof query === "string" && query.startsWith("FETCH "));
    try {
      const stream = db.selectNoFrom(sql<number>`generate_series(1, 10000)`.as("n")).stream(2);
      expect(await nextRow(stream)).toEqual({ n: 1 });
      expect(fetches()).toHaveLength(1);
      expect(fetches()[0]?.[0]).toStartWith("FETCH FORWARD 2 FROM");
      expect(pool.idleCount).toBe(0);
      await stream.return?.();
      expect(pool.idleCount).toBe(1);
      expect(fetches()).toHaveLength(1);
      const client = await pool.connect();
      try {
        expect((await client.query<{ n: number }>("SELECT count(*)::int AS n FROM pg_cursors")).rows).toEqual([{ n: 0 }]);
        // A cursor must clear pg's deadline instead of poisoning a later checkout.
        await Bun.sleep(250);
        expect((await client.query<{ n: number }>("SELECT 42 AS n")).rows).toEqual([{ n: 42 }]);
      } finally { client.release(); }
      for (const size of [0, 1, 20, 4294967297, Number.MAX_SAFE_INTEGER]) {
        const rows: { n: number }[] = [];
        for await (const row of db.selectNoFrom(sql<number>`generate_series(1, 3)`.as("n")).stream(size)) rows.push(row);
        expect(rows).toEqual([{ n: 1 }, { n: 2 }, { n: 3 }]);
      }
      expect(await db.selectNoFrom(sql<number>`1`.as("n")).where(sql<boolean>`false`).stream(1).next()).toEqual({ done: true, value: undefined });
      expect(pool.idleCount).toBe(1);
    } finally { read.mockRestore(); await db.destroy(); await pool.end(); }
  });

  test("uses the action transaction for reads and write RETURNING streams without committing it", async () => {
    const pool = createPool();
    const adapter = createPostgresEngineAdapter(pool, platform);
    const db = adapter.createKysely<{ streaming_items: { id: number } }>();
    try {
      await adapter.beginTransaction();
      await sql`CREATE TEMP TABLE streaming_items (id integer)`.execute(db);
      const inserted: { id: number }[] = [];
      for await (const row of db.insertInto("streaming_items").values([{ id: 1 }, { id: 2 }, { id: 3 }]).returningAll().stream(1)) {
        expect((await sql<{ n: number }>`SELECT 42 AS n`.execute(db)).rows).toEqual([{ n: 42 }]);
        inserted.push(row);
      }
      expect(inserted).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
      const readRows: { id: number }[] = [];
      for await (const row of db.selectFrom("streaming_items").selectAll().orderBy("id").stream(1)) {
        expect((await sql<{ n: number }>`SELECT 42 AS n`.execute(db)).rows).toEqual([{ n: 42 }]);
        readRows.push(row);
      }
      expect(readRows).toEqual(inserted);
      const changed: { id: number }[] = [];
      for await (const row of db.with("changed", (builder) => builder.updateTable("streaming_items")
        .set({ id: 5 }).where("id", "=", 2).returning("id").returningAll())
        .selectFrom("changed").selectAll().stream(1)) {
        expect((await sql<{ n: number }>`SELECT 42 AS n`.execute(db)).rows).toEqual([{ n: 42 }]);
        changed.push(row);
      }
      expect(changed).toEqual([{ id: 5 }]);
      for await (const row of db.selectFrom("streaming_items").selectAll().orderBy("id").stream(1)) {
        expect(row.id).toBe(1); break;
      }
      const updated: { id: number }[] = [];
      for await (const row of db.updateTable("streaming_items").set({ id: 4 }).where("id", "=", 3).returningAll().stream(1)) updated.push(row);
      expect(updated).toEqual([{ id: 4 }]);
      const deleted: { id: number }[] = [];
      for await (const row of db.deleteFrom("streaming_items").where("id", "=", 4).returningAll().stream(1)) deleted.push(row);
      expect(deleted).toEqual([{ id: 4 }]);
      const compiled = db.updateTable("streaming_items").set({ id: 10 }).compile();
      const metadata: QueryResult<unknown>[] = [];
      for await (const result of db.getExecutor().stream(compiled, 1)) metadata.push(result);
      expect(metadata).toEqual([{ rows: [], numAffectedRows: 2n }]);
      expect(pool.totalCount).toBe(1);
      expect(pool.idleCount).toBe(0);
      expect((await sql<{ n: number }>`SELECT count(*)::int AS n FROM pg_cursors`.execute(db)).rows).toEqual([{ n: 0 }]);
      await adapter.rollbackTransaction();
      expect((await pool.query<{ name: string | null }>("SELECT to_regclass('streaming_items') AS name")).rows).toEqual([{ name: null }]);
    } finally { await adapter.rollbackTransaction(); await db.destroy(); await pool.end(); }
  });

  test("preserves duplicate RETURNING columns and safely binds text, null, JSON, timestamps and binary values", async () => {
    const pool = createPool();
    const adapter = createPostgresEngineAdapter(pool, platform);
    type Row = { id: number; label: string; optional: string | null; data: Record<string, string>; happened_at: Date; bytes: Buffer };
    const db = adapter.createKysely<{ streaming_values: Row }>();
    const value: Row = { id: 1, label: "O'Reilly\\'; DROP TABLE streaming_values; --", optional: null,
      data: { quoted: "'\\" }, happened_at: new Date("2020-01-01T00:00:00Z"), bytes: Buffer.from([0, 1, 255]) };
    try {
      await adapter.beginTransaction();
      await sql`CREATE TEMP TABLE streaming_values (id integer, label text, optional text, data jsonb, happened_at timestamptz, bytes bytea)`.execute(db);
      const rows: Row[] = [];
      for await (const row of db.insertInto("streaming_values").values(value).returning("id").returningAll().stream(1)) {
        expect(await db.selectFrom("streaming_values").selectAll().where("label", "=", value.label).execute()).toEqual([value]);
        rows.push(row);
      }
      expect(rows).toEqual([value]);
      const selected: Row[] = [];
      for await (const row of db.selectFrom("streaming_values").selectAll().where("label", "=", value.label).stream(4294967297)) selected.push(row);
      expect(selected).toEqual([value]);
      expect((await sql<{ n: number }>`SELECT count(*)::int AS n FROM pg_prepared_statements WHERE name LIKE 'chimpbase_stream_%'`.execute(db)).rows).toEqual([{ n: 0 }]);
    } finally { await adapter.rollbackTransaction(); await db.destroy(); await pool.end(); }
  });

  test("discards failed standalone cursors and blocks writes in a failed runtime transaction", async () => {
    const pool = createPool();
    const adapter = createPostgresEngineAdapter(pool, platform);
    const db = adapter.createKysely<{ missing_stream_table: { id: number } }>();
    try {
      await expect(db.selectFrom("missing_stream_table").selectAll().stream(1).next()).rejects.toThrow("does not exist");
      expect(pool.totalCount).toBe(0);
      expect((await pool.query<{ n: number }>("SELECT 1 AS n")).rows).toEqual([{ n: 1 }]);
      await adapter.beginTransaction();
      await expect(db.selectFrom("missing_stream_table").selectAll().stream(1).next()).rejects.toThrow("does not exist");
      await expect(sql`SELECT 2`.execute(db)).rejects.toThrow("does not exist");
      await expect(adapter.commitTransaction()).rejects.toThrow("does not exist");
      expect(pool.totalCount).toBe(0);
      await adapter.rollbackTransaction();
      expect((await sql<{ n: number }>`SELECT 3 AS n`.execute(db)).rows).toEqual([{ n: 3 }]);
    } finally { await adapter.rollbackTransaction(); await db.destroy(); await pool.end(); }
  });

  test("cleans up a timed-out cursor and preserves the runtime transaction failure", async () => {
    const pool = createPool(60);
    const adapter = createPostgresEngineAdapter(pool, platform);
    const db = adapter.createKysely();
    try {
      await adapter.beginTransaction();
      await expect(db.selectNoFrom(sql`pg_sleep(1)`.as("delay")).stream(1).next()).rejects.toThrow();
      expect(pool.totalCount).toBe(0);
      await expect(sql`SELECT 1`.execute(db)).rejects.toThrow();
      await adapter.rollbackTransaction();
      expect((await pool.query<{ n: number }>("SELECT 1 AS n")).rows).toEqual([{ n: 1 }]);
    } finally { await adapter.rollbackTransaction(); await db.destroy(); await pool.end(); }
  });

  test("reports a cursor deadline reached while the consumer is paused without leaking its client", async () => {
    const pool = createPool(60);
    const db = createPostgresEngineAdapter(pool, platform).createKysely();
    try {
      const stream = db.selectNoFrom(sql<number>`generate_series(1, 10000)`.as("n")).stream(1);
      expect(await nextRow(stream)).toEqual({ n: 1 });
      await Bun.sleep(100);
      expect(pool.totalCount).toBe(0);
      await expect(stream.next()).rejects.toThrow("idle-in-transaction timeout");
      expect((await pool.query<{ n: number }>("SELECT 1 AS n")).rows).toEqual([{ n: 1 }]);
    } finally { await db.destroy(); await pool.end(); }
  });

  test("discards clients when a later fetch or early cursor close fails", async () => {
    const pool = createPool();
    const db = createPostgresEngineAdapter(pool, platform).createKysely();
    try {
      const failing = db.selectNoFrom(sql<number>`1 / (generate_series(1, 3) - 2)`.as("n")).stream(1);
      expect(await nextRow(failing)).toEqual({ n: -1 });
      await expect(failing.next()).rejects.toThrow("division by zero");
      expect(pool.totalCount).toBe(0);
      const stream = db.selectNoFrom(sql<number>`generate_series(1, 10000)`.as("n")).stream(1);
      expect(await nextRow(stream)).toEqual({ n: 1 });
      const failure = new Error("cursor close failed");
      const prototype = Client.prototype as unknown as { query(sql: string, params?: unknown[]): Promise<unknown> };
      const originalQuery = prototype.query;
      const close = spyOn(prototype, "query").mockImplementation(function (this: Client, query, params) {
        if (query.startsWith("CLOSE ")) return Promise.reject(failure);
        return originalQuery.call(this, query, params);
      });
      try { await expect(stream.return!()).rejects.toBe(failure); }
      finally { close.mockRestore(); }
      expect(pool.totalCount).toBe(0);
      expect((await pool.query<{ n: number }>("SELECT 1 AS n")).rows).toEqual([{ n: 1 }]);
    } finally { await db.destroy(); await pool.end(); }
  });
});
