import { afterEach, describe, expect, mock, spyOn, test } from "bun:test";
import { Pool } from "pg";
import { EventEmitter } from "node:events";

import { normalizeProjectConfig } from "../packages/core/index.ts";
import { createDefaultChimpbasePlatformShim } from "../packages/core/host.ts";
import { createPostgresEngineAdapter, ensurePostgresInternalTables, openPostgresPool, openPostgresStorage, PostgresPollingEventBus } from "../packages/postgres/src/index.ts";
import { action, v } from "../packages/runtime/index.ts";
import { createChimpbase } from "../packages/bun/src/library.ts";

const config = normalizeProjectConfig({ storage: { engine: "postgres", url: "postgres://unused" } });
const platform = createDefaultChimpbasePlatformShim();
// Narrow pg's overloaded methods to the promise API exercised here.
const poolPrototype = Pool.prototype as unknown as {
  connect(): Promise<unknown>;
  query(sql: string): Promise<unknown>;
  end(): Promise<void>;
};

afterEach(() => mock.restore());

describe("PostgreSQL storage startup", () => {
  (process.env.CHIMPBASE_TEST_PG_URL ? test : test.skip)("multipart finalization shares metadata writes and keeps its standalone transaction", async () => {
    const pool = openPostgresPool(normalizeProjectConfig({ storage: { engine: "postgres", url: process.env.CHIMPBASE_TEST_PG_URL } }));
    const adapter = createPostgresEngineAdapter(pool, platform);
    const uploadId = crypto.randomUUID();
    const original = { bucket: uploadId, key: "target", size: 1, etag: "before", contentType: "text/plain",
      metadata: { version: "before" }, driverRef: "before", createdAt: "2020-01-01T00:00:00.000Z", updatedAt: "2020-01-01T00:00:00.000Z" };
    const final = { ...original, size: 2, etag: "after", contentType: "application/json", metadata: { version: "after" },
      driverRef: "after", createdAt: "2021-01-01T00:00:00.000Z", updatedAt: "2021-01-01T00:00:00.000Z" };
    try {
      await ensurePostgresInternalTables(pool);
      await adapter.blobPutMetadata(original);
      await adapter.blobInitUpload({ uploadId, bucket: uploadId, key: "target", contentType: null, metadata: {},
        driverRef: "staging", createdAtMs: 0, expiresAtMs: 1 });
      await adapter.blobRecordPart({ uploadId, partNumber: 1, size: 2, etag: "part", driverRef: "staging", createdAt: final.createdAt });
      await expect(adapter.blobFinalizeUpload(uploadId, { ...final, updatedAt: "invalid" })).rejects.toThrow();
      expect((await adapter.blobGetMetadata(uploadId, "target"))?.etag).toBe("before");
      expect(await adapter.blobGetUpload(uploadId)).not.toBeNull();
      expect(await adapter.blobListParts(uploadId)).toHaveLength(1);
      await adapter.blobFinalizeUpload(uploadId, final);
      const stored = await adapter.blobGetMetadata(uploadId, "target");
      expect(stored).not.toBeNull();
      expect(stored).toMatchObject({ ...final, createdAt: stored!.createdAt, updatedAt: stored!.updatedAt });
      expect(Date.parse(stored!.createdAt)).toBe(Date.parse(original.createdAt));
      expect(Date.parse(stored!.updatedAt)).toBe(Date.parse(final.updatedAt));
      expect(await adapter.blobGetUpload(uploadId)).toBeNull();
      expect(await adapter.blobListParts(uploadId)).toEqual([]);
    } finally {
      try { await adapter.blobAbortUpload(uploadId); await adapter.blobDeleteMetadata(uploadId, "target"); }
      finally { await pool.end(); }
    }
  });

  test("normalizes bound placeholders without changing literals, quoted identifiers or comments", async () => {
    const query = spyOn(poolPrototype, "query").mockResolvedValue({ rows: [] });
    const pool = openPostgresPool(config);
    const sql = `SELECT '?1', 'it''s ?2', E'escaped\\' ?3', "quoted?4", "quote""?5",
      $$?6$$, $tag$?7$tag$, $é$?12$é$, $标签$?13$标签$, ?1, $1 /* ?8 /* ?9 */ ?10 */ -- ?11\r\n`;
    try {
      await createPostgresEngineAdapter(pool, platform).query(sql, ["actual"], v.object({}));
      expect(query.mock.calls[0]?.[0]).toBe(sql.replace(", ?1, $1", ", $1, $1"));
    } finally { await pool.end(); }
  });

  (process.env.CHIMPBASE_TEST_PG_URL ? test : test.skip)("public PostgreSQL queries preserve literal placeholders and bind actual parameters", async () => {
    const host = await createChimpbase({
      storage: { engine: "postgres", url: process.env.CHIMPBASE_TEST_PG_URL },
      registrations: [action("literalQuery", async (ctx) => await ctx.db.query(
        `SELECT '?1' AS literal, 'it''s ?2' AS apostrophe, E'escaped\\' ?3' AS escaped,
          $$?4$$ AS dollar, $tag$?5$tag$ AS tagged, $é$?11$é$ AS unicode,
          $标签$?12$标签$ AS unicodeLong, ?1 AS "bound?6", $2::int AS native
          /* ?7 /* ?8 */ ?9 */ -- ?10\n`,
        ["actual", 42],
      ))],
    });
    try {
      expect((await host.executeAction("literalQuery")).result).toEqual([{
        literal: "?1", apostrophe: "it's ?2", escaped: "escaped' ?3",
        dollar: "?4", tagged: "?5", unicode: "?11", unicodelong: "?12", "bound?6": "actual", native: 42,
      }]);
    } finally { await host.close(); }
  });

  (process.env.CHIMPBASE_TEST_PG_URL ? test : test.skip)("KV expiry advances during a public PostgreSQL action transaction", async () => {
    const key = `expiry-${crypto.randomUUID()}`;
    const host = await createChimpbase({
      storage: { engine: "postgres", url: process.env.CHIMPBASE_TEST_PG_URL },
      registrations: [
        action("checkExpiry", async (ctx) => {
          await ctx.kv.set(key, "original", { ttlMs: 20 });
          await ctx.db.query("SELECT pg_sleep(0.08)");
          expect(await ctx.kv.get(key)).toBeNull();
          expect(await ctx.kv.list({ prefix: key })).toEqual([]);
          expect(await ctx.kv.setIfAbsent(key, "replacement")).toBe(true);
          expect(await ctx.kv.get(key)).toBe("replacement");
          expect(await ctx.kv.setIfAbsent(key, "unexpected")).toBe(false);
        }),
        action("cleanupExpiry", async (ctx) => await ctx.kv.delete(key)),
      ],
    });
    try { await host.executeAction("checkExpiry"); }
    finally {
      try { await host.executeAction("cleanupExpiry"); }
      finally { await host.close(); }
    }
  });

  test("stream IDs reject bigint values outside the public numeric range", async () => {
    spyOn(poolPrototype, "query").mockResolvedValue({ rows: [{ id: "9007199254740993" }] });
    const pool = openPostgresPool(config);
    try {
      const adapter = createPostgresEngineAdapter(pool, platform);
      await expect(adapter.streamAppend("stream", "event", {})).rejects.toThrow("stream event id exceeds");
      await expect(adapter.streamRead("stream", {}, v.unknown())).rejects.toThrow("stream event id exceeds");
    } finally { await pool.end(); }
  });

  (process.env.CHIMPBASE_TEST_PG_URL ? test : test.skip)("public PostgreSQL stream IDs are numeric and work as pagination cursors", async () => {
    const stream = `numeric-stream-${crypto.randomUUID()}`;
    const host = await createChimpbase({
      storage: { engine: "postgres", url: process.env.CHIMPBASE_TEST_PG_URL },
      registrations: [
        action("checkStreamIds", async (ctx) => {
          const first = await ctx.stream.append(stream, "first", { position: 1 });
          const second = await ctx.stream.append(stream, "second", { position: 2 });
          expect(Number.isSafeInteger(first)).toBe(true);
          expect(second).toBeGreaterThan(first);
          expect((await ctx.stream.read(stream)).map((event) => event.id)).toEqual([first, second]);
          expect((await ctx.stream.read(stream, { sinceId: first })).map((event) => event.id)).toEqual([second]);
        }),
        action("cleanupStreamIds", async (ctx) => await ctx.db.query(
          "DELETE FROM _chimpbase_stream_events WHERE stream_name = ?1", [stream],
        )),
      ],
    });
    try { await host.executeAction("checkStreamIds"); }
    finally {
      try { await host.executeAction("cleanupStreamIds"); }
      finally { await host.close(); }
    }
  });

  test("invalid queue delays fail before a PostgreSQL query", async () => {
    const query = spyOn(poolPrototype, "query").mockResolvedValue({ rows: [] });
    const pool = openPostgresPool(config);
    try {
      const adapter = createPostgresEngineAdapter(pool, platform);
      for (const delayMs of [-1, NaN, Infinity, 0.5, Number.MAX_SAFE_INTEGER]) {
        await expect(adapter.queueEnqueue("invalid-delay", {}, { delayMs })).rejects.toThrow("delayMs");
      }
      expect(query).not.toHaveBeenCalled();
    } finally { await pool.end(); }
  });

  test("configures finite database deadlines and preserves programmatic overrides", async () => {
    const pool = openPostgresPool(config);
    expect(pool.options.connectionTimeoutMillis).toBe(5_000);
    expect(pool.options.query_timeout).toBe(30_000);
    expect(pool.options.statement_timeout).toBe(30_000);
    expect(pool.options.idle_in_transaction_session_timeout).toBe(30_000);
    // Idle failures are emitted after pg removes the client; they must be handled.
    expect(() => pool.emit("error", new Error("connection lost"))).not.toThrow();
    await pool.end();
    const custom = openPostgresPool(normalizeProjectConfig({ storage: {
      engine: "postgres", url: "postgres://unused", connectionTimeoutMs: 20, queryTimeoutMs: 60_000,
    } }));
    expect(custom.options.connectionTimeoutMillis).toBe(20);
    expect(custom.options.query_timeout).toBe(60_000);
    await custom.end();
    for (const timeout of [0, -1, Infinity, NaN, 1.5, 2_147_483_648]) {
      expect(() => openPostgresPool(normalizeProjectConfig({ storage: {
        engine: "postgres", url: "postgres://unused", queryTimeoutMs: timeout,
      } }))).toThrow("must be a positive integer");
    }
  });

  test("discards a failed BEGIN client and can start the next transaction", async () => {
    const failure = new Error("connection lost during BEGIN");
    const failedClient = Object.assign(new EventEmitter(), {
      query: mock(async () => { throw failure; }), release: mock(),
    });
    const nextClient = Object.assign(new EventEmitter(), {
      query: mock(async (_sql: string) => ({ rows: [] })), release: mock(),
    });
    const connect = spyOn(poolPrototype, "connect").mockResolvedValueOnce(failedClient).mockResolvedValue(nextClient);
    const pool = openPostgresPool(config);
    const adapter = createPostgresEngineAdapter(pool, platform);
    await expect(adapter.beginTransaction()).rejects.toBe(failure);
    expect(failedClient.release).toHaveBeenCalledWith(true);
    await adapter.rollbackTransaction();
    await adapter.beginTransaction();
    await adapter.commitTransaction();
    expect(connect).toHaveBeenCalledTimes(2);
    expect(nextClient.query.mock.calls.map(([sql]) => sql)).toEqual(["BEGIN", "COMMIT"]);
    expect(nextClient.release).toHaveBeenCalledTimes(1);
    await pool.end();
  });

  test("a caught query failure cannot continue writes outside its failed transaction", async () => {
    const failure = new Error("Query read timeout");
    const client = Object.assign(new EventEmitter(), {
      query: mock(async (sql: string) => {
        if (sql !== "BEGIN") throw failure;
        return { rows: [] };
      }), release: mock(),
    });
    spyOn(poolPrototype, "connect").mockResolvedValue(client);
    const pool = openPostgresPool(config);
    const adapter = createPostgresEngineAdapter(pool, platform);
    await adapter.beginTransaction();
    await expect(adapter.query("SELECT pg_sleep(100)", [], v.object({}))).rejects.toBe(failure);
    await expect(adapter.query("INSERT INTO table VALUES (1)", [], v.object({}))).rejects.toBe(failure);
    await expect(adapter.commitTransaction()).rejects.toBe(failure);
    await adapter.rollbackTransaction();
    expect(client.query).toHaveBeenCalledTimes(2);
    expect(client.release).toHaveBeenCalledTimes(1);
    expect(client.release).toHaveBeenCalledWith(true);
    await pool.end();
  });

  test("closes ordinary, RPC, and detached pools and reuses each isolated pool", async () => {
    const pools = new Set<unknown>();
    spyOn(poolPrototype, "connect").mockResolvedValue({
      query: async () => ({ rows: [] }), release: mock(),
    });
    spyOn(poolPrototype, "query").mockImplementation(async function (this: Pool) {
      pools.add(this);
      return { rows: [] };
    });
    const end = spyOn(poolPrototype, "end").mockResolvedValue(undefined);
    const resources = await openPostgresStorage(config, platform, [], []);
    await resources.createAdapter().query("SELECT 1", [], v.object({}));
    await resources.createAdapter("rpc").query("SELECT 1", [], v.object({}));
    await resources.createAdapter("rpc").query("SELECT 1", [], v.object({}));
    await resources.createAdapter("detached").query("SELECT 1", [], v.object({}));
    expect(pools.size).toBe(3);
    await resources.storage.close();
    expect(end).toHaveBeenCalledTimes(3);
  });

  for (const phase of ["named migrations", "inline migrations", "internal tables"] as const) {
    test(`closes the pool when ${phase} fail`, async () => {
      const failure = new Error(phase);
      if (phase !== "inline migrations") {
        spyOn(poolPrototype, "connect").mockRejectedValue(failure);
      } else {
        spyOn(poolPrototype, "query").mockRejectedValue(failure);
      }
      const end = spyOn(poolPrototype, "end").mockResolvedValue(undefined);

      await expect(openPostgresStorage(config, platform,
        phase === "named migrations" ? [{ name: "001", sql: "SELECT 1" }] : [],
        phase === "inline migrations" ? ["SELECT 1"] : [],
      )).rejects.toBe(failure);
      expect(end).toHaveBeenCalledTimes(1);
    });
  }

  test("awaits failed-pool cleanup and preserves the startup error if cleanup rejects", async () => {
    const failure = new Error("migration failed");
    spyOn(poolPrototype, "query").mockRejectedValue(failure);
    let finishClose = () => {};
    const closing = new Promise<void>((resolve) => { finishClose = resolve; });
    let notifyCloseStarted = () => {};
    const closeStarted = new Promise<void>((resolve) => { notifyCloseStarted = resolve; });
    spyOn(poolPrototype, "end").mockImplementation(async () => {
      notifyCloseStarted();
      await closing;
      throw new Error("close failed");
    });
    let settled = false;
    const startup = openPostgresStorage(config, platform, [], ["SELECT 1"]).catch((error: unknown) => {
      settled = true;
      return error;
    });

    await closeStarted;
    await Promise.resolve();
    expect(settled).toBe(false);
    finishClose();
    expect(await startup).toBe(failure);
  });

  test("runs migrations in order and keeps the pool open until storage.close", async () => {
    const queries: string[] = [];
    let released = false;
    const client = {
      async query(sql: string) { queries.push(sql); return { rows: [] }; },
      release() { released = true; },
    };
    spyOn(poolPrototype, "connect").mockResolvedValue(client);
    spyOn(poolPrototype, "query").mockImplementation(async (sql: string) => {
      queries.push(String(sql));
      return { rows: [] };
    });
    const end = spyOn(poolPrototype, "end").mockResolvedValue(undefined);

    const resources = await openPostgresStorage(config, platform,
      [{ name: "001", sql: "SELECT 'named migration'" }], ["SELECT 'inline migration'"],
    );
    const namedIndex = queries.indexOf("SELECT 'named migration'");
    const commitIndex = queries.indexOf("COMMIT");
    const inlineIndex = queries.indexOf("SELECT 'inline migration'");
    expect(namedIndex).toBeGreaterThan(-1);
    expect(commitIndex).toBeGreaterThan(namedIndex);
    expect(inlineIndex).toBeGreaterThan(commitIndex);
    expect(queries[inlineIndex + 1]).toBe("BEGIN");
    expect(queries[inlineIndex + 2]).toBe("SELECT pg_advisory_xact_lock(hashtext('_chimpbase_internal_tables'))");
    expect(queries[inlineIndex + 3]).toContain("CREATE TABLE IF NOT EXISTS _chimpbase_events");
    expect(queries.at(-1)).toBe("COMMIT");
    expect(released).toBe(true);
    expect(resources.eventBus).toBeInstanceOf(PostgresPollingEventBus);
    expect(resources.supportsConcurrentWorkers).toBe(true);
    expect(await resources.createAdapter().query("SELECT 'adapter'", [], v.object({}))).toEqual([]);
    expect(queries.at(-1)).toBe("SELECT 'adapter'");
    expect(end).not.toHaveBeenCalled();
    await resources.storage.close();
    expect(end).toHaveBeenCalledTimes(1);
  });

  test("discards and rolls back a client when internal schema DDL fails", async () => {
    const failure = new Error("schema DDL failed");
    const client = {
      query: mock(async (sql: string) => {
        if (sql.includes("CREATE TABLE")) throw failure;
        return { rows: [] };
      }),
      release: mock(),
    };
    spyOn(poolPrototype, "connect").mockResolvedValue(client);
    const pool = openPostgresPool(config);
    try {
      await expect(ensurePostgresInternalTables(pool)).rejects.toBe(failure);
      expect(client.query.mock.calls.map(([sql]) => sql).slice(0, 2)).toEqual([
        "BEGIN", "SELECT pg_advisory_xact_lock(hashtext('_chimpbase_internal_tables'))",
      ]);
      expect(client.query.mock.calls.some(([sql]) => sql === "COMMIT")).toBe(false);
      expect(client.release).toHaveBeenCalledTimes(1);
      expect(client.release).toHaveBeenCalledWith(true);
    } finally {
      await pool.end();
    }
  });
});
