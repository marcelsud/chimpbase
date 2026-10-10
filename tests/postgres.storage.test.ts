import { afterEach, describe, expect, mock, spyOn, test } from "bun:test";
import { Pool } from "pg";
import { EventEmitter } from "node:events";

import { normalizeProjectConfig } from "../packages/core/index.ts";
import { createDefaultChimpbasePlatformShim } from "../packages/core/host.ts";
import { createPostgresEngineAdapter, openPostgresPool, openPostgresStorage, PostgresPollingEventBus } from "../packages/postgres/src/index.ts";
import { v } from "../packages/runtime/index.ts";

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
    spyOn(poolPrototype, "query").mockImplementation(async function (this: Pool) {
      pools.add(this);
      return { rows: [] };
    });
    const end = spyOn(poolPrototype, "end").mockResolvedValue(undefined);
    const resources = await openPostgresStorage(config, platform, [], []);
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
      if (phase === "named migrations") {
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
    expect(queries[inlineIndex + 1]).toContain("CREATE TABLE IF NOT EXISTS _chimpbase_events");
    expect(released).toBe(true);
    expect(resources.eventBus).toBeInstanceOf(PostgresPollingEventBus);
    expect(resources.supportsConcurrentWorkers).toBe(true);
    expect(await resources.createAdapter().query("SELECT 'adapter'", [], v.object({}))).toEqual([]);
    expect(queries.at(-1)).toBe("SELECT 'adapter'");
    expect(end).not.toHaveBeenCalled();
    await resources.storage.close();
    expect(end).toHaveBeenCalledTimes(1);
  });
});
