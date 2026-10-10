import { afterEach, describe, expect, mock, spyOn, test } from "bun:test";
import { Pool } from "pg";

import { normalizeProjectConfig } from "../packages/core/index.ts";
import { createDefaultChimpbasePlatformShim } from "../packages/core/host.ts";
import { openPostgresStorage, PostgresPollingEventBus } from "../packages/postgres/src/index.ts";
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
