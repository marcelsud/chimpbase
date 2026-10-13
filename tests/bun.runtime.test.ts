import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { Database } from "bun:sqlite";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { bunRuntimeShim, ChimpbaseBunHost } from "../packages/bun/src/runtime.ts";
import { readJsonResponse } from "./support/http.ts";
import {
  action,
  cron,
  route,
  subscription,
  v,
  worker,
  workflow,
  type ChimpbaseDlqEnvelope,
} from "../packages/runtime/index.ts";
import { applySqliteMigrations, createDefaultChimpbasePlatformShim, defineChimpbaseMigrations, ensureSqliteInternalTables, normalizeProjectConfig } from "../packages/core/index.ts";
import { computeNextCronFireTime } from "../packages/core/cron.ts";

const cleanupDirs: string[] = [];

test("cron uses leading-star day restrictions and strict decimal tokens", async () => {
  for (const [expression, after, expected] of [
    ["0 0 */2 * 1", "2026-10-12", "2026-10-19"],
    ["0 0 */2,2 * 1", "2026-10-12", "2026-10-19"],
    ["0 0 1,*/2 * 1", "2026-10-12", "2026-10-13"],
    ["0 0 1-31 * 1", "2026-10-12", "2026-10-13"],
    ["0 0 */2 * */2", "2026-10-08", "2026-10-11"],
    ["0 0 13 * */2", "2026-10-13", "2026-12-13"],
    ["0 0 * * 7", "2026-10-12", "2026-10-18"],
    ["0 0 13 * 1", "2026-10-12", "2026-10-13"],
  ]) {
    expect(new Date(computeNextCronFireTime(expression, Date.parse(after + "T00:00:00Z"))).toISOString())
      .toBe(expected + "T00:00:00.000Z");
  }
  const now = Date.parse("2026-10-12T00:00:00Z");
  expect(new Date(computeNextCronFireTime("05-09/2 * * * *", now)).toISOString()).toBe("2026-10-12T00:05:00.000Z");
  for (const field of ["1-2-3", "0-", "1e1", "0x10", "+1", "1.0", "-0", "*//2", "*/1e1", "*/0", "*/-1", "*/9007199254740992", "1,,2", "1/", "*-2", "1-2/2/3"]) {
    expect(() => computeNextCronFireTime(`${field} * * * *`, now)).toThrow();
  }
  const host = await ChimpbaseBunHost.create({
    config: normalizeProjectConfig({ storage: { engine: "memory" } }),
    platform: { ...createDefaultChimpbasePlatformShim(), now: () => now },
  });
  try {
    host.register(cron("odd-mondays", "0 0 */2 * 1", () => {}));
    await host.syncCronSchedules();
    expect(await host.engine.getBlobsAdapter().query(
      "SELECT next_fire_at_ms FROM _chimpbase_cron_schedules WHERE schedule_name = 'odd-mondays'", [],
      v.object({ next_fire_at_ms: v.number() }),
    )).toEqual([{ next_fire_at_ms: Date.parse("2026-10-19T00:00:00Z") }]);
  } finally { await host.close(); }
});

test("invalid queue delays fail before SQLite insertion", async () => {
  const host = await createChimpbase({ storage: { engine: "memory" } });
  try {
    const adapter = host.engine.getBlobsAdapter();
    for (const delayMs of [-1, NaN, Infinity, 0.5, Number.MAX_SAFE_INTEGER]) {
      await expect(adapter.queueEnqueue("invalid-delay", {}, { delayMs })).rejects.toThrow("delayMs");
    }
    expect(await adapter.query("SELECT id FROM _chimpbase_queue_jobs", [], v.object({ id: v.number() }))).toEqual([]);
    await adapter.queueEnqueue("valid-delay", {}, { delayMs: 0 });
    expect(await adapter.query("SELECT queue_name FROM _chimpbase_queue_jobs", [], v.object({ queue_name: v.string() })))
      .toEqual([{ queue_name: "valid-delay" }]);
  } finally { await host.close(); }
});

for (const engine of ["sqlite", "memory"] as const) {
  test(`worker retention prunes only old completions and preserves SQLite job IDs (${engine})`, async () => {
    const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-worker-retention-"));
    cleanupDirs.push(projectDir);
    const now = Date.parse("2026-10-11T12:00:00.000Z");
    const cutoff = now - 86_400_000;
    const host = await ChimpbaseBunHost.create({
      projectDir,
      config: normalizeProjectConfig({ storage: { engine, path: "retention.db" }, worker: { retention: { maxAgeDays: 1 } } }),
      platform: { ...createDefaultChimpbasePlatformShim(), now: () => now },
    });
    try {
      const cleanup = host.registry.crons.get("__chimpbase.worker.cleanup");
      if (!cleanup) throw new Error("missing worker cleanup cron");
      host.registerAction("cleanup", async (ctx) => await cleanup.handler(ctx, {
        fireAt: new Date(now).toISOString(), fireAtMs: now, name: cleanup.name, schedule: cleanup.schedule,
      }));
      const adapter = host.engine.getBlobsAdapter();
      for (const [id, status, completedAt] of [
        [1, "completed", new Date(cutoff - 1).toISOString()],
        [2, "completed", "2000-01-01 00:00:00"],
        [3, "completed", new Date(cutoff).toISOString()],
        [4, "pending", "2000-01-01 00:00:00"],
        [5, "processing", "2000-01-01 00:00:00"],
        [6, "failed", "2000-01-01 00:00:00"],
        [7, "dlq", "2000-01-01 00:00:00"],
        [8, "completed", "2000-01-01 00:00:00"],
      ] as const) {
        await adapter.query(
          "INSERT INTO _chimpbase_queue_jobs (id, queue_name, payload_json, status, available_at_ms, completed_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
          [id, "retention-fixture", JSON.stringify({ id }), status, now + 60_000, completedAt], v.object({}),
        );
      }
      await host.executeAction("cleanup");
      const rows = await adapter.query("SELECT id FROM _chimpbase_queue_jobs ORDER BY id", [], v.object({ id: v.number() }));
      expect(rows.map((row) => row.id)).toEqual([3, 4, 5, 6, 7, 8]);
      await adapter.queueEnqueue("new-job", {}, { delayMs: 0 });
      expect(await adapter.query("SELECT id FROM _chimpbase_queue_jobs WHERE queue_name = 'new-job'", [], v.object({ id: v.number() })))
        .toEqual([{ id: 9 }]);
      expect(await adapter.query("SELECT payload_json FROM _chimpbase_queue_jobs WHERE id = 8", [], v.object({ payload_json: v.string() })))
        .toEqual([{ payload_json: JSON.stringify({ id: 8 }) }]);
      await host.executeAction("cleanup");
      expect(await adapter.query("SELECT id FROM _chimpbase_queue_jobs WHERE id = 8", [], v.object({ id: v.number() }))).toEqual([]);
    } finally { await host.close(); }
    const disabled = await createChimpbase({ storage: { engine: "memory" }, workerRuntime: { retention: { enabled: false } } });
    try { expect(disabled.registry.crons.has("__chimpbase.worker.cleanup")).toBe(false); }
    finally { await disabled.close(); }
  });
}

test("SQLite internal upgrades propagate failures and can retry without losing KV data", async () => {
  const db = new Database(":memory:");
  try {
    db.exec("CREATE TABLE _chimpbase_kv (key TEXT PRIMARY KEY, value_json TEXT NOT NULL, updated_at TEXT DEFAULT CURRENT_TIMESTAMP)");
    db.query("INSERT INTO _chimpbase_kv (key, value_json) VALUES (?, ?)").run("saved", JSON.stringify("retained"));
    await expect(ensureSqliteInternalTables({
      exec(sql) {
        if (sql.startsWith("ALTER TABLE _chimpbase_kv")) throw new Error("upgrade failed");
        return db.exec(sql);
      },
      query(sql) { return db.query(sql); },
    })).rejects.toThrow("upgrade failed");
    await ensureSqliteInternalTables(db);
    await ensureSqliteInternalTables(db);
    expect(db.query("SELECT key, value_json, expires_at FROM _chimpbase_kv").all()).toEqual([
      { key: "saved", value_json: JSON.stringify("retained"), expires_at: null },
    ]);
  } finally { db.close(); }
});

test("named SQLite migrations reject transaction control without committing pending work", () => {
  const db = new Database(":memory:");
  try {
    applySqliteMigrations(db, [{ name: "initial", sql: "CREATE TABLE migrated_items (value TEXT)" }]);
    for (const transaction of [
      "BEGIN", "BEGIN IMMEDIATE", "COMMIT", "END TRANSACTION", "ROLLBACK", "SAVEPOINT user_savepoint", "RELEASE user_savepoint",
      "/* /* */ COMMIT; -- */\nINSERT INTO migrated_items VALUES ('escaped')",
      "SELECT $value$; COMMIT; -- $value$",
      "SELECT e'\\' FROM (SELECT 1 AS e); COMMIT; -- '",
    ]) {
      expect(() => applySqliteMigrations(db, [
        { name: "pending", sql: "INSERT INTO migrated_items VALUES ('pending')" },
        { name: "escape", sql: `-- comment\nINSERT INTO migrated_items VALUES ('escape'); /* comment */ ${transaction};` },
        { name: "failure", sql: "INSERT INTO missing_table VALUES (1)" },
      ])).toThrow("cannot control the migration transaction");
      expect(db.inTransaction).toBe(false);
      expect(db.query("SELECT * FROM migrated_items").all()).toEqual([]);
      expect(db.query("SELECT name FROM _chimpbase_migrations").all()).toEqual([{ name: "initial" }]);
    }
  } finally { db.close(); }
});

test("named SQLite migrations allow trigger bodies and preserve errors from automatic rollback", () => {
  const db = new Database(":memory:");
  try {
    applySqliteMigrations(db, [{ name: "initial", sql: `
      CREATE TABLE migrated_items (value TEXT);
      CREATE TABLE \`quoted'; COMMIT\` ([quoted'; END] TEXT);
      CREATE TRIGGER reject_insert BEFORE INSERT ON migrated_items BEGIN
        SELECT CASE WHEN NEW.value = 'rejected' THEN RAISE(ROLLBACK, 'migration rejected by trigger') END;
        SELECT '; COMMIT; END;';
      END;
      INSERT INTO migrated_items VALUES ('initial');
    ` }]);
    expect(() => applySqliteMigrations(db, [
      { name: "update", sql: "UPDATE migrated_items SET value = 'pending'" },
      { name: "rejected", sql: "INSERT INTO migrated_items VALUES ('rejected')" },
    ])).toThrow("migration rejected by trigger");
    expect(db.inTransaction).toBe(false);
    expect(db.query("SELECT * FROM migrated_items").all()).toEqual([{ value: "initial" }]);
    expect(db.query("SELECT name FROM _chimpbase_migrations").all()).toEqual([{ name: "initial" }]);
    applySqliteMigrations(db, [{ name: "retry", sql: "INSERT INTO migrated_items VALUES ('retry')" }]);
    expect(db.query("SELECT * FROM migrated_items").all()).toEqual([{ value: "initial" }, { value: "retry" }]);
    expect(() => applySqliteMigrations(db, [{ name: "trigger_escape", sql: `
      CREATE TRIGGER harmless AFTER INSERT ON migrated_items BEGIN SELECT 1; END;
      COMMIT;
    ` }])).toThrow("cannot control the migration transaction");
    expect(db.query("SELECT name FROM sqlite_master WHERE type = 'trigger' AND name = 'harmless'").all()).toEqual([]);
  } finally { db.close(); }
});

test("named SQLite migrations run once, apply new names and roll back failed batches", () => {
  const db = new Database(":memory:");
  const initial = { name: "001_create", sql: "CREATE TABLE migrated_items (value TEXT)" };
  const insert = { name: "002_insert", sql: "INSERT INTO migrated_items VALUES ('once')" };
  try {
    applySqliteMigrations(db, [initial, insert]);
    applySqliteMigrations(db, [initial, insert]);
    expect(db.query("SELECT value FROM migrated_items").all()).toEqual([{ value: "once" }]);

    const pending = { name: "003_update", sql: "UPDATE migrated_items SET value = 'updated'" };
    expect(() => applySqliteMigrations(db, [pending, {
      name: "004_bad", sql: "INSERT INTO missing_table VALUES (1)",
    }])).toThrow("missing_table");
    expect(db.query("SELECT value FROM migrated_items").all()).toEqual([{ value: "once" }]);
    expect(db.query("SELECT name FROM _chimpbase_migrations ORDER BY name").all()).toEqual([
      { name: initial.name }, { name: insert.name },
    ]);

    applySqliteMigrations(db, [initial, insert, pending]);
    expect(db.query("SELECT value FROM migrated_items").all()).toEqual([{ value: "updated" }]);
  } finally {
    db.close();
  }
});

test("shared SQLite claims only registered cron, subscription and workflow destinations", async () => {
  const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-shared-destinations-"));
  cleanupDirs.push(projectDir);
  const storage = { engine: "sqlite" as const, path: join(projectDir, "shared.sqlite") };
  const calls: string[] = [];
  const a = await createChimpbase({
    projectDir, storage,
    registrations: [
      cron("cron-a", "* * * * *", () => { calls.push("cron"); }),
      subscription("shared", () => { calls.push("left"); }, { dispatch: "async", name: "left" }),
      subscription("shared", () => { throw new Error("right belongs to b"); }, { dispatch: "async", name: "right" }),
      workflow({ name: "versioned", version: 1, initialState: () => ({}),
        run(ctx) { calls.push("workflow"); return ctx.complete(); },
      }),
      action("launch", async (ctx) => {
        ctx.pubsub.publish("shared", {});
        await ctx.workflow.start("versioned", {}, { workflowId: "one" });
      }),
    ],
  });
  const b = await createChimpbase({
    projectDir, storage,
    registrations: [
      cron("cron-b", "* * * * *", () => { throw new Error("cron b is not due"); }),
      subscription("shared", () => { calls.push("right"); }, { dispatch: "async", name: "right" }),
      workflow({ name: "versioned", version: 2, initialState: () => ({}), run(ctx) { return ctx.complete(); } }),
    ],
  });
  try {
    await a.syncCronSchedules();
    await b.syncCronSchedules();
    const adapter = a.engine.getBlobsAdapter();
    const schedules = await adapter.query(
      "SELECT schedule_name FROM _chimpbase_cron_schedules WHERE schedule_name IN ('cron-a', 'cron-b') ORDER BY schedule_name",
      [], v.object({ schedule_name: v.string() }),
    );
    expect(schedules).toEqual([{ schedule_name: "cron-a" }, { schedule_name: "cron-b" }]);
    await adapter.query("UPDATE _chimpbase_cron_schedules SET next_fire_at_ms = ?1 WHERE schedule_name = 'cron-a'", [Date.now() - 120_000], v.object({}));
    expect(await b.processNextCronSchedule()).toBeNull();
    await a.executeAction("launch");
    a.registry.subscriptions.set("shared", (a.registry.subscriptions.get("shared") ?? []).filter((entry) => entry.name === "left"));
    expect((await a.processNextCronSchedule())?.scheduleName).toBe("cron-a");
    expect(await b.processNextQueueJob()).not.toBeNull();
    expect(calls).toEqual(["right"]);
    expect(await b.processNextQueueJob()).toBeNull();
    const pending = await adapter.query(
      "SELECT attempt_count FROM _chimpbase_queue_jobs WHERE status = 'pending' ORDER BY id",
      [], v.object({ attempt_count: v.number() }),
    );
    expect(pending).toEqual([{ attempt_count: 0 }, { attempt_count: 0 }, { attempt_count: 0 }]);
    await a.drain();
    expect([...calls].sort()).toEqual(["cron", "left", "right", "workflow"]);
    const completed = await adapter.query(
      "SELECT status, attempt_count FROM _chimpbase_queue_jobs ORDER BY id",
      [], v.object({ status: v.string(), attempt_count: v.number() }),
    );
    expect(completed).toEqual(Array.from({ length: 4 }, () => ({ status: "completed", attempt_count: 1 })));
  } finally {
    await a.close();
    await b.close();
  }
});

for (const name of [undefined, "shared-name"]) {
  test(`async publishing delivers shared ${name === undefined ? "unnamed" : "named"} subscription identities once`, async () => {
    const calls: string[] = [];
    const host = await createChimpbase({
      storage: { engine: "memory" },
      registrations: [
        action("emit", (ctx) => { ctx.pubsub.publish("shared", {}); }),
        subscription("shared", () => { calls.push("left"); }, { dispatch: "async", name }),
        subscription("shared", () => { calls.push("right"); }, { dispatch: "async", name }),
      ],
    });
    try {
      await host.executeAction("emit");
      expect(await host.processNextQueueJob()).not.toBeNull();
      expect(calls).toEqual(["left", "right"]);
      expect(await host.processNextQueueJob()).toBeNull();
    } finally { await host.close(); }
  });
}

for (const engine of ["memory", "sqlite"] as const) {
  test(`shared SQLite Kysely preserves query results, streaming and transaction restrictions (${engine})`, async () => {
    const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-kysely-"));
    cleanupDirs.push(projectDir);
    const host = await createChimpbase({
      projectDir, storage: { engine },
      registrations: [action("verifyKysely", async (ctx) => {
        const db = ctx.db.kysely<{ kysely_items: { id: number; label: string } }>();
        await db.schema.createTable("kysely_items")
          .addColumn("id", "integer", (column) => column.primaryKey())
          .addColumn("label", "text", (column) => column.notNull()).execute();
        const inserted = await db.insertInto("kysely_items")
          .values(Array.from({ length: 5 }, (_, i) => ({ id: i + 1, label: `item-${i + 1}` }))).executeTakeFirst();
        expect(inserted.numInsertedOrUpdatedRows).toBe(5n);
        expect(inserted.insertId).toBe(5n);
        const updated = await db.updateTable("kysely_items").set({ label: "updated" }).where("id", "=", 1).executeTakeFirst();
        expect(updated.numUpdatedRows).toBe(1n);
        const deleted = await db.deleteFrom("kysely_items").where("id", "=", 5).executeTakeFirst();
        expect(deleted.numDeletedRows).toBe(1n);
        const expected = [{ id: 1, label: "updated" }, ...[2, 3, 4].map((id) => ({ id, label: `item-${id}` }))];
        expect(await db.selectFrom("kysely_items").selectAll().orderBy("id").execute()).toEqual(expected);
        for (const chunkSize of [0, 2, 20]) {
          const rows: { id: number; label: string }[] = [];
          for await (const row of db.selectFrom("kysely_items").selectAll().orderBy("id").stream(chunkSize)) rows.push(row);
          expect(rows).toEqual(expected);
        }
        await expect(db.transaction().execute(async () => {})).rejects.toThrow("runtime-managed transactions");
        await db.destroy();
      })],
    });
    try { await host.executeAction("verifyKysely"); }
    finally { await host.close(); }
  });
}

for (const engine of ["memory", "sqlite"] as const) {
  for (const scenario of ["single failure", "exhausted retries", "recovered retry"] as const) {
    test(`workflow ${scenario} persists status and clears leases (${engine})`, async () => {
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-workflow-failure-"));
      cleanupDirs.push(projectDir);
      let attempts = 0;
      const maxAttempts = scenario === "single failure" ? 1 : 2;
      const host = await createChimpbase({
        project: { name: "workflow-failure" }, projectDir,
        storage: { engine }, worker: { maxAttempts, retryDelayMs: 0 },
        registrations: [
          workflow({
            name: "failure", version: 1, initialState: () => ({}),
            run(ctx) {
              attempts += 1;
              if (scenario === "recovered retry" && attempts === 2) return ctx.complete(ctx.state);
              throw new Error(`failure ${attempts}`);
            },
          }),
          action("launch", async (ctx) => await ctx.workflow.start("failure", {})),
          action("inspect", async (ctx) => await ctx.db.query(
            `SELECT w.status, w.last_error, w.lease_token, w.lease_expires_at_ms,
              j.status AS job_status, j.last_error AS job_error, j.attempt_count,
              j.lease_expires_at_ms AS job_lease
             FROM _chimpbase_workflow_instances w CROSS JOIN _chimpbase_queue_jobs j`, [],
            v.object({
              status: v.string(), last_error: v.string().nullable(), lease_token: v.string().nullable(),
              lease_expires_at_ms: v.number().nullable(), job_status: v.string(),
              job_error: v.string().nullable(), attempt_count: v.number(), job_lease: v.number().nullable(),
            }),
          )),
        ],
      });
      const inspect = async () => (await host.executeAction("inspect")).result;
      try {
        await host.executeAction("launch");
        await expect(host.processNextQueueJob()).rejects.toThrow("failure 1");
        expect(await inspect()).toEqual([{
          status: maxAttempts === 1 ? "failed" : "running", last_error: maxAttempts === 1 ? "failure 1" : null,
          lease_token: null, lease_expires_at_ms: null, job_status: maxAttempts === 1 ? "failed" : "pending",
          job_error: "failure 1", attempt_count: 1, job_lease: null,
        }]);
        if (maxAttempts === 2) {
          if (scenario === "recovered retry") await host.processNextQueueJob();
          else await expect(host.processNextQueueJob()).rejects.toThrow("failure 2");
          expect(await inspect()).toEqual([{
            status: scenario === "recovered retry" ? "completed" : "failed",
            last_error: scenario === "recovered retry" ? null : "failure 2",
            lease_token: null, lease_expires_at_ms: null,
            job_status: scenario === "recovered retry" ? "completed" : "failed",
            job_error: scenario === "recovered retry" ? "failure 1" : "failure 2", attempt_count: 2, job_lease: null,
          }]);
        }
        expect(await host.processNextQueueJob()).toBeNull();
      } finally {
        await host.close();
      }
    });
  }
}
for (const engine of ["memory", "sqlite"] as const) {
  for (const scenario of ["early signal", "immediate timeout"] as const) {
    for (const callback of [false, true]) {
      test(`workflow keeps directive state on ${scenario}, callback=${callback} (${engine})`, async () => {
        const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-workflow-directive-state-"));
        cleanupDirs.push(projectDir);
        const definition = workflow<unknown, { phase: string; marker: string }>({
          name: "directive-state", version: 1,
          initialState: () => ({ phase: "initial", marker: "old" }),
          run(ctx) {
            if (ctx.state.phase === "done") return ctx.complete();
            return ctx.waitForSignal("ready", {
              state: { phase: callback ? "waiting" : "done", marker: "new" },
              timeoutMs: scenario === "immediate timeout" ? 0 : undefined,
              onSignal: callback ? ({ state }) => ({ ...state, phase: "done" }) : undefined,
              onTimeout: callback ? ({ state }) => ({ ...state, phase: "done" }) : "continue",
            });
          },
        });
        const host = await createChimpbase({
          projectDir, storage: { engine },
          registrations: [
            definition,
            action("launch", async (ctx) => {
              await ctx.workflow.start(definition, {}, { workflowId: "flow" });
              if (scenario === "early signal") await ctx.workflow.signal("flow", "ready", {});
            }),
            action("inspect", async (ctx) => ctx.workflow.get("flow")),
          ],
        });
        try {
          await host.executeAction("launch");
          await host.drain();
          expect((await host.executeAction("inspect")).result).toMatchObject({
            status: "completed", state: { phase: "done", marker: "new" },
          });
        } finally {
          await host.close();
        }
      });
    }
  }
}
const countRowValidator = v.object({ count: v.number() });
const detailRowValidator = v.object({ detail: v.string() });
const itemRowValidator = v.object({
  amount: v.number(),
  id: v.number(),
  label: v.string(),
});
const rowIdValidator = v.object({ id: v.number() });
function isBunServer(value: unknown): value is Bun.Server<unknown> {
  return typeof value === "object"
    && value !== null
    && "stop" in value
    && typeof value.stop === "function";
}



afterEach(async () => {
  while (cleanupDirs.length > 0) {
    const dir = cleanupDirs.pop();
    if ((dir !== undefined && dir.length > 0)) await rm(dir, { recursive: true, force: true });
  }
});

async function bootInlineApp(overrides?: {
  storage?: "memory" | "sqlite";
  projectDir?: string;
}): Promise<{
  host: Awaited<ReturnType<typeof createChimpbase>>;
  started: Awaited<ReturnType<Awaited<ReturnType<typeof createChimpbase>>["start"]>>;
  baseUrl: string;
}> {
  const createItem = action({
    name: "createItem",
    args: v.object({ label: v.string(), amount: v.number() }),
    async handler(ctx, input) {
      const [row] = await ctx.db.query(
        "INSERT INTO items (label, amount) VALUES (?1, ?2) RETURNING id",
        [input.label, input.amount],
        rowIdValidator,
      );
      ctx.pubsub.publish("item.created", { id: row.id, label: input.label, amount: input.amount });
      return row;
    },
  });

  const listItems = action({
    name: "listItems",
    async handler(ctx) {
      return await ctx.db.query(
        "SELECT id, label, amount FROM items ORDER BY id",
        undefined,
        itemRowValidator,
      );
    },
  });

  const listNotifications = action({
    name: "listNotifications",
    async handler(ctx) {
      return await ctx.db.query(
        "SELECT detail FROM notifications ORDER BY id",
        undefined,
        detailRowValidator,
      );
    },
  });

  const auditItemCreated = async (
    ctx: Parameters<typeof createItem.handler>[0],
    event: { id: number; label: string; amount: number },
  ) => {
    await ctx.db.query(
      "INSERT INTO audit_log (item_id, label) VALUES (?1, ?2)",
      [event.id, event.label],
    );
    await ctx.enqueue("item.notify", event);
  };

  const notifyItem = async (
    ctx: Parameters<typeof createItem.handler>[0],
    payload: { id: number; label: string },
  ) => {
    ctx.log.info("notifying", { id: payload.id });
    ctx.metric("items.notified", 1);
    await ctx.db.query(
      "INSERT INTO notifications (item_id, detail) VALUES (?1, ?2)",
      [payload.id, `notified ${payload.label}`],
    );
  };

  const notifyItemDlq = async (
    ctx: Parameters<typeof createItem.handler>[0],
    envelope: ChimpbaseDlqEnvelope<{ id: number }>,
  ) => {
    await ctx.db.query(
      "INSERT INTO notifications (item_id, detail) VALUES (?1, ?2)",
      [envelope.payload.id, `dlq:${envelope.error}`],
    );
  };

  const snapshotCounts = async (ctx: Parameters<typeof createItem.handler>[0]) => {
    const [row] = await ctx.db.query("SELECT COUNT(*) AS count FROM items", undefined, countRowValidator);
    await ctx.db.query(
      "INSERT INTO snapshots (total_count) VALUES (?1)",
      [Number(row?.count ?? 0)],
    );
  };

  const apiRoute = route("api", async (request, env) => {
    const url = new URL(request.url);
    if (url.pathname !== "/items") return null;
    if (request.method === "POST") {
      const body = (await request.json()) as { label: string; amount: number };
      const item = await env.action("createItem", body);
      return Response.json(item, { status: 201 });
    }
    if (request.method === "GET") {
      const items = await env.action("listItems", {});
      return Response.json(items);
    }
    return null;
  });

  const migrations = defineChimpbaseMigrations({
    sqlite: [
      {
        name: "001_init",
        sql: `
          CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            label TEXT NOT NULL,
            amount INTEGER NOT NULL
          );
          CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id INTEGER NOT NULL,
            label TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
          );
          CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id INTEGER NOT NULL,
            detail TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
          );
          CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            total_count INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
          );
        `,
      },
    ],
  });

  const host = await createChimpbase({
    migrations,
    project: { name: "inline-bun-runtime-test" },
    registrations: [
      createItem,
      listItems,
      listNotifications,
      apiRoute,
      subscription("item.created", auditItemCreated, {
        idempotent: true,
        name: "auditItemCreated",
      }),
      worker("item.notify", notifyItem),
      worker("item.notify.dlq", notifyItemDlq, { dlq: false }),
      cron("items.snapshot", "*/5 * * * *", snapshotCounts),
    ],
    storage: overrides?.storage === "sqlite"
      ? { engine: "sqlite", path: join(overrides.projectDir ?? tmpdir(), "runtime-test.db") }
      : { engine: "memory" },
    server: { port: 0 },
    subscriptions: { dispatch: "sync" },
    projectDir: overrides?.projectDir,
  });

  const started = await host.start();
  const port = started.server?.port;
  if (!(port !== undefined && port > 0)) throw new Error("server failed to bind a port");
  return { host, started, baseUrl: `http://127.0.0.1:${port}` };
}

describe("bun runtime regression — inline fixtures", () => {
  for (const engine of ["memory", "sqlite"] as const) {
    test(`caught child failures roll back parent writes, events and jobs (${engine})`, async () => {
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-rollback-only-"));
      cleanupDirs.push(projectDir);
      const failure = new Error("child failed after writing");
      let deliveries = 0;
      const child = action("failingChild", async (ctx) => {
        await ctx.kv.set("child", true);
        await ctx.db.query("INSERT INTO effects VALUES ('child')");
        await ctx.queue.enqueue("effects", {});
        ctx.pubsub.publish("child.written", {});
        throw failure;
      });
      const parent = action("catchingParent", async (ctx) => {
        await ctx.kv.set("parent", true);
        try { await ctx.action(child); } catch (error) { expect(error).toBe(failure); }
        await ctx.kv.set("afterCatch", true);
        return "caught";
      });
      const host = await createChimpbase({
        storage: { engine, path: "rollback.db" }, projectDir,
        migrationsSql: ["CREATE TABLE effects (label TEXT)"],
        registrations: [child, parent,
          subscription("child.written", () => { deliveries += 1; }),
          worker("effects", () => { deliveries += 1; }),
          action("inspectRollback", async (ctx) => ({
            keys: await ctx.kv.list(), effects: await ctx.db.query("SELECT * FROM effects"),
            events: await ctx.db.query("SELECT * FROM _chimpbase_events"),
            jobs: await ctx.db.query("SELECT * FROM _chimpbase_queue_jobs"),
          })),
          action("healthyAfterRollback", async (ctx) => { await ctx.kv.set("healthy", true); return "committed"; }),
        ],
      });
      try {
        await expect(host.executeAction(parent)).rejects.toBe(failure);
        expect((await host.executeAction("inspectRollback")).result).toEqual({ keys: [], effects: [], events: [], jobs: [] });
        expect(deliveries).toBe(0);
        expect((await host.executeAction("healthyAfterRollback")).result).toBe("committed");
      } finally { await host.close(); }
    });

    test(`slow HTTP parsing leaves routes, actions and workers available (${engine})`, async () => {
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-route-concurrency-"));
      cleanupDirs.push(projectDir);
      const entered = Promise.withResolvers<void>();
      const parsed = Promise.withResolvers<void>();
      const release = Promise.withResolvers<void>();
      const host = await createChimpbase({
        storage: { engine, path: "routes.db" }, projectDir,
        registrations: [
          action("writeRoute", async (ctx, key: string) => { await ctx.kv.set(key, true); ctx.pubsub.publish(key, {}); ctx.log.info(key); return key; }),
          action("enqueueWhileParsing", async (ctx) => { await ctx.queue.enqueue("whileParsing", {}); }),
          worker("whileParsing", async (ctx) => { await ctx.kv.set("worker", true); }),
          route("context", (request, env) => { env.set("path", new URL(request.url).pathname); return null; }),
          route("POST", "/slow", async (request, env) => {
            entered.resolve();
            const body: unknown = await request.json();
            parsed.resolve();
            await release.promise;
            expect(body).toEqual({ ready: true });
            expect(env.get("path")).toBe("/slow");
            return new Response(v.string().parse(await env.action("writeRoute", "slow")));
          }),
          route("GET", "/fast", async (_request, env) => {
            expect(env.get("path")).toBe("/fast");
            return new Response(v.string().parse(await env.action("writeRoute", "fast")));
          }),
          action("readRoutes", async (ctx) => await ctx.kv.list()),
        ],
        telemetry: { persist: { log: true, metric: false, trace: false } },
      });
      const stream = new TransformStream<Uint8Array, Uint8Array>();
      const writer = stream.writable.getWriter();
      const slow = host.executeRoute(new Request("http://test/slow", { method: "POST", body: stream.readable }));
      try {
        await entered.promise;
        const fast = await Promise.race([
          host.executeRoute(new Request("http://test/fast")),
          Bun.sleep(1000).then(() => { throw new Error("fast request blocked by body parsing"); }),
        ]);
        expect(await fast.response?.text()).toBe("fast");
        expect(fast.emittedEvents).toMatchObject([{ name: "fast" }]);
        await host.executeAction("enqueueWhileParsing");
        expect(await host.processNextQueueJob()).not.toBeNull();
        expect((await host.executeAction("readRoutes")).result).toEqual(["fast", "worker"]);
        await writer.write(new TextEncoder().encode('{"ready":true}'));
        await writer.close();
        await parsed.promise;
        expect((await host.executeAction("readRoutes")).result).toEqual(["fast", "worker"]);
        release.resolve();
        const completed = await slow;
        expect(await completed.response?.text()).toBe("slow");
        expect(completed.emittedEvents).toMatchObject([{ name: "slow" }]);
        expect(host.drainTelemetryRecords().filter((record) => record.kind === "log").map((record) => record.message).sort())
          .toEqual(["fast", "slow"]);
      } finally {
        release.resolve();
        await writer.abort().catch(() => {});
        await slow.catch(() => {});
        await host.close();
      }
    });

    test(`route actions commit independently and serialize concurrent calls (${engine})`, async () => {
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-action-boundaries-"));
      cleanupDirs.push(projectDir);
      const host = await createChimpbase({
        storage: { engine, path: "actions.db" }, projectDir,
        registrations: [
          action("increment", async (ctx) => {
            const value = (await ctx.kv.get("count", v.number())) ?? 0;
            await Bun.sleep(2);
            await ctx.kv.set("count", value + 1);
            return value + 1;
          }),
          action("readCount", async (ctx) => await ctx.kv.get("count")),
          route("GET", "/increments", async (_request, env) => {
            expect(await Promise.all([env.action("increment"), env.action("increment")])).toEqual([1, 2]);
            throw new Response("later route failure", { status: 409 });
          }),
        ],
      });
      try {
        expect((await host.executeRoute(new Request("http://test/increments"))).response?.status).toBe(409);
        expect((await host.executeAction("readCount")).result).toBe(2);
        expect(await Promise.all([host.routeEnv().action("increment"), host.routeEnv().action("increment")])).toEqual([3, 4]);
      } finally { await host.close(); }
    });

    test(`invalid HTTP action input is 400 while handler and result failures remain errors (${engine})`, async () => {
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-http-validation-"));
      cleanupDirs.push(projectDir);
      let called = false;
      const validInput = action({ name: "validInput", args: v.string(), handler: () => { called = true; return "ok"; } });
      const badResult = action({ name: "badResult", args: v.string(), result: v.string(), handler: async (ctx) => {
        await ctx.kv.set("invalidResult", true);
        return 42 as unknown as string;
      } });
      let host: Awaited<ReturnType<typeof createChimpbase>>;
      host = await createChimpbase({
        storage: { engine, path: "validation.db" }, projectDir,
        server: { port: 0 },
        registrations: [validInput, badResult,
          action("catchInputFailure", async (ctx) => {
            await ctx.kv.set("invalidParent", true);
            try { await ctx.action("validInput", 42); } catch { /* The root transaction must still roll back. */ }
            return "caught";
          }),
          action("beforeInputFailure", async (ctx) => {
            await ctx.kv.set("beforeInputFailure", true);
            ctx.pubsub.publish("beforeInputFailure", {});
          }),
          action("badHandler", async (ctx) => { await ctx.kv.set("badHandler", true); throw new Error("handler failed"); }),
          action("badDatabase", async (ctx) => { await ctx.db.query("INSERT INTO missing_table VALUES (1)"); }),
          action("validationKeys", async (ctx) => await ctx.kv.list()),
          route("POST", "/input", async (request, env) => {
            const body: unknown = await request.json();
            return Response.json(await env.action("validInput", body));
          }),
          route("GET", "/caught-input", async (_request, env) => Response.json(await env.action("catchInputFailure"))),
          route("GET", "/reference-arity", async (_request, env) => {
            // @ts-expect-error Runtime validation also covers untyped HTTP callers.
            return Response.json(await env.action(validInput, "input", "extra"));
          }),
          route("GET", "/host-reference-arity", async () => {
            // @ts-expect-error Runtime validation also covers untyped host callers.
            return Response.json((await host.executeAction(validInput, "input", "extra")).result);
          }),
          route("GET", "/callable-reference-arity", async () => {
            // @ts-expect-error Runtime validation also covers untyped callable references.
            return Response.json(await validInput("input", "extra"));
          }),
          route("GET", "/partial-input", async (_request, env) => {
            await env.action("beforeInputFailure");
            return Response.json(await env.action("validInput", 42));
          }),
          route("GET", "/result", async (_request, env) => Response.json(await env.action(badResult, "input"))),
          route("GET", "/handler", async (_request, env) => Response.json(await env.action("badHandler"))),
          route("GET", "/database", async (_request, env) => Response.json(await env.action("badDatabase"))),
        ],
      });
      try {
        const started = await host.start({ serve: true, runWorker: false });
        try {
          const response = await fetch(`http://localhost:${started.server!.port}/input`, { method: "POST", body: "42" });
          expect(response.status).toBe(400);
          const payload: unknown = await response.json();
          expect(payload).toMatchObject({ error: { code: "INVALID_ACTION_INPUT", message: "args must be a string" } });
          expect(called).toBe(false);
          for (const path of ["reference-arity", "host-reference-arity", "callable-reference-arity"]) {
            const invalidReference = await fetch(`http://localhost:${started.server!.port}/${path}`);
            expect(invalidReference.status).toBe(400);
            const invalidPayload: unknown = await invalidReference.json();
            expect(invalidPayload).toEqual({ error: {
              code: "INVALID_ACTION_INPUT", message: "action validInput expects a single argument",
            } });
          }
          expect(called).toBe(false);
          expect((await host.executeRoute(new Request("http://test/caught-input"))).response?.status).toBe(400);
          await expect(host.executeRoute(new Request("http://test/result"))).rejects.toThrow("output must be a string");
          await expect(host.executeRoute(new Request("http://test/handler"))).rejects.toThrow("handler failed");
          await expect(host.executeRoute(new Request("http://test/database"))).rejects.toThrow("missing_table");
          expect((await host.executeAction("validationKeys")).result).toEqual([]);
          const partial = await host.executeRoute(new Request("http://test/partial-input"));
          expect(partial.response?.status).toBe(400);
          expect(partial.emittedEvents).toMatchObject([{ name: "beforeInputFailure" }]);
        } finally { await started.stop(); }
        // The host remains available for explicit action calls after server stop.
        await expect(host.executeAction(badResult, "input")).rejects.toThrow("output must be a string");
        await expect(host.executeAction("badHandler")).rejects.toThrow("handler failed");
        await expect(host.executeAction("badDatabase")).rejects.toThrow("missing_table");
        expect((await host.executeAction("validationKeys")).result).toEqual(["beforeInputFailure"]);
      } finally { await host.close(); }
    });

    test(`KV expiration hides millisecond TTL and legacy timestamps and cleanup removes them (${engine})`, async () => {
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-kv-expiration-"));
      cleanupDirs.push(projectDir);
      const host = await ChimpbaseBunHost.create({
        projectDir,
        config: normalizeProjectConfig({
          storage: { engine, path: "kv.db" },
          kv: { retention: { enabled: true } },
        }),
      });
      host.register(
        action("test.seedKv", async (ctx) => {
          await ctx.kv.set("ttl.permanent", "permanent");
          await ctx.kv.set("ttl.live", "live", { ttlMs: 60_000 });
          await ctx.kv.set("ttl.expired", "expired", { ttlMs: 1 });
          for (const [key, expiresAt] of [
            ["ttl.legacyIso", new Date(Date.now() - 10).toISOString()],
            ["ttl.legacySqlite", "2000-01-01 00:00:00.000"],
          ]) {
            await ctx.db.query(
              "INSERT INTO _chimpbase_kv (key, value_json, expires_at) VALUES (?1, ?2, ?3)",
              [key, JSON.stringify("legacy"), expiresAt],
            );
          }
        }),
        action("test.inspectKv", async (ctx) => ({
          expired: await ctx.kv.get("ttl.expired", v.string()),
          legacyIso: await ctx.kv.get("ttl.legacyIso", v.string()),
          legacySqlite: await ctx.kv.get("ttl.legacySqlite", v.string()),
          live: await ctx.kv.get("ttl.live", v.string()),
          permanent: await ctx.kv.get("ttl.permanent", v.string()),
          keys: await ctx.kv.list({ prefix: "ttl." }),
          rows: await ctx.db.query("SELECT key FROM _chimpbase_kv ORDER BY key", undefined, v.object({ key: v.string() })),
        })),
        action("test.makeCleanupDue", async (ctx) => {
          await ctx.db.query(
            "UPDATE _chimpbase_cron_schedules SET next_fire_at_ms = ?1 WHERE schedule_name = ?2",
            [Date.now() - 1, "__chimpbase.kv.cleanup"],
          );
        }),
      );
      const snapshotValidator = v.object({
        expired: v.string().nullable(), legacyIso: v.string().nullable(), legacySqlite: v.string().nullable(),
        live: v.string().nullable(), permanent: v.string().nullable(), keys: v.string().array(),
        rows: v.object({ key: v.string() }).array(),
      });
      try {
        await host.executeAction("test.seedKv");
        await Bun.sleep(20);
        const before = snapshotValidator.parse((await host.executeAction("test.inspectKv")).result);
        expect(before).toMatchObject({
          expired: null, legacyIso: null, legacySqlite: null,
          live: "live", permanent: "permanent", keys: ["ttl.live", "ttl.permanent"],
        });
        expect(before.rows).toHaveLength(5);
        await host.syncCronSchedules();
        await host.executeAction("test.makeCleanupDue");
        expect((await host.processNextCronSchedule())?.scheduleName).toBe("__chimpbase.kv.cleanup");
        expect(await host.processNextQueueJob()).not.toBeNull();
        const after = snapshotValidator.parse((await host.executeAction("test.inspectKv")).result);
        expect(after).toEqual({ ...before, rows: [{ key: "ttl.live" }, { key: "ttl.permanent" }] });
      } finally {
        await host.close();
      }
    });
  }

  test("actions + route + subscription + worker pipeline (memory)", async () => {
    const { host, started, baseUrl } = await bootInlineApp();
    try {
      const created = await fetch(`${baseUrl}/items`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ label: "widget", amount: 42 }),
      });
      expect(created.status).toBe(201);

      const listed = (await (await fetch(`${baseUrl}/items`)).json()) as Array<{ label: string }>;
      expect(listed).toHaveLength(1);
      expect(listed[0].label).toBe("widget");

      await host.drain({ maxDurationMs: 5_000 });

      const auditRows = await host.executeAction("listItems", {});
      expect(auditRows.result).toHaveLength(1);

      const notificationRows = v.object({ detail: v.string() }).array().parse(
        (await host.executeAction("listNotifications", {})).result,
        "notification rows",
      );
      expect(notificationRows.some((r) => r.detail === "notified widget")).toBe(true);
    } finally {
      await started.stop();
    }
  });

  test("executes actions without a running server", async () => {
    const { host, started } = await bootInlineApp();
    try {
      const outcome = await host.executeAction("createItem", { label: "headless", amount: 7 });
      expect(outcome.result).toMatchObject({ id: 1 });
      await host.drain({ maxDurationMs: 5_000 });
      const list = await host.executeAction("listItems", {});
      expect(list.result).toHaveLength(1);
    } finally {
      await started.stop();
    }
  });

  test("raw SQL validators reject schema drift at the query boundary", async () => {
    const { host, started } = await bootInlineApp();
    host.register(
      action("readInvalidRow", async (ctx) =>
        await ctx.db.query(
          "SELECT 'not-a-number' AS id",
          undefined,
          v.object({ id: v.number() }),
        )
      ),
    );
    try {
      await expect(host.executeAction("readInvalidRow")).rejects.toThrow(
        "database query rows[0].id must be a finite number",
      );
    } finally {
      await started.stop();
    }
  });

  test("persisted JSON fails with its boundary label", async () => {
    const { host, started } = await bootInlineApp();
    host.register(
      action("readInvalidJson", async (ctx) => {
        await ctx.db.query(
          "INSERT OR REPLACE INTO _chimpbase_kv (key, value_json, updated_at, expires_at) VALUES (?1, ?2, CURRENT_TIMESTAMP, NULL)",
          ["invalid-json", "not-json"],
        );
        return await ctx.kv.get("invalid-json", v.object({ value: v.string() }));
      }),
    );
    try {
      await expect(host.executeAction("readInvalidJson")).rejects.toThrow(
        "key-value entry invalid-json must be valid JSON",
      );
    } finally {
      await started.stop();
    }
  });

  test("sqlite engine persists state across boots", async () => {
    const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-bun-sqlite-"));
    cleanupDirs.push(projectDir);

    {
      const { host, started } = await bootInlineApp({ storage: "sqlite", projectDir });
      try {
        await host.executeAction("createItem", { label: "persisted", amount: 9 });
      } finally {
        await started.stop();
      }
    }

    {
      const { host, started } = await bootInlineApp({ storage: "sqlite", projectDir });
      try {
        const list = v.object({ label: v.string() }).array().parse(
          (await host.executeAction("listItems", {})).result,
          "item rows",
        );
        expect(list.map((r) => r.label)).toContain("persisted");
      } finally {
        await started.stop();
      }
    }
  });

  test("health endpoint responds out of the box", async () => {
    const { started, baseUrl } = await bootInlineApp();
    try {
      const res = await fetch(`${baseUrl}/health`);
      expect(res.status).toBe(200);
      expect(await readJsonResponse<{ ok: boolean }>(res)).toEqual({ ok: true });
    } finally {
      await started.stop();
    }
  });

  test("waits for and reports Bun server cleanup", async () => {
    let releaseCleanup!: () => void;
    let stopForce: boolean | undefined;
    const cleanup = new Promise<void>((resolve) => {
      releaseCleanup = resolve;
    });
    const server: unknown = {
      stop(force?: boolean) {
        stopForce = force;
        return cleanup;
      },
    };
    if (!isBunServer(server)) throw new Error("invalid Bun server fixture");

    let stopped = false;
    const stopping = bunRuntimeShim.server.stop(server).then(() => {
      stopped = true;
    });
    await Promise.resolve();
    expect(stopForce).toBe(true);
    expect(stopped).toBe(false);

    releaseCleanup();
    await stopping;
    expect(stopped).toBe(true);

    const rejectingServer: unknown = {
      stop() {
        return Promise.reject(new Error("server cleanup failed"));
      },
    };
    if (!isBunServer(rejectingServer)) throw new Error("invalid Bun server fixture");
    await expect(bunRuntimeShim.server.stop(rejectingServer)).rejects.toThrow("server cleanup failed");
  });
});
