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
import { applySqliteMigrations, defineChimpbaseMigrations, ensureSqliteInternalTables, normalizeProjectConfig } from "../packages/core/index.ts";

const cleanupDirs: string[] = [];

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
