import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { bunRuntimeShim } from "../packages/bun/src/runtime.ts";
import { readJsonResponse } from "./support/http.ts";
import {
  action,
  cron,
  route,
  subscription,
  v,
  worker,
  type ChimpbaseDlqEnvelope,
} from "../packages/runtime/index.ts";
import { defineChimpbaseMigrations } from "../packages/core/index.ts";

const cleanupDirs: string[] = [];
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
