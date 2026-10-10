import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";

const repoRoot = resolve(import.meta.dir, "..");
const cleanupDirs: string[] = [];
const nodeSupportsSqlite = detectNodeSqliteSupport();

function detectNodeSqliteSupport(): boolean {
  try {
    return Bun.spawnSync(
      ["node", "--input-type=module", "-e", 'await import("node:sqlite");'],
      {
        cwd: repoRoot,
        env: process.env,
        stderr: "ignore",
        stdout: "ignore",
      },
    ).exitCode === 0;
  } catch (error) {
    const code = error instanceof Error && "code" in error
      ? String((error as Error & { code?: unknown }).code)
      : null;
    if (code === "ENOENT") return false;
    throw error;
  }
}

afterEach(async () => {
  while (cleanupDirs.length > 0) {
    const dir = cleanupDirs.pop();
    if ((dir !== undefined && dir.length > 0)) {
      await rm(dir, { recursive: true, force: true });
    }
  }
});

describe("chimpbase-node runtime", () => {
  (nodeSupportsSqlite ? test : test.skip)("supports sqlite storage in a real Node process", async () => {
    const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-node-sqlite-"));
    cleanupDirs.push(projectDir);

    const build = Bun.spawnSync(
      [
        "node",
        "./scripts/build-package.mjs",
        "runtime",
        "core",
        "tooling",
        "postgres",
        "host",
        "node",
      ],
      {
        cwd: repoRoot,
        env: process.env,
        stderr: "pipe",
        stdout: "pipe",
      },
    );

    if (build.exitCode !== 0) {
      throw new Error(
        `failed to build Node runtime packages\n${build.stdout.toString()}\n${build.stderr.toString()}`,
      );
    }

    const scriptPath = resolve(projectDir, "index.mjs");
    const nodeLibraryPath = resolve(repoRoot, "packages/node/dist/src/library.js").replaceAll("\\", "\\\\");
    const nodeRuntimePath = resolve(repoRoot, "packages/node/dist/src/runtime.js");
    const corePath = resolve(repoRoot, "packages/core/dist/index.js");

    await writeFile(
      scriptPath,
      [
        `import { createChimpbase } from "${nodeLibraryPath}";`,
        `import { ChimpbaseNodeHost } from ${JSON.stringify(nodeRuntimePath)};`,
        `import { normalizeProjectConfig } from ${JSON.stringify(corePath)};`,
        'import assert from "node:assert/strict";',
        "",
        "const migrationOptions = {",
        `  projectDir: ${JSON.stringify(projectDir)},`,
        '  storage: { engine: "sqlite", path: "data/named-migrations.db" },',
        '  migrations: { sqlite: [{ name: "001_create", sql: "CREATE TABLE migrated_items (id INTEGER PRIMARY KEY)" }] },',
        "};",
        "await (await createChimpbase(migrationOptions)).close();",
        "await (await createChimpbase(migrationOptions)).close();",
        "",
        "const host = await createChimpbase({",
        '  project: { name: "node-sqlite-app" },',
        `  projectDir: ${JSON.stringify(projectDir)},`,
        '  storage: { engine: "sqlite", path: "data/node-sqlite.db" },',
        "  worker: { retryDelayMs: 0 },",
        "});",
        "",
        'host.registerAction("enqueueJobs", async (ctx) => {',
        '  await ctx.enqueue("batch.job", { value: "job-1" });',
        '  await ctx.enqueue("batch.job", { value: "job-2" });',
        "  return null;",
        "});",
        "",
        'host.registerWorker("batch.job", async (ctx, payload) => {',
        '  const processed = await ctx.kv.get("processed") ?? [];',
        '  await ctx.kv.set("processed", [...processed, payload.value]);',
        "});",
        "",
        'host.registerAction("readProcessed", async (ctx) => await ctx.kv.get("processed") ?? []);',
        "",
        'await host.executeAction("enqueueJobs");',
        "const firstDrain = await host.drain({ maxRuns: 1 });",
        "const secondDrain = await host.drain();",
        'const processed = (await host.executeAction("readProcessed")).result;',
        "const kvHost = await ChimpbaseNodeHost.create({",
        `  projectDir: ${JSON.stringify(projectDir)},`,
        '  config: normalizeProjectConfig({ storage: { engine: "memory" }, kv: { retention: { enabled: true } } }),',
        "});",
        "try {",
        '  kvHost.registerAction("seedKv", async (ctx) => {',
        '    await ctx.kv.set("ttl.permanent", "permanent");',
        '    await ctx.kv.set("ttl.live", "live", { ttlMs: 60000 });',
        '    await ctx.kv.set("ttl.expired", "expired", { ttlMs: 1 });',
        '    await ctx.db.query("INSERT INTO _chimpbase_kv (key, value_json, expires_at) VALUES (?1, ?2, ?3)", ["ttl.legacy", JSON.stringify("legacy"), new Date(Date.now() - 10).toISOString()]);',
        "  });",
        '  kvHost.registerAction("inspectKv", async (ctx) => ({',
        '    expired: await ctx.kv.get("ttl.expired"), legacy: await ctx.kv.get("ttl.legacy"),',
        '    live: await ctx.kv.get("ttl.live"), permanent: await ctx.kv.get("ttl.permanent"),',
        '    keys: await ctx.kv.list({ prefix: "ttl." }),',
        '    rows: await ctx.db.query("SELECT key FROM _chimpbase_kv ORDER BY key"),',
        "  }));",
        '  await kvHost.executeAction("seedKv");',
        "  await new Promise((resolve) => setTimeout(resolve, 20));",
        '  const before = (await kvHost.executeAction("inspectKv")).result;',
        '  assert.equal(before.expired, null); assert.equal(before.legacy, null);',
        '  assert.equal(before.live, "live"); assert.equal(before.permanent, "permanent");',
        '  assert.deepEqual(before.keys, ["ttl.live", "ttl.permanent"]); assert.equal(before.rows.length, 4);',
        "  await kvHost.syncCronSchedules();",
        '  kvHost.registerAction("makeCleanupDue", async (ctx) => await ctx.db.query("UPDATE _chimpbase_cron_schedules SET next_fire_at_ms = ?1 WHERE schedule_name = ?2", [Date.now() - 1, "__chimpbase.kv.cleanup"]));',
        '  await kvHost.executeAction("makeCleanupDue");',
        '  assert.equal((await kvHost.processNextCronSchedule())?.scheduleName, "__chimpbase.kv.cleanup");',
        "  assert.notEqual(await kvHost.processNextQueueJob(), null);",
        '  const after = (await kvHost.executeAction("inspectKv")).result;',
        '  assert.deepEqual(after, { ...before, rows: [{ key: "ttl.live" }, { key: "ttl.permanent" }] });',
        '  const adapter = kvHost.engine.getBlobsAdapter();',
        '  const keys = [...Array.from({ length: 1002 }, (_, i) => "a/" + String(i).padStart(4, "0")), "b/one", "b/two", "c.txt"];',
        '  for (const key of keys) await adapter.blobPutMetadata({ bucket: "pagination", key, size: 1, etag: "hash", contentType: "text/plain", metadata: {}, driverRef: key, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() });',
        '  let cursor; const results = [];',
        '  for (let i = 0; i < 4; i++) {',
        '    const page = await adapter.blobListMetadata("pagination", { delimiter: "/", limit: 1, cursor });',
        '    results.push(...page.commonPrefixes, ...page.entries.map((entry) => entry.key));',
        '    if (page.nextCursor === null) break;',
        '    assert(page.nextCursor > (cursor ?? "")); cursor = page.nextCursor;',
        '    if (i === 3) throw new Error("pagination did not terminate");',
        '  }',
        '  assert.deepEqual(results, ["a/", "b/", "c.txt"]);',
        "  const literalKeys = [\"user_one\", \"userXtwo\", \"literal%one\", \"literalXtwo\", \"bang!one\", \"bangXone\", \"bang!_one\", \"bang!%one\", \"ordinary/one\"];",
        "  const existingKeys = await adapter.kvList();",
        "  for (const [i, key] of literalKeys.entries()) {",
        "    await adapter.kvSet(key, true);",
        "    await adapter.blobPutMetadata({ bucket: \"prefixes\", key, size: 1, etag: \"hash\", contentType: \"text/plain\", metadata: {}, driverRef: key, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() });",
        "    await adapter.blobInitUpload({ uploadId: \"prefix-\" + String(i).padStart(2, \"0\"), bucket: \"prefixes\", key, contentType: null, metadata: {}, driverRef: key, createdAtMs: Date.now(), expiresAtMs: Date.now() + 60000 });",
        "  }",
        "  for (const prefix of [\"user_\", \"literal%\", \"bang!\", \"bang!_\", \"bang!%\", \"ordinary/\", \"\", \"missing\"]) {",
        "    const expected = literalKeys.filter((key) => key.startsWith(prefix)).sort();",
        "    assert.deepEqual(await adapter.kvList({ prefix }), [...expected, ...existingKeys.filter((key) => key.startsWith(prefix))].sort());",
        "    assert.deepEqual((await adapter.blobListMetadata(\"prefixes\", { prefix })).entries.map((entry) => entry.key), expected);",
        "    assert.deepEqual((await adapter.blobListUploads(\"prefixes\", { prefix })).uploads.map((entry) => entry.key).sort(), expected);",
        "  }",
        "  await adapter.blobInitUpload({ uploadId: \"expired-recovery\", bucket: \"prefixes\", key: \"expired\", contentType: null, metadata: {}, driverRef: \"expired\", createdAtMs: 0, expiresAtMs: 0 });",
        "  for (const uploadId of [\"expired-recovery\", \"prefix-00\"]) await adapter.blobRecordPart({ uploadId, partNumber: 1, size: 1, etag: \"hash\", driverRef: uploadId, createdAt: new Date().toISOString() });",
        "  assert.deepEqual(await adapter.blobListExpiredUploads(0), [\"expired-recovery\"]);",
        "  assert.deepEqual(await adapter.blobListExpiredUploads(0), [\"expired-recovery\"]);",
        "  assert.equal((await adapter.blobListParts(\"expired-recovery\")).length, 1);",
        "  await adapter.blobAbortUpload(\"expired-recovery\");",
        "  assert.deepEqual(await adapter.blobListExpiredUploads(0), []);",
        "  assert.deepEqual(await adapter.blobListParts(\"expired-recovery\"), []);",
        "  assert.equal((await adapter.blobListParts(\"prefix-00\")).length, 1);",
        "  const sqliteKysely = adapter.createKysely();",
        "  await sqliteKysely.schema.createTable(\"kysely_items\").addColumn(\"id\", \"integer\", (column) => column.primaryKey()).addColumn(\"label\", \"text\").execute();",
        "  const inserted = await sqliteKysely.insertInto(\"kysely_items\").values(Array.from({ length: 5 }, (_, i) => ({ id: i + 1, label: \"item-\" + (i + 1) }))).executeTakeFirst();",
        "  assert.equal(inserted.numInsertedOrUpdatedRows, 5n); assert.equal(inserted.insertId, 5n);",
        "  assert.equal((await sqliteKysely.updateTable(\"kysely_items\").set({ label: \"updated\" }).where(\"id\", \"=\", 1).executeTakeFirst()).numUpdatedRows, 1n);",
        "  assert.equal((await sqliteKysely.deleteFrom(\"kysely_items\").where(\"id\", \"=\", 5).executeTakeFirst()).numDeletedRows, 1n);",
        "  const expectedRows = [{ id: 1, label: \"updated\" }, ...[2, 3, 4].map((id) => ({ id, label: \"item-\" + id }))];",
        "  assert.deepEqual((await sqliteKysely.selectFrom(\"kysely_items\").selectAll().orderBy(\"id\").execute()).map((row) => ({ ...row })), expectedRows);",
        "  for (const chunkSize of [0, 2, 20]) {",
        "    const rows = []; for await (const row of sqliteKysely.selectFrom(\"kysely_items\").selectAll().orderBy(\"id\").stream(chunkSize)) rows.push({ ...row });",
        "    assert.deepEqual(rows, expectedRows);",
        "  }",
        "  await assert.rejects(sqliteKysely.transaction().execute(async () => {}), /runtime-managed transactions/);",
        "  await sqliteKysely.destroy();",
        "} finally { await kvHost.close(); }",
        "console.log(JSON.stringify({",
        "  firstDrain,",
        "  kvExpiration: true,",
        "  processed,",
        "  secondDrain,",
        "  storage: host.config.storage,",
        "}));",
        "await host.close();",
      ].join("\n"),
    );

    const child = Bun.spawn(
      [
        "node",
        scriptPath,
      ],
      {
        cwd: projectDir,
        env: {
          ...process.env,
          NODE_NO_WARNINGS: "1",
        },
        stderr: "pipe",
        stdout: "pipe",
      },
    );

    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
      child.exited,
    ]);

    if (exitCode !== 0) {
      throw new Error(`node sqlite runtime failed\n${stdout}\n${stderr}`);
    }

    expect(stderr.trim()).toBe("");

    const output = JSON.parse(stdout.trim()) as {
      kvExpiration: boolean;
      firstDrain: {
        cronSchedules: number;
        idle: boolean;
        queueJobs: number;
        runs: number;
        stopReason: string;
      };
      processed: string[];
      secondDrain: {
        cronSchedules: number;
        idle: boolean;
        queueJobs: number;
        runs: number;
        stopReason: string;
      };
      storage: {
        engine: string;
        path: string | null;
        url: string | null;
      };
    };

    expect(output.storage).toEqual({
      engine: "sqlite",
      path: "data/node-sqlite.db",
      url: null,
    });
    expect(output.firstDrain).toEqual({
      cronSchedules: 0,
      idle: false,
      queueJobs: 1,
      runs: 1,
      stopReason: "max_runs",
    });
    expect(output.secondDrain).toEqual({
      cronSchedules: 0,
      idle: true,
      queueJobs: 1,
      runs: 1,
      stopReason: "idle",
    });
    expect(output.processed).toEqual(["job-1", "job-2"]);
    expect(output.kvExpiration).toBe(true);
  }, 120_000);
});
