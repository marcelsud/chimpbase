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
