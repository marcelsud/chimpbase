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
    if (dir) {
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

    await writeFile(
      scriptPath,
      [
        `import { createChimpbase } from "${nodeLibraryPath}";`,
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
        "console.log(JSON.stringify({",
        "  firstDrain,",
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
  }, 120_000);
});
