import { afterEach, describe, expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";

import type { ChimpbaseJob, ChimpbaseRegistration } from "../packages/runtime/index.ts";
import { parseJson } from "../packages/runtime/index.ts";
import { loadProjectAppDefinition } from "../packages/tooling/src/app.ts";
import {
  runChimpbaseCli,
  type RunChimpbaseCliDependencies,
} from "../packages/tooling/src/cli.ts";
import { loadProjectConfig } from "../packages/tooling/src/config.ts";
import { defineChimpbaseApp, normalizeProjectConfig } from "../packages/core/index.ts";
import { inferNumberEnv } from "../packages/host/src/runtime.ts";
import { createChimpbase } from "../packages/bun/src/library.ts";
import {
  loadProjectMigrations,
  loadProjectPostgresMigrations,
  readSqlMigrations,
  resolveLocalMigrationsDir,
  resolvePostgresMigrationsDir,
} from "../packages/tooling/src/migrations.ts";
import { loadLocalSecretStore } from "../packages/tooling/src/secrets.ts";

const cleanupDirs: string[] = [];

afterEach(async () => {
  while (cleanupDirs.length > 0) {
    const dir = cleanupDirs.pop();
    if (dir !== undefined) {
      await rm(dir, { force: true, recursive: true });
    }
  }
});

describe("@chimpbase/tooling", () => {
  test("telemetry and subscription retention reject unsafe ages before creating a host", async () => {
    for (const enabled of [true, false]) {
      for (const maxAgeDays of [0, -1, NaN, Infinity, -Infinity, 0.5, 100_000_001, Number.MAX_SAFE_INTEGER + 1]) {
        expect(() => normalizeProjectConfig({ telemetry: { retention: { enabled, maxAgeDays } } }))
          .toThrow("telemetry.retention.maxAgeDays");
        expect(() => normalizeProjectConfig({ subscriptions: { idempotency: { retention: { enabled, maxAgeDays } } } }))
          .toThrow("subscriptions.idempotency.retention.maxAgeDays");
      }
    }
    for (const maxAgeDays of [1, 30, 100_000_000]) {
      const config = normalizeProjectConfig({
        telemetry: { retention: { maxAgeDays } }, subscriptions: { idempotency: { retention: { maxAgeDays } } },
      });
      expect(config.telemetry.retention.maxAgeDays).toBe(maxAgeDays);
      expect(config.subscriptions.idempotency.retention.maxAgeDays).toBe(maxAgeDays);
    }
    await expect(createChimpbase({ storage: { engine: "memory" }, telemetryRetention: { enabled: true, maxAgeDays: -1 } }))
      .rejects.toThrow("telemetry.retention.maxAgeDays");
    await expect(createChimpbase({ storage: { engine: "memory" }, subscriptions: { idempotency: { retention: { enabled: true, maxAgeDays: -1 } } } }))
      .rejects.toThrow("subscriptions.idempotency.retention.maxAgeDays");
    const projectDir = await createTempDir("invalid-retention-config");
    for (const section of ["telemetry.retention", "subscriptions.idempotency.retention"]) {
      await writeFile(resolve(projectDir, "chimpbase.toml"), `[${section}]\nmax_age_days = -1\n`);
      await expect(loadProjectConfig(projectDir)).rejects.toThrow(`${section}.maxAgeDays`);
    }
  });

  test("worker settings reject malformed, unsafe and out-of-range numbers at configuration boundaries", async () => {
    for (const key of ["concurrency", "leaseMs", "maxAttempts", "pollIntervalMs"] as const) {
      for (const value of [0, -1, NaN, Infinity, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
        expect(() => normalizeProjectConfig({ worker: { [key]: value } })).toThrow(`worker.${key}`);
      }
    }
    for (const value of [-1, NaN, Infinity, 0.5, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() => normalizeProjectConfig({ worker: { retryDelayMs: value } })).toThrow("worker.retryDelayMs");
      expect(() => defineChimpbaseApp({ worker: { retryDelayMs: value } })).toThrow("worker.retryDelayMs");
    }
    expect(() => defineChimpbaseApp({ worker: { maxAttempts: NaN } })).toThrow("worker.maxAttempts");
    expect(() => normalizeProjectConfig({ worker: { pollIntervalMs: 2_147_483_648 } })).toThrow("worker.pollIntervalMs");
    expect(normalizeProjectConfig({ worker: { retryDelayMs: 0 } }).worker.retryDelayMs).toBe(0);
    expect(() => normalizeProjectConfig({ worker: { retention: { maxAgeDays: 0 } } })).toThrow("worker.retention.maxAgeDays");

    for (const value of ["", " ", "NaN", "Infinity", "oops"]) {
      expect(() => inferNumberEnv({ get: () => value, toObject: () => ({}) }, "CHIMPBASE_WORKER_LEASE_MS"))
        .toThrow("CHIMPBASE_WORKER_LEASE_MS");
    }
    expect(inferNumberEnv({ get: () => undefined, toObject: () => ({}) }, "CHIMPBASE_WORKER_LEASE_MS")).toBeUndefined();
    const projectDir = await createTempDir("invalid-worker-config");
    await writeFile(resolve(projectDir, "chimpbase.toml"), '[worker]\nmax_attempts = "NaN"\n');
    await expect(loadProjectConfig(projectDir)).rejects.toThrow("worker.max_attempts");
  });

  test("loads chimpbase.toml with nested telemetry sections", async () => {
    const projectDir = await createTempDir("config");
    await writeFile(
      resolve(projectDir, "chimpbase.toml"),
      [
        "# project metadata",
        "[project]",
        'name = "tooling-config"',
        "",
        "[server]",
        "port = 4123",
        "",
        "[storage]",
        'engine = "postgres"',
        'url = "postgres://example.test/chimpbase"',
        "",
        "[subscriptions]",
        'dispatch = "async"',
        "",
        "[subscriptions.idempotency.retention]",
        "enabled = true",
        "max_age_days = 21",
        'schedule = "15 2 * * *"',
        "",
        "[worker]",
        "lease_ms = 45000",
        "max_attempts = 9",
        "poll_interval_ms = 800",
        "retry_delay_ms = 1200",
        "concurrency = 2",
        "",
        "[worker.retention]",
        "enabled = false",
        "max_age_days = 60",
        'schedule = "15 3 * * *"',
        "",
        "[secrets]",
        'dir = "run/secrets"',
        'env_file = ".env.local"',
        "",
        "[workflows]",
        'contracts_dir = "contracts"',
        "",
        "[telemetry]",
        'min_level = "warn"',
        "",
        "[telemetry.persist]",
        "log = true",
        "metric = true",
        "trace = false",
        "",
        "[telemetry.retention]",
        "enabled = true",
        "max_age_days = 14",
        'schedule = "0 3 * * *"',
        "",
      ].join("\n"),
    );

    const config = await loadProjectConfig(projectDir);

    expect(config.project.name).toBe("tooling-config");
    expect(config.server.port).toBe(4123);
    expect(config.storage).toEqual({
      engine: "postgres",
      path: null,
      url: "postgres://example.test/chimpbase",
    });
    expect(config.subscriptions).toEqual({
      dispatch: "async",
      idempotency: {
        retention: {
          enabled: true,
          maxAgeDays: 21,
          schedule: "15 2 * * *",
        },
      },
    });
    expect(config.worker).toEqual({
      concurrency: 2,
      leaseMs: 45000,
      maxAttempts: 9,
      pollIntervalMs: 800,
      retryDelayMs: 1200,
      retention: { enabled: false, maxAgeDays: 60, schedule: "15 3 * * *" },
    });
    expect(config.secrets).toEqual({
      dir: "run/secrets",
      envFile: ".env.local",
    });
    expect(config.workflows.contractsDir).toBe("contracts");
    expect(config.telemetry).toEqual({
      minLevel: "warn",
      persist: {
        log: true,
        metric: true,
        trace: false,
      },
      retention: {
        enabled: true,
        maxAgeDays: 14,
        schedule: "0 3 * * *",
      },
    });
  });

  test("loads local secrets with dotenv < env < mounted file precedence", async () => {
    const projectDir = await createTempDir("secrets");
    await mkdir(resolve(projectDir, "run/secrets"), { recursive: true });
    await writeFile(resolve(projectDir, ".env"), "APP_TOKEN=dotenv-token\nSHARED=dotenv-shared\n");
    await writeFile(resolve(projectDir, "run/secrets/APP_TOKEN"), "mounted-token");

    const store = await loadLocalSecretStore(
      projectDir,
      {
        project: { name: "tooling" },
        secrets: { dir: "run/secrets", envFile: ".env" },
        server: { port: 3000 },
        storage: { engine: "memory", path: null, url: null },
        subscriptions: {
          dispatch: "sync",
          idempotency: {
            retention: { enabled: false, maxAgeDays: 30, schedule: "0 2 * * *" },
          },
        },
        telemetry: {
          minLevel: "debug",
          persist: { log: false, metric: false, trace: false },
          retention: { enabled: false, maxAgeDays: 30, schedule: "0 2 * * *" },
        },
        worker: {
          concurrency: 1,
          leaseMs: 30000,
          maxAttempts: 5,
          pollIntervalMs: 250,
          retryDelayMs: 1000,
          retention: { enabled: true, maxAgeDays: 30, schedule: "0 3 * * *" },
        },
        kv: { retention: { enabled: false, schedule: "0 3 * * *" } },
        workflows: { contractsDir: "workflow-contracts" },
      },
      {
        env: {
          APP_TOKEN: "env-token",
          SHARED: "env-shared",
        },
      },
    );

    expect(store.get("APP_TOKEN")).toBe("mounted-token");
    expect(store.get("SHARED")).toBe("env-shared");
    expect(store.get("MISSING")).toBeNull();
  });

  test("resolves engine-specific migration directories and reads sorted SQL files", async () => {
    const projectDir = await createTempDir("migrations");
    await mkdir(resolve(projectDir, "migrations/sqlite"), { recursive: true });
    await writeFile(resolve(projectDir, "migrations/sqlite/002_second.sql"), "SELECT 2;");
    await writeFile(resolve(projectDir, "migrations/sqlite/001_first.sql"), "SELECT 1;");

    const migrationsDir = await resolveLocalMigrationsDir(resolve(projectDir, "migrations"), "memory");
    const migrations = await readSqlMigrations(migrationsDir);

    expect(migrationsDir).toBe(resolve(projectDir, "migrations/sqlite"));
    expect(migrations.map((entry) => entry.name)).toEqual([
      "001_first.sql",
      "002_second.sql",
    ]);
    expect(migrations.map((entry) => entry.sql.trim())).toEqual([
      "SELECT 1;",
      "SELECT 2;",
    ]);
  });

  test("prefers postgres migrations folder and falls back to base migrations", async () => {
    const postgresProjectDir = await createTempDir("postgres-migrations");
    await mkdir(resolve(postgresProjectDir, "migrations/postgres"), { recursive: true });
    await writeFile(resolve(postgresProjectDir, "migrations/postgres/001_init.sql"), "SELECT 1;");

    const baseProjectDir = await createTempDir("base-migrations");
    await mkdir(resolve(baseProjectDir, "migrations"), { recursive: true });
    await writeFile(resolve(baseProjectDir, "migrations/001_init.sql"), "SELECT 1;");

    expect(await resolvePostgresMigrationsDir(postgresProjectDir)).toBe(resolve(postgresProjectDir, "migrations/postgres"));
    expect(await resolvePostgresMigrationsDir(baseProjectDir)).toBe(resolve(baseProjectDir, "migrations"));
  });

  test("prefers chimpbase.migrations.ts over local SQL files", async () => {
    const projectDir = await createTempDir("typed-migrations");
    await mkdir(resolve(projectDir, "migrations/sqlite"), { recursive: true });
    await mkdir(resolve(projectDir, "migrations/postgres"), { recursive: true });
    await writeFile(resolve(projectDir, "migrations/sqlite/001_disk.sql"), "SELECT 'disk-sqlite';");
    await writeFile(resolve(projectDir, "migrations/postgres/001_disk.sql"), "SELECT 'disk-postgres';");
    await writeFile(
      resolve(projectDir, "chimpbase.migrations.ts"),
      [
        "export default {",
        "  sqlite: [",
        "    { name: \"001_inline\", sql: \"SELECT 'sqlite-inline';\" },",
        "    { name: \"002_inline\", sql: \"SELECT 'sqlite-inline-2';\" },",
        "  ],",
        "  postgres: [",
        "    { name: \"001_pg_inline\", sql: \"SELECT 'postgres-inline';\" },",
        "  ],",
        "};",
      ].join("\n"),
    );

    const sqliteMigrations = await loadProjectMigrations(projectDir, "memory");
    const postgresMigrations = await loadProjectPostgresMigrations(projectDir);

    expect(sqliteMigrations).toEqual([
      { name: "001_inline", owner: "framework", sql: "SELECT 'sqlite-inline';" },
      { name: "002_inline", owner: "framework", sql: "SELECT 'sqlite-inline-2';" },
    ]);
    expect(postgresMigrations).toEqual([
      { name: "001_pg_inline", owner: "framework", sql: "SELECT 'postgres-inline';" },
    ]);
  });

  test("loads chimpbase.app.ts as the canonical code-first app definition", async () => {
    const projectDir = await createTempDir("app-module");
    await writeFile(
      resolve(projectDir, "chimpbase.app.ts"),
      [
        "export default {",
        '  project: { name: "tooling-app" },',
        "  migrations: {",
        "    sqlite: [",
        '      { name: "001_init", sql: "SELECT 1;" },',
        "    ],",
        "  },",
        "  registrations: [",
        '    { kind: "action", name: "ping", handler: async () => "pong" },',
        "  ],",
        "};",
      ].join("\n"),
    );

    const app = await loadProjectAppDefinition(projectDir);

    expect(app).not.toBeNull();
    expect(app?.project.name).toBe("tooling-app");
    expect(app?.registrations).toHaveLength(1);
    expect(app?.migrations.sqlite).toEqual([
      { name: "001_init", owner: "framework", sql: "SELECT 1;" },
    ]);
  });

  test("infers names for exported unnamed actions from chimpbase.app.ts", async () => {
    const projectDir = await createTempDir("unnamed-app-actions");
    const runtimeImportPath = resolve(import.meta.dir, "../packages/runtime/index.ts");
    await writeFile(
      resolve(projectDir, "chimpbase.app.ts"),
      [
        `import { action } from ${JSON.stringify(runtimeImportPath)};`,
        "",
        "export const health = action({",
        '  async handler() { return { ok: true }; },',
        "});",
        "",
        "export default {",
        '  project: { name: "tooling-unnamed-action" },',
        "  registrations: [health],",
        "};",
      ].join("\n"),
    );

    const app = await loadProjectAppDefinition(projectDir);

    expect(app).not.toBeNull();
    expect(app?.registrations).toHaveLength(1);
    expect(app?.registrations[0]).toEqual(
      expect.objectContaining({
        kind: "action",
        name: "chimpbase.app.ts#health",
      }) as ChimpbaseRegistration,
    );
    const reloaded = await loadProjectAppDefinition(projectDir);
    expect(reloaded?.registrations[0]).toMatchObject({ name: "chimpbase.app.ts#health" });
  });

  test("loads original modules from read-only sources and reloads changed app definitions", async () => {
    const projectDir = await createTempDir("read-only-app");
    const appPath = resolve(projectDir, "chimpbase.app.ts");
    const source = `export default {
      project: { name: "read-only-app" },
      registrations: [{ kind: "action", name: "source", handler() { throw new Error("from original"); } }],
    };`;
    await writeFile(appPath, source);
    await chmod(projectDir, 0o555);
    try {
      const app = await loadProjectAppDefinition(projectDir);
      expect(app?.project.name).toBe("read-only-app");
      expect((await loadProjectAppDefinition(projectDir))?.project.name).toBe("read-only-app");
      expect(await readdir(projectDir)).toEqual(["chimpbase.app.ts"]);
      const registration = app?.registrations[0];
      if (registration?.kind !== "action" || typeof registration.handler !== "function") throw new Error("missing action");
      const handler = registration.handler as () => unknown;
      try {
        handler();
        throw new Error("handler must throw");
      } catch (error) {
        expect(error instanceof Error && error.stack?.includes(appPath)).toBe(true);
        expect(error instanceof Error && error.stack?.includes(".__chimpbase_app_")).toBe(false);
      }
    } finally {
      await chmod(projectDir, 0o755);
    }
    await writeFile(appPath, source.replace("read-only-app", "reloaded-app"));
    expect((await loadProjectAppDefinition(projectDir))?.project.name).toBe("reloaded-app");
  });

  test("CLI rejects invalid options before invoking any project operation", async () => {
    let calls = 0;
    const unexpected = async (): Promise<never> => { calls += 1; throw new Error("project was started"); };
    const dependencies: RunChimpbaseCliDependencies = {
      loadProject: unexpected,
      runAction: unexpected, startProject: unexpected, syncModules: unexpected,
      syncSchema: unexpected, syncWorkflowContracts: unexpected, writeLine() {},
    };
    for (const argv of [
      ["dev", "--typo"], ["dev", "--action"], ["dev", "--action", "--serve"],
      ["dev", "--action", ""], ["dev", "--project-dir"], ["dev", "--serve=false"],
      ["dev", "--serve", "false"], ["dev", "unexpected"], ["dev", "--args", "{}"],
      ["dev", "--action", "ping", "--watch"], ["dev", "--action", "ping", "--worker"],
      ["dev", "--args", "not-json", "--action", "ping"], ["schema", "generate", "--watch"],
      ["contracts", "--serve"], ["modules", "sync", "--modules-dir"], ["schema", "unknown"],
      ["jobs", "unknown"], ["jobs", "inspect"], ["jobs", "inspect", "0"], ["jobs", "retry", "1.5"],
      ["jobs", "list", "1"], ["jobs", "list", "--limit", "Infinity"], ["jobs", "list", "--status", "bogus"],
      ["jobs", "inspect", "1", "--queue", "email"], ["jobs", "retry", "1", "--typo"],
    ]) {
      await expect(runChimpbaseCli(argv, dependencies)).rejects.toThrow();
    }
    expect(calls).toBe(0);
  });

  test("CLI job commands expose unregistered queues and always close the host", async () => {
    const lines: string[] = [];
    const calls: unknown[] = [];
    let closed = 0;
    const job: ChimpbaseJob = { id: 7, queue: "email.dlq", payload: { error: "provider down" }, status: "pending",
      attempt: 0, availableAtMs: 0, leaseExpiresAtMs: null, lastError: null, registered: false };
    const unexpected = async (): Promise<never> => { throw new Error("unrelated project operation"); };
    const dependencies: RunChimpbaseCliDependencies = {
      runAction: unexpected, startProject: unexpected, syncModules: unexpected, syncSchema: unexpected,
      syncWorkflowContracts: unexpected, writeLine(line) { lines.push(line); },
      async loadProject(projectDir) {
        calls.push(projectDir);
        return { config: { project: { name: "jobs" } }, async close() { closed += 1; },
          async listJobs(options) { calls.push(options); return [job]; },
          async inspectJob(id) { calls.push(id); return id === 7 ? job : null; },
          async retryJob(id) { calls.push(id); if (id !== 7) throw new Error("job in progress"); return job; },
        };
      },
    };
    await runChimpbaseCli(["jobs", "list", "--queue", "email.dlq", "--status", "dlq", "--limit", "4"], dependencies);
    expect(calls[1]).toEqual({ queue: "email.dlq", status: "dlq", limit: 4 });
    expect(parseJson(lines[0]!)).toEqual([job]);
    await runChimpbaseCli(["jobs", "inspect", "7"], dependencies);
    await runChimpbaseCli(["jobs", "retry", "7"], dependencies);
    await expect(runChimpbaseCli(["jobs", "inspect", "8"], dependencies)).rejects.toThrow("job not found");
    await expect(runChimpbaseCli(["jobs", "retry", "8"], dependencies)).rejects.toThrow("job in progress");
    expect(closed).toBe(5);
  });

  test("CLI help exits without invoking any project operation", async () => {
    const lines: string[] = [];
    const unexpected = async (): Promise<never> => { throw new Error("project was started"); };
    const dependencies: RunChimpbaseCliDependencies = {
      runAction: unexpected, startProject: unexpected, syncModules: unexpected,
      syncSchema: unexpected, syncWorkflowContracts: unexpected,
      writeLine(line) { lines.push(line); },
    };
    for (const argv of [["--help"], ["dev", "--help"], ["-h"], ["schema", "--help"], ["modules", "check", "--help"]]) {
      await runChimpbaseCli(argv, dependencies);
    }
    expect(lines).toHaveLength(5);
    expect(lines.every((line) => line.includes("Usage:") && line.includes("--watch"))).toBe(true);
  });

  test("CLI native watch restarts edited actions and retains SQLite data", async () => {
    const projectDir = await createTempDir("native-watch");
    const appPath = resolve(projectDir, "chimpbase.app.ts");
    const cleanupPath = resolve(projectDir, "cleanup.log");
    const source = (message: string) => `import { appendFile } from "node:fs/promises";
    export default {
      project: { name: "watch-test" },
      registrations: [
        { kind: "onStop", name: "cleanup", async handler() {
          await appendFile(${JSON.stringify(cleanupPath)}, "stopped\\n");
        } },
        { kind: "action", name: "message", async handler(ctx) {
          const count = (await ctx.kv.get("count") ?? 0) + 1;
          await ctx.kv.set("count", count);
          return { message: ${JSON.stringify(message)}, count };
        } },
        { kind: "route", name: "http", async handler(request, env) {
          return Response.json(await env.action("message"));
        } },
      ],
    };`;
    await writeFile(appPath, source("before"));
    const reservation = Bun.serve({ port: 0, fetch: () => new Response() });
    const port = reservation.port;
    reservation.stop(true);
    const child = spawn("bun", [resolve(import.meta.dir, "../packages/bun/src/cli.ts"), "dev", "--serve", "--watch", "--project-dir", projectDir], {
      cwd: resolve(import.meta.dir, ".."),
      env: { ...process.env, CHIMPBASE_SERVER_PORT: String(port), CHIMPBASE_STORAGE_ENGINE: "sqlite", DATABASE_URL: "", CHIMPBASE_DATABASE_URL: "" },
      detached: true, stdio: ["ignore", "pipe", "pipe"],
    });
    let output = "";
    child.stdout?.on("data", (chunk: Buffer) => { output += String(chunk); });
    child.stderr?.on("data", (chunk: Buffer) => { output += String(chunk); });
    const waitForMessage = async (message: string): Promise<{ message: string; count: number }> => {
      const deadline = Date.now() + 6_000;
      while (Date.now() < deadline) {
        try {
          const response = await fetch(`http://127.0.0.1:${port}`, { signal: AbortSignal.timeout(500) });
          const body = await response.json() as { message: string; count: number };
          if (body.message === message) return body;
        } catch {}
        await new Promise((resolveWait) => setTimeout(resolveWait, 30));
      }
      throw new Error(`watch did not serve ${message}: ${output.slice(-2_000)}`);
    };
    try {
      const before = await waitForMessage("before");
      await writeFile(appPath, source("after"));
      const after = await waitForMessage("after");
      expect(after.count).toBeGreaterThan(before.count);
    } finally {
      if (child.pid !== undefined && child.exitCode === null) process.kill(-child.pid, "SIGTERM");
      await new Promise<void>((resolveExit) => {
        if (child.exitCode !== null || child.signalCode !== null) resolveExit();
        else child.once("exit", () => resolveExit());
      });
    }
    expect(await readFile(cleanupPath, "utf8")).toContain("stopped\n");
  }, 20_000);

  test("CLI preserves default and trailing boolean options", async () => {
    const starts: Array<{ runWorker?: boolean; serve?: boolean }> = [];
    const dependencies: RunChimpbaseCliDependencies = {
      async runAction() {
        throw new Error("unexpected action");
      },
      async startProject(options) {
        starts.push(options ?? {});
        return { server: { port: 3000 }, async stop() {} };
      },
      async syncModules() {
        throw new Error("unexpected module sync");
      },
      async syncSchema() {
        throw new Error("unexpected schema sync");
      },
      async syncWorkflowContracts() {
        throw new Error("unexpected contract sync");
      },
      writeLine() {},
    };

    await runChimpbaseCli([], dependencies);
    await runChimpbaseCli(["dev", "--worker"], dependencies);

    expect(starts.map(({ runWorker, serve }) => ({ runWorker, serve }))).toEqual([
      { runWorker: undefined, serve: undefined },
      { runWorker: true, serve: undefined },
    ]);
  });

});

async function createTempDir(label: string): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), `chimpbase-tooling-${label}-`));
  cleanupDirs.push(dir);
  return dir;
}
