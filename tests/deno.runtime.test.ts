import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { access, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";

import {
  defineChimpbaseApp,
  defineChimpbaseMigration,
  defineChimpbaseMigrations,
  normalizeProjectConfig,
} from "../packages/core/index.ts";
import { createChimpbaseDeno, loadChimpbaseProject as loadChimpbaseDenoProject } from "../packages/deno/src/library.ts";
import { ChimpbaseDenoHost } from "../packages/deno/src/runtime.ts";
import { action, v } from "../packages/runtime/index.ts";
import {
  canUseDocker,
  startPostgresDocker,
  type PostgresDockerHandle,
} from "../packages/tooling/src/postgres_docker.ts";
import { readJsonResponse } from "./support/http.ts";
import { installLocalPackage } from "./support/local_package.ts";

interface FakeDenoRuntimeOptions {
  env?: Record<string, string>;
  serve?: (
    options: { hostname?: string; port?: number },
    handler: (request: Request) => Response | Promise<Response>,
  ) => {
    finished?: Promise<void>;
    shutdown?(): void;
  };
}

const repoRoot = resolve(import.meta.dir, "..");
const dockerAvailable = await canUseDocker();
const cleanupDirs: string[] = [];
const originalDeno: unknown = Reflect.get(globalThis, "Deno");
// SQLite for @chimpbase/deno is validated in a real Deno process because Bun is not the target runtime here.
const bunSupportsBetterSqlite3 = false;

afterEach(async () => {
  restoreDenoRuntime();

  while (cleanupDirs.length > 0) {
    const dir = cleanupDirs.pop();
    if (dir) {
      await rm(dir, { recursive: true, force: true });
    }
  }
});

if (!bunSupportsBetterSqlite3) {
  test.skip("deno sqlite runtime is covered in a real Deno process", () => {});
} else {
  describe("chimpbase-deno sqlite runtime", () => {
    test("createChimpbaseDeno.from defaults to sqlite storage and drains queue work", async () => {
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-deno-sqlite-defaults-"));
      cleanupDirs.push(projectDir);

      installFakeDenoRuntime({
        env: {
          CHIMPBASE_SERVER_PORT: "4814",
          CHIMPBASE_WORKER_CONCURRENCY: "3",
          CHIMPBASE_WORKER_LEASE_MS: "41000",
          CHIMPBASE_WORKER_MAX_ATTEMPTS: "4",
          CHIMPBASE_WORKER_POLL_INTERVAL_MS: "25",
          CHIMPBASE_WORKER_RETRY_DELAY_MS: "0",
        },
      });
      await writeFile(
        resolve(projectDir, "chimpbase.app.ts"),
        [
          "export default {",
          '  project: { name: "deno-sqlite-app" },',
          "  registrations: [],",
          "};",
        ].join("\n"),
      );

      const host = await createChimpbaseDeno.from(projectDir, {});
      const processed: string[] = [];

      host.registerAction("enqueueJobs", async (ctx) => {
        await ctx.enqueue("batch.job", { value: "job-1" });
        await ctx.enqueue("batch.job", { value: "job-2" });
        return null;
      });
      host.registerWorker("batch.job", async (_ctx, payload) => {
        processed.push((payload as { value: string }).value);
      });

      try {
        expect(host.config.project.name).toBe("deno-sqlite-app");
        expect(host.config.server.port).toBe(4814);
        expect(host.config.storage).toEqual({
          engine: "sqlite",
          path: join("data", "deno-sqlite-app.db"),
          url: null,
        });
        expect(host.config.worker).toEqual({
          concurrency: 3,
          leaseMs: 41000,
          maxAttempts: 5,
          pollIntervalMs: 25,
          retryDelayMs: 1000,
        });
        await access(resolve(projectDir, host.config.storage.path!));

        await host.executeAction("enqueueJobs");

        const firstDrain = await host.drain({ maxRuns: 1 });
        expect(firstDrain).toEqual({
          cronSchedules: 0,
          idle: false,
          queueJobs: 1,
          runs: 1,
          stopReason: "max_runs",
        });
        expect(processed).toEqual(["job-1"]);

        const secondDrain = await host.drain();
        expect(secondDrain).toEqual({
          cronSchedules: 0,
          idle: true,
          queueJobs: 1,
          runs: 1,
          stopReason: "idle",
        });
        expect(processed).toEqual(["job-1", "job-2"]);
      } finally {
        await host.close();
      }
    });
  });
}

if (!dockerAvailable) {
  test.skip("deno runtime integration requires Docker", () => {});
} else {
  describe("chimpbase-deno runtime", () => {
    let postgres: PostgresDockerHandle;

    beforeAll(async () => {
      postgres = await startPostgresDocker();
    }, 30000);

    afterAll(async () => {
      await postgres?.stop();
    }, 30000);

    test("createChimpbaseDeno.from applies Deno env defaults and drains queue work", async () => {
      const database = await postgres.createDatabase("deno_env_defaults");
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-deno-env-defaults-"));
      cleanupDirs.push(projectDir);

      installFakeDenoRuntime({
        env: {
          CHIMPBASE_SERVER_PORT: "4810",
          CHIMPBASE_WORKER_CONCURRENCY: "4",
          CHIMPBASE_WORKER_LEASE_MS: "41000",
          CHIMPBASE_WORKER_MAX_ATTEMPTS: "6",
          CHIMPBASE_WORKER_POLL_INTERVAL_MS: "25",
          CHIMPBASE_WORKER_RETRY_DELAY_MS: "0",
          DATABASE_URL: database.url,
        },
      });
      await writeFile(
        resolve(projectDir, "chimpbase.app.ts"),
        [
          "export default {",
          '  project: { name: "deno-env-app" },',
          "  registrations: [],",
          "};",
        ].join("\n"),
      );

      const host = await createChimpbaseDeno.from(projectDir, {});
      const processed: string[] = [];

      host.registerAction("enqueueJobs", async (ctx) => {
        await ctx.enqueue("batch.job", { value: "job-1" });
        await ctx.enqueue("batch.job", { value: "job-2" });
        return null;
      });
      host.registerWorker("batch.job", async (_ctx, payload) => {
        processed.push((payload as { value: string }).value);
      });

      try {
        expect(host.config.project.name).toBe("deno-env-app");
        expect(host.config.server.port).toBe(4810);
        expect(host.config.storage).toEqual({
          engine: "postgres",
          path: null,
          url: database.url,
        });
        expect(host.config.worker).toEqual({
          concurrency: 4,
          leaseMs: 41000,
          maxAttempts: 5,
          pollIntervalMs: 25,
          retryDelayMs: 1000,
        });

        await host.executeAction("enqueueJobs");

        const firstDrain = await host.drain({ maxRuns: 1 });
        expect(firstDrain).toEqual({
          cronSchedules: 0,
          idle: false,
          queueJobs: 1,
          runs: 1,
          stopReason: "max_runs",
        });
        expect(processed).toEqual(["job-1"]);

        const secondDrain = await host.drain();
        expect(secondDrain).toEqual({
          cronSchedules: 0,
          idle: true,
          queueJobs: 1,
          runs: 1,
          stopReason: "idle",
        });
        expect(processed).toEqual(["job-1", "job-2"]);
      } finally {
        await host.close();
      }
    }, 30000);

    test("supports validator-backed action references", async () => {
      const database = await postgres.createDatabase("deno_action_refs");
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-deno-action-refs-"));
      cleanupDirs.push(projectDir);

      installFakeDenoRuntime({
        env: {},
      });

      const host = await ChimpbaseDenoHost.create({
        config: normalizeProjectConfig({
          project: { name: "deno-action-refs" },
          storage: {
            engine: "postgres",
            url: database.url,
          },
        }),
        projectDir,
      });

      const createAccount = action({
        args: v.object({
          email: v.string(),
          name: v.string(),
        }),
        async handler(_ctx, input) {
          return {
            email: input.email,
            name: input.name,
            slug: input.name.toLowerCase(),
          };
        },
        name: "createDenoAccountRef",
      });

      const seedAccounts = action({
        args: v.object({
          accounts: v.array(
            v.object({
              email: v.string(),
              name: v.string(),
            }),
          ),
        }),
        async handler(_ctx, input) {
          const created: Array<Awaited<ReturnType<typeof createAccount>>> = [];
          for (const account of input.accounts) {
            created.push(await createAccount(account));
          }

          return {
            created,
            total: created.length,
          };
        },
        name: "seedDenoAccountsRef",
      });

      try {
        await expect(
          createAccount({
            email: "outside@deno.test",
            name: "Outside",
          }),
        ).rejects.toThrow("requires an active chimpbase runtime context or a registered host binding");

        host.register(createAccount, seedAccounts);

        await expect(createAccount({
          email: "bound@deno.test",
          name: "Bound",
        })).resolves.toEqual({
          email: "bound@deno.test",
          name: "Bound",
          slug: "bound",
        });

        const seeded = await host.executeAction(seedAccounts, {
          accounts: [
            { email: "alice@deno.test", name: "Alice" },
            { email: "bruno@deno.test", name: "Bruno" },
          ],
        });

        expect(seeded.result).toEqual({
          created: [
            { email: "alice@deno.test", name: "Alice", slug: "alice" },
            { email: "bruno@deno.test", name: "Bruno", slug: "bruno" },
          ],
          total: 2,
        });

        await expect(
          host.executeAction(createAccount, {
            email: 10,
            name: "Broken",
          } as never),
        ).rejects.toThrow("args.email must be a string");
      } finally {
        await host.close();
      }
    }, 30000);

    test("serializes sqlite-style engine operations across actions and worker drains", async () => {
      const order: string[] = [];
      const host = Object.create(ChimpbaseDenoHost.prototype) as Record<string, unknown>;
      host.config = normalizeProjectConfig({
        project: { name: "deno-sqlite-serialization" },
        storage: { engine: "memory" },
      });
      host.cronRegistryDirty = false;
      host.cronSyncPromise = null;
      host.engine = {
        async drain() {
          order.push("drain:start");
          await new Promise((resolve) => setTimeout(resolve, 5));
          order.push("drain:end");
          return {
            cronSchedules: 0,
            idle: true,
            queueJobs: 0,
            runs: 0,
            stopReason: "idle" as const,
          };
        },
        async executeAction(name: string, args: unknown[]) {
          order.push(`action:${name}:start`);
          await new Promise((resolve) => setTimeout(resolve, 5));
          order.push(`action:${name}:end`);
          return {
            emittedEvents: [],
            result: { args, name },
          };
        },
      };
      host.serializedEngineOperations = Promise.resolve();
      const typedHost = host as unknown as ChimpbaseDenoHost;

      const actionPromise = typedHost.executeAction("health");
      const drainPromise = typedHost.drain({ maxRuns: 1 });

      await expect(actionPromise).resolves.toEqual({
        emittedEvents: [],
        result: { args: [], name: "health" },
      });
      await expect(drainPromise).resolves.toEqual({
        cronSchedules: 0,
        idle: true,
        queueJobs: 0,
        runs: 0,
        stopReason: "idle",
      });
      expect(order).toEqual([
        "action:health:start",
        "action:health:end",
        "drain:start",
        "drain:end",
      ]);
    });

    test("dispatches postgres subscriptions across Deno hosts", async () => {
      const database = await postgres.createDatabase("deno_cross_process_subscriptions");
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-deno-cross-process-"));
      cleanupDirs.push(projectDir);

      installFakeDenoRuntime({
        env: {},
      });

      const migrationsSql = [
        "CREATE TABLE IF NOT EXISTS cross_process_audit (id SERIAL PRIMARY KEY, value TEXT NOT NULL);",
      ];
      const subscriber = await ChimpbaseDenoHost.create({
        config: normalizeProjectConfig({
          project: { name: "deno-cross-process-subscriber" },
          storage: { engine: "postgres", url: database.url },
        }),
        migrationsSql,
        projectDir,
      });
      const publisher = await ChimpbaseDenoHost.create({
        config: normalizeProjectConfig({
          project: { name: "deno-cross-process-publisher" },
          storage: { engine: "postgres", url: database.url },
        }),
        migrationsSql,
        projectDir,
      });

      subscriber.registerSubscription("audit.created", async (ctx, payload) => {
        await ctx.db.query("INSERT INTO cross_process_audit (value) VALUES (?1)", [(payload as { value: string }).value]);
      });
      publisher.registerAction("publishAudit", async (ctx, value) => {
        ctx.pubsub.publish("audit.created", { value });
        return null;
      });
      publisher.registerAction(
        "listAudit",
        async (ctx) => await ctx.db.query("SELECT value FROM cross_process_audit ORDER BY id ASC"),
      );

      const startedSubscriber = await subscriber.start({ runWorker: false, serve: false });

      try {
        await sleep(100);
        await publisher.executeAction("publishAudit", ["from-publisher"]);

        await waitFor(async () => {
          const audit = await publisher.executeAction("listAudit");
          return audit.result as Array<{ value: string }>;
        }, (rows) => rows.length === 1);

        const audit = await publisher.executeAction("listAudit");
        expect(audit.result).toEqual([{ value: "from-publisher" }]);
      } finally {
        await startedSubscriber.stop();
        await publisher.close();
        await subscriber.close();
      }
    }, 30000);

    test("createChimpbaseDeno accepts typed TS migrations", async () => {
      const database = await postgres.createDatabase("deno_typed_migrations");
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-deno-typed-migrations-"));
      cleanupDirs.push(projectDir);

      installFakeDenoRuntime({
        env: {},
      });

      const host = await createChimpbaseDeno({
        app: defineChimpbaseApp({
          migrations: defineChimpbaseMigrations({
            postgres: [
              defineChimpbaseMigration({
                name: "001_worker_audit",
                sql: "CREATE TABLE IF NOT EXISTS worker_audit (id SERIAL PRIMARY KEY, value TEXT NOT NULL);",
              }),
            ],
          }),
          project: { name: "deno-typed-migrations" },
          worker: {
            retryDelayMs: 0,
          },
        }),
        projectDir,
        storage: {
          engine: "postgres",
          url: database.url,
        },
      });

      host.registerAction("enqueueAudit", async (ctx, value) => {
        await ctx.enqueue("audit.job", { value });
        return null;
      });
      host.registerAction(
        "listAudit",
        async (ctx) => await ctx.db.query("SELECT value FROM worker_audit ORDER BY id ASC"),
      );
      host.registerWorker("audit.job", async (ctx, payload) => {
        await ctx.db.query("INSERT INTO worker_audit (value) VALUES (?1)", [(payload as { value: string }).value]);
      });

      try {
        await host.executeAction("enqueueAudit", ["typed-migration"]);

        expect(await host.drain()).toEqual({
          cronSchedules: 0,
          idle: true,
          queueJobs: 1,
          runs: 1,
          stopReason: "idle",
        });

        const audit = await host.executeAction("listAudit");
        expect(audit.result).toEqual([{ value: "typed-migration" }]);
      } finally {
        await host.close();
      }
    }, 30000);

    test("createChimpbaseDeno accepts inline app fields with registrations", async () => {
      const database = await postgres.createDatabase("deno_app_definition");
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-deno-app-definition-"));
      cleanupDirs.push(projectDir);

      installFakeDenoRuntime({
        env: {},
      });

      const host = await createChimpbaseDeno({
        migrations: defineChimpbaseMigrations({
          postgres: [
            defineChimpbaseMigration({
              name: "001_worker_audit",
              sql: "CREATE TABLE IF NOT EXISTS worker_audit (id SERIAL PRIMARY KEY, value TEXT NOT NULL);",
            }),
          ],
        }),
        projectDir,
        project: { name: "deno-app-definition" },
        registrations: [
          {
            eventName: "audit.created",
            handler: async (ctx, event) => {
              await ctx.enqueue("audit.job", event);
            },
            kind: "subscription",
          },
          action("enqueueAudit", async (ctx, value) => {
            ctx.pubsub.publish("audit.created", { value });
            return { queued: value };
          }),
          action("listAudit", async (ctx) => await ctx.db.query("SELECT value FROM worker_audit ORDER BY id ASC")),
          {
            definition: undefined,
            handler: async (ctx, payload) => {
              await ctx.db.query("INSERT INTO worker_audit (value) VALUES (?1)", [(payload as { value: string }).value]);
            },
            kind: "worker",
            name: "audit.job",
          },
        ],
        storage: {
          engine: "postgres",
          url: database.url,
        },
        workerRuntime: {
          pollIntervalMs: 25,
        },
      });

      try {
        const queued = await host.executeAction("enqueueAudit", ["from-app"]);
        expect(queued.result).toEqual({ queued: "from-app" });

        expect(await host.drain()).toEqual({
          cronSchedules: 0,
          idle: true,
          queueJobs: 1,
          runs: 1,
          stopReason: "idle",
        });

        const audit = await host.executeAction("listAudit");
        expect(audit.result).toEqual([{ value: "from-app" }]);
      } finally {
        await host.close();
      }
    }, 30000);

    test("supports host action and worker registration helpers", async () => {
      const database = await postgres.createDatabase("deno_host_helpers");
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-deno-host-helpers-"));
      cleanupDirs.push(projectDir);

      installFakeDenoRuntime({
        env: {},
      });

      const host = await createChimpbaseDeno({
        projectDir,
        storage: {
          engine: "postgres",
          url: database.url,
        },
      });

      host
        .action("enqueueAudit", async (ctx, value) => {
          await ctx.enqueue("audit.job", { value });
          return { queued: value };
        })
        .action(
          "listAudit",
          async (ctx) => await ctx.collection.find("audit_log"),
        )
        .worker("audit.job", async (ctx, payload) => {
          await ctx.collection.insert("audit_log", { value: (payload as { value: string }).value });
        });

      try {
        const queued = await host.executeAction("enqueueAudit", ["from-helper"]);
        expect(queued.result).toEqual({ queued: "from-helper" });

        expect(await host.drain()).toEqual({
          cronSchedules: 0,
          idle: true,
          queueJobs: 1,
          runs: 1,
          stopReason: "idle",
        });

        const audit = await host.executeAction("listAudit");
        expect(audit.result).toEqual([
          expect.objectContaining({ value: "from-helper" }),
        ]);
      } finally {
        await host.close();
      }
    }, 30000);

    test("createChimpbaseDeno.from loads chimpbase.app.ts and serves requests through Deno.serve", async () => {
      const database = await postgres.createDatabase("deno_load");
      const projectDir = await createDenoProjectFixture("load", database.url);
      let shutdownCalled = false;
      let servedHandler: ((request: Request) => Response | Promise<Response>) | null = null;

      installFakeDenoRuntime({
        env: {},
        serve(_options, handler) {
          servedHandler = handler;
          return {
            finished: Promise.resolve(),
            shutdown() {
              shutdownCalled = true;
            },
          };
        },
      });

      const host = await createChimpbaseDeno.from(projectDir, {
        server: { port: 4821 },
        storage: { engine: "postgres", url: database.url },
      });

      try {
        const queueResult = await host.executeAction("enqueueAudit", ["from-load"]);
        expect(queueResult.result).toEqual({ queued: "from-load" });

        const drained = await host.drain();
        expect(drained).toEqual({
          cronSchedules: 0,
          idle: true,
          queueJobs: 1,
          runs: 1,
          stopReason: "idle",
        });

        const audit = await host.executeAction("listAudit");
        expect(audit.result).toEqual([{ value: "from-load" }]);

        const routeOutcome = await host.executeRoute(new Request("http://deno.test/audit"));
        expect(routeOutcome.response?.status).toBe(200);
        expect(await readJsonResponse<Array<{ value: string }>>(routeOutcome.response)).toEqual([{ value: "from-load" }]);

        const started = await host.start({ runWorker: false, serve: true });
        expect(started.server?.port).toBe(4821);
        const routeHandler: (request: Request) => Response | Promise<Response> = servedHandler ?? (() => {
          throw new Error("expected Deno.serve handler to be registered");
        });

        const healthResponse = await routeHandler(new Request("http://127.0.0.1:4821/health"));
        expect(healthResponse.status).toBe(200);
        expect(await readJsonResponse<{ ok: boolean }>(healthResponse)).toEqual({ ok: true });

        const auditResponse = await routeHandler(new Request("http://127.0.0.1:4821/audit"));
        expect(auditResponse.status).toBe(200);
        expect(await readJsonResponse<Array<{ value: string }>>(auditResponse)).toEqual([{ value: "from-load" }]);

        await started.stop();
        expect(shutdownCalled).toBe(true);
      } finally {
        await host.close();
      }
    }, 30000);

    test("loadChimpbaseProject prefers chimpbase.app.ts over legacy project discovery", async () => {
      const database = await postgres.createDatabase("deno_app_module");
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-deno-app-module-"));
      cleanupDirs.push(projectDir);

      installFakeDenoRuntime({
        env: {
          DATABASE_URL: database.url,
        },
      });

      await installLocalPackage(projectDir, "@chimpbase/runtime", resolve(repoRoot, "packages/runtime"));

      await writeFile(
        resolve(projectDir, "package.json"),
        JSON.stringify(
          {
            dependencies: {
              "@chimpbase/runtime": "file:./packages/runtime",
            },
          },
          null,
          2,
        ),
      );
      await writeFile(
        resolve(projectDir, "chimpbase.app.ts"),
        [
          'import { action } from "@chimpbase/runtime";',
          "",
          "export default {",
          '  project: { name: "deno-app-module" },',
          "  migrations: {",
          "    postgres: [",
          '      { name: "001_worker_audit", sql: "CREATE TABLE IF NOT EXISTS worker_audit (id SERIAL PRIMARY KEY, value TEXT NOT NULL);" },',
          "    ],",
          "  },",
          "  registrations: [",
          '    action("enqueueAudit", async (ctx, value) => {',
          '      await ctx.enqueue("audit.job", { value });',
          "      return null;",
          "    }),",
          '    action("listAudit", async (ctx) => await ctx.db.query("SELECT value FROM worker_audit ORDER BY id ASC")),',
          '    { kind: "worker", name: "audit.job", handler: async (ctx, payload) => { await ctx.db.query("INSERT INTO worker_audit (value) VALUES (?1)", [(payload).value]); } },',
          "  ],",
          "};",
        ].join("\n"),
      );

      const host = await loadChimpbaseDenoProject(projectDir);

      try {
        await host.executeAction("enqueueAudit", ["from-app-module"]);
        await host.drain();
        const audit = await host.executeAction("listAudit");
        expect(audit.result).toEqual([{ value: "from-app-module" }]);
      } finally {
        await host.close();
      }
    }, 30000);
  });
}

describe("chimpbase-deno runtime guards", () => {
  if (!bunSupportsBetterSqlite3) {
    test.skip("memory storage is covered in a real Deno process", () => {});
  } else {
    test("supports memory storage through the sqlite adapter", async () => {
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-deno-memory-"));
      cleanupDirs.push(projectDir);
      installFakeDenoRuntime({ env: {} });

      const processed: string[] = [];
      const host = await ChimpbaseDenoHost.create({
        config: normalizeProjectConfig({
          project: { name: "deno-memory" },
          storage: { engine: "memory" },
          worker: { retryDelayMs: 0 },
        }),
        projectDir,
      });

      host.registerAction("enqueueMemoryJob", async (ctx) => {
        await ctx.enqueue("memory.job", { value: "memory" });
        return null;
      });
      host.registerWorker("memory.job", async (_ctx, payload) => {
        processed.push((payload as { value: string }).value);
      });

      try {
        expect(host.config.storage).toEqual({
          engine: "memory",
          path: null,
          url: null,
        });

        await host.executeAction("enqueueMemoryJob");
        const drain = await host.drain();

        expect(drain).toEqual({
          cronSchedules: 0,
          idle: true,
          queueJobs: 1,
          runs: 1,
          stopReason: "idle",
        });
        expect(processed).toEqual(["memory"]);
      } finally {
        await host.close();
      }
    });

    test("infers an unnamed action name from register({ key })", async () => {
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-deno-register-map-"));
      cleanupDirs.push(projectDir);

      installFakeDenoRuntime({
        env: {},
      });

      const host = await ChimpbaseDenoHost.create({
        config: normalizeProjectConfig({
          project: { name: "deno-register-map" },
          storage: {
            engine: "memory",
          },
        }),
        projectDir,
      });

      const health = action({
        async handler() {
          return { ok: true };
        },
      });

      try {
        host.register({ health });

        expect(health.name).toBe("health");
        await expect(health()).resolves.toEqual({ ok: true });

        const outcome = await host.executeAction("health");
        expect(outcome.result).toEqual({ ok: true });
      } finally {
        await host.close();
      }
    });
  }

  test("requires a postgres url when opening storage", async () => {
    const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-deno-postgres-url-"));
    cleanupDirs.push(projectDir);
    installFakeDenoRuntime({ env: {} });

    await expect(
      ChimpbaseDenoHost.create({
        config: normalizeProjectConfig({
          project: { name: "deno-postgres" },
          storage: { engine: "postgres" },
        }),
        projectDir,
      }),
    ).rejects.toThrow("@chimpbase/deno requires storage.url for postgres storage");
  });
});

function installFakeDenoRuntime(options: FakeDenoRuntimeOptions): void {
  const env = options.env ?? {};

  Reflect.set(globalThis, "Deno", {
    args: [],
    env: {
      get(name: string) {
        return env[name];
      },
      toObject() {
        return { ...env };
      },
    },
    serve: options.serve,
  });
}

function restoreDenoRuntime(): void {
  if (originalDeno === undefined) {
    Reflect.deleteProperty(globalThis, "Deno");
    return;
  }

  Reflect.set(globalThis, "Deno", originalDeno);
}

async function createDenoProjectFixture(label: string, databaseUrl: string): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), `chimpbase-deno-inline-${label}-`));
  cleanupDirs.push(dir);

  await installLocalPackage(dir, "@chimpbase/runtime", resolve(repoRoot, "packages/runtime"));

  await writeFile(
    resolve(dir, "package.json"),
    JSON.stringify(
      {
        dependencies: {
          "@chimpbase/runtime": "file:./packages/runtime",
        },
      },
      null,
      2,
    ),
  );
  await writeFile(
    resolve(dir, "tsconfig.json"),
    JSON.stringify(
      {
        compilerOptions: {
          allowImportingTsExtensions: true,
          lib: ["ES2022", "DOM"],
          module: "ESNext",
          moduleResolution: "Bundler",
          noEmit: true,
          skipLibCheck: true,
          strict: true,
          target: "ES2022",
        },
        include: ["**/*.ts"],
      },
      null,
      2,
    ),
  );
  await writeFile(
    resolve(dir, "chimpbase.app.ts"),
    [
      'import { action, worker } from "@chimpbase/runtime";',
      "",
      "async function fetch(request, env) {",
      "  const pathname = new URL(request.url).pathname;",
      "  if (pathname === \"/health\") {",
      "    return Response.json({ ok: true });",
      "  }",
      "  if (pathname === \"/audit\") {",
      "    const rows = await env.action(\"listAudit\");",
      "    return Response.json(rows);",
      "  }",
      "  return new Response(\"not found\", { status: 404 });",
      "}",
      "",
      "export default {",
      "  httpHandler: fetch,",
      "  migrations: {",
      '    postgres: [{ name: "001_worker_audit", sql: "CREATE TABLE IF NOT EXISTS worker_audit (id SERIAL PRIMARY KEY, value TEXT NOT NULL);" }],',
      "  },",
      '  project: { name: "deno-load" },',
      "  worker: {",
      "    retryDelayMs: 0,",
      "  },",
      "  registrations: [",
      '    action("enqueueAudit", async (ctx, value) => {',
      '      await ctx.enqueue("audit.job", { value });',
      '      return { queued: value };',
      "    }),",
      '    action("listAudit", async (ctx) => await ctx.db.query("SELECT value FROM worker_audit ORDER BY id ASC")),',
      '    worker("audit.job", async (ctx, payload) => {',
      '      await ctx.db.query("INSERT INTO worker_audit (value) VALUES (?1)", [(payload).value]);',
      "    }),",
      "  ],",
      "};",
    ].join("\n"),
  );

  return dir;
}

async function waitFor<T>(
  load: () => Promise<T>,
  predicate: (value: T) => boolean,
  options: { intervalMs?: number; timeoutMs?: number } = {},
): Promise<T> {
  const intervalMs = options.intervalMs ?? 50;
  const timeoutMs = options.timeoutMs ?? 5_000;
  const deadline = Date.now() + timeoutMs;

  while (Date.now() <= deadline) {
    const value = await load();
    if (predicate(value)) {
      return value;
    }

    await sleep(intervalMs);
  }

  throw new Error(`condition not met within ${timeoutMs}ms`);
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}
