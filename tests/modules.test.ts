import { afterEach, describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createChimpbase } from "../packages/bun/src/library.ts";
import {
  chimpbaseModuleResourceName,
  assertChimpbaseModuleMigrationSql,
  assertChimpbaseModuleRuntimeSql,
  composeChimpbaseModuleMigrations,
  defineChimpbaseApp,
  defineChimpbaseModuleImplementation,
  defineChimpbaseModuleInterface,
  defineChimpbaseModuleSubscription,
} from "../packages/core/index.ts";
import {
  action,
  cron,
  onStop,
  onStart,
  route,
  v,
  subscription,
  worker,
  workflow,
  workflowActionStep,
} from "../packages/runtime/index.ts";
import { createDefaultChimpbasePlatformShim } from "../packages/core/host.ts";
import { createPostgresEngineAdapter } from "../packages/postgres/src/index.ts";
import { pactFromChimpbaseModuleInterface } from "../packages/pact/src/index.ts";
import {
  checkChimpbaseModuleArchitecture,
  compareChimpbaseModuleManifests,
  renderChimpbaseModuleDatabaseTypes,
  generateChimpbaseModuleManifest,
} from "../packages/tooling/src/modules.ts";

const cleanupDirs: string[] = [];

afterEach(async () => {
  while (cleanupDirs.length > 0) {
    const dir = cleanupDirs.pop();
    if (dir !== undefined) await rm(dir, { force: true, recursive: true });
  }
});

describe("business modules", () => {
  test("public interfaces contain contracts but no provider code", () => {
    const catalog = defineChimpbaseModuleInterface({
      name: "catalog",
      version: 1,
      calls: {
        find: {
          input: v.object({ id: v.string() }),
          output: v.object({ id: v.string() }).nullable(),
          errors: ["not_found"],
          guarantees: ["committed reads"],
        },
      },
      events: {
        changedV1: { name: "changed", payload: v.object({ id: v.string() }), version: 1 },
        changedV2: { name: "changed", payload: v.object({ id: v.string(), revision: v.integer() }), version: 2 },
      },
    });

    expect(catalog.calls.find).toEqual(expect.objectContaining({
      id: "catalog/find@v1",
      module: "catalog",
      version: 1,
    }));
    expect("handler" in catalog.calls.find).toBe(false);
    expect(catalog.events.changedV1.id).toBe("catalog/changed@v1");
    expect(catalog.events.changedV2.id).toBe("catalog/changed@v2");

    expect(() => defineChimpbaseModuleImplementation({
      interface: catalog,
      calls: {} as never,
    })).toThrow("must implement each public call exactly once");
  });

  test("assembly rejects missing dependencies, duplicate names, and complete cycles", () => {
    const emptyCalls = {};
    const alpha = defineChimpbaseModuleInterface({
      name: "alpha",
      version: 1,
      dependencies: ["missing"],
      calls: emptyCalls,
      events: {},
    });
    const alphaImplementation = defineChimpbaseModuleImplementation({ interface: alpha, calls: {} });
    expect(() => defineChimpbaseApp({ modules: [alphaImplementation] })).toThrow(
      "module alpha declares missing dependency missing",
    );

    const alphaDuplicate = defineChimpbaseModuleImplementation({ interface: alpha, calls: {} });
    expect(() => defineChimpbaseApp({ modules: [alphaImplementation, alphaDuplicate] })).toThrow(
      "duplicate module identity: alpha",
    );

    const cycleA = defineChimpbaseModuleInterface({
      name: "cycle-a",
      version: 1,
      dependencies: ["cycle-b"],
      calls: {},
      events: {},
    });
    const cycleB = defineChimpbaseModuleInterface({
      name: "cycle-b",
      version: 1,
      dependencies: ["cycle-a"],
      calls: {},
      events: {},
    });
    expect(() => defineChimpbaseApp({
      modules: [

        defineChimpbaseModuleImplementation({ interface: cycleA, calls: {} }),
        defineChimpbaseModuleImplementation({ interface: cycleB, calls: {} }),
      ],
    })).toThrow("cycle-a -> cycle-b -> cycle-a");
  });
  test("orders owned migrations and rejects schema escape or duplicate identities", () => {
    const storageB = defineChimpbaseModuleInterface({
      name: "storage-b",
      version: 1,
      calls: {},
      events: {},
    });
    const storageA = defineChimpbaseModuleInterface({
      name: "storage-a",
      version: 1,
      dependencies: ["storage-b"],
      calls: {},
      events: {},
    });
    const implementationB = defineChimpbaseModuleImplementation({
      interface: storageB,
      calls: {},
      migrations: {
        postgres: [{
          name: "create-items",
          sql: "CREATE TABLE chimpbase_storage_b.items (id TEXT PRIMARY KEY)",
        }],
      },
    });
    const implementationA = defineChimpbaseModuleImplementation({
      interface: storageA,
      calls: {},
      migrations: {
        postgres: [{
          name: "create-orders",
          sql: "CREATE TABLE chimpbase_storage_a.orders (id TEXT PRIMARY KEY)",
        }],
      },
    });
    const migrations = composeChimpbaseModuleMigrations([implementationA, implementationB]);
    expect(migrations.postgres.map((entry) => [entry.name, entry.owner])).toEqual([
      ["storage-b:__schema", "storage-b"],
      ["storage-b:create-items", "storage-b"],
      ["storage-a:__schema", "storage-a"],
      ["storage-a:create-orders", "storage-a"],
    ]);

    expect(() => composeChimpbaseModuleMigrations([
      defineChimpbaseModuleImplementation({
        interface: storageA,
        calls: {},
        migrations: {
          postgres: [{
            name: "escape",
            sql: "CREATE TABLE chimpbase_storage_b.stolen (id TEXT)",
          }],
        },
      }),
      implementationB,
    ])).toThrow("module storage-a migration cannot access schema chimpbase_storage_b");

    expect(() => composeChimpbaseModuleMigrations([
      defineChimpbaseModuleImplementation({
        interface: storageB,
        calls: {},
        migrations: {
          sqlite: [
            { name: "duplicate", sql: "SELECT 1" },
            { name: "duplicate", sql: "SELECT 2" },
          ],
        },
      }),
    ])).toThrow("duplicate module migration identity: storage-b:duplicate");

    const once = defineChimpbaseApp({
      migrations: { sqlite: [{ name: "framework", sql: "SELECT 1" }] },
      modules: [implementationA, implementationB],
    });
    const twice = defineChimpbaseApp(once);
    expect(twice.migrations).toEqual(once.migrations);
    expect(twice.migrations.sqlite[0]?.owner).toBe("framework");
  });

  test("rejects app call collisions and raw module subscriptions", async () => {
    const guarded = defineChimpbaseModuleInterface({
      name: "guarded",
      version: 1,
      calls: {
        run: { input: v.object({}), output: v.boolean(), errors: [], guarantees: [] },
      },
      events: {
        happened: { payload: v.object({ id: v.string() }), version: 1 },
      },
    });
    const implementation = defineChimpbaseModuleImplementation({
      interface: guarded,
      calls: { run: () => true },
    });
    await expect(createChimpbase({
      app: defineChimpbaseApp({
        modules: [implementation],
        registrations: [action(guarded.calls.run.id, async () => false)],
      }),
      storage: { engine: "memory" },
    })).rejects.toThrow("already owned by app-global infrastructure");

    await expect(createChimpbase({
      app: defineChimpbaseApp({
        modules: [defineChimpbaseModuleImplementation({
          interface: guarded,
          calls: { run: () => true },
          registrations: [subscription(guarded.events.happened.id, async () => {})],
        })],
      }),
      storage: { engine: "memory" },
    })).rejects.toThrow("use defineChimpbaseModuleSubscription");
  });

  test("Postgres Kysely scope rejects attempts to select another module schema", async () => {
    const queries: string[] = [];
    const pool = {
      async query(sql: string): Promise<{ rowCount: number; rows: unknown[] }> {
        queries.push(sql);
        return { rowCount: 0, rows: [] };
      },
    } as unknown as Parameters<typeof createPostgresEngineAdapter>[0];
    const adapter = createPostgresEngineAdapter(pool, createDefaultChimpbasePlatformShim());
    const database = adapter.createKysely<{ items: { id: string } }>("chimpbase_left");
    expect(() => assertChimpbaseModuleRuntimeSql(
      "left",
      "chimpbase_left",
      "TABLE chimpbase_right.secrets",
    )).toThrow("allows one SELECT, INSERT, UPDATE, DELETE, or WITH statement");
    expect(() => assertChimpbaseModuleRuntimeSql(
      "left",
      "chimpbase_left",
      "TRUNCATE chimpbase_right.orders",
    )).toThrow("allows one SELECT, INSERT, UPDATE, DELETE, or WITH statement");
    expect(() => assertChimpbaseModuleMigrationSql(
      "left",
      "chimpbase_left",
      "CREATE INDEX chimpbase_left.stolen ON chimpbase_right.orders(id)",
    )).toThrow("cannot access schema chimpbase_right");

    await database.selectFrom("items").select("id").execute();
    expect(queries[0]).toContain('from "chimpbase_left"."items"');
    await database
      .with("active", (builder) => builder.selectFrom("items").select("id"))
      .selectFrom("active")
      .select("id")
      .execute();
    await expect(
      database.withSchema("chimpbase_right").selectFrom("items").select("id").execute(),
    ).rejects.toThrow("cannot access schema chimpbase_right");
    await database.destroy();
  });

  test("validates calls and events and enforces public, internal, and dependency guards", async () => {
    const provider = defineChimpbaseModuleInterface({
      name: "provider",
      version: 1,
      calls: {
        lookup: {
          input: v.object({ id: v.integer() }),
          output: v.object({ label: v.string() }),
          errors: [],
          guarantees: ["immediate result"],
        },
        publish: {
          input: v.object({ id: v.integer() }),
          output: v.boolean(),
          errors: [],
          guarantees: ["event commits with call state"],
        },
        rawPublish: {
          input: v.object({ id: v.integer() }),
          output: v.boolean(),
          errors: [],
          guarantees: [],
        },
      },
      events: {
        published: { payload: v.object({ id: v.integer() }), version: 1 },
      },
    });
    const consumer = defineChimpbaseModuleInterface({
      name: "consumer",
      version: 1,
      dependencies: ["provider"],
      calls: {
        consume: {
          input: v.object({ id: v.integer() }),
          output: v.string(),
          errors: [],
          guarantees: [],
        },
        received: {
          input: v.object({ id: v.integer() }),
          output: v.boolean(),
          errors: [],
          guarantees: [],
        },
        rawBypass: {
          input: v.object({ id: v.integer() }),
          output: v.string(),
          errors: [],
          guarantees: [],
        },
      },
      events: {},
    });
    const undeclared = defineChimpbaseModuleInterface({
      name: "undeclared",
      version: 1,
      calls: {
        bypass: {
          input: v.object({ id: v.integer() }),
          output: v.string(),
          errors: [],
          guarantees: [],
        },
      },
      events: {},
    });

    let providerRuns = 0;
    let privateRuns = 0;
    const privateFormat = action({
      name: "consumer.private.format",
      args: v.object({ label: v.string() }),
      result: v.string(),
      handler: (_ctx, input) => {
        privateRuns += 1;
        return input.label.toUpperCase();
      },
    });
    const providerImplementation = defineChimpbaseModuleImplementation({
      interface: provider,
      calls: {
        lookup(_ctx, input) {
          providerRuns += 1;
          return { label: `item-${input.id}` };
        },
        publish(ctx, input) {
          ctx.publish(provider.events.published, input);
          return true;
        },
        rawPublish(ctx, input) {
          ctx.pubsub.publish("provider.raw", input);
          return true;
        },
      },
    });
    const consumerImplementation = defineChimpbaseModuleImplementation({
      interface: consumer,
      calls: {
        async consume(ctx, input) {
          const item = await ctx.call(provider.calls.lookup, input);
          return await ctx.action(privateFormat, { label: item.label });
        },
        async received(ctx, input) {
          return await ctx.kv.get(`received:${input.id}`, v.boolean()) ?? false;
        },
        async rawBypass(ctx, input) {
          const item = await ctx.action<[{ id: number }], { label: string }>(provider.calls.lookup.id, input);
          return item.label;
        },
      },
      registrations: [privateFormat],
      subscriptions: [
        defineChimpbaseModuleSubscription(provider.events.published, "published", async (ctx, event) => {
          await ctx.kv.set(`received:${event.id}`, true);
        }),
      ],
    });
    const undeclaredImplementation = defineChimpbaseModuleImplementation({
      interface: undeclared,
      calls: {
        async bypass(ctx, input) {
          const item = await ctx.call(provider.calls.lookup, input);
          return item.label;
        },
      },
    });
    const host = await createChimpbase({
      app: defineChimpbaseApp({
        modules: [providerImplementation, consumerImplementation, undeclaredImplementation],
        worker: { retryDelayMs: 0 },
      }),
      storage: { engine: "memory" },
    });

    try {
      const consumed = await host.executeAction(consumer.calls.consume.id, { id: 7 });
      expect(consumed.result).toBe("ITEM-7");
      expect(providerRuns).toBe(1);
      expect(privateRuns).toBe(1);
      expect(host.registry.actionOwnership.get(privateFormat.name)).toEqual({
        module: "consumer",
        visibility: "internal",
      });

      await expect(host.executeAction(consumer.calls.consume.id, { id: "bad" })).rejects.toThrow("integer");
      await expect(host.executeAction(privateFormat.name, { label: "no" })).rejects.toThrow("internal operation");
      await expect(host.executeAction(consumer.calls.rawBypass.id, { id: 1 })).rejects.toThrow("contract reference required");
      await expect(host.executeAction(provider.calls.rawPublish.id, { id: 1 })).rejects.toThrow("raw event names are forbidden");
      await expect(host.executeAction(undeclared.calls.bypass.id, { id: 1 })).rejects.toThrow("undeclared dependency");
      expect(providerRuns).toBe(1);

      await host.executeAction(provider.calls.publish.id, { id: 9 });
      expect((await host.executeAction(consumer.calls.received.id, { id: 9 })).result).toBe(false);
      await host.processNextQueueJob();
      expect((await host.executeAction(consumer.calls.received.id, { id: 9 })).result).toBe(true);

      const invalidOutput = defineChimpbaseModuleInterface({
        name: "invalid-output",
        version: 1,
        calls: {
          run: { input: v.object({}), output: v.string(), errors: [], guarantees: [] },
        },
        events: {},
      });
      const badHost = await createChimpbase({
        app: defineChimpbaseApp({ modules: [defineChimpbaseModuleImplementation({
          interface: invalidOutput,
          calls: { run: (() => 123) as never },
        })] }),
        storage: { engine: "memory" },
      });
      try {
        await expect(badHost.executeAction(invalidOutput.calls.run.id, {})).rejects.toThrow("module call invalid-output/run@v1 output");
      } finally {
        await badHost.close();
      }

      const invalidEvent = defineChimpbaseModuleInterface({
        name: "invalid-event",
        version: 1,
        calls: {
          run: { input: v.object({}), output: v.boolean(), errors: [], guarantees: [] },
        },
        events: { happened: { payload: v.object({ id: v.integer() }), version: 1 } },
      });
      const eventHost = await createChimpbase({
        app: defineChimpbaseApp({ modules: [defineChimpbaseModuleImplementation({
          interface: invalidEvent,
          calls: {
            run(ctx) {
              ctx.publish(
                { ...invalidEvent.events.happened, payload: v.unknown() },
                { id: "bad" },
              );
              return true;
            },
          },
        })] }),
        storage: { engine: "memory" },
      });
      try {
        await expect(eventHost.executeAction(invalidEvent.calls.run.id, {})).rejects.toThrow("module event invalid-event/happened@v1 payload");
      } finally {
        await eventHost.close();
      }
    } finally {
      await host.close();
    }
  });

  test("scopes route, worker, lifecycle, state, and raw SQL to the owning module", async () => {
    const left = defineChimpbaseModuleInterface({
      name: "left",
      version: 1,
      calls: {
        enqueue: { input: v.object({}), output: v.boolean(), errors: [], guarantees: [] },
        rawSql: { input: v.object({}), output: v.boolean(), errors: [], guarantees: [] },
        read: { input: v.object({}), output: v.string().nullable(), errors: [], guarantees: [] },
        startBadFlow: { input: v.object({}), output: v.boolean(), errors: [], guarantees: [] },
        startFlow: { input: v.object({}), output: v.boolean(), errors: [], guarantees: [] },
        write: { input: v.object({ value: v.string() }), output: v.string(), errors: [], guarantees: [] },
      },
      events: {},
    });
    const right = defineChimpbaseModuleInterface({
      name: "right",
      version: 1,
      calls: {
        read: { input: v.object({}), output: v.string().nullable(), errors: [], guarantees: [] },
        write: { input: v.object({ value: v.string() }), output: v.string(), errors: [], guarantees: [] },
      },
      events: {},
    });
    const seenModules: string[] = [];
    let rightPrivateRuns = 0;
    const rightPrivate = action("right.private", async () => {
      rightPrivateRuns += 1;
    });
    const privateStep = action<[], unknown>("left.private.step", async (ctx) => {
      seenModules.push(ctx.module?.name ?? "none");
    });
    const flow = workflow({
      initialState: () => ({}),
      name: "flow",
      steps: [workflowActionStep("private", privateStep)],
      version: 1,
    });
    const badFlow = workflow({
      initialState: () => ({}),
      name: "bad-flow",
      steps: [workflowActionStep("bypass", rightPrivate.name)],
      version: 1,
    });
    const forceCron = action({
      name: "test.force-cron",
      args: v.object({ name: v.string(), nextFireAtMs: v.number() }),
      async handler(ctx, input) {
        await ctx.db.query(
          "UPDATE _chimpbase_cron_schedules SET next_fire_at_ms = ?2 WHERE schedule_name = ?1",
          [input.name, input.nextFireAtMs],
        );
        return true;
      },
    });
    const leftImplementation = defineChimpbaseModuleImplementation({
      interface: left,
      calls: {
        async enqueue(ctx) {
          await ctx.enqueue("job", {});
          return true;
        },
        async rawSql(ctx) {
          await ctx.db.query("SELECT * FROM chimpbase_right.secret");
          return true;
        },
        async read(ctx) {
          return await ctx.kv.get("same", v.string());
        },
        async startBadFlow(ctx) {
          await ctx.workflow.start(badFlow, {}, { workflowId: "bad" });
          return true;
        },
        async startFlow(ctx) {
          const started = await ctx.workflow.start(flow, {}, { workflowId: "one" });
          const loaded = await ctx.workflow.get(started.workflowId);
          if (loaded === null) throw new Error("started workflow was not found");
          return true;
        },
        async write(ctx, input) {
          await ctx.kv.set("same", input.value);
          await ctx.collection.insert("same", { value: input.value });
          await ctx.stream.append("same", "written", input);
          return input.value;
        },
      },
      registrations: [
        route("owner", (_request, env) => new Response(env.module?.name ?? "none")),
        worker("job", async (ctx) => { seenModules.push(ctx.module?.name ?? "none"); }),
        onStart("boot", (ctx) => { seenModules.push(ctx.module?.name ?? "none"); }),
        onStop("shutdown", (ctx) => { seenModules.push(ctx.module?.name ?? "none"); }),
        privateStep,
        flow,
        badFlow,
        cron("tick", "* * * * *", async (ctx) => { seenModules.push(ctx.module?.name ?? "none"); }),
      ],
      resources: { collections: ["same"], kvPrefixes: ["same"], queues: ["job"], streams: ["same"] },
    });
    const rightImplementation = defineChimpbaseModuleImplementation({
      interface: right,
      calls: {
        async read(ctx) {
          return await ctx.kv.get("same", v.string());
        },
        async write(ctx, input) {
          await ctx.kv.set("same", input.value);
          return input.value;
        },
      },
      registrations: [rightPrivate],
    });
    const host = await createChimpbase({
      app: defineChimpbaseApp({
        modules: [leftImplementation, rightImplementation],
        registrations: [forceCron],
      }),
      storage: { engine: "memory" },
    });

    try {
      await host.executeAction(left.calls.write.id, { value: "L" });
      await host.executeAction(right.calls.write.id, { value: "R" });
      expect((await host.executeAction(left.calls.read.id, {})).result).toBe("L");
      expect((await host.executeAction(right.calls.read.id, {})).result).toBe("R");
      expect(host.registry.workers.has(chimpbaseModuleResourceName("left", "queue", "job"))).toBe(true);

      const routed = await host.executeRoute(new Request("http://localhost/owner"));
      expect(await routed.response?.text()).toBe("left");
      await host.executeAction(left.calls.enqueue.id, {});
      await host.processNextQueueJob();
      await host.executeAction(left.calls.startFlow.id, {});
      await host.processNextQueueJob();
      await host.executeAction(left.calls.startBadFlow.id, {});
      await expect(host.processNextQueueJob()).rejects.toThrow("internal operation");
      expect(rightPrivateRuns).toBe(0);

      await host.engine.syncRegisteredCrons();
      const cronName = chimpbaseModuleResourceName("left", "cron", "tick");
      await host.executeAction(forceCron.name, { name: cronName, nextFireAtMs: Date.now() - 1_000 });
      await host.engine.processNextCronSchedule();
      await host.processNextQueueJob();
      const started = await host.start({ runWorker: false, serve: false });
      await started.stop();
      expect(seenModules).toEqual(["left", "left", "left", "left", "left"]);
      await expect(host.executeAction(left.calls.rawSql.id, {})).rejects.toThrow(
        "module left raw SQL cannot access schema chimpbase_right",
      );
    } finally {
      await host.close();
    }
  });

  test("persists event outbox jobs atomically and resumes after restart", async () => {
    const dir = await mkdtemp(join(tmpdir(), "chimpbase-modules-durable-"));
    cleanupDirs.push(dir);
    const databasePath = join(dir, "events.sqlite");
    const facts = defineChimpbaseModuleInterface({
      name: "facts",
      version: 1,
      calls: {
        emit: { input: v.object({ id: v.string() }), output: v.boolean(), errors: [], guarantees: [] },
      },
      events: { committed: { payload: v.object({ id: v.string() }), version: 1 } },
    });
    const projector = defineChimpbaseModuleInterface({
      name: "projector",
      version: 1,
      calls: {
        audited: { input: v.object({ id: v.string() }), output: v.boolean(), errors: [], guarantees: [] },
        seen: { input: v.object({ id: v.string() }), output: v.boolean(), errors: [], guarantees: [] },
      },
      events: {},
    });
    let attempts = 0;
    const inspectDeadLetters = action({
      name: "test.inspect-module-dlq",
      args: v.object({}),
      result: v.integer(),
      async handler(ctx) {
        const [row] = await ctx.db.query(
          "SELECT COUNT(*) AS count FROM _chimpbase_queue_jobs WHERE status = 'dlq'",
          undefined,
          v.object({ count: v.integer() }),
        );
        return row?.count ?? 0;
      },
    });
    const app = defineChimpbaseApp({
      modules: [
        defineChimpbaseModuleImplementation({
          interface: facts,
          calls: {
            emit(ctx, input) {
              ctx.publish(facts.events.committed, input);
              return true;
            },
          },
        }),
        defineChimpbaseModuleImplementation({
          interface: projector,
          calls: {
            async audited(ctx, input) {
              return await ctx.kv.get(`audited:${input.id}`, v.boolean()) ?? false;
            },
            async seen(ctx, input) {
              return await ctx.kv.get(`seen:${input.id}`, v.boolean()) ?? false;
            },
          },
          subscriptions: [
            defineChimpbaseModuleSubscription(facts.events.committed, "project", async (ctx, event) => {
              attempts += 1;
              await ctx.kv.set(`seen:${event.id}`, true);
              if (event.id === "poison") throw new Error("poison event");
              if (attempts === 1) throw new Error("retry me");
            }),
            defineChimpbaseModuleSubscription(facts.events.committed, "audit", async (ctx, event) => {
              await ctx.kv.set(`audited:${event.id}`, true);
            }),
          ],
        }),
      ],
      registrations: [inspectDeadLetters],
      worker: { maxAttempts: 2, retryDelayMs: 0 },
    });

    const first = await createChimpbase({
      app,
      projectDir: dir,
      storage: { engine: "sqlite", path: databasePath },
      subscriptions: { dispatch: "async" },
    });
    await first.executeAction(facts.calls.emit.id, { id: "f1" });
    expect((await first.executeAction(projector.calls.seen.id, { id: "f1" })).result).toBe(false);
    await first.close();

    const restarted = await createChimpbase({
      app,
      projectDir: dir,
      storage: { engine: "sqlite", path: databasePath },
      subscriptions: { dispatch: "async" },
    });
    try {
      await expect(restarted.processNextQueueJob()).rejects.toThrow("retry me");
      expect((await restarted.executeAction(projector.calls.seen.id, { id: "f1" })).result).toBe(false);
      await restarted.processNextQueueJob();
      await restarted.processNextQueueJob();
      expect((await restarted.executeAction(projector.calls.seen.id, { id: "f1" })).result).toBe(true);
      expect((await restarted.executeAction(projector.calls.audited.id, { id: "f1" })).result).toBe(true);
      expect(attempts).toBe(2);
      await restarted.executeAction(facts.calls.emit.id, { id: "poison" });
      await expect(restarted.processNextQueueJob()).rejects.toThrow("poison event");
      const poisonOutcomes: Array<PromiseSettledResult<unknown>> = [];
      for (let run = 0; run < 2; run += 1) {
        poisonOutcomes.push(...await Promise.allSettled([restarted.processNextQueueJob()]));
      }
      expect(poisonOutcomes.filter((outcome) => outcome.status === "rejected")).toHaveLength(1);
      expect((await restarted.executeAction(projector.calls.audited.id, { id: "poison" })).result).toBe(true);
      expect((await restarted.executeAction(inspectDeadLetters.name, {})).result).toBe(1);
    } finally {
      await restarted.close();
    }
  });
});

describe("module architecture tooling", () => {
  test("resolves aliases and re-exports and reports actionable import violations", async () => {
    const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-module-architecture-"));
    cleanupDirs.push(projectDir);
    await mkdir(join(projectDir, "src/modules/alpha"), { recursive: true });
    await mkdir(join(projectDir, "src/modules/beta"), { recursive: true });
    await mkdir(join(projectDir, "src/modules/gamma"), { recursive: true });
    await writeFile(join(projectDir, "tsconfig.json"), JSON.stringify({
      compilerOptions: {
        baseUrl: ".",
        moduleResolution: "Bundler",
        paths: { "@mods/*": ["src/modules/*"] },
      },
    }));
    await writeFile(join(projectDir, "src/modules/alpha/interface.ts"), "export const alpha = true;\n");
    await writeFile(join(projectDir, "src/modules/alpha/legal.ts"), "import '../beta/interface.ts';\n");
    await writeFile(join(projectDir, "src/modules/alpha/alias.ts"), "import '@mods/beta/implementation';\n");
    await writeFile(join(projectDir, "src/modules/alpha/reexport.ts"), "export * from '../beta/implementation.ts';\n");
    await writeFile(
      join(projectDir, "src/modules/beta/interface.ts"),
      "export const beta = true;\nexport * from './implementation.ts';\n",
    );
    await writeFile(join(projectDir, "src/modules/beta/implementation.ts"), "export const privateValue = true;\n");
    await writeFile(join(projectDir, "src/modules/gamma/interface.ts"), "export const gamma = true;\n");
    await writeFile(join(projectDir, "src/modules/gamma/illegal.ts"), "import '../beta/interface.ts';\n");
    await writeFile(join(projectDir, "src/outside.ts"), "import './modules/beta/implementation.ts';\n");
    await writeFile(join(projectDir, "chimpbase.app.ts"), "import './src/modules/beta/implementation.ts';\n");

    const alpha = defineChimpbaseModuleInterface({
      name: "alpha",
      version: 1,
      dependencies: ["beta"],
      calls: {},
      events: {},
    });
    const beta = defineChimpbaseModuleInterface({ name: "beta", version: 1, calls: {}, events: {} });
    const gamma = defineChimpbaseModuleInterface({ name: "gamma", version: 1, calls: {}, events: {} });
    const diagnostics = await checkChimpbaseModuleArchitecture([alpha, beta, gamma], { projectDir });
    expect(diagnostics).toEqual(expect.arrayContaining([
      expect.objectContaining({ file: "src/modules/alpha/alias.ts", rule: "deep-import", sourceModule: "alpha", targetModule: "beta" }),
      expect.objectContaining({ file: "src/modules/alpha/reexport.ts", rule: "deep-import", sourceModule: "alpha", targetModule: "beta" }),
      expect.objectContaining({ file: "src/modules/beta/interface.ts", rule: "deep-import", sourceModule: "beta", targetModule: "beta" }),
      expect.objectContaining({ file: "src/modules/gamma/illegal.ts", rule: "undeclared-dependency", sourceModule: "gamma", targetModule: "beta" }),
      expect.objectContaining({ file: "src/outside.ts", rule: "composition-root-only", sourceModule: null, targetModule: "beta" }),
    ]));
    expect(diagnostics.some((entry) => entry.file === "src/modules/alpha/legal.ts")).toBe(false);
    expect(diagnostics.some((entry) => entry.file === "chimpbase.app.ts")).toBe(false);
  });

  test("generates deterministic manifests and classifies compatibility", () => {
    const previousInterface = defineChimpbaseModuleInterface({
      name: "contracts",
      version: 1,
      calls: {
        fetch: {
          input: v.object({ id: v.string() }),
          output: v.object({ id: v.string(), label: v.string() }),
          errors: ["not_found"],
          guarantees: [],
        },
      },
      events: { changed: { payload: v.object({ id: v.string() }), version: 1 } },
    });
    const previousImplementation = defineChimpbaseModuleImplementation({
      interface: previousInterface,
      calls: { fetch: () => ({ id: "1", label: "one" }) },
      resources: { tables: ["items"], projections: ["item_summary"] },
    });
    const previous = generateChimpbaseModuleManifest([previousImplementation]);
    expect(generateChimpbaseModuleManifest([previousImplementation])).toEqual(previous);
    expect(renderChimpbaseModuleDatabaseTypes(previous)).toContain('\"items\": Record<string, unknown>');
    expect(renderChimpbaseModuleDatabaseTypes(previous)).toContain('\"item_summary\": Record<string, unknown>');
    expect(previous.pactInteractions).toEqual(expect.arrayContaining([
      expect.objectContaining({ kind: "action", name: "contracts/fetch@v1" }),
      expect.objectContaining({ kind: "event", name: "changed", version: 1 }),
    ]));
    const generatedPact = pactFromChimpbaseModuleInterface("consumer", previousInterface);
    expect(generatedPact.interactions).toEqual(expect.arrayContaining([
      expect.objectContaining({
        args: previousInterface.calls.fetch.input,
        kind: "action",
        name: previousInterface.calls.fetch.id,
        result: previousInterface.calls.fetch.output,
      }),
      expect.objectContaining({
        eventName: previousInterface.events.changed.id,
        kind: "event",
        payload: previousInterface.events.changed.payload,
      }),
    ]));

    const additiveInterface = defineChimpbaseModuleInterface({
      name: "contracts",
      version: 1,
      calls: {
        fetch: {
          input: v.object({ id: v.string() }),
          output: v.object({
            description: v.string().optional(),
            id: v.string(),
            label: v.string(),
          }),
          errors: ["not_found"],
          guarantees: [],
        },
        list: {
          input: v.object({}),
          output: v.string().array(),
          errors: [],
          guarantees: [],
        },
      },
      events: {
        changed: { payload: v.object({ id: v.string() }), version: 1 },
        changedV2: { name: "changed", payload: v.object({ id: v.string(), label: v.string() }), version: 2 },
      },
    });
    const additive = generateChimpbaseModuleManifest([defineChimpbaseModuleImplementation({
      interface: additiveInterface,
      calls: {
        fetch: () => ({ description: "new", id: "1", label: "one" }),
        list: () => [],
      },
    })]);
    expect(compareChimpbaseModuleManifests(previous, additive).classification).toBe("compatible");

    const newErrorInterface = defineChimpbaseModuleInterface({
      name: "contracts",
      version: 1,
      calls: {
        fetch: {
          input: v.object({ id: v.string() }),
          output: v.object({ id: v.string(), label: v.string() }),
          errors: ["not_found", "rate_limited"],
          guarantees: [],
        },
      },
      events: { changed: { payload: v.object({ id: v.string() }), version: 1 } },
    });
    const newErrorManifest = generateChimpbaseModuleManifest([
      defineChimpbaseModuleImplementation({
        interface: newErrorInterface,
        calls: { fetch: () => ({ id: "1", label: "one" }) },
      }),
    ]);
    expect(compareChimpbaseModuleManifests(previous, newErrorManifest).classification).toBe("migration-required");

    const breakingInterface = defineChimpbaseModuleInterface({
      name: "contracts",
      version: 1,
      calls: {
        fetch: {
          input: v.object({ id: v.string(), required: v.string() }),
          output: v.object({ id: v.string() }),
          errors: ["not_found"],
          guarantees: [],
        },
      },
      events: { changed: { payload: v.object({ id: v.string(), label: v.string() }), version: 1 } },
    });
    const breakingManifest = generateChimpbaseModuleManifest([defineChimpbaseModuleImplementation({
      interface: breakingInterface,
      calls: { fetch: () => ({ id: "1" }) },
    })]);
    const compatibility = compareChimpbaseModuleManifests(previous, breakingManifest);
    expect(compatibility.classification).toBe("breaking");
    expect(compatibility.diagnostics.map((entry) => entry.contract)).toEqual(expect.arrayContaining([
      "contracts/fetch@v1",
      "contracts/changed@v1",
    ]));
  });
});
