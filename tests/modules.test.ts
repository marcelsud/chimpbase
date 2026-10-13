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
  type ChimpbaseDlqEnvelope,
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

    expect(catalog.calls.find.id).toBe("catalog/find@v1");
    expect(catalog.calls.find.module).toBe("catalog");
    expect(catalog.calls.find.version).toBe(1);
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

  test("module SQL checks comma joins and query-scoped CTEs without mistaking expressions for tables", () => {
    const check = (sql: string) => assertChimpbaseModuleRuntimeSql("left", "chimpbase_left", sql);
    for (const sql of [
      "WITH owned AS (SELECT 1) SELECT secret.* FROM owned, _chimpbase_kv secret",
      "WITH owned AS (SELECT 1) SELECT secret.* FROM (owned, _chimpbase_kv secret)",
      "SELECT * FROM (WITH _chimpbase_kv AS (SELECT 1) SELECT * FROM _chimpbase_kv) nested, _chimpbase_kv secret",
      "WITH owned AS (SELECT 1) SELECT * FROM owned JOIN _chimpbase_kv secret ON true",
      "SELECT * FROM chimpbase_left.items WHERE EXISTS (SELECT 1 FROM _chimpbase_kv)",
      'SELECT * FROM chimpbase_left.items, "public"."secret"',
      "WITH owned AS (TABLE _chimpbase_kv) SELECT * FROM owned",
      "SELECT 1 UNION TABLE _chimpbase_kv",
      "SELECT 1 WHERE EXISTS (TABLE _chimpbase_kv)",
      "SELECT key -- hidden relation\rFROM _chimpbase_kv",
      "SELECT * FROM if",
      "WITH owned AS (SELECT 1) SELECT * FROM owned, if",
      "WITH owned AS (SELECT * FROM if) SELECT * FROM owned",
      "SELECT 1 INTO unowned",
      "SELECT 1 INTO TABLE unowned",
      "SELECT 1 INTO TEMP unowned",
      "SELECT 1 INTO TEMPORARY TABLE unowned",
      "SELECT 1 INTO UNLOGGED unowned",
      "SELECT 1 INTO public.unowned",
      "WITH unowned AS (SELECT 1) SELECT 1 INTO unowned",
      "WITH _chimpbase_kv AS (SELECT 1) INSERT INTO _chimpbase_kv(key, value_json) VALUES ('key', 'null')",
      "WITH _chimpbase_kv AS (SELECT 1) UPDATE _chimpbase_kv SET value_json = 'null'",
      "WITH _chimpbase_kv AS (SELECT 1) DELETE FROM _chimpbase_kv",
      "WITH _chimpbase_kv AS (SELECT 1) DELETE FROM ONLY (_chimpbase_kv)",
    ]) expect(() => check(sql)).toThrow();
    for (const sql of [
      "SELECT EXTRACT(year FROM created_at) FROM chimpbase_left.items",
      "SELECT trim(both ' ' FROM label) FROM chimpbase_left.items",
      "WITH owned(id) AS MATERIALIZED (SELECT id FROM chimpbase_left.items) SELECT * FROM owned, chimpbase_left.items",
      'WITH "FROM" AS (SELECT 1) SELECT * FROM "FROM"',
      'SELECT "comma,FROM;public.secret" FROM chimpbase_left.items',
      "WITH owned AS (SELECT 1) SELECT * FROM (owned JOIN chimpbase_left.items ON true), chimpbase_left.items other",
      "SELECT $$FROM _chimpbase_kv;$$, $value$public.secret$value$ FROM chimpbase_left.items /* outer /* nested */ comment */",
      "SELECT E'escaped\\' FROM _chimpbase_kv;' FROM chimpbase_left.items",
      "INSERT INTO chimpbase_left.items(id) VALUES (1) ON CONFLICT(id) DO UPDATE SET id = 1",
      "WITH owned AS (TABLE chimpbase_left.items) SELECT * FROM owned",
      "SELECT 1 WHERE EXISTS (TABLE chimpbase_left.items)",
      "WITH if AS (SELECT 1) SELECT * FROM if",
      "SELECT 1 INTO chimpbase_left.items",
      "SELECT 1 INTO TABLE chimpbase_left.items",
      "SELECT 1 INTO UNLOGGED TABLE chimpbase_left.items",
      "SELECT 1 INTO TEMP chimpbase_left.items",
      "SELECT 1 INTO TEMPORARY TABLE chimpbase_left.items",
      "WITH owned AS (SELECT 1) SELECT 1 INTO chimpbase_left.items FROM owned",
      "WITH owned AS (SELECT 1) INSERT INTO chimpbase_left.items(id) SELECT * FROM owned",
      "WITH owned AS (SELECT 1) UPDATE chimpbase_left.items SET id = 1 FROM owned",
      "WITH owned AS (SELECT 1) DELETE FROM chimpbase_left.items USING owned",
    ]) expect(() => check(sql)).not.toThrow();
    expect(() => assertChimpbaseModuleMigrationSql("left", "chimpbase_left",
      "CREATE TABLE IF NOT EXISTS chimpbase_left.items (id integer)",
    )).not.toThrow();
    expect(() => assertChimpbaseModuleMigrationSql("left", "chimpbase_left",
      "DROP TABLE IF EXISTS chimpbase_left.items",
    )).not.toThrow();
    for (const sql of [
      "CREATE TABLE IF NOT EXISTS public.secret (id integer)",
      "DROP TABLE IF EXISTS public.secret",
      "CREATE TABLE IF NOT EXISTS if (id integer)",
      "DROP TABLE IF EXISTS if",
    ]) expect(() => assertChimpbaseModuleMigrationSql("left", "chimpbase_left", sql)).toThrow();
  });

  test("module SQL follows CTE declaration order and recursive visibility", () => {
    const check = (sql: string) => assertChimpbaseModuleRuntimeSql("left", "chimpbase_left", sql);
    for (const sql of [
      "WITH _chimpbase_kv AS (SELECT * FROM _chimpbase_kv) SELECT * FROM _chimpbase_kv",
      'WITH "_chimpbase_kv" AS NOT MATERIALIZED (SELECT * FROM "_chimpbase_kv") SELECT * FROM "_chimpbase_kv"',
      "WITH first AS (SELECT * FROM _chimpbase_kv), _chimpbase_kv AS (SELECT 1) SELECT * FROM first",
      "WITH first AS (SELECT * FROM _chimpbase_kv), second AS (SELECT 1), _chimpbase_kv AS (SELECT 1) SELECT * FROM first",
      "WITH outer_cte AS (WITH inner_cte AS (SELECT * FROM outer_cte) SELECT * FROM inner_cte) SELECT * FROM outer_cte",
      "WITH owned AS (SELECT 1) SELECT * FROM (WITH _chimpbase_kv AS (SELECT * FROM _chimpbase_kv) SELECT * FROM _chimpbase_kv) nested",
    ]) expect(() => check(sql)).toThrow("must qualify owned relations");
    for (const sql of [
      "WITH first AS (SELECT * FROM chimpbase_left.items), second AS (SELECT * FROM first) SELECT * FROM second",
      'WITH "owned"(id) AS NOT MATERIALIZED (SELECT id FROM chimpbase_left.items), later AS (SELECT * FROM "owned") SELECT * FROM later',
      "WITH RECURSIVE counter(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM counter WHERE id < 3) SELECT * FROM counter",
      "WITH RECURSIVE first AS (SELECT * FROM later), later AS (SELECT id FROM chimpbase_left.items) SELECT * FROM first",
      "WITH outer_cte AS (WITH _chimpbase_kv AS (SELECT 1) SELECT * FROM _chimpbase_kv) SELECT * FROM outer_cte",
      "WITH owned AS (SELECT 1), nested AS (WITH owned AS (SELECT * FROM owned) SELECT * FROM owned) SELECT * FROM nested",
      "WITH _chimpbase_kv AS (SELECT 1 AS id), nested AS (WITH _chimpbase_kv AS (SELECT id FROM _chimpbase_kv) SELECT * FROM _chimpbase_kv) SELECT * FROM nested",
    ]) expect(() => check(sql)).not.toThrow();
  });

  test("module migrations keep objects in their owned schema", () => {
    const check = (sql: string) => assertChimpbaseModuleMigrationSql("left", "chimpbase_left", sql);
    for (const sql of [
      "ALTER TABLE chimpbase_left.items SET SCHEMA public",
      'ALTER TABLE chimpbase_left.items SET /* destination */ SCHEMA "public"',
      "ALTER VIEW chimpbase_left.items SET SCHEMA archive",
      'ALTER SEQUENCE chimpbase_left.ids SET SCHEMA "Archive"',
      "ALTER TABLE chimpbase_left.items SET SCHEMA chimpbase_right",
      'ALTER TABLE chimpbase_left.items SET SCHEMA "Chimpbase_Left"',
    ]) expect(() => check(sql)).toThrow();
    for (const sql of [
      "ALTER TABLE chimpbase_left.items SET SCHEMA chimpbase_left",
      'ALTER TABLE chimpbase_left.items SET SCHEMA "chimpbase_left"',
      "ALTER TABLE chimpbase_left.items ALTER COLUMN label SET DEFAULT 'SET SCHEMA public'",
      "ALTER TABLE chimpbase_left.items ALTER COLUMN label SET DEFAULT $$SET SCHEMA public$$",
      "ALTER TABLE chimpbase_left.items ADD COLUMN label text /* SET SCHEMA public */",
      "UPDATE chimpbase_left.items SET schema = 'public'",
    ]) expect(() => check(sql)).not.toThrow();
  });

  test("a module cannot read framework KV through a comma join in its public call", async () => {
    const guarded = defineChimpbaseModuleInterface({
      name: "sqlguard", version: 1, events: {},
      calls: { read: { input: v.object({}), output: v.unknown() } },
    });
    const host = await createChimpbase({
      storage: { engine: "memory" },
      modules: [defineChimpbaseModuleImplementation({
        interface: guarded,
        calls: { read: async (ctx) => await ctx.db.query(
          "WITH owned AS (SELECT 1) SELECT secret.key, secret.value_json FROM owned, _chimpbase_kv secret",
        ) },
      })],
      registrations: [action("seedSqlGuard", async (ctx) => await ctx.kv.set("global-secret", "private-data"))],
    });
    try {
      await host.executeAction("seedSqlGuard");
      await expect(host.executeAction(guarded.calls.read.id, {})).rejects.toThrow("must qualify owned relations");
    } finally { await host.close(); }
  });

  (process.env.CHIMPBASE_TEST_PG_URL ? test : test.skip)("PostgreSQL module calls allow owned joins, CTEs and EXTRACT while rejecting framework tables", async () => {
    const table = `sqlguard_${crypto.randomUUID().replaceAll("-", "")}`;
    const copyTable = `${table}_copy`;
    const guarded = defineChimpbaseModuleInterface({
      name: "sqlguard", version: 1, events: {},
      calls: {
        read: { input: v.object({}), output: v.unknown() },
        leak: { input: v.object({}), output: v.unknown() },
        tableRead: { input: v.object({}), output: v.unknown() },
        leakTable: { input: v.string(), output: v.unknown() },
      },
    });
    const host = await createChimpbase({
      storage: { engine: "postgres", url: process.env.CHIMPBASE_TEST_PG_URL },
      modules: [defineChimpbaseModuleImplementation({
        interface: guarded,
        calls: {
          read: async (ctx) => await ctx.db.query(
            `WITH owned(id) AS (SELECT id FROM chimpbase_sqlguard.${table})
             SELECT EXTRACT(year FROM actual.created_at)::int AS year
             FROM owned, chimpbase_sqlguard.${table} actual WHERE owned.id = actual.id`,
          ),
          leak: async (ctx) => await ctx.db.query(
            "WITH owned AS (SELECT 1) SELECT secret.key FROM owned, _chimpbase_kv secret",
          ),
          tableRead: async (ctx) => await ctx.db.query(
            `WITH owned AS (TABLE chimpbase_sqlguard.${table}) SELECT id FROM owned`,
          ),
          leakTable: async (ctx, sql) => await ctx.db.query(sql),
        },
      })],
      registrations: [
        action("setupSqlGuard", async (ctx) => {
          await ctx.db.query(`CREATE TABLE chimpbase_sqlguard.${table}(id integer, created_at timestamptz)`);
          await ctx.db.query(`INSERT INTO chimpbase_sqlguard.${table} VALUES (1, '2026-01-01')`);
          await ctx.db.query("CREATE TEMP TABLE if(value text)");
          await ctx.db.query("INSERT INTO if VALUES ('private-temp-data')");
          expect(await ctx.db.query("SELECT value FROM if")).toEqual([{ value: "private-temp-data" }]);
        }),
        action("cleanupSqlGuard", async (ctx) => {
          await ctx.db.query(`DROP TABLE IF EXISTS chimpbase_sqlguard.${copyTable}`);
          await ctx.db.query(`DROP TABLE IF EXISTS chimpbase_sqlguard.${table}`);
        }),
      ],
    });
    try {
      await host.executeAction("setupSqlGuard");
      expect((await host.executeAction(guarded.calls.read.id, {})).result).toEqual([{ year: 2026 }]);
      await expect(host.executeAction(guarded.calls.leak.id, {})).rejects.toThrow("must qualify owned relations");
      expect((await host.executeAction(guarded.calls.tableRead.id, {})).result).toEqual([{ id: 1 }]);
      expect((await host.executeAction(guarded.calls.leakTable.id,
        `WITH first AS (SELECT id FROM chimpbase_sqlguard.${table}), second AS (SELECT * FROM first) SELECT id FROM second`,
      )).result).toEqual([{ id: 1 }]);
      expect((await host.executeAction(guarded.calls.leakTable.id,
        "WITH RECURSIVE counter(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM counter WHERE id < 3) SELECT id FROM counter ORDER BY id",
      )).result).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
      expect((await host.executeAction(guarded.calls.leakTable.id,
        `WITH RECURSIVE first AS (SELECT id FROM later), later AS (SELECT id FROM chimpbase_sqlguard.${table}) SELECT id FROM first`,
      )).result).toEqual([{ id: 1 }]);
      await host.executeAction(guarded.calls.leakTable.id,
        `SELECT id INTO TABLE chimpbase_sqlguard.${copyTable} FROM chimpbase_sqlguard.${table}`);
      expect((await host.executeAction(guarded.calls.leakTable.id, `SELECT id FROM chimpbase_sqlguard.${copyTable}`)).result)
        .toEqual([{ id: 1 }]);
      for (const sql of [
        "WITH _chimpbase_kv AS (SELECT * FROM _chimpbase_kv) SELECT key, value_json FROM _chimpbase_kv",
        "WITH first AS (SELECT * FROM _chimpbase_kv), _chimpbase_kv AS (SELECT 1) SELECT key, value_json FROM first",
        'WITH "_chimpbase_kv" AS NOT MATERIALIZED (SELECT * FROM "_chimpbase_kv") SELECT key, value_json FROM "_chimpbase_kv"',
        "WITH owned AS (TABLE _chimpbase_kv) SELECT key, value_json FROM owned",
        "SELECT key, value_json FROM chimpbase_sqlguard.sqlguard_missing UNION TABLE _chimpbase_kv",
        "SELECT 1 WHERE EXISTS (TABLE _chimpbase_kv)",
        "SELECT key,value_json -- hidden relation\rFROM _chimpbase_kv",
        "SELECT value FROM if",
        "WITH owned AS (SELECT value FROM if) SELECT value FROM owned",
        "SELECT 1 INTO unowned",
        "WITH unowned AS (SELECT 1) SELECT 1 INTO unowned",
        "WITH _chimpbase_kv AS (SELECT 1) INSERT INTO _chimpbase_kv(key, value_json) VALUES ('key', 'null')",
        "WITH _chimpbase_kv AS (SELECT 1) UPDATE _chimpbase_kv SET value_json = 'null'",
        "WITH _chimpbase_kv AS (SELECT 1) DELETE FROM _chimpbase_kv",
      ]) await expect(host.executeAction(guarded.calls.leakTable.id, sql)).rejects.toThrow("must qualify owned relations");
    } finally {
      try { await host.executeAction("cleanupSqlGuard"); }
      finally { await host.close(); }
    }
  });

  (process.env.CHIMPBASE_TEST_PG_URL ? test : test.skip)("PostgreSQL module migration cannot move an owned table to public", async () => {
    const suffix = crypto.randomUUID().replaceAll("-", "").slice(0, 16);
    const guarded = defineChimpbaseModuleInterface({ name: `migration-${suffix}`, version: 1, calls: {}, events: {} });
    const schema = `chimpbase_migration_${suffix}`;
    await expect(createChimpbase({
      storage: { engine: "postgres", url: process.env.CHIMPBASE_TEST_PG_URL },
      modules: [defineChimpbaseModuleImplementation({
        interface: guarded,
        calls: {},
        migrations: { postgres: [{
          name: "move-items",
          sql: `CREATE TABLE ${schema}.items(id integer); ALTER TABLE ${schema}.items SET SCHEMA public`,
        }] },
      })],
    })).rejects.toThrow("cannot move objects to schema public");
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

  test("module route calls retain ownership and publish only after handler success", async () => {
    const provider = defineChimpbaseModuleInterface({
      name: "route-provider", version: 1,
      calls: { echo: { input: v.string(), output: v.string() } },
      events: { written: { payload: v.string(), version: 1 } },
    });
    const privateAction = action("privateRouteAction", () => "private");
    const host = await createChimpbase({
      storage: { engine: "memory" },
      modules: [defineChimpbaseModuleImplementation({
        interface: provider, calls: { echo: (_ctx, input) => input },
        registrations: [privateAction,
          route("GET", "/published", async (_request, env) => {
            expect(await env.action(privateAction)).toBe("private");
            env.publish(provider.events.written, "published");
            return new Response(await env.call(provider.calls.echo, "owned"));
          }),
          route("GET", "/discarded", (_request, env) => {
            env.publish(provider.events.written, "discarded");
            throw new Response("failed", { status: 409 });
          }),
        ],
      })],
    });
    try {
      const successful = await host.executeRoute(new Request("http://test/published"));
      expect(await successful.response?.text()).toBe("owned");
      expect(successful.emittedEvents).toMatchObject([{ name: provider.events.written.id, payload: "published" }]);
      const failed = await host.executeRoute(new Request("http://test/discarded"));
      expect(failed.response?.status).toBe(409);
      expect(failed.emittedEvents).toEqual([]);
      await expect(host.executeAction(privateAction)).rejects.toThrow("internal operation");
    } finally { await host.close(); }
  });

  for (const source of ["worker", "cron"] as const) {
    for (const target of ["own", "global", "foreign"] as const) {
      test(`module ${source} imported actions preserve ownership (${target})`, async () => {
        const calls: string[] = [];
        const own = action("owned", (ctx) => { calls.push(ctx.module?.name ?? "none"); });
        const global = action("global", () => { calls.push("global"); });
        const foreign = action("foreign", () => { calls.push("foreign"); });
        const selected = { own, global, foreign }[target];
        const owner = defineChimpbaseModuleInterface({
          name: "owner", version: 1, events: {},
          calls: { enqueue: { input: v.object({}), output: v.boolean() } },
        });
        const other = defineChimpbaseModuleInterface({ name: "other", version: 1, events: {}, calls: {} });
        const host = await createChimpbase({
          storage: { engine: "memory" }, worker: { maxAttempts: 1, retryDelayMs: 0 },
          registrations: [global],
          modules: [
            defineChimpbaseModuleImplementation({
              interface: owner,
              calls: { async enqueue(ctx) { await ctx.enqueue("job", {}); return true; } },
              registrations: [own, source === "worker"
                ? worker("job", async () => await selected(), { dlq: false })
                : cron("tick", "* * * * *", async () => await selected())],
            }),
            defineChimpbaseModuleImplementation({ interface: other, calls: {}, registrations: [foreign] }),
          ],
        });
        try {
          if (source === "worker") await host.executeAction(owner.calls.enqueue.id, {});
          else {
            await host.syncCronSchedules();
            await host.engine.getBlobsAdapter().query(
              "UPDATE _chimpbase_cron_schedules SET next_fire_at_ms = ?1 WHERE schedule_name = ?2",
              [Date.now() - 1_000, chimpbaseModuleResourceName("owner", "cron", "tick")], v.object({}),
            );
            await host.processNextCronSchedule();
          }
          if (target === "own") {
            expect(await host.processNextQueueJob()).not.toBeNull();
            expect(calls).toEqual(["owner"]);
          } else {
            await expect(host.processNextQueueJob()).rejects.toThrow(target === "global" ? "unowned registration" : "internal operation");
            expect(calls).toEqual([]);
          }
        } finally { await host.close(); }
      });
    }
  }

  for (const engine of ["memory", "sqlite"] as const) {
    test(`module custom DLQs reach their local handler (${engine})`, async () => {
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-module-dlq-"));
      cleanupDirs.push(projectDir);
      const mail = defineChimpbaseModuleInterface({
        name: "mail", version: 1, events: {},
        calls: { send: { input: v.object({}), output: v.boolean() } },
      });
      const failures: ChimpbaseDlqEnvelope[] = [];
      const host = await createChimpbase({
        projectDir, storage: { engine }, worker: { maxAttempts: 1, retryDelayMs: 0 },
        modules: [defineChimpbaseModuleImplementation({
          interface: mail,
          calls: { async send(ctx) { await ctx.enqueue("send", { to: "test@example.com" }); return true; } },
          registrations: [
            worker("send", () => { throw new Error("mail provider unavailable"); }, { dlq: "failed" }),
            worker<ChimpbaseDlqEnvelope>("failed", (ctx, payload) => {
              expect(ctx.module?.name).toBe("mail");
              failures.push(payload);
            }, { dlq: false }),
            worker("default", () => {}),
          ],
        })],
      });
      try {
        expect(host.registry.workers.get(chimpbaseModuleResourceName("mail", "queue", "default"))?.definition.dlq)
          .toBe(`${chimpbaseModuleResourceName("mail", "queue", "default")}.dlq`);
        expect(host.registry.workers.get(chimpbaseModuleResourceName("mail", "queue", "failed"))?.definition.dlq).toBe(false);
        await host.executeAction(mail.calls.send.id, {});
        await expect(host.processNextQueueJob()).rejects.toThrow("mail provider unavailable");
        expect((await host.processNextQueueJob())?.queueName).toBe(chimpbaseModuleResourceName("mail", "queue", "failed"));
        expect(failures).toMatchObject([{
          attempts: 1, error: "mail provider unavailable", payload: { to: "test@example.com" },
          queue: chimpbaseModuleResourceName("mail", "queue", "send"),
        }]);
      } finally { await host.close(); }
    });
  }

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
          const started = await ctx.workflow.start(flow, {});
          const loaded = await ctx.workflow.get(started.workflowId);
          if (loaded === null) throw new Error("started workflow was not found");
          return true;
        },
        async write(ctx, input) {
          expect(await ctx.kv.setIfAbsent("same.reservation", input.value)).toBe(true);
          expect(await ctx.kv.setIfAbsent("same.reservation", "replaced")).toBe(false);
          await ctx.kv.set("same", input.value);
          await ctx.collection.insert("same", { value: input.value });
          await ctx.stream.append("same", "written", input);
          return input.value;
        },
      },
      registrations: [
        route("owner", (_request, env) => new Response(env.module?.name ?? "none")),
        worker("job", async (ctx) => { seenModules.push(ctx.module?.name ?? "none"); }),
        onStart("boot", (ctx) => { seenModules.push(ctx.module?.name ?? "none"); }, { phase: "ready" }),
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
          expect(await ctx.kv.setIfAbsent("same.reservation", input.value)).toBe(true);
          expect(await ctx.kv.setIfAbsent("same.reservation", "replaced")).toBe(false);
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
      expect(host.registry.onStartHooks.find((hook) => hook.name === "boot")?.phase).toBe("ready");
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
    expect(diagnostics.some((entry) =>
      entry.file === "src/modules/alpha/alias.ts" && entry.rule === "deep-import"
    )).toBe(true);
    expect(diagnostics.some((entry) =>
      entry.file === "src/modules/alpha/reexport.ts" && entry.rule === "deep-import"
    )).toBe(true);
    expect(diagnostics.some((entry) =>
      entry.file === "src/modules/beta/interface.ts" && entry.rule === "deep-import"
    )).toBe(true);
    expect(diagnostics.some((entry) =>
      entry.file === "src/modules/gamma/illegal.ts" && entry.rule === "undeclared-dependency"
    )).toBe(true);
    expect(diagnostics.some((entry) =>
      entry.file === "src/outside.ts" && entry.rule === "composition-root-only"
    )).toBe(true);
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
    expect(previous.pactInteractions.some((entry) =>
      entry.kind === "action" && entry.name === "contracts/fetch@v1"
    )).toBe(true);
    expect(previous.pactInteractions.some((entry) =>
      entry.kind === "event" && entry.name === "changed" && entry.version === 1
    )).toBe(true);
    const generatedPact = pactFromChimpbaseModuleInterface("consumer", previousInterface);
    expect(generatedPact.interactions.some((entry) =>
      entry.kind === "action"
      && entry.args === previousInterface.calls.fetch.input
      && entry.name === previousInterface.calls.fetch.id
      && entry.result === previousInterface.calls.fetch.output
    )).toBe(true);
    expect(generatedPact.interactions.some((entry) =>
      entry.kind === "event"
      && entry.eventName === previousInterface.events.changed.id
      && entry.payload === previousInterface.events.changed.payload
    )).toBe(true);

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
    const incompatibleContracts = compatibility.diagnostics.map((entry) => entry.contract);
    expect(incompatibleContracts).toContain("contracts/fetch@v1");
    expect(incompatibleContracts).toContain("contracts/changed@v1");
  });
});
