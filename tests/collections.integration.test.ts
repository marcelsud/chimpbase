import { describe, expect, test } from "bun:test";
import { Pool } from "pg";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { action } from "../packages/runtime/index.ts";

const PG_URL = process.env.CHIMPBASE_TEST_PG_URL;
const describeIfPg = PG_URL ? describe : describe.skip;

for (const engine of ["memory", "postgres"] as const) {
  const describeEngine = engine === "postgres" ? describeIfPg : describe;
  describeEngine(`collection semantics (${engine})`, () => {
    test("preserves strict filters, missing fields, limits and shallow patches", async () => {
      const name = `collection.'semantics.${crypto.randomUUID()}`;
      const host = await createChimpbase({
        storage: engine === "postgres" ? { engine, url: PG_URL } : { engine },
        registrations: [action("checkCollections", async (ctx) => {
          for (const document of [
            { kind: "number", value: 7, nullable: null, obsolete: "remove", nested: { left: 1, right: 2 }, array: [1, 2], "odd'field": "quoted" },
            { kind: "string", value: "7" },
            { kind: "null", value: null },
            { kind: "missing" },
            { kind: "boolean", value: false },
          ]) await ctx.collection.insert(name, document);
          try {
            const all = await ctx.collection.find(name);
            const filters: Record<string, unknown>[] = [
              { value: 7 }, { value: "7" }, { value: false }, { value: null }, { value: undefined },
              { nullable: null }, { nullable: undefined }, { nested: { left: 1, right: 2 } }, { array: [1, 2] },
              { value: NaN }, { value: Infinity }, { value: 7n }, { "odd'field": "quoted" },
              { constructor: undefined }, { toString: Object.prototype.toString },
            ];
            for (const filter of filters) {
              const expected = all.filter((document) => Object.entries(filter).every(([key, value]) => document[key] === value));
              expect(await ctx.collection.find(name, filter)).toEqual(expected);
              expect(await ctx.collection.findOne(name, filter)).toEqual(expected[0] ?? null);
            }
            for (const limit of [0, 1, 1.9, -1, -1.9, -0.5, NaN, Infinity, -Infinity, Number.MAX_VALUE, -Number.MAX_VALUE]) {
              expect(await ctx.collection.find(name, {}, { limit })).toEqual(all.slice(0, limit));
            }
            expect(await ctx.collection.update(name, { kind: "number" }, {
              nested: { replacement: 3 }, array: [9], obsolete: undefined, nullable: null,
            })).toBe(1);
            const updated = await ctx.collection.findOne(name, { kind: "number" });
            expect(updated).toMatchObject({ nested: { replacement: 3 }, array: [9], nullable: null });
            expect(updated).not.toHaveProperty("obsolete");
            expect(await ctx.collection.update(name, { nested: { replacement: 3 } }, { wrong: true })).toBe(0);
            const oldId = updated?.id;
            expect(await ctx.collection.update(name, { id: oldId }, { id: "changed-id" })).toBe(1);
            expect(await ctx.collection.findOne(name, { id: oldId })).toBeNull();
            expect(await ctx.collection.findOne(name, { id: "changed-id" })).toMatchObject({ kind: "number" });
            expect(await ctx.collection.update(name, { id: "changed-id" }, { id: undefined })).toBe(1);
            expect(await ctx.collection.findOne(name, { id: undefined })).toMatchObject({ kind: "number" });
            expect(await ctx.collection.delete(name, { array: [9] })).toBe(0);
            expect(await ctx.collection.delete(name, { id: undefined })).toBe(1);
          } finally {
            await ctx.collection.delete(name);
          }
        })],
      });
      try { await host.executeAction("checkCollections"); }
      finally { await host.close(); }
    });
  });
}

describeIfPg("PostgreSQL collection concurrency and Kysely", () => {
  test("preserves disjoint patches while a competing transaction holds the row lock", async () => {
    const name = `collection.concurrent.${crypto.randomUUID()}`;
    const pool = new Pool({ connectionString: PG_URL });
    let releaseFirst = () => {};
    let firstUpdated = () => {};
    const held = new Promise<void>((resolve) => { releaseFirst = resolve; });
    const updated = new Promise<void>((resolve) => { firstUpdated = resolve; });
    const hostA = await createChimpbase({
      storage: { engine: "postgres", url: PG_URL },
      registrations: [
        action("seed", async (ctx) => await ctx.collection.insert(name, { base: true })),
        action("patchA", async (ctx) => {
          expect(await ctx.collection.update(name, {}, { left: 1 })).toBe(1);
          firstUpdated();
          await held;
        }),
        action("read", async (ctx) => await ctx.collection.findOne(name, {})),
      ],
    });
    const hostB = await createChimpbase({
      storage: { engine: "postgres", url: PG_URL },
      registrations: [action("patchB", async (ctx) => await ctx.collection.update(name, {}, { right: 2 }))],
    });
    const running: Promise<unknown>[] = [];
    try {
      await hostA.executeAction("seed");
      const first = hostA.executeAction("patchA");
      running.push(first);
      await Promise.race([updated, first.then(() => { throw new Error("first patch did not hold its row lock"); })]);
      running.push(hostB.executeAction("patchB"));
      const deadline = Date.now() + 4000;
      while (true) {
        const waiting = await pool.query<{ blocked: boolean }>(
          `SELECT EXISTS (
            SELECT 1 FROM pg_stat_activity
            WHERE datname = current_database() AND wait_event_type = 'Lock'
              AND query LIKE '%UPDATE _chimpbase_collections%'
          ) AS blocked`,
        );
        if (waiting.rows[0]?.blocked) break;
        if (Date.now() > deadline) throw new Error("competing collection update did not reach its row lock");
        await Bun.sleep(10);
      }
      releaseFirst();
      await Promise.all(running);
      expect((await hostA.executeAction("read")).result).toMatchObject({ base: true, left: 1, right: 2 });
    } finally {
      releaseFirst();
      await Promise.allSettled(running);
      await hostA.close();
      await hostB.close();
      await pool.query("DELETE FROM _chimpbase_collections WHERE collection_name = $1", [name]);
      await pool.end();
    }
  });

  test("shares Kysely execution, streaming and transaction restrictions", async () => {
    const table = `kysely_test_${crypto.randomUUID().replaceAll("-", "")}`;
    const host = await createChimpbase({
      storage: { engine: "postgres", url: PG_URL },
      registrations: [action("checkKysely", async (ctx) => {
        const db = ctx.db.kysely<Record<string, { id: number; label: string }>>();
        await db.schema.createTable(table).addColumn("id", "integer", (column) => column.primaryKey())
          .addColumn("label", "text", (column) => column.notNull()).execute();
        expect((await db.insertInto(table).values([1, 2, 3].map((id) => ({ id, label: `item-${id}` }))).executeTakeFirst())
          .numInsertedOrUpdatedRows).toBe(3n);
        expect((await db.updateTable(table).set({ label: "updated" }).where("id", "=", 1).executeTakeFirst()).numUpdatedRows).toBe(1n);
        expect((await db.deleteFrom(table).where("id", "=", 3).executeTakeFirst()).numDeletedRows).toBe(1n);
        const expected = [{ id: 1, label: "updated" }, { id: 2, label: "item-2" }];
        expect(await db.selectFrom(table).selectAll().orderBy("id").execute()).toEqual(expected);
        for (const chunkSize of [0, 1, 20]) {
          const rows: { id: number; label: string }[] = [];
          for await (const row of db.selectFrom(table).selectAll().orderBy("id").stream(chunkSize)) rows.push(row);
          expect(rows).toEqual(expected);
        }
        await expect(db.transaction().execute(async () => {})).rejects.toThrow("runtime-managed transactions");
        await db.schema.dropTable(table).execute();
        await db.destroy();
      })],
    });
    try { await host.executeAction("checkKysely"); }
    finally { await host.close(); }
  });
});
