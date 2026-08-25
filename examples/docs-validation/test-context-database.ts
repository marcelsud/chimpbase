import { createChimpbase } from "@chimpbase/bun";
import { action, v } from "@chimpbase/runtime";
const chimpbase = await createChimpbase({ storage: { engine: "memory" }, server: { port: 0 }, migrationsSql: [`CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1)`] });
const rawQuery = action({ name: "rawQuery", args: v.object({}), async handler(ctx) { await ctx.db.query("insert into users (email) values (?1)", ["test@test.com"]); return await ctx.db.query("select id, email from users where active = ?1", [true], v.object({ id: v.number(), email: v.string() })); } });
interface Database { users: { id: number; email: string; active: boolean }; }
const kyselyQuery = action({ name: "kyselyQuery", args: v.object({}), async handler(ctx) { return await ctx.db.kysely<Database>().selectFrom("users").where("active", "=", true).selectAll().execute(); } });
chimpbase.register({ rawQuery, kyselyQuery }); await chimpbase.start();
const r1 = await chimpbase.executeAction("rawQuery", {}); console.log("database (raw):", JSON.stringify(r1.result));
const r2 = await chimpbase.executeAction("kyselyQuery", {}); console.log("database (kysely):", JSON.stringify(r2.result));
console.log("database: OK"); await chimpbase.close(); process.exit(0);
