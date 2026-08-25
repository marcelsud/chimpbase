import { createChimpbase } from "@chimpbase/bun";
import { action, v } from "@chimpbase/runtime";
const chimpbase = await createChimpbase({ storage: { engine: "memory" }, server: { port: 0 }, migrationsSql: [
  `CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT NOT NULL UNIQUE, name TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)`,
] });

// Raw SQL
const rawInsert = action({ name: "rawInsert", args: v.object({}), async handler(ctx) {
  await ctx.db.query("INSERT INTO users (email, name) VALUES (?1, ?2)", ["alice@example.com", "Alice"]);
  const users = await ctx.db.query("SELECT id, email FROM users WHERE email = ?1", ["alice@example.com"], v.object({ id: v.number(), email: v.string() }));
  if (users.length !== 1) throw new Error("should find 1 user");
  return users[0];
} });

// Kysely
interface Database { users: { id: number; email: string; name: string; created_at: string } }
const kyselyQuery = action({ name: "kyselyQuery", args: v.object({}), async handler(ctx) {
  const db = ctx.db.kysely<Database>();
  const users = await db.selectFrom("users").where("email", "=", "alice@example.com").selectAll().execute();
  if (users.length !== 1) throw new Error("kysely should find 1 user");
  return users[0];
} });

chimpbase.register({ rawInsert, kyselyQuery }); await chimpbase.start();
const r1 = await chimpbase.executeAction("rawInsert", {}); console.log("database (raw):", JSON.stringify(r1.result));
const r2 = await chimpbase.executeAction("kyselyQuery", {}); console.log("database (kysely):", JSON.stringify(r2.result));
console.log("database: OK"); await chimpbase.close(); process.exit(0);
