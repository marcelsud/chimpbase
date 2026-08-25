import { createChimpbase } from "@chimpbase/bun";
import { action, v } from "@chimpbase/runtime";
const chimpbase = await createChimpbase({ storage: { engine: "memory" }, server: { port: 0 } });
const testKv = action({ name: "testKv", args: v.object({}), async handler(ctx) {
  // Set and get
  await ctx.kv.set("workspace.theme", "dark");
  const theme = await ctx.kv.get<string>("workspace.theme");
  if (theme !== "dark") throw new Error("get should return dark");

  // Set with TTL
  await ctx.kv.set("session:abc", { userId: 42 }, { ttlMs: 3_600_000 });
  const session = await ctx.kv.get<{ userId: number }>("session:abc");
  if (!session || session.userId !== 42) throw new Error("TTL key should be readable");

  // List with prefix
  await ctx.kv.set("workspace.language", "en");
  const keys = await ctx.kv.list({ prefix: "workspace." });
  if (keys.length !== 2) throw new Error("list should return 2 workspace keys");

  // Delete
  await ctx.kv.delete("workspace.theme");
  const deleted = await ctx.kv.get("workspace.theme");
  if (deleted !== null) throw new Error("deleted key should be null");

  return { theme, session, keys: keys.length };
} });
chimpbase.register({ testKv }); await chimpbase.start();
const r = await chimpbase.executeAction("testKv", {}); console.log("kv:", JSON.stringify(r.result));
console.log("kv: OK"); await chimpbase.close(); process.exit(0);
