import { createChimpbase } from "@chimpbase/bun";
import { action, v } from "@chimpbase/runtime";
const chimpbase = await createChimpbase({ storage: { engine: "memory" }, server: { port: 0 } });
const testStreams = action({ name: "testStreams", args: v.object({}), async handler(ctx) {
  // Append
  const id1 = await ctx.stream.append("todo.activity", "todo.created", { todoId: 1, title: "First" });
  const id2 = await ctx.stream.append("todo.activity", "todo.completed", { todoId: 1, title: "First" });
  if (typeof id1 !== "number") throw new Error("append should return event id");

  // Read all
  const all = await ctx.stream.read("todo.activity");
  if (all.length !== 2) throw new Error("read should return 2 events");
  if (all[0].event !== "todo.created") throw new Error("first event should be todo.created");

  // Read with limit
  const limited = await ctx.stream.read("todo.activity", { limit: 1 });
  if (limited.length !== 1) throw new Error("read with limit 1 should return 1 event");

  // Read with sinceId (pagination)
  const newer = await ctx.stream.read("todo.activity", { sinceId: id1 });
  if (newer.length !== 1) throw new Error("sinceId should return 1 newer event");
  if (newer[0].event !== "todo.completed") throw new Error("newer event should be todo.completed");

  return { total: all.length, event: all[0].event, sinceId: newer.length };
} });
chimpbase.register({ testStreams }); await chimpbase.start();
const r = await chimpbase.executeAction("testStreams", {}); console.log("streams:", JSON.stringify(r.result));
console.log("streams: OK"); await chimpbase.close(); process.exit(0);
