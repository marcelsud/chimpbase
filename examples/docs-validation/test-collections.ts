import { createChimpbase } from "@chimpbase/bun";
import { action, v } from "@chimpbase/runtime";
const chimpbase = await createChimpbase({ storage: { engine: "memory" }, server: { port: 0 } });
const testCollections = action({ name: "testCollections", args: v.object({}), async handler(ctx) {
  // Insert
  const id = await ctx.collection.insert("notes", { body: "First note", todoId: 42, createdAt: new Date().toISOString() });
  if (!id) throw new Error("insert should return id");

  // Find with filter
  const notes = await ctx.collection.find("notes", { todoId: 42 });
  if (notes.length !== 1) throw new Error("find should return 1 note");

  // Find with limit
  const limited = await ctx.collection.find("notes", {}, { limit: 10 });
  if (limited.length !== 1) throw new Error("find with limit should return 1 note");

  // Find one
  const note = await ctx.collection.findOne("notes", { id });
  if (!note) throw new Error("findOne should return the note");

  // Update
  const updated = await ctx.collection.update("notes", { id }, { body: "Updated note" });
  if (updated !== 1) throw new Error("update should return 1");

  // List collections
  const names = await ctx.collection.list();
  if (!names.includes("notes")) throw new Error("list should include notes");

  // Delete
  const deleted = await ctx.collection.delete("notes", { id });
  if (deleted !== 1) throw new Error("delete should return 1");

  return { id, found: notes.length, updated, deleted };
} });
chimpbase.register({ testCollections }); await chimpbase.start();
const r = await chimpbase.executeAction("testCollections", {}); console.log("collections:", JSON.stringify(r.result));
console.log("collections: OK"); await chimpbase.close(); process.exit(0);
