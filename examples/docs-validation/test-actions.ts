import { createChimpbase } from "@chimpbase/bun";
import { action, v, Action, registrationsFrom } from "@chimpbase/runtime";
import type { ChimpbaseContext } from "@chimpbase/runtime";
const chimpbase = await createChimpbase({ storage: { engine: "memory" }, server: { port: 0 }, migrationsSql: [`CREATE TABLE IF NOT EXISTS projects (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, description TEXT)`] });
const createProject = action({ args: v.object({ name: v.string(), description: v.string().optional() }), async handler(ctx, input) { const [project] = await ctx.db.query("insert into projects (name, description) values (?1, ?2) returning id", [input.name, input.description ?? null], v.object({ id: v.number() })); return project; } });
const setupWorkspace = action({ args: v.object({ name: v.string() }), async handler(ctx, input) { const project = await ctx.action("createProject", { name: input.name }); return project; } });
const _validators = v.object({ str: v.string(), num: v.number(), bool: v.boolean(), obj: v.object({ x: v.number() }), arr: v.array(v.string()), enm: v.enum(["a", "b"]), lit: v.literal("active"), uni: v.union(v.string(), v.number()), nul: v.null(), unk: v.unknown(), any: v.any(), opt: v.string().optional(), nullable: v.string().nullable(), arrShort: v.string().array() });
class ProjectModule { @Action("createProjectDeco") async createProjectDeco(_ctx: ChimpbaseContext, input: { name: string }) { return { name: input.name }; } }
const decoRegs = registrationsFrom(new ProjectModule());
chimpbase.register({ createProject, setupWorkspace }); chimpbase.register(...decoRegs);
await chimpbase.start();
const r1 = await chimpbase.executeAction("createProject", { name: "Test" }); console.log("actions (create):", JSON.stringify(r1.result));
const r2 = await chimpbase.executeAction("setupWorkspace", { name: "Workspace" }); console.log("actions (cross-call):", JSON.stringify(r2.result));
const r3 = await chimpbase.executeAction("createProjectDeco", { name: "Deco" }); console.log("actions (decorator):", JSON.stringify(r3.result));
console.log("actions: OK"); await chimpbase.close(); process.exit(0);
