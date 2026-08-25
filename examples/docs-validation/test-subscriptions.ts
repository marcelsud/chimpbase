import { createChimpbase } from "@chimpbase/bun";
import { action, subscription, v, Subscription, registrationsFrom } from "@chimpbase/runtime";
import type { ChimpbaseContext } from "@chimpbase/runtime";
const chimpbase = await createChimpbase({ storage: { engine: "memory" }, server: { port: 0 }, migrationsSql: [`CREATE TABLE IF NOT EXISTS audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, event TEXT NOT NULL, payload TEXT NOT NULL)`] });
const publishEvent = action({ args: v.object({ title: v.string() }), async handler(ctx, input) { ctx.pubsub.publish("todo.created", { todoId: 1, title: input.title }); ctx.pubsub.publish("todo.completed", { todoId: 1 }); return { ok: true }; } });
const onTodoCreated = subscription("todo.created", async (ctx, event: { todoId: number; title: string }) => { await ctx.db.query("insert into audit_log (event, payload) values (?1, ?2)", ["todo.created", JSON.stringify(event)]); }, { idempotent: true, name: "auditTodoCreated" });
const auditTodoCompleted = subscription("todo.completed", async (ctx) => { ctx.log.info("todo completed - audit"); }, { idempotent: true, name: "auditTodoCompleted" });
const notifyTodoCompleted = subscription("todo.completed", async (ctx) => { ctx.log.info("todo completed - notify"); }, { idempotent: true, name: "notifyTodoCompleted" });
class TodoModule { @Subscription("todo.created") async auditDeco(_ctx: ChimpbaseContext, _event: { todoId: number }) {} }
const decoRegs = registrationsFrom(new TodoModule());
chimpbase.register({ publishEvent, onTodoCreated, auditTodoCompleted, notifyTodoCompleted }); chimpbase.register(...decoRegs);
await chimpbase.start();
const result = await chimpbase.executeAction("publishEvent", { title: "Test" }); console.log("subscriptions:", JSON.stringify(result.result));
await new Promise((r) => setTimeout(r, 1000)); console.log("subscriptions: OK"); await chimpbase.close(); process.exit(0);
