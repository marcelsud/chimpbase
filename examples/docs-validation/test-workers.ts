import { createChimpbase } from "@chimpbase/bun";
import { action, worker, v, Worker, registrationsFrom } from "@chimpbase/runtime";
const chimpbase = await createChimpbase({ storage: { engine: "memory" }, server: { port: 0 }, migrationsSql: [`CREATE TABLE IF NOT EXISTS customers (id INTEGER PRIMARY KEY AUTOINCREMENT, synced_at TEXT)`] });
const syncCustomer = worker("customer.sync", async (ctx, payload: any) => { ctx.log.info("syncing", { customerId: payload.customerId }); await ctx.db.query("update customers set synced_at = datetime('now') where id = ?1", [payload.customerId]); });
const syncCustomerDlq = worker("customer.sync.dlq", async (ctx, envelope: any) => { ctx.log.error("failed", { queue: envelope.queue, attempts: envelope.attempts, error: envelope.error, payload: envelope.payload }); });
const noDlqWorker = worker("nodlq.queue", async () => {}, { dlq: false });
const triggerSync = action({ args: v.object({ customerId: v.number() }), async handler(ctx, input) { await ctx.db.query("insert into customers (id) values (?1)", [input.customerId]); await ctx.enqueue("customer.sync", { customerId: input.customerId }); await ctx.enqueue("nodlq.queue", { test: true }, { delayMs: 60_000 }); return { ok: true }; } });
class SyncModule { @Worker("deco.sync") async syncDeco(_ctx: any, _p: any) {} }
chimpbase.register({ syncCustomer, syncCustomerDlq, noDlqWorker, triggerSync }); chimpbase.register(...registrationsFrom(new SyncModule()));
await chimpbase.start();
const result = await chimpbase.executeAction("triggerSync", { customerId: 1 }); console.log("workers:", JSON.stringify(result.result));
await new Promise((r) => setTimeout(r, 1500)); console.log("workers: OK"); chimpbase.close(); process.exit(0);
