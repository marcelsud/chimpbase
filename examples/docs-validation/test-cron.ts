import { createChimpbase } from "@chimpbase/bun";
import { cron, Cron, registrationsFrom } from "@chimpbase/runtime";
import type { ChimpbaseContext, ChimpbaseCronInvocation } from "@chimpbase/runtime";
const _check = (inv: ChimpbaseCronInvocation) => { const _a: string = inv.fireAt; const _b: number = inv.fireAtMs; const _c: string = inv.name; const _d: string = inv.schedule; };
const chimpbase = await createChimpbase({ storage: { engine: "memory" }, server: { port: 0 } });
const dailyReport = cron("reports.daily", "0 9 * * *", async (ctx, invocation) => { ctx.log.info("report", { fireAt: invocation.fireAt }); });
class ReportsModule { @Cron("reports.weekly", "0 0 * * 1") async weekly(_ctx: ChimpbaseContext, _inv: ChimpbaseCronInvocation) {} }
chimpbase.register(dailyReport); chimpbase.register(...registrationsFrom(new ReportsModule()));
await chimpbase.start(); console.log("cron: OK"); chimpbase.close(); process.exit(0);
