import { createChimpbase } from "@chimpbase/bun";
import { action, v } from "@chimpbase/runtime";
const chimpbase = await createChimpbase({ storage: { engine: "memory" }, server: { port: 0 } });
const testTelemetry = action({ name: "testTelemetry", args: v.object({}), async handler(ctx) {
  // Logging
  ctx.log.debug("processing batch", { batchId: 42 });
  ctx.log.info("order created", { orderId: 123, total: 99.99 });
  ctx.log.warn("rate limit approaching", { current: 90, max: 100 });
  ctx.log.error("payment failed", { orderId: 123, error: "card declined" });

  // Metrics
  ctx.metric("orders.created", 1, { region: "us-east" });
  ctx.metric("order.total", 99.99, { currency: "USD" });

  // Tracing
  const result = await ctx.trace("process.payment", async (span) => {
    span.setAttribute("order.id", 123);
    span.setAttribute("amount", 99.99);
    return { chargeId: "ch_123" };
  }, { provider: "stripe" });

  if (result.chargeId !== "ch_123") throw new Error("trace should return result");

  // Secrets
  const missing = ctx.secret("NONEXISTENT");
  if (missing !== null) throw new Error("missing secret should be null");

  return { traced: result.chargeId };
} });
chimpbase.register({ testTelemetry }); await chimpbase.start();
const r = await chimpbase.executeAction("testTelemetry", {}); console.log("telemetry:", JSON.stringify(r.result));
console.log("telemetry: OK"); await chimpbase.close(); process.exit(0);
