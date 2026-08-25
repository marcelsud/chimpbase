import { createChimpbase } from "@chimpbase/bun";
import { action, v } from "@chimpbase/runtime";

// Test memory storage engine
const chimpbase = await createChimpbase({
  storage: { engine: "memory" },
  server: { port: 0 },
  worker: { maxAttempts: 3, retryDelayMs: 500 },
  telemetry: { minLevel: "info", persist: { log: false, metric: false, trace: false } },
});

const testConfig = action({ name: "testConfig", args: v.object({}), async handler(ctx) {
  // Verify db is accessible (storage engine works)
  const result = await ctx.db.query("SELECT 1 AS ok");
  if ((result[0] as { ok: number }).ok !== 1) throw new Error("db query should work");

  // Verify secrets accessor works
  const missing = ctx.secret("NONEXISTENT");
  if (missing !== null) throw new Error("missing secret should be null");

  return { ok: true };
} });

chimpbase.register({ testConfig }); await chimpbase.start();
const r = await chimpbase.executeAction("testConfig", {}); console.log("configuration:", JSON.stringify(r.result));

// Test start options
const started = await chimpbase.start({ serve: false, runWorker: false });
await started.stop();

console.log("configuration: OK"); await chimpbase.close(); process.exit(0);
