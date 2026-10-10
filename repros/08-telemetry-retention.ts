import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { action } from "../packages/runtime/index.ts";

const maxRetainedRecords = 10_000;
const emittedRecords = maxRetainedRecords * 2;
const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-repro-08-"));
try {
  const host = await createChimpbase({
    projectDir,
    storage: { engine: "memory" },
    secrets: { get: () => null },
    telemetry: { persist: { log: false, metric: false, trace: false } },
    registrations: [action("log", (ctx, index: number) => ctx.log.info("repro record", { index }))],
  });
  try {
    for (let index = 0; index < emittedRecords; index += 1) {
      await host.executeAction("log", [index]);
    }
    const retainedRecords = host.drainTelemetryRecords().length;
    console.log({ issue: 8, emittedRecords, retainedRecords, maxRetainedRecords });
    assert.ok(retainedRecords <= maxRetainedRecords,
      `retained telemetry must be capped at ${maxRetainedRecords}; retained ${retainedRecords}`);
  } finally {
    await host.close();
  }
} finally {
  await rm(projectDir, { recursive: true, force: true });
}
