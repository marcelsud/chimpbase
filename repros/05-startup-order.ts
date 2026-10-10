import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { action, onStart, worker } from "../packages/runtime/index.ts";

const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-repro-05-"));
let initialized = false;
let ranBeforeInitialization = false;
let processedJobs = 0;
const startupErrors: string[] = [];
try {
  const host = await createChimpbase({
    projectDir,
    storage: { engine: "memory" },
    secrets: { get: () => null },
    workerRuntime: { pollIntervalMs: 5 },
    registrations: [
      action("seed", async (ctx) => ctx.enqueue("repro.work", {})),
      worker("repro.work", async () => {
        ranBeforeInitialization ||= !initialized;
        processedJobs += 1;
      }),
      onStart("repro.initialize", async () => {
        await Bun.sleep(50);
        initialized = true;
      }),
    ],
  });
  try {
    await host.executeAction("seed");
    const originalError = console.error;
    console.error = (...args: unknown[]) => {
      startupErrors.push(args.map((arg) => arg instanceof Error ? arg.message : String(arg)).join(" "));
    };
    try {
      const started = await host.start({ serve: false, runWorker: true });
      await started.stop();
    } finally {
      console.error = originalError;
    }
    await host.drain();
    console.log({ issue: 5, startupErrors, ranBeforeInitialization, processedJobs });
    assert.equal(processedJobs, 1, "the queued job must still be processed once");
    assert.equal(ranBeforeInitialization, false, "workers must wait for initialization");
    assert.deepEqual(startupErrors, [], "initialization must not overlap worker transactions");
  } finally {
    await host.close();
  }
} finally {
  await rm(projectDir, { recursive: true, force: true });
}
