import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { fsBlobDriver, memoryBlobDriver } from "../packages/blobs/src/index.ts";
import { createChimpbase } from "../packages/bun/src/library.ts";
import { action } from "../packages/runtime/index.ts";

const results: { driver: string; expectedBytes: string; actualBytes: string }[] = [];
for (const driverName of ["memory", "filesystem"]) {
  const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-repro-02-"));
  try {
    const host = await createChimpbase({
      projectDir,
      storage: { engine: "memory" },
      secrets: { get: () => null },
      blobs: {
        driver: driverName === "memory" ? memoryBlobDriver() : fsBlobDriver({ root: join(projectDir, "blobs") }),
        buckets: ["uploads"],
      },
    });
    try {
      const upload = await host.routeEnv().blobs.createUpload("uploads", "file.txt");
      await upload.writePart(1, new TextEncoder().encode("original"));
      const originalParts = await upload.listParts();
      host.register(action("replaceThenFail", async (ctx) => {
        const resumed = await ctx.blobs.resumeUpload(upload.id);
        await resumed.writePart(1, new TextEncoder().encode("replacement"));
        throw new Error("intentional rollback");
      }));

      await assert.rejects(host.executeAction("replaceThenFail"), /intentional rollback/);
      assert.deepEqual(await upload.listParts(), originalParts, "part metadata must roll back");
      await upload.complete();
      const object = await host.routeEnv().blobs.get("uploads", "file.txt");
      assert.ok(object, "completed object must exist");
      results.push({
        driver: driverName,
        expectedBytes: "original",
        actualBytes: await new Response(object.body).text(),
      });
    } finally {
      await host.close();
    }
  } finally {
    await rm(projectDir, { recursive: true, force: true });
  }
}
console.log({ issue: 2, results });
assert.deepEqual(results.map((result) => result.actualBytes), ["original", "original"],
  "a rolled-back part replacement must preserve the original bytes in both drivers");
