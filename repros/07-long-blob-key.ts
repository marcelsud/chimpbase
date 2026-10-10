import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { fsBlobDriver } from "../packages/blobs/src/fs_driver.ts";

const root = await mkdtemp(join(tmpdir(), "chimpbase-repro-07-"));
const key = "a".repeat(230);
try {
  const driver = fsBlobDriver({ root });
  console.log({ issue: 7, keyLength: key.length, expected: "write and read succeed" });
  const stored = await driver.put("uploads", key, new Blob(["original payload"]).stream());
  const object = await driver.get("uploads", key, stored.driverRef);
  assert.ok(object, "a stored object must be readable");
  assert.equal(await new Response(object.body).text(), "original payload");
  console.log({ issue: 7, actual: "write and read succeeded" });
} finally {
  await rm(root, { recursive: true, force: true });
}
