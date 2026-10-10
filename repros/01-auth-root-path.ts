import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { chimpbaseAuth } from "../packages/auth/src/index.ts";
import { createChimpbase } from "../packages/bun/src/library.ts";

const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-repro-01-"));
try {
  const host = await createChimpbase({
    projectDir,
    storage: { engine: "memory" },
    secrets: { get: () => null },
    registrations: [chimpbaseAuth({ protectedPaths: ["/"] })],
  });
  try {
    const { response } = await host.executeRoute(new Request("http://repro.local/_auth/users", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ email: "repro@example.com", name: "Unauthorized user" }),
    }));
    console.log({ issue: 1, expectedStatus: 401, actualStatus: response?.status });
    assert.equal(response?.status, 401, "protectedPaths: ['/'] must protect management routes");
  } finally {
    await host.close();
  }
} finally {
  await rm(projectDir, { recursive: true, force: true });
}
