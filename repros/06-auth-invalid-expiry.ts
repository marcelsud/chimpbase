import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { chimpbaseAuth } from "../packages/auth/src/index.ts";
import { createChimpbase } from "../packages/bun/src/library.ts";
import { v } from "../packages/runtime/index.ts";

const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-repro-06-"));
const headers = { "content-type": "application/json", "x-api-key": "repro-bootstrap-key" };
try {
  const host = await createChimpbase({
    projectDir,
    storage: { engine: "memory" },
    secrets: { get: (name) => name === "REPRO_BOOTSTRAP" ? headers["x-api-key"] : null },
    registrations: [chimpbaseAuth({ bootstrapKeySecret: "REPRO_BOOTSTRAP" })],
  });
  try {
    const userResponse = (await host.executeRoute(new Request("http://repro.local/_auth/users", {
      method: "POST", headers,
      body: JSON.stringify({ email: "repro@example.com", name: "Repro" }),
    }))).response;
    assert.equal(userResponse?.status, 201, "fixture user creation must succeed");
    assert.ok(userResponse);
    const user = v.object({ id: v.string() }).parse(await userResponse.json());

    const keyResponse = (await host.executeRoute(new Request(`http://repro.local/_auth/users/${user.id}/keys`, {
      method: "POST", headers,
      body: JSON.stringify({ expiresAt: "invalid-date" }),
    }))).response;
    assert.ok(keyResponse);
    let acceptedAsValid: boolean | undefined;
    if (keyResponse.status === 201) {
      const { key } = v.object({ key: v.string() }).parse(await keyResponse.json());
      const validation = (await host.executeAction("__chimpbase.auth.validateApiKey", [key])).result;
      acceptedAsValid = v.object({ valid: v.boolean() }).parse(validation).valid;
    }
    console.log({ issue: 6, expectedStatus: 400, actualStatus: keyResponse.status, acceptedAsValid });
    assert.equal(keyResponse.status, 400, "an invalid expiry must be rejected instead of creating a permanent key");
  } finally {
    await host.close();
  }
} finally {
  await rm(projectDir, { recursive: true, force: true });
}
