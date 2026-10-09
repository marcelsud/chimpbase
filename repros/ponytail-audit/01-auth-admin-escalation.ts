import assert from "node:assert/strict";

import { chimpbaseAuth } from "../../packages/auth/src/index.ts";
import { createChimpbase } from "../../packages/bun/src/library.ts";
import { v } from "../../packages/runtime/index.ts";

const host = await createChimpbase({
  project: { name: "audit-01" },
  storage: { engine: "memory" },
  secrets: { get: () => null },
});
host.register(chimpbaseAuth());

try {
  const user = v.object({ id: v.string() }).parse((await host.executeAction(
    "__chimpbase.auth.createUser", { email: "audit@example.invalid", name: "Audit" },
  )).result);
  const writer = v.object({ key: v.string() }).parse((await host.executeAction(
    "__chimpbase.auth.createApiKey", { userId: user.id, scopes: ["write"] },
  )).result);
  const request = (path: string) => new Request(`http://audit.local${path}`, {
    method: "POST",
    headers: { "x-api-key": writer.key, "content-type": "application/json" },
    body: JSON.stringify({ scopes: ["admin"] }),
  });

  const canonical = await host.executeRoute(request(`/_auth/users/${user.id}/keys`));
  assert.equal(canonical.response?.status, 403);

  for (const path of [
    `//_auth/users/${user.id}/keys`,
    `/_auth//users/${user.id}//keys`,
    `///_auth///users///${user.id}///keys///`,
  ]) {
    const denied = await host.executeRoute(request(path));
    assert.equal(denied.response?.status, 403);
    assert(denied.response);
    const body: unknown = await denied.response.json();
    assert.deepEqual(body, { error: "insufficient permissions" });
  }

  const keys = v.object({ scopes: v.string().array() }).array().parse((await host.executeAction(
    "__chimpbase.auth.listApiKeys", user.id,
  )).result);
  assert.deepEqual(keys.map((key) => key.scopes), [["write"]]);

  for (const path of ["/_auth/users", "//_auth//users//"]) {
    const denied = await host.executeRoute(new Request(`http://audit.local${path}`, {
      headers: { "x-api-key": writer.key },
    }));
    assert.equal(denied.response?.status, 403);
  }
  console.log("FIXED 01: equivalent auth management paths deny the write key, and no admin key is created.");
} finally {
  await host.close();
}
