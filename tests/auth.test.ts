import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { chimpbaseAuth, type AuthApiKey, type AuthScope, type AuthUser } from "../packages/auth/src/index.ts";
import { readJsonResponse } from "./support/http.ts";

/** The API key route returns the raw key once, alongside the stored record's public fields. */
interface CreatedApiKey extends Omit<AuthApiKey, "keyHash" | "revokedAt" | "scopes"> {
  key: string;
  scopes: string[];
}

/** Listing keys returns the public fields only: the raw key and its hash are never included. */
type ListedApiKey = Omit<CreatedApiKey, "key"> & { key?: undefined; keyHash?: undefined };

const cleanupDirs: string[] = [];

afterEach(async () => {
  while (cleanupDirs.length > 0) {
    const dir = cleanupDirs.pop();
    if (dir) {
      await rm(dir, { recursive: true, force: true });
    }
  }
});

const BOOTSTRAP_KEY = "test-bootstrap-key";

async function createAuthHost(options?: { excludePaths?: string[]; rateLimit?: { maxAttempts: number; windowMs: number; blockDurationMs: number } }) {
  const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-auth-test-"));
  cleanupDirs.push(projectDir);

  const host = await createChimpbase({
    project: { name: "auth-test" },
    projectDir,
    storage: { engine: "memory" },
    secrets: { get: (name: string) => name === "BOOTSTRAP_KEY" ? BOOTSTRAP_KEY : null },
  });

  host.register({
    authPlugin: chimpbaseAuth({
      bootstrapKeySecret: "BOOTSTRAP_KEY",
      excludePaths: options?.excludePaths,
      rateLimit: options?.rateLimit,
    }),
  });

  return host;
}

function authHeaders(key: string = BOOTSTRAP_KEY) {
  return { "x-api-key": key };
}

describe("@chimpbase/auth", () => {
  // ── Guard ───────────────────────────────────────────────────────────────

  test("blocks request without API key", async () => {
    const host = await createAuthHost();
    try {
      const outcome = await host.executeRoute(new Request("http://test.local/some-path"));
      expect(outcome.response?.status).toBe(401);
      expect(await readJsonResponse<{ error: string }>(outcome.response)).toEqual({ error: "missing API key" });
    } finally {
      await host.close();
    }
  });

  test("passes with bootstrap key via X-API-Key header", async () => {
    const host = await createAuthHost();
    try {
      const outcome = await host.executeRoute(
        new Request("http://test.local/some-path", { headers: { "x-api-key": BOOTSTRAP_KEY } }),
      );
      // No matching route → null response (guard passed through)
      expect(outcome.response).toBeNull();
    } finally {
      await host.close();
    }
  });

  test("passes with bootstrap key via Authorization Bearer header", async () => {
    const host = await createAuthHost();
    try {
      const outcome = await host.executeRoute(
        new Request("http://test.local/some-path", {
          headers: { authorization: `Bearer ${BOOTSTRAP_KEY}` },
        }),
      );
      expect(outcome.response).toBeNull();
    } finally {
      await host.close();
    }
  });

  test("excluded paths bypass guard", async () => {
    const host = await createAuthHost();
    try {
      const outcome = await host.executeRoute(new Request("http://test.local/health"));
      // No 401 — guard skipped, no matching route → null
      expect(outcome.response).toBeNull();
    } finally {
      await host.close();
    }
  });

  // ── User management ─────────────────────────────────────────────────────

  test("creates a user via management route", async () => {
    const host = await createAuthHost();
    try {
      const outcome = await host.executeRoute(
        new Request("http://test.local/_auth/users", {
          method: "POST",
          headers: { "content-type": "application/json", ...authHeaders() },
          body: JSON.stringify({ email: "admin@test.com", name: "Admin" }),
        }),
      );
      expect(outcome.response?.status).toBe(201);
      const user = await readJsonResponse<AuthUser>(outcome.response);
      expect(user).toEqual(
        expect.objectContaining({ email: "admin@test.com", name: "Admin", role: "user" }) as AuthUser,
      );
      expect(user.id).toBeDefined();
    } finally {
      await host.close();
    }
  });

  test("lists users", async () => {
    const host = await createAuthHost();
    try {
      await host.executeRoute(
        new Request("http://test.local/_auth/users", {
          method: "POST",
          headers: { "content-type": "application/json", ...authHeaders() },
          body: JSON.stringify({ email: "a@test.com", name: "A" }),
        }),
      );

      const outcome = await host.executeRoute(
        new Request("http://test.local/_auth/users", { headers: authHeaders() }),
      );
      expect(outcome.response?.status).toBe(200);
      const users = await readJsonResponse<AuthUser[]>(outcome.response);
      expect(users).toHaveLength(1);
      expect(users[0].email).toBe("a@test.com");
    } finally {
      await host.close();
    }
  });

  test("deletes a user", async () => {
    const host = await createAuthHost();
    try {
      const createOutcome = await host.executeRoute(
        new Request("http://test.local/_auth/users", {
          method: "POST",
          headers: { "content-type": "application/json", ...authHeaders() },
          body: JSON.stringify({ email: "del@test.com", name: "Del" }),
        }),
      );
      const user = await readJsonResponse<AuthUser>(createOutcome.response);

      const deleteOutcome = await host.executeRoute(
        new Request(`http://test.local/_auth/users/${user.id}`, {
          method: "DELETE",
          headers: authHeaders(),
        }),
      );
      expect(deleteOutcome.response?.status).toBe(204);

      const listOutcome = await host.executeRoute(
        new Request("http://test.local/_auth/users", { headers: authHeaders() }),
      );
      const users = await readJsonResponse<AuthUser[]>(listOutcome.response);
      expect(users).toHaveLength(0);
    } finally {
      await host.close();
    }
  });

  // ── API key management ──────────────────────────────────────────────────

  test("creates an API key for a user", async () => {
    const host = await createAuthHost();
    try {
      const userOutcome = await host.executeRoute(
        new Request("http://test.local/_auth/users", {
          method: "POST",
          headers: { "content-type": "application/json", ...authHeaders() },
          body: JSON.stringify({ email: "key@test.com", name: "Key User" }),
        }),
      );
      const user = await readJsonResponse<AuthUser>(userOutcome.response);

      const keyOutcome = await host.executeRoute(
        new Request(`http://test.local/_auth/users/${user.id}/keys`, {
          method: "POST",
          headers: { "content-type": "application/json", ...authHeaders() },
          body: JSON.stringify({ label: "test-key" }),
        }),
      );
      expect(keyOutcome.response?.status).toBe(201);
      const keyData = await readJsonResponse<CreatedApiKey>(keyOutcome.response);
      expect(keyData.key).toBeDefined();
      expect(keyData.key.length).toBe(64);
      expect(keyData.keyPrefix).toBe(keyData.key.substring(0, 8));
      expect(keyData.label).toBe("test-key");
    } finally {
      await host.close();
    }
  });

  test("authenticates with a generated API key", async () => {
    const host = await createAuthHost();
    try {
      const userOutcome = await host.executeRoute(
        new Request("http://test.local/_auth/users", {
          method: "POST",
          headers: { "content-type": "application/json", ...authHeaders() },
          body: JSON.stringify({ email: "auth@test.com", name: "Auth User" }),
        }),
      );
      const user = await readJsonResponse<AuthUser>(userOutcome.response);

      const keyOutcome = await host.executeRoute(
        new Request(`http://test.local/_auth/users/${user.id}/keys`, {
          method: "POST",
          headers: { "content-type": "application/json", ...authHeaders() },
          body: JSON.stringify({ label: "auth-key" }),
        }),
      );
      const keyData = await readJsonResponse<CreatedApiKey>(keyOutcome.response);

      // Use the generated key
      const authOutcome = await host.executeRoute(
        new Request("http://test.local/some-path", { headers: authHeaders(keyData.key) }),
      );
      // Guard passes → null (no matching route)
      expect(authOutcome.response).toBeNull();
    } finally {
      await host.close();
    }
  });

  test("lists API keys without raw key", async () => {
    const host = await createAuthHost();
    try {
      const userOutcome = await host.executeRoute(
        new Request("http://test.local/_auth/users", {
          method: "POST",
          headers: { "content-type": "application/json", ...authHeaders() },
          body: JSON.stringify({ email: "list@test.com", name: "List" }),
        }),
      );
      const user = await readJsonResponse<AuthUser>(userOutcome.response);

      await host.executeRoute(
        new Request(`http://test.local/_auth/users/${user.id}/keys`, {
          method: "POST",
          headers: { "content-type": "application/json", ...authHeaders() },
          body: JSON.stringify({ label: "my-key" }),
        }),
      );

      const listOutcome = await host.executeRoute(
        new Request(`http://test.local/_auth/users/${user.id}/keys`, { headers: authHeaders() }),
      );
      expect(listOutcome.response?.status).toBe(200);
      const keys = await readJsonResponse<ListedApiKey[]>(listOutcome.response);
      expect(keys).toHaveLength(1);
      expect(keys[0].keyPrefix).toBeDefined();
      expect(keys[0].key).toBeUndefined();
      expect(keys[0].keyHash).toBeUndefined();
    } finally {
      await host.close();
    }
  });

  test("revoked key fails authentication", async () => {
    const host = await createAuthHost();
    try {
      const userOutcome = await host.executeRoute(
        new Request("http://test.local/_auth/users", {
          method: "POST",
          headers: { "content-type": "application/json", ...authHeaders() },
          body: JSON.stringify({ email: "revoke@test.com", name: "Revoke" }),
        }),
      );
      const user = await readJsonResponse<AuthUser>(userOutcome.response);

      const keyOutcome = await host.executeRoute(
        new Request(`http://test.local/_auth/users/${user.id}/keys`, {
          method: "POST",
          headers: { "content-type": "application/json", ...authHeaders() },
          body: JSON.stringify({ label: "revokable" }),
        }),
      );
      const keyData = await readJsonResponse<CreatedApiKey>(keyOutcome.response);

      // Revoke
      const revokeOutcome = await host.executeRoute(
        new Request(`http://test.local/_auth/keys/${keyData.id}`, {
          method: "DELETE",
          headers: authHeaders(),
        }),
      );
      expect(revokeOutcome.response?.status).toBe(204);

      // Try to use revoked key
      const authOutcome = await host.executeRoute(
        new Request("http://test.local/some-path", { headers: authHeaders(keyData.key) }),
      );
      expect(authOutcome.response?.status).toBe(401);
    } finally {
      await host.close();
    }
  });

  test("expired key fails authentication", async () => {
    const host = await createAuthHost();
    try {
      const userResult = await host.executeAction("__chimpbase.auth.createUser", [{
        email: "expire@test.com",
        name: "Expire",
      }]);
      const user = userResult.result as { id: string };

      const keyResult = await host.executeAction("__chimpbase.auth.createApiKey", [{
        userId: user.id,
        label: "expired",
        expiresAt: "2020-01-01T00:00:00Z",
      }]);
      const keyData = keyResult.result as { key: string };

      const authOutcome = await host.executeRoute(
        new Request("http://test.local/some-path", { headers: authHeaders(keyData.key) }),
      );
      expect(authOutcome.response?.status).toBe(401);
    } finally {
      await host.close();
    }
  });

  // ── Error handling ──────────────────────────────────────────────────────

  test("invalid JSON body returns 400", async () => {
    const host = await createAuthHost();
    try {
      const outcome = await host.executeRoute(
        new Request("http://test.local/_auth/users", {
          method: "POST",
          headers: { "content-type": "application/json", ...authHeaders() },
          body: "not json",
        }),
      );
      expect(outcome.response?.status).toBe(400);
    } finally {
      await host.close();
    }
  });

  test("missing required fields returns 400", async () => {
    const host = await createAuthHost();
    try {
      const outcome = await host.executeRoute(
        new Request("http://test.local/_auth/users", {
          method: "POST",
          headers: { "content-type": "application/json", ...authHeaders() },
          body: JSON.stringify({}),
        }),
      );
      expect(outcome.response?.status).toBe(400);
    } finally {
      await host.close();
    }
  });

  test("delete non-existent user returns 404", async () => {
    const host = await createAuthHost();
    try {
      const outcome = await host.executeRoute(
        new Request("http://test.local/_auth/users/nonexistent", {
          method: "DELETE",
          headers: authHeaders(),
        }),
      );
      expect(outcome.response?.status).toBe(404);
    } finally {
      await host.close();
    }
  });

  test("revoke non-existent key returns 404", async () => {
    const host = await createAuthHost();
    try {
      const outcome = await host.executeRoute(
        new Request("http://test.local/_auth/keys/nonexistent", {
          method: "DELETE",
          headers: authHeaders(),
        }),
      );
      expect(outcome.response?.status).toBe(404);
    } finally {
      await host.close();
    }
  });

  // ── Rate limiting ─────────────────────────────────────────────────────────

  test("rate limit blocks after max failures", async () => {
    const host = await createAuthHost({ rateLimit: { maxAttempts: 3, windowMs: 60_000, blockDurationMs: 5_000 } });
    try {
      const badKey = "aaaa1111bbbb2222cccc3333dddd4444eeee5555ffff6666aabb7788ccdd9900";
      for (let i = 0; i < 3; i++) {
        const outcome = await host.executeRoute(
          new Request("http://test.local/some-path", { headers: { "x-api-key": badKey } }),
        );
        expect(outcome.response?.status).toBe(i < 2 ? 401 : 429);
      }

      // Subsequent requests should be 429
      const blocked = await host.executeRoute(
        new Request("http://test.local/some-path", { headers: { "x-api-key": badKey } }),
      );
      expect(blocked.response?.status).toBe(429);
      expect(blocked.response?.headers.get("retry-after")).toBe("5");
    } finally {
      await host.close();
    }
  });

  test("successful auth resets rate limit counter", async () => {
    const host = await createAuthHost({ rateLimit: { maxAttempts: 3, windowMs: 60_000, blockDurationMs: 5_000 } });
    try {
      // Create a real user + key so we can succeed with the same prefix
      const userResult = await host.executeAction("__chimpbase.auth.createUser", [{
        email: "rl@test.com", name: "RL",
      }]);
      const user = userResult.result as { id: string };
      const keyResult = await host.executeAction("__chimpbase.auth.createApiKey", [{
        userId: user.id,
      }]);
      const keyData = keyResult.result as { key: string };
      const prefix = keyData.key.substring(0, 8);

      // Build a bad key with the same prefix
      const badKey = prefix + "0".repeat(56);

      // 2 failures with the bad key
      for (let i = 0; i < 2; i++) {
        await host.executeRoute(
          new Request("http://test.local/some-path", { headers: { "x-api-key": badKey } }),
        );
      }

      // Success with the real key (same prefix) — resets counter
      const success = await host.executeRoute(
        new Request("http://test.local/some-path", { headers: { "x-api-key": keyData.key } }),
      );
      expect(success.response).toBeNull();

      // 2 more failures — should still be 401, not 429 (counter was reset)
      for (let i = 0; i < 2; i++) {
        const outcome = await host.executeRoute(
          new Request("http://test.local/some-path", { headers: { "x-api-key": badKey } }),
        );
        expect(outcome.response?.status).toBe(401);
      }
    } finally {
      await host.close();
    }
  });

  test("rate limit applies per key prefix", async () => {
    const host = await createAuthHost({ rateLimit: { maxAttempts: 2, windowMs: 60_000, blockDurationMs: 5_000 } });
    try {
      const badKeyA = "aaaa0000" + "x".repeat(56);
      const badKeyB = "bbbb0000" + "x".repeat(56);

      // Exhaust key A
      for (let i = 0; i < 2; i++) {
        await host.executeRoute(
          new Request("http://test.local/some-path", { headers: { "x-api-key": badKeyA } }),
        );
      }
      const blockedA = await host.executeRoute(
        new Request("http://test.local/some-path", { headers: { "x-api-key": badKeyA } }),
      );
      expect(blockedA.response?.status).toBe(429);

      // Key B should still work (401, not 429)
      const outcomeB = await host.executeRoute(
        new Request("http://test.local/some-path", { headers: { "x-api-key": badKeyB } }),
      );
      expect(outcomeB.response?.status).toBe(401);
    } finally {
      await host.close();
    }
  });

  // ── Scopes ────────────────────────────────────────────────────────────────

  test("read scope can GET app routes", async () => {
    const host = await createAuthHost();
    try {
      const userResult = await host.executeAction("__chimpbase.auth.createUser", [{
        email: "scope@test.com", name: "Scope",
      }]);
      const user = userResult.result as { id: string };

      const keyResult = await host.executeAction("__chimpbase.auth.createApiKey", [{
        userId: user.id, scopes: ["read"],
      }]);
      const keyData = keyResult.result as { key: string };

      const outcome = await host.executeRoute(
        new Request("http://test.local/some-path", { headers: authHeaders(keyData.key) }),
      );
      // Guard passes → null (no matching route)
      expect(outcome.response).toBeNull();
    } finally {
      await host.close();
    }
  });

  test("read scope cannot POST app routes", async () => {
    const host = await createAuthHost();
    try {
      const userResult = await host.executeAction("__chimpbase.auth.createUser", [{
        email: "readonly@test.com", name: "ReadOnly",
      }]);
      const user = userResult.result as { id: string };

      const keyResult = await host.executeAction("__chimpbase.auth.createApiKey", [{
        userId: user.id, scopes: ["read"],
      }]);
      const keyData = keyResult.result as { key: string };

      const outcome = await host.executeRoute(
        new Request("http://test.local/some-path", {
          method: "POST",
          headers: { "content-type": "application/json", ...authHeaders(keyData.key) },
          body: JSON.stringify({}),
        }),
      );
      expect(outcome.response?.status).toBe(403);
      expect(await readJsonResponse<{ error: string }>(outcome.response)).toEqual({ error: "insufficient permissions" });
    } finally {
      await host.close();
    }
  });

  test("write scope can POST and GET", async () => {
    const host = await createAuthHost();
    try {
      const userResult = await host.executeAction("__chimpbase.auth.createUser", [{
        email: "writer@test.com", name: "Writer",
      }]);
      const user = userResult.result as { id: string };

      const keyResult = await host.executeAction("__chimpbase.auth.createApiKey", [{
        userId: user.id, scopes: ["write"],
      }]);
      const keyData = keyResult.result as { key: string };

      const getOutcome = await host.executeRoute(
        new Request("http://test.local/app-route", { headers: authHeaders(keyData.key) }),
      );
      expect(getOutcome.response).toBeNull();

      const postOutcome = await host.executeRoute(
        new Request("http://test.local/app-route", {
          method: "POST",
          headers: { "content-type": "application/json", ...authHeaders(keyData.key) },
          body: JSON.stringify({}),
        }),
      );
      expect(postOutcome.response).toBeNull();
    } finally {
      await host.close();
    }
  });

  test("admin scope can access management routes", async () => {
    const host = await createAuthHost();
    try {
      const userResult = await host.executeAction("__chimpbase.auth.createUser", [{
        email: "admin@test.com", name: "Admin",
      }]);
      const user = userResult.result as { id: string };

      const keyResult = await host.executeAction("__chimpbase.auth.createApiKey", [{
        userId: user.id, scopes: ["admin"],
      }]);
      const keyData = keyResult.result as { key: string };

      const outcome = await host.executeRoute(
        new Request("http://test.local/_auth/users", { headers: authHeaders(keyData.key) }),
      );
      expect(outcome.response?.status).toBe(200);
    } finally {
      await host.close();
    }
  });

  test("read scope cannot access management routes", async () => {
    const host = await createAuthHost();
    try {
      const userResult = await host.executeAction("__chimpbase.auth.createUser", [{
        email: "noauth@test.com", name: "NoAuth",
      }]);
      const user = userResult.result as { id: string };

      const keyResult = await host.executeAction("__chimpbase.auth.createApiKey", [{
        userId: user.id, scopes: ["read"],
      }]);
      const keyData = keyResult.result as { key: string };

      const outcome = await host.executeRoute(
        new Request("http://test.local/_auth/users", { headers: authHeaders(keyData.key) }),
      );
      expect(outcome.response?.status).toBe(403);
    } finally {
      await host.close();
    }
  });

  test("auth:manage scope can access auth management", async () => {
    const host = await createAuthHost();
    try {
      const userResult = await host.executeAction("__chimpbase.auth.createUser", [{
        email: "authmgr@test.com", name: "AuthMgr",
      }]);
      const user = userResult.result as { id: string };

      const keyResult = await host.executeAction("__chimpbase.auth.createApiKey", [{
        userId: user.id, scopes: ["auth:manage"],
      }]);
      const keyData = keyResult.result as { key: string };

      const outcome = await host.executeRoute(
        new Request("http://test.local/_auth/users", { headers: authHeaders(keyData.key) }),
      );
      expect(outcome.response?.status).toBe(200);
    } finally {
      await host.close();
    }
  });

  test("webhooks:manage cannot access auth management", async () => {
    const host = await createAuthHost();
    try {
      const userResult = await host.executeAction("__chimpbase.auth.createUser", [{
        email: "whmgr@test.com", name: "WhMgr",
      }]);
      const user = userResult.result as { id: string };

      const keyResult = await host.executeAction("__chimpbase.auth.createApiKey", [{
        userId: user.id, scopes: ["webhooks:manage"],
      }]);
      const keyData = keyResult.result as { key: string };

      const outcome = await host.executeRoute(
        new Request("http://test.local/_auth/users", { headers: authHeaders(keyData.key) }),
      );
      expect(outcome.response?.status).toBe(403);
    } finally {
      await host.close();
    }
  });

  test("bootstrap key has all permissions", async () => {
    const host = await createAuthHost();
    try {
      // Can access management
      const outcome = await host.executeRoute(
        new Request("http://test.local/_auth/users", { headers: authHeaders() }),
      );
      expect(outcome.response?.status).toBe(200);

      // Can POST to app routes
      const postOutcome = await host.executeRoute(
        new Request("http://test.local/app-route", {
          method: "POST",
          headers: { ...authHeaders() },
        }),
      );
      expect(postOutcome.response).toBeNull();
    } finally {
      await host.close();
    }
  });

  test("invalid scope in create request returns 400", async () => {
    const host = await createAuthHost();
    try {
      const userResult = await host.executeAction("__chimpbase.auth.createUser", [{
        email: "badscope@test.com", name: "BadScope",
      }]);
      const user = userResult.result as { id: string };

      await expect(
        host.executeAction("__chimpbase.auth.createApiKey", [{
          userId: user.id, scopes: ["invalid-scope"],
        }]),
      ).rejects.toThrow("invalid scope");
    } finally {
      await host.close();
    }
  });

  test("key created without scopes defaults to read+write", async () => {
    const host = await createAuthHost();
    try {
      const userResult = await host.executeAction("__chimpbase.auth.createUser", [{
        email: "default@test.com", name: "Default",
      }]);
      const user = userResult.result as { id: string };

      const keyResult = await host.executeAction("__chimpbase.auth.createApiKey", [{
        userId: user.id,
      }]);
      const keyData = keyResult.result as { scopes: string[] };
      expect(keyData.scopes).toEqual(["read", "write"]);
    } finally {
      await host.close();
    }
  });

  // ── Request context ───────────────────────────────────────────────────

  test("auth guard sets request context for downstream routes", async () => {
    const host = await createAuthHost();
    try {
      const userResult = await host.executeAction("__chimpbase.auth.createUser", [{
        email: "ctx@test.com", name: "Ctx",
      }]);
      const user = userResult.result as { id: string };

      const keyResult = await host.executeAction("__chimpbase.auth.createApiKey", [{
        userId: user.id, scopes: ["read", "write"],
      }]);
      const keyData = keyResult.result as { key: string };

      // Register a route that reads auth context
      let capturedUserId: string | undefined;
      let capturedScopes: string[] | undefined;
      let capturedBootstrap: boolean | undefined;

      const { route } = await import("../packages/runtime/index.ts");
      host.register({
        contextReader: route("test.contextReader", async (_request, env) => {
          capturedUserId = env.get<string>("auth.userId");
          capturedScopes = env.get<string[]>("auth.scopes");
          capturedBootstrap = env.get<boolean>("auth.bootstrap");
          return Response.json({ userId: capturedUserId });
        }),
      });

      const outcome = await host.executeRoute(
        new Request("http://test.local/context-test", { headers: authHeaders(keyData.key) }),
      );

      expect(outcome.response?.status).toBe(200);
      expect(capturedUserId).toBe(user.id);
      expect(capturedScopes).toEqual(["read", "write"]);
      expect(capturedBootstrap).toBe(false);
    } finally {
      await host.close();
    }
  });

  test("bootstrap key sets auth.bootstrap to true", async () => {
    const host = await createAuthHost();
    try {
      let capturedBootstrap: boolean | undefined;

      const { route } = await import("../packages/runtime/index.ts");
      host.register({
        reader: route("test.bootstrapReader", async (_request, env) => {
          capturedBootstrap = env.get<boolean>("auth.bootstrap");
          return Response.json({ bootstrap: capturedBootstrap });
        }),
      });

      await host.executeRoute(
        new Request("http://test.local/bootstrap-test", { headers: authHeaders() }),
      );

      expect(capturedBootstrap).toBe(true);
    } finally {
      await host.close();
    }
  });

  // ── Timing-safe bootstrap key comparison ──────────────────────────────

  test("bootstrap key with wrong value is rejected", async () => {
    const host = await createAuthHost();
    try {
      const outcome = await host.executeRoute(
        new Request("http://test.local/some-path", { headers: { "x-api-key": "wrong-bootstrap-key" } }),
      );
      expect(outcome.response?.status).toBe(401);
    } finally {
      await host.close();
    }
  });

  test("bootstrap key with same length but wrong value is rejected", async () => {
    const host = await createAuthHost();
    try {
      // Same length as BOOTSTRAP_KEY ("test-bootstrap-key" = 18 chars)
      const sameLength = "x".repeat(BOOTSTRAP_KEY.length);
      const outcome = await host.executeRoute(
        new Request("http://test.local/some-path", { headers: { "x-api-key": sameLength } }),
      );
      expect(outcome.response?.status).toBe(401);
    } finally {
      await host.close();
    }
  });

  test("bootstrap key with partial prefix match is rejected", async () => {
    const host = await createAuthHost();
    try {
      const partial = BOOTSTRAP_KEY.substring(0, 10) + "xxxxxxxx";
      const outcome = await host.executeRoute(
        new Request("http://test.local/some-path", { headers: { "x-api-key": partial } }),
      );
      expect(outcome.response?.status).toBe(401);
    } finally {
      await host.close();
    }
  });

  test("bootstrap key with different length is rejected", async () => {
    const host = await createAuthHost();
    try {
      const outcome = await host.executeRoute(
        new Request("http://test.local/some-path", { headers: { "x-api-key": BOOTSTRAP_KEY + "extra" } }),
      );
      expect(outcome.response?.status).toBe(401);
    } finally {
      await host.close();
    }
  });

  test("exact bootstrap key is accepted", async () => {
    const host = await createAuthHost();
    try {
      const outcome = await host.executeRoute(
        new Request("http://test.local/some-path", { headers: { "x-api-key": BOOTSTRAP_KEY } }),
      );
      expect(outcome.response).toBeNull();
    } finally {
      await host.close();
    }
  });
});
