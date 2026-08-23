import {
  action,
  isStringArray,
  plugin,
  readJsonBody,
  route,
  tryParseJson,
  type ChimpbasePluginDependency,
  type ChimpbasePluginRegistration,
  type ChimpbaseRegistrationSource,
} from "@chimpbase/runtime";

// ── Public types ──────────────────────────────────────────────────────────────

export type AuthScope = "admin" | "read" | "write" | "auth:manage" | "webhooks:manage";

const VALID_SCOPES: ReadonlySet<string> = new Set<AuthScope>([
  "admin", "read", "write", "auth:manage", "webhooks:manage",
]);

const DEFAULT_SCOPES: AuthScope[] = ["read", "write"];

export interface ChimpbaseAuthRateLimitOptions {
  /** Maximum failed attempts before blocking. Default: `5`. */
  maxAttempts?: number;
  /** Time window in milliseconds for counting failures. Default: `60000` (1 minute). */
  windowMs?: number;
  /** Duration in milliseconds to block after exceeding max attempts. Default: `300000` (5 minutes). */
  blockDurationMs?: number;
}

export interface ChimpbaseAuthOptions {
  /** Which path prefixes require authentication. Default: `"all"`. */
  protectedPaths?: string[] | "all";
  /** Paths explicitly excluded from authentication. Default: `["/health"]`. */
  excludePaths?: string[];
  /** Secret name whose value is accepted as a bootstrap API key for initial setup. */
  bootstrapKeySecret?: string;
  /** Base path for the management REST API. Set to `null` to disable. Default: `"/_auth"`. */
  managementBasePath?: string | null;
  /** Rate limiting for failed authentication attempts. Disabled if not provided. */
  rateLimit?: ChimpbaseAuthRateLimitOptions;
  /** Path prefixes that require the `webhooks:manage` scope. Default: `["/_webhooks"]`. */
  webhooksManagementPaths?: string[];
  /** Plugin name. */
  name?: string;
  /** Plugin dependencies. */
  dependsOn?: readonly ChimpbasePluginDependency[];
}

export interface AuthUser {
  id: string;
  email: string;
  name: string;
  role: string;
  createdAt: string;
  updatedAt: string;
}

export interface AuthApiKey {
  id: string;
  userId: string;
  keyHash: string;
  keyPrefix: string;
  label: string;
  scopes: string;
  createdAt: string;
  revokedAt: string | null;
  expiresAt: string | null;
}

// ── Constants ─────────────────────────────────────────────────────────────────

const USERS_COLLECTION = "__chimpbase.auth.users";
const API_KEYS_COLLECTION = "__chimpbase.auth.api_keys";

// ── Helpers ───────────────────────────────────────────────────────────────────

async function hashKey(raw: string): Promise<string> {
  const data = new TextEncoder().encode(raw);
  const digest = await crypto.subtle.digest("SHA-256", data);
  return Array.from(new Uint8Array(digest))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

function generateRawKey(): string {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

function extractApiKey(request: Request): string | null {
  const xApiKey = request.headers.get("x-api-key");
  if (xApiKey) {
    return xApiKey;
  }

  const authorization = request.headers.get("authorization");
  if (authorization) {
    const match = /^Bearer\s+(.+)$/i.exec(authorization);
    if (match) {
      return match[1]!;
    }
  }

  return null;
}

function normalizePath(path: string): string {
  const trimmed = path.trim();
  if (!trimmed || trimmed === "/") {
    return "/";
  }

  const withLeading = trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
  return withLeading.replace(/\/+$/g, "") || "/";
}

function splitPath(path: string): string[] {
  return normalizePath(path).split("/").filter(Boolean);
}

function isExcludedPath(pathname: string, excludePaths: string[]): boolean {
  const normalized = normalizePath(pathname);
  return excludePaths.some((excluded) => {
    const normalizedExcluded = normalizePath(excluded);
    return normalized === normalizedExcluded || normalized.startsWith(normalizedExcluded + "/");
  });
}

function isProtectedPath(pathname: string, protectedPaths: string[] | "all"): boolean {
  if (protectedPaths === "all") {
    return true;
  }

  const normalized = normalizePath(pathname);
  return protectedPaths.some((path) => {
    const normalizedPath = normalizePath(path);
    return normalized === normalizedPath || normalized.startsWith(normalizedPath + "/");
  });
}

function pathStartsWith(pathname: string, prefixes: string[]): boolean {
  const normalized = normalizePath(pathname);
  return prefixes.some((prefix) => {
    const normalizedPrefix = normalizePath(prefix);
    return normalized === normalizedPrefix || normalized.startsWith(normalizedPrefix + "/");
  });
}

function resolveRequiredScopes(
  method: string,
  pathname: string,
  managementBasePath: string | null,
  webhooksManagementPaths: string[],
): AuthScope[] {
  if (managementBasePath && pathStartsWith(pathname, [managementBasePath])) {
    return ["admin", "auth:manage"];
  }

  if (pathStartsWith(pathname, webhooksManagementPaths)) {
    return ["admin", "webhooks:manage"];
  }

  if (method === "GET" || method === "HEAD" || method === "OPTIONS") {
    return ["admin", "read", "write"];
  }

  return ["admin", "write"];
}

function hasRequiredScope(keyScopes: string[], requiredScopes: AuthScope[]): boolean {
  return requiredScopes.some((scope) => keyScopes.includes(scope));
}

function parseScopes(raw: string | undefined | null): string[] {
  if (!raw) {
    return DEFAULT_SCOPES;
  }

  const parsed = tryParseJson(raw);
  return isStringArray(parsed) ? parsed : DEFAULT_SCOPES;
}

function validateScopes(scopes: string[]): void {
  for (const scope of scopes) {
    if (!VALID_SCOPES.has(scope)) {
      throw new AuthRequestError(400, `invalid scope: "${scope}". Valid scopes: ${[...VALID_SCOPES].join(", ")}`);
    }
  }
}

function timingSafeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) {
    return false;
  }

  let result = 0;
  for (let i = 0; i < a.length; i++) {
    result |= a.charCodeAt(i) ^ b.charCodeAt(i);
  }

  return result === 0;
}

function nowIso(): string {
  return new Date().toISOString();
}

function jsonError(status: number, error: string, headers?: Record<string, string>): Response {
  return Response.json({ error }, { status, headers });
}

async function parseJsonBody(request: Request): Promise<Record<string, unknown>> {
  let body: unknown;
  try {
    body = await readJsonBody(request);
  } catch {
    throw new AuthRequestError(400, "request body must be valid JSON");
  }

  if (!body || typeof body !== "object" || Array.isArray(body)) {
    throw new AuthRequestError(400, "request body must be a JSON object");
  }

  return body as Record<string, unknown>;
}

class AuthRequestError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "AuthRequestError";
    this.status = status;
  }
}

// ── Plugin ────────────────────────────────────────────────────────────────────

export function chimpbaseAuth(
  options: ChimpbaseAuthOptions = {},
): ChimpbasePluginRegistration {
  const protectedPaths = options.protectedPaths ?? "all";
  const excludePaths = (options.excludePaths ?? ["/health"]).map(normalizePath);
  const bootstrapKeySecret = options.bootstrapKeySecret ?? null;
  const managementBasePath = options.managementBasePath === undefined ? "/_auth" : options.managementBasePath;
  const webhooksManagementPaths = (options.webhooksManagementPaths ?? ["/_webhooks"]).map(normalizePath);
  const rateLimitConfig = options.rateLimit ?? null;
  const rlMaxAttempts = rateLimitConfig?.maxAttempts ?? 5;
  const rlWindowMs = rateLimitConfig?.windowMs ?? 60_000;
  const rlBlockDurationMs = rateLimitConfig?.blockDurationMs ?? 300_000;

  const entries: ChimpbaseRegistrationSource[] = [];

  // ── Validate API key action ───────────────────────────────────────────────

  entries.push(
    action("__chimpbase.auth.validateApiKey", async (ctx, rawKey: string) => {
      // Check bootstrap key first
      if (bootstrapKeySecret) {
        const bootstrapKey = ctx.secret(bootstrapKeySecret);
        if (bootstrapKey && timingSafeEqual(rawKey, bootstrapKey)) {
          return { valid: true, userId: null, bootstrap: true, scopes: ["admin"] };
        }
      }

      const keyHash = await hashKey(rawKey);
      const record = await ctx.collection.findOne<AuthApiKey>(API_KEYS_COLLECTION, { keyHash });
      if (!record) {
        return { valid: false };
      }

      if (record.revokedAt) {
        return { valid: false, reason: "revoked" };
      }

      if (record.expiresAt && new Date(record.expiresAt) < new Date()) {
        return { valid: false, reason: "expired" };
      }

      return {
        valid: true,
        userId: record.userId,
        bootstrap: false,
        scopes: parseScopes(record.scopes),
      };
    }),
  );

  // ── Rate limit action ─────────────────────────────────────────────────────

  if (rateLimitConfig) {
    entries.push(
      action("__chimpbase.auth.rateLimit", async (ctx, input: { keyPrefix: string; outcome: "check" | "fail" | "success" }) => {
        const blockKey = `__chimpbase.auth.ratelimit:block:${input.keyPrefix}`;
        const countKey = `__chimpbase.auth.ratelimit:count:${input.keyPrefix}`;

        if (input.outcome === "check") {
          const blocked = await ctx.kv.get<boolean>(blockKey);
          return { blocked: !!blocked };
        }

        if (input.outcome === "success") {
          await ctx.kv.delete(countKey);
          return { blocked: false };
        }

        // outcome === "fail"
        const blocked = await ctx.kv.get<boolean>(blockKey);
        if (blocked) {
          return { blocked: true };
        }

        const existing = await ctx.kv.get<{ count: number }>(countKey);
        const count = (existing?.count ?? 0) + 1;

        if (count >= rlMaxAttempts) {
          await ctx.kv.set(blockKey, true, { ttlMs: rlBlockDurationMs });
          await ctx.kv.delete(countKey);
          return { blocked: true };
        }

        await ctx.kv.set(countKey, { count }, { ttlMs: rlWindowMs });
        return { blocked: false };
      }),
    );
  }

  // ── Guard route ───────────────────────────────────────────────────────────

  entries.push(
    route("__chimpbase.auth.guard", async (request, env) => {
      const url = new URL(request.url);

      if (isExcludedPath(url.pathname, excludePaths)) {
        return null;
      }

      if (!isProtectedPath(url.pathname, protectedPaths)) {
        return null;
      }

      const rawKey = extractApiKey(request);
      if (!rawKey) {
        return jsonError(401, "missing API key");
      }

      const keyPrefix = rawKey.substring(0, 8);

      // Rate limit: check if blocked
      if (rateLimitConfig) {
        const rlCheck = await env.action("__chimpbase.auth.rateLimit", {
          keyPrefix,
          outcome: "check",
        }) as { blocked: boolean };
        if (rlCheck.blocked) {
          return jsonError(429, "too many failed authentication attempts", {
            "retry-after": String(Math.ceil(rlBlockDurationMs / 1000)),
          });
        }
      }

      const result = await env.action("__chimpbase.auth.validateApiKey", rawKey) as {
        valid: boolean;
        userId?: string | null;
        bootstrap?: boolean;
        scopes?: string[];
      };

      if (!result.valid) {
        // Rate limit: record failure
        if (rateLimitConfig) {
          const rlFail = await env.action("__chimpbase.auth.rateLimit", {
            keyPrefix,
            outcome: "fail",
          }) as { blocked: boolean };
          if (rlFail.blocked) {
            return jsonError(429, "too many failed authentication attempts", {
              "retry-after": String(Math.ceil(rlBlockDurationMs / 1000)),
            });
          }
        }
        return jsonError(401, "invalid API key");
      }

      // Rate limit: clear counter on success
      if (rateLimitConfig) {
        await env.action("__chimpbase.auth.rateLimit", { keyPrefix, outcome: "success" });
      }

      // Scope check
      const keyScopes = result.scopes ?? DEFAULT_SCOPES;
      const requiredScopes = resolveRequiredScopes(
        request.method,
        url.pathname,
        managementBasePath,
        webhooksManagementPaths,
      );

      if (!hasRequiredScope(keyScopes, requiredScopes)) {
        return jsonError(403, "insufficient permissions");
      }

      // Set request context for downstream routes
      env.set("auth.userId", result.userId ?? null);
      env.set("auth.scopes", keyScopes);
      env.set("auth.bootstrap", result.bootstrap ?? false);

      return null;
    }),
  );

  // ── User management actions ───────────────────────────────────────────────

  entries.push(
    action("__chimpbase.auth.createUser", async (ctx, input: { email: string; name: string; role?: string }) => {
      const now = nowIso();
      const id = await ctx.collection.insert(USERS_COLLECTION, {
        email: input.email,
        name: input.name,
        role: input.role ?? "user",
        createdAt: now,
        updatedAt: now,
      });

      return await ctx.collection.findOne<AuthUser>(USERS_COLLECTION, { id });
    }),
  );

  entries.push(
    action("__chimpbase.auth.listUsers", async (ctx) => {
      return await ctx.collection.find<AuthUser>(USERS_COLLECTION, {});
    }),
  );

  entries.push(
    action("__chimpbase.auth.getUser", async (ctx, id: string) => {
      return await ctx.collection.findOne<AuthUser>(USERS_COLLECTION, { id });
    }),
  );

  entries.push(
    action("__chimpbase.auth.deleteUser", async (ctx, id: string) => {
      // Revoke all API keys for this user
      const keys = await ctx.collection.find<AuthApiKey>(API_KEYS_COLLECTION, { userId: id });
      for (const key of keys) {
        if (!key.revokedAt) {
          await ctx.collection.update(API_KEYS_COLLECTION, { id: key.id }, { revokedAt: nowIso() });
        }
      }

      return await ctx.collection.delete(USERS_COLLECTION, { id });
    }),
  );

  // ── API key management actions ────────────────────────────────────────────

  entries.push(
    action("__chimpbase.auth.createApiKey", async (ctx, input: { userId: string; label?: string; expiresAt?: string; scopes?: string[] }) => {
      // Verify user exists
      const user = await ctx.collection.findOne<AuthUser>(USERS_COLLECTION, { id: input.userId });
      if (!user) {
        throw new AuthRequestError(404, "user not found");
      }

      const scopes = input.scopes ?? DEFAULT_SCOPES;
      validateScopes(scopes);

      const rawKey = generateRawKey();
      const keyHash = await hashKey(rawKey);
      const keyPrefix = rawKey.substring(0, 8);
      const now = nowIso();

      const id = await ctx.collection.insert(API_KEYS_COLLECTION, {
        userId: input.userId,
        keyHash,
        keyPrefix,
        label: input.label ?? "",
        scopes: JSON.stringify(scopes),
        createdAt: now,
        revokedAt: null,
        expiresAt: input.expiresAt ?? null,
      });

      return {
        id,
        userId: input.userId,
        key: rawKey,
        keyPrefix,
        label: input.label ?? "",
        scopes,
        createdAt: now,
        expiresAt: input.expiresAt ?? null,
      };
    }),
  );

  entries.push(
    action("__chimpbase.auth.listApiKeys", async (ctx, userId: string) => {
      const keys = await ctx.collection.find<AuthApiKey>(API_KEYS_COLLECTION, { userId });
      return keys.map((key) => ({
        id: key.id,
        userId: key.userId,
        keyPrefix: key.keyPrefix,
        label: key.label,
        scopes: parseScopes(key.scopes),
        createdAt: key.createdAt,
        revokedAt: key.revokedAt,
        expiresAt: key.expiresAt,
      }));
    }),
  );

  entries.push(
    action("__chimpbase.auth.revokeApiKey", async (ctx, keyId: string) => {
      const key = await ctx.collection.findOne<AuthApiKey>(API_KEYS_COLLECTION, { id: keyId });
      if (!key) {
        return 0;
      }

      if (key.revokedAt) {
        return 0;
      }

      return await ctx.collection.update(API_KEYS_COLLECTION, { id: keyId }, { revokedAt: nowIso() });
    }),
  );

  // ── Management routes ─────────────────────────────────────────────────────

  if (managementBasePath !== null) {
    const baseSegments = splitPath(managementBasePath);

    entries.push(
      route("__chimpbase.auth.management", async (request, env) => {
        const url = new URL(request.url);
        const segments = splitPath(url.pathname);

        try {
          // POST /_auth/users
          if (matchSegments(segments, [...baseSegments, "users"]) && request.method === "POST") {
            const body = await parseJsonBody(request);
            const user = await env.action("__chimpbase.auth.createUser", {
              email: requireString(body, "email"),
              name: requireString(body, "name"),
              role: optionalString(body, "role"),
            });
            return Response.json(user, { status: 201 });
          }

          // GET /_auth/users
          if (matchSegments(segments, [...baseSegments, "users"]) && request.method === "GET") {
            const users = await env.action("__chimpbase.auth.listUsers");
            return Response.json(users);
          }

          // DELETE /_auth/users/:id
          if (
            matchPrefixWithId(segments, [...baseSegments, "users"]) &&
            request.method === "DELETE"
          ) {
            const userId = decodeURIComponent(segments[segments.length - 1]!);
            const deleted = await env.action("__chimpbase.auth.deleteUser", userId);
            return deleted === 0
              ? jsonError(404, "user not found")
              : new Response(null, { status: 204 });
          }

          // POST /_auth/users/:userId/keys
          if (
            matchPrefixWithIdAndSuffix(segments, [...baseSegments, "users"], "keys") &&
            request.method === "POST"
          ) {
            const userId = decodeURIComponent(segments[baseSegments.length + 1]!);
            const body = await parseJsonBody(request);
            const result = await env.action("__chimpbase.auth.createApiKey", {
              userId,
              label: optionalString(body, "label"),
              expiresAt: optionalString(body, "expiresAt"),
              scopes: optionalStringArray(body, "scopes"),
            });
            return Response.json(result, { status: 201 });
          }

          // GET /_auth/users/:userId/keys
          if (
            matchPrefixWithIdAndSuffix(segments, [...baseSegments, "users"], "keys") &&
            request.method === "GET"
          ) {
            const userId = decodeURIComponent(segments[baseSegments.length + 1]!);
            const keys = await env.action("__chimpbase.auth.listApiKeys", userId);
            return Response.json(keys);
          }

          // DELETE /_auth/keys/:id
          if (
            matchPrefixWithId(segments, [...baseSegments, "keys"]) &&
            request.method === "DELETE"
          ) {
            const keyId = decodeURIComponent(segments[segments.length - 1]!);
            const revoked = await env.action("__chimpbase.auth.revokeApiKey", keyId);
            return revoked === 0
              ? jsonError(404, "API key not found")
              : new Response(null, { status: 204 });
          }

          return null;
        } catch (error) {
          if (error instanceof AuthRequestError) {
            return jsonError(error.status, error.message);
          }
          throw error;
        }
      }),
    );
  }

  // ── Build plugin ──────────────────────────────────────────────────────────

  if (options.dependsOn || options.name) {
    return plugin(
      {
        dependsOn: options.dependsOn,
        name: options.name ?? "chimpbase-auth",
      },
      ...entries,
    );
  }

  return plugin({ name: "chimpbase-auth" }, ...entries);
}

// ── Route matching helpers ──────────────────────────────────────────────────

function matchSegments(actual: string[], expected: string[]): boolean {
  if (actual.length !== expected.length) {
    return false;
  }
  return actual.every((segment, i) => segment === expected[i]);
}

function matchPrefixWithId(actual: string[], prefix: string[]): boolean {
  if (actual.length !== prefix.length + 1) {
    return false;
  }
  return prefix.every((segment, i) => segment === actual[i]);
}

function matchPrefixWithIdAndSuffix(actual: string[], prefix: string[], suffix: string): boolean {
  if (actual.length !== prefix.length + 2) {
    return false;
  }
  if (actual[actual.length - 1] !== suffix) {
    return false;
  }
  return prefix.every((segment, i) => segment === actual[i]);
}

function requireString(body: Record<string, unknown>, field: string): string {
  const value = body[field];
  if (typeof value !== "string" || !value) {
    throw new AuthRequestError(400, `"${field}" is required and must be a non-empty string`);
  }
  return value;
}

function optionalString(body: Record<string, unknown>, field: string): string | undefined {
  const value = body[field];
  if (value === undefined || value === null) {
    return undefined;
  }
  if (typeof value !== "string") {
    throw new AuthRequestError(400, `"${field}" must be a string`);
  }
  return value;
}

function optionalStringArray(body: Record<string, unknown>, field: string): string[] | undefined {
  const value = body[field];
  if (value === undefined || value === null) {
    return undefined;
  }
  if (!isStringArray(value)) {
    throw new AuthRequestError(400, `"${field}" must be an array of strings`);
  }
  return value;
}
