import {
  action,
  isJsonObject,
  isStringArray,
  plugin,
  readJsonBody,
  route,
  tryParseJson,
  v,
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
const apiKeyValidator = v.object({
  createdAt: v.string(),
  expiresAt: v.string().nullable(),
  id: v.string(),
  keyHash: v.string(),
  keyPrefix: v.string(),
  label: v.string(),
  revokedAt: v.string().nullable(),
  scopes: v.string(),
  userId: v.string(),
});
const authUserValidator = v.object({
  createdAt: v.string(),
  email: v.string(),
  id: v.string(),
  name: v.string(),
  role: v.string(),
  updatedAt: v.string(),
});
const blockedResultValidator = v.object({ blocked: v.boolean() });
const rateLimitCountValidator = v.object({ count: v.number() });
const validatedApiKeyResultValidator = v.object({
  bootstrap: v.boolean().optional(),
  scopes: v.string().array().optional(),
  userId: v.string().nullable().optional(),
  valid: v.boolean(),
});


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
  if ((xApiKey !== null && xApiKey.length > 0)) {
    return xApiKey;
  }

  const authorization = request.headers.get("authorization");
  if ((authorization !== null && authorization.length > 0)) {
    const match = /^Bearer\s+(.+)$/i.exec(authorization);
    if ((match !== null)) {
      return match[1]!;
    }
  }

  return null;
}

function normalizePath(path: string): string {
  const trimmed = path.trim();
  if (!(trimmed.length > 0) || trimmed === "/") {
    return "/";
  }

  const withLeading = trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
  const normalized = withLeading.replace(/\/+/g, "/").replace(/\/$/, "");
  return normalized.length > 0 ? normalized : "/";
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
    return normalizedPath === "/" || normalized === normalizedPath || normalized.startsWith(normalizedPath + "/");
  });
}

function pathStartsWith(pathname: string, prefixes: string[]): boolean {
  const normalized = normalizePath(pathname);
  return prefixes.some((prefix) => {
    const normalizedPrefix = normalizePath(prefix);
    return normalizedPrefix === "/" || normalized === normalizedPrefix || normalized.startsWith(normalizedPrefix + "/");
  });
}

function resolveRequiredScopes(
  method: string,
  pathname: string,
  managementBasePath: string | null,
  webhooksManagementPaths: string[],
): AuthScope[] {
  if (managementBasePath !== null && pathStartsWith(pathname, [managementBasePath])) {
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
  if (!(raw !== null && raw !== undefined && raw.length > 0)) {
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
  if (!isJsonObject(body)) {
    throw new AuthRequestError(400, "request body must be a JSON object");
  }

  return body;
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
      if ((bootstrapKeySecret !== null && bootstrapKeySecret.length > 0)) {
        const bootstrapKey = ctx.secret(bootstrapKeySecret);
        if ((bootstrapKey !== null && bootstrapKey.length > 0) && timingSafeEqual(rawKey, bootstrapKey)) {
          return { valid: true, userId: null, bootstrap: true, scopes: ["admin"] };
        }
      }

      const keyHash = await hashKey(rawKey);
      const record = await ctx.collection.findOne(API_KEYS_COLLECTION, { keyHash }, apiKeyValidator);
      if (!(record !== null)) {
        return { valid: false };
      }

      if ((record.revokedAt !== null && record.revokedAt.length > 0)) {
        return { valid: false, reason: "revoked" };
      }

      if (record.expiresAt !== null) {
        const expiresAtMs = Date.parse(record.expiresAt);
        if (!Number.isFinite(expiresAtMs) || expiresAtMs <= Date.now()) {
          return { valid: false, reason: "expired" };
        }
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

  if ((rateLimitConfig !== null)) {
    entries.push(
      action("__chimpbase.auth.rateLimit", async (ctx, input: { keyPrefix: string; outcome: "check" | "fail" | "success" }) => {
        const blockKey = `__chimpbase.auth.ratelimit:block:${input.keyPrefix}`;
        const countKey = `__chimpbase.auth.ratelimit:count:${input.keyPrefix}`;

        if (input.outcome === "check") {
          const blocked = await ctx.kv.get(blockKey, v.boolean());
          return { blocked: !!(blocked === true) };
        }

        if (input.outcome === "success") {
          await ctx.kv.delete(countKey);
          return { blocked: false };
        }

        // outcome === "fail"
        const blocked = await ctx.kv.get(blockKey, v.boolean());
        if ((blocked === true)) {
          return { blocked: true };
        }

        const existing = await ctx.kv.get(countKey, rateLimitCountValidator);
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
      if (!(rawKey !== null && rawKey.length > 0)) {
        return jsonError(401, "missing API key");
      }

      const keyPrefix = rawKey.substring(0, 8);

      // Rate limit: check if blocked
      if ((rateLimitConfig !== null)) {
        const rlCheck = blockedResultValidator.parse(await env.action("__chimpbase.auth.rateLimit", {
          keyPrefix,
          outcome: "check",
        }), "rate limit result");
        if (rlCheck.blocked) {
          return jsonError(429, "too many failed authentication attempts", {
            "retry-after": String(Math.ceil(rlBlockDurationMs / 1000)),
          });
        }
      }

      const result = validatedApiKeyResultValidator.parse(
        await env.action("__chimpbase.auth.validateApiKey", rawKey),
        "API key validation result",
      );

      if (!result.valid) {
        // Rate limit: record failure
        if ((rateLimitConfig !== null)) {
          const rlFail = blockedResultValidator.parse(await env.action("__chimpbase.auth.rateLimit", {
            keyPrefix,
            outcome: "fail",
          }), "rate limit result");
          if (rlFail.blocked) {
            return jsonError(429, "too many failed authentication attempts", {
              "retry-after": String(Math.ceil(rlBlockDurationMs / 1000)),
            });
          }
        }
        return jsonError(401, "invalid API key");
      }

      // Rate limit: clear counter on success
      if ((rateLimitConfig !== null)) {
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

      return await ctx.collection.findOne(USERS_COLLECTION, { id }, authUserValidator);
    }),
  );

  entries.push(
    action("__chimpbase.auth.listUsers", async (ctx) => {
      return await ctx.collection.find(USERS_COLLECTION, {}, undefined, authUserValidator);
    }),
  );

  entries.push(
    action("__chimpbase.auth.getUser", async (ctx, id: string) => {
      return await ctx.collection.findOne(USERS_COLLECTION, { id }, authUserValidator);
    }),
  );

  entries.push(
    action("__chimpbase.auth.deleteUser", async (ctx, id: string) => {
      // Revoke all API keys for this user
      const keys = await ctx.collection.find(API_KEYS_COLLECTION, { userId: id }, undefined, apiKeyValidator);
      for (const key of keys) {
        if (!(key.revokedAt !== null && key.revokedAt.length > 0)) {
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
      const user = await ctx.collection.findOne(USERS_COLLECTION, { id: input.userId }, authUserValidator);
      if (!(user !== null)) {
        throw new AuthRequestError(404, "user not found");
      }

      const scopes = input.scopes ?? DEFAULT_SCOPES;
      validateScopes(scopes);
      if (input.expiresAt !== undefined && (
        typeof input.expiresAt !== "string" || !Number.isFinite(Date.parse(input.expiresAt))
      )) {
        throw new AuthRequestError(400, '"expiresAt" must be a valid date');
      }

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
      const keys = await ctx.collection.find(API_KEYS_COLLECTION, { userId }, undefined, apiKeyValidator);
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
      const key = await ctx.collection.findOne(API_KEYS_COLLECTION, { id: keyId }, apiKeyValidator);
      if (!(key !== null)) {
        return 0;
      }

      if ((key.revokedAt !== null && key.revokedAt.length > 0)) {
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

  if (options.dependsOn !== undefined || (options.name ?? "").length > 0) {
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
  if (typeof value !== "string" || !(value.length > 0)) {
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
