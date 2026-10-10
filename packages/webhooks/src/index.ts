import {
  action,
  isJsonObject,
  isStringArray,
  plugin,
  readJsonBody,
  route,
  subscription,
  tryParseJson,
  v,
  worker,
  type ChimpbasePluginDependency,
  type ChimpbasePluginRegistration,
  type ChimpbaseRegistrationSource,
} from "@chimpbase/runtime";

// ── Public types ──────────────────────────────────────────────────────────────

/**
 * Secret accessor provided to inbound verify functions.
 * Reads from the Chimpbase secrets system (`.env`, `/run/secrets`, etc.).
 */
export type SecretAccessor = (name: string) => string | null;

/**
 * Custom verification function for inbound webhooks.
 * Return `true` to accept the request, `false` to reject with 401.
 */
export type InboundVerifyFn = (
  request: Request,
  body: string,
  secret: SecretAccessor,
) => boolean | Promise<boolean>;

/**
 * Extracts a deduplication key from an inbound webhook request.
 * Return `null` to skip dedup for this particular request.
 */
export type InboundDeduplicationKeyFn = (
  request: Request,
  body: string,
) => string | null | Promise<string | null>;

export interface InboundWebhookDefinition {
  /** URL path for this inbound endpoint (e.g., `/webhooks/stripe`). */
  path: string;
  /** Event name to publish verified payloads as (e.g., `stripe.event`). */
  publishAs: string;
  /** Verification function. Use `hmac()`, `basicAuth()`, `bearerToken()`, or a custom function. */
  verify: InboundVerifyFn;
  /** Extract a dedup key from the request. Duplicates within the TTL window are ignored. */
  deduplicationKey?: InboundDeduplicationKeyFn;
  /** How long to remember dedup keys in seconds. Default: `86400` (24 hours). */
  deduplicationTtlSeconds?: number;
}

export interface HmacOptions {
  /** Header containing the signature. */
  signatureHeader: string;
  /** Secret name to read from the Chimpbase secrets system. */
  secretName: string;
  /** Hash algorithm. Default: `"SHA-256"`. */
  algorithm?: string;
  /** Prefix to strip from the header value before comparing (e.g., `"sha256="`). */
  prefix?: string;
  /** Custom function to extract signature and optional timestamp from the header value. */
  extractSignature?: (headerValue: string) => { signature: string; timestamp?: string };
  /** Custom function to build the payload to sign. Receives (body, timestamp). Default: just the body. */
  computePayload?: (body: string, timestamp?: string) => string;
}

export interface BasicAuthOptions {
  /** Expected username. */
  username: string;
  /** Secret name whose value is the expected password. */
  passwordSecretName: string;
}

export interface BearerTokenOptions {
  /** Secret name whose value is the expected bearer token. */
  secretName: string;
  /** Header to read the token from. Default: `"authorization"` (expects `Bearer <token>`). */
  header?: string;
}

export interface HeaderTokenOptions {
  /** Header name to read the token from (e.g., `"x-webhook-token"`). */
  header: string;
  /** Secret name whose value is the expected token. */
  secretName: string;
}

export interface ChimpbaseWebhooksOptions {
  /** Event names that can trigger outbound webhooks. Required. */
  allowedEvents: string[];
  /** Base path for the management REST API. Set to `null` to disable. Default: `"/_webhooks"`. */
  managementBasePath?: string | null;
  /** Timeout for outbound webhook HTTP delivery in milliseconds. Default: `10000`. */
  deliveryTimeoutMs?: number;
  /** Inbound webhook definitions keyed by source name. */
  inbound?: Record<string, InboundWebhookDefinition>;
  /** Plugin name. */
  name?: string;
  /** Plugin dependencies. */
  dependsOn?: readonly ChimpbasePluginDependency[];
}

export interface WebhookRegistration {
  id: string;
  url: string;
  events: string;
  secret: string;
  label: string;
  active: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface WebhookDeliveryLog {
  id: string;
  webhookId: string;
  event: string;
  deliveryId: string;
  status: string;
  statusCode: number | null;
  attempt: number;
  error: string | null;
  createdAt: string;
}

class WebhookDeliveryError extends Error {
  constructor(readonly record: Omit<WebhookDeliveryLog, "id">) {
    super(`webhook delivery failed: ${record.error}`);
  }
}
const deliveryLogValidator = v.object({
  attempt: v.number(),
  createdAt: v.string(),
  deliveryId: v.string(),
  error: v.string().nullable(),
  event: v.string(),
  id: v.string(),
  status: v.string(),
  statusCode: v.number().nullable(),
  webhookId: v.string(),
});
const webhookRegistrationValidator = v.object({
  active: v.boolean(),
  createdAt: v.string(),
  events: v.string(),
  id: v.string(),
  label: v.string(),
  secret: v.string(),
  updatedAt: v.string(),
  url: v.string(),
});


// ── Verification helpers (exported) ─────────────────────────────────────────

/**
 * HMAC signature verification.
 *
 * Supports standard formats (`sha256=<hex>`) and custom formats via
 * `extractSignature` and `computePayload` options.
 */
export function hmac(options: HmacOptions): InboundVerifyFn {
  const algorithm = options.algorithm ?? "SHA-256";
  const prefix = options.prefix ?? "";

  return async (request: Request, body: string, secret: SecretAccessor): Promise<boolean> => {
    const headerValue = request.headers.get(options.signatureHeader);
    if (!(headerValue !== null && headerValue.length > 0)) {
      return false;
    }

    const secretValue = secret(options.secretName);
    if (!(secretValue !== null && secretValue.length > 0)) {
      return false;
    }

    let receivedSignature: string;
    let timestamp: string | undefined;

    if ((options.extractSignature !== undefined)) {
      const extracted = options.extractSignature(headerValue);
      receivedSignature = extracted.signature;
      timestamp = extracted.timestamp;
    } else {
      receivedSignature = (prefix.length > 0) ? headerValue.replace(prefix, "") : headerValue;
    }

    const payloadToSign = (options.computePayload !== undefined)
      ? options.computePayload(body, timestamp)
      : body;

    const expectedSignature = await computeHmac(algorithm, secretValue, payloadToSign);

    return timingSafeEqual(receivedSignature, expectedSignature);
  };
}

/**
 * HTTP Basic Authentication verification.
 */
export function basicAuth(options: BasicAuthOptions): InboundVerifyFn {
  return (_request: Request, _body: string, secret: SecretAccessor): boolean => {
    const authorization = _request.headers.get("authorization");
    if (!(authorization !== null && authorization.length > 0)) {
      return false;
    }

    const match = /^Basic\s+(.+)$/i.exec(authorization);
    if (!(match !== null)) {
      return false;
    }

    let decoded: string;
    try {
      decoded = atob(match[1]!);
    } catch {
      return false;
    }

    const colonIndex = decoded.indexOf(":");
    if (colonIndex < 0) {
      return false;
    }

    const username = decoded.substring(0, colonIndex);
    const password = decoded.substring(colonIndex + 1);

    const expectedPassword = secret(options.passwordSecretName);
    if (!(expectedPassword !== null && expectedPassword.length > 0)) {
      return false;
    }

    return username === options.username && timingSafeEqual(password, expectedPassword);
  };
}

/**
 * Bearer token verification from the `Authorization` header.
 */
export function bearerToken(options: BearerTokenOptions): InboundVerifyFn {
  const headerName = options.header ?? "authorization";

  return (_request: Request, _body: string, secret: SecretAccessor): boolean => {
    const headerValue = _request.headers.get(headerName);
    if (!(headerValue !== null && headerValue.length > 0)) {
      return false;
    }

    let token: string;
    if (headerName.toLowerCase() === "authorization") {
      const match = /^Bearer\s+(.+)$/i.exec(headerValue);
      if (!(match !== null)) {
        return false;
      }
      token = match[1]!;
    } else {
      token = headerValue;
    }

    const expectedToken = secret(options.secretName);
    if (!(expectedToken !== null && expectedToken.length > 0)) {
      return false;
    }

    return timingSafeEqual(token, expectedToken);
  };
}

/**
 * Simple shared token in a custom header.
 */
export function headerToken(options: HeaderTokenOptions): InboundVerifyFn {
  return (_request: Request, _body: string, secret: SecretAccessor): boolean => {
    const headerValue = _request.headers.get(options.header);
    if (!(headerValue !== null && headerValue.length > 0)) {
      return false;
    }

    const expectedToken = secret(options.secretName);
    if (!(expectedToken !== null && expectedToken.length > 0)) {
      return false;
    }

    return timingSafeEqual(headerValue, expectedToken);
  };
}

// ── Constants ─────────────────────────────────────────────────────────────────

const REGISTRATIONS_COLLECTION = "__chimpbase.webhooks.registrations";
const DELIVERY_LOG_COLLECTION = "__chimpbase.webhooks.delivery_log";
const DELIVERY_WORKER = "__chimpbase.webhooks.deliver";
const DELIVERY_DLQ = "__chimpbase.webhooks.deliver.dlq";

// ── Internal helpers ────────────────────────────────────────────────────────

function generateSecret(): string {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

function generateDeliveryId(): string {
  return crypto.randomUUID();
}

async function computeOutboundSignature(secret: string, timestamp: number, body: string): Promise<string> {
  return await computeHmac("SHA-256", secret, `${timestamp}.${body}`);
}

async function computeHmac(algorithm: string, secret: string, payload: string): Promise<string> {
  const normalizedAlgorithm = normalizeHashAlgorithm(algorithm);
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: normalizedAlgorithm },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign(
    "HMAC",
    key,
    new TextEncoder().encode(payload),
  );
  return Array.from(new Uint8Array(signature))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

function normalizeHashAlgorithm(algorithm: string): string {
  const upper = algorithm.toUpperCase();
  if (upper === "SHA256" || upper === "SHA-256") return "SHA-256";
  if (upper === "SHA384" || upper === "SHA-384") return "SHA-384";
  if (upper === "SHA512" || upper === "SHA-512") return "SHA-512";
  if (upper === "SHA1" || upper === "SHA-1") return "SHA-1";
  return upper;
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

function parseEvents(events: string): string[] {
  const parsed = tryParseJson(events);
  return isStringArray(parsed) ? parsed : [];
}

function normalizePath(path: string): string {
  const trimmed = path.trim();
  if (!(trimmed.length > 0) || trimmed === "/") {
    return "/";
  }
  const withLeading = trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
  const normalized = withLeading.replace(/\/+$/g, "");
  return normalized.length > 0 ? normalized : "/";
}

function splitPath(path: string): string[] {
  return normalizePath(path).split("/").filter(Boolean);
}

function jsonError(status: number, error: string): Response {
  return Response.json({ error }, { status });
}

async function parseJsonBody(request: Request): Promise<Record<string, unknown>> {
  let body: unknown;
  try {
    body = await readJsonBody(request);
  } catch {
    throw new WebhooksRequestError(400, "request body must be valid JSON");
  }
  if (!isJsonObject(body)) {
    throw new WebhooksRequestError(400, "request body must be a JSON object");
  }

  return body;
}

class WebhooksRequestError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "WebhooksRequestError";
    this.status = status;
  }
}

// ── Plugin ────────────────────────────────────────────────────────────────────

export function chimpbaseWebhooks(
  options: ChimpbaseWebhooksOptions,
): ChimpbasePluginRegistration {
  const managementBasePath = options.managementBasePath === undefined ? "/_webhooks" : options.managementBasePath;
  const deliveryTimeoutMs = options.deliveryTimeoutMs ?? 10_000;
  // Note: maxAttempts and retryDelayMs are configured at the project level in the app definition under worker

  const entries: ChimpbaseRegistrationSource[] = [];

  // ── Inbound webhook routes ──────────────────────────────────────────────

  if ((options.inbound !== undefined)) {
    for (const [sourceName, definition] of Object.entries(options.inbound)) {
      const inboundSegments = splitPath(definition.path);

      entries.push(
        route(`__chimpbase.webhooks.inbound.${sourceName}`, async (request, env) => {
          if (request.method !== "POST") {
            return null;
          }

          const url = new URL(request.url);
          const segments = splitPath(url.pathname);

          if (!matchSegments(segments, inboundSegments)) {
            return null;
          }

          const body = await request.text();

          const verified = await env.action(
            `__chimpbase.webhooks.inbound.verify.${sourceName}`,
            request,
            body,
          ) as boolean;

          if (!verified) {
            return jsonError(401, "webhook signature verification failed");
          }

          let payload: unknown;
          try {
            payload = JSON.parse(body);
          } catch {
            payload = body;
          }

          let dedupKey: string | null = null;
          if ((definition.deduplicationKey !== undefined)) {
            dedupKey = await definition.deduplicationKey(request, body);
          }

          await env.action(`__chimpbase.webhooks.inbound.accept.${sourceName}`, payload, dedupKey);

          return Response.json({ accepted: true });
        }),
      );

      entries.push(
        action(
          `__chimpbase.webhooks.inbound.verify.${sourceName}`,
          async (ctx, request: Request, body: string) => {
            const secretAccessor: SecretAccessor = (name) => ctx.secret(name);
            return await definition.verify(request, body, secretAccessor);
          },
        ),
      );

      const deduplicationTtlMs = (definition.deduplicationTtlSeconds ?? 86_400) * 1000;

      entries.push(
        action(
          `__chimpbase.webhooks.inbound.accept.${sourceName}`,
          async (ctx, payload: unknown, dedupKey: string | null) => {
            if ((dedupKey !== null && dedupKey.length > 0)) {
              const kvKey = `__chimpbase.webhooks.dedup:${sourceName}:${dedupKey}`;
              const existing = await ctx.kv.get(kvKey);
              if ((existing !== null)) {
                ctx.log.info("inbound webhook deduplicated", {
                  source: sourceName,
                  dedupKey,
                });
                return;
              }
              await ctx.kv.set(kvKey, true, { ttlMs: deduplicationTtlMs });
            }

            ctx.pubsub.publish(definition.publishAs, payload);
            ctx.log.info("inbound webhook accepted", {
              source: sourceName,
              event: definition.publishAs,
            });
          },
        ),
      );
    }
  }

  // ── Outbound webhook management actions ─────────────────────────────────

  entries.push(
    action("__chimpbase.webhooks.register", async (ctx, input: { url: string; events: string[]; label?: string }) => {
      const now = nowIso();
      const secret = generateSecret();

      const id = await ctx.collection.insert(REGISTRATIONS_COLLECTION, {
        url: input.url,
        events: JSON.stringify(input.events),
        secret,
        label: input.label ?? "",
        active: true,
        createdAt: now,
        updatedAt: now,
      });

      return {
        id,
        url: input.url,
        events: input.events,
        secret,
        label: input.label ?? "",
        active: true,
        createdAt: now,
        updatedAt: now,
      };
    }),
  );

  entries.push(
    action("__chimpbase.webhooks.list", async (ctx) => {
      const records = await ctx.collection.find(REGISTRATIONS_COLLECTION, {}, undefined, webhookRegistrationValidator);
      return records.map((r) => ({
        id: r.id,
        url: r.url,
        events: parseEvents(r.events),
        label: r.label,
        active: r.active,
        createdAt: r.createdAt,
        updatedAt: r.updatedAt,
      }));
    }),
  );

  entries.push(
    action("__chimpbase.webhooks.get", async (ctx, id: string) => {
      const record = await ctx.collection.findOne(REGISTRATIONS_COLLECTION, { id }, webhookRegistrationValidator);
      if (!(record !== null)) {
        return null;
      }
      return {
        id: record.id,
        url: record.url,
        events: parseEvents(record.events),
        secret: record.secret,
        label: record.label,
        active: record.active,
        createdAt: record.createdAt,
        updatedAt: record.updatedAt,
      };
    }),
  );

  entries.push(
    action("__chimpbase.webhooks.update", async (ctx, input: { id: string; url?: string; events?: string[]; active?: boolean; label?: string }) => {
      const existing = await ctx.collection.findOne(REGISTRATIONS_COLLECTION, { id: input.id }, webhookRegistrationValidator);
      if (!(existing !== null)) {
        return null;
      }

      const patch: Record<string, unknown> = { updatedAt: nowIso() };
      if (input.url !== undefined) patch.url = input.url;
      if (input.events !== undefined) patch.events = JSON.stringify(input.events);
      if (input.active !== undefined) patch.active = input.active;
      if (input.label !== undefined) patch.label = input.label;

      await ctx.collection.update(REGISTRATIONS_COLLECTION, { id: input.id }, patch);
      return await ctx.collection.findOne(REGISTRATIONS_COLLECTION, { id: input.id }, webhookRegistrationValidator);
    }),
  );

  entries.push(
    action("__chimpbase.webhooks.delete", async (ctx, id: string) => {
      return await ctx.collection.delete(REGISTRATIONS_COLLECTION, { id });
    }),
  );

  entries.push(
    action("__chimpbase.webhooks.listDeliveries", async (ctx, webhookId: string) => {
      return await ctx.collection.find(DELIVERY_LOG_COLLECTION, { webhookId }, undefined, deliveryLogValidator);
    }),
  );

  // ── Outbound delivery action ──────────────────────────────────────────────

  entries.push(
    action(
      "__chimpbase.webhooks.deliver",
      async (ctx, input: { webhookId: string; event: string; payload: unknown; deliveryId: string; attempt: number }) => {
        const webhook = await ctx.collection.findOne(REGISTRATIONS_COLLECTION, {
          id: input.webhookId,
        }, webhookRegistrationValidator);

        if (!(webhook !== null) || !webhook.active) {
          ctx.log.warn("webhook not found or inactive, skipping delivery", {
            webhookId: input.webhookId,
            deliveryId: input.deliveryId,
          });
          return;
        }

        const bodyJson = JSON.stringify({
          event: input.event,
          payload: input.payload,
          deliveryId: input.deliveryId,
        });

        const timestamp = Math.floor(Date.now() / 1000);
        const signature = await computeOutboundSignature(webhook.secret, timestamp, bodyJson);

        let statusCode: number | null = null;
        let error: string | null = null;
        let status: string;

        try {
          const controller = new AbortController();
          const timeout = setTimeout(() => controller.abort(), deliveryTimeoutMs);

          const response = await fetch(webhook.url, {
            method: "POST",
            headers: {
              "content-type": "application/json",
              "x-chimpbase-signature": `sha256=${signature}`,
              "x-chimpbase-timestamp": String(timestamp),
              "x-chimpbase-delivery-id": input.deliveryId,
              "x-chimpbase-event": input.event,
            },
            body: bodyJson,
            signal: controller.signal,
          });

          clearTimeout(timeout);
          statusCode = response.status;

          if (response.ok) {
            status = "delivered";
          } else {
            status = "failed";
            error = `HTTP ${response.status}`;
          }
        } catch (err) {
          status = "failed";
          error = err instanceof Error ? err.message : String(err);
        }

        // Log delivery attempt
        const record = {
          webhookId: input.webhookId,
          event: input.event,
          deliveryId: input.deliveryId,
          status,
          statusCode,
          attempt: input.attempt,
          error,
          createdAt: nowIso(),
        };
        await ctx.collection.insert(DELIVERY_LOG_COLLECTION, record);

        if (status === "failed") {
          throw new WebhookDeliveryError(record);
        }
      },
    ),
  );

  // ── Event subscriptions → enqueue outbound delivery jobs ──────────────────

  for (const eventName of options.allowedEvents) {
    entries.push(
      subscription(eventName, async (ctx, payload) => {
        const webhooks = await ctx.collection.find(REGISTRATIONS_COLLECTION, {
          active: true,
        }, undefined, webhookRegistrationValidator);

        for (const webhook of webhooks) {
          const events = parseEvents(webhook.events);
          if (events.includes(eventName) || events.includes("*")) {
            const deliveryId = generateDeliveryId();
            await ctx.enqueue(DELIVERY_WORKER, {
              webhookId: webhook.id,
              event: eventName,
              payload,
              deliveryId,
              attempt: 1,
            });
          }
        }
      }, { name: `__chimpbase.webhooks.sub.${eventName}`, idempotent: true }),
    );
  }

  // ── Outbound delivery worker ──────────────────────────────────────────────

  entries.push(
    worker(
      DELIVERY_WORKER,
      async (ctx, payload: { webhookId: string; event: string; payload: unknown; deliveryId: string; attempt: number }, execution) => {
        await ctx.action("__chimpbase.webhooks.deliver", { ...payload, attempt: execution?.attempt ?? payload.attempt });
      },
      {
        dlq: DELIVERY_DLQ,
        async onFailure(ctx, error) {
          if (error instanceof WebhookDeliveryError) {
            await ctx.collection.insert(DELIVERY_LOG_COLLECTION, error.record);
          }
        },
      },
    ),
  );

  // ── DLQ worker ────────────────────────────────────────────────────────────

  entries.push(
    worker(
      DELIVERY_DLQ,
      async (ctx, envelope: { payload: { webhookId: string; event: string; deliveryId: string } }) => {
        ctx.log.error("webhook delivery exhausted all retries", {
          webhookId: envelope.payload.webhookId,
          event: envelope.payload.event,
          deliveryId: envelope.payload.deliveryId,
        });
      },
      { dlq: false },
    ),
  );

  // ── Management routes ─────────────────────────────────────────────────────

  if (managementBasePath !== null) {
    const baseSegments = splitPath(managementBasePath);

    entries.push(
      route("__chimpbase.webhooks.management", async (request, env) => {
        const url = new URL(request.url);
        const segments = splitPath(url.pathname);

        try {
          // POST /_webhooks
          if (matchSegments(segments, baseSegments) && request.method === "POST") {
            const body = await parseJsonBody(request);
            const result = await env.action("__chimpbase.webhooks.register", {
              url: requireString(body, "url"),
              events: requireStringArray(body, "events"),
              label: optionalString(body, "label"),
            });
            return Response.json(result, { status: 201 });
          }

          // GET /_webhooks
          if (matchSegments(segments, baseSegments) && request.method === "GET") {
            const webhooks = await env.action("__chimpbase.webhooks.list");
            return Response.json(webhooks);
          }

          // GET /_webhooks/:id
          if (matchPrefixWithId(segments, baseSegments) && request.method === "GET") {
            const lastSegment = decodeURIComponent(segments[segments.length - 1]!);

            const webhook = await env.action("__chimpbase.webhooks.get", lastSegment);
            return webhook === null
              ? jsonError(404, "webhook not found")
              : Response.json(webhook);
          }

          // GET /_webhooks/:id/deliveries
          if (
            matchPrefixWithId(segments.slice(0, -1), baseSegments) &&
            segments[segments.length - 1] === "deliveries" &&
            request.method === "GET"
          ) {
            const webhookId = decodeURIComponent(segments[baseSegments.length]!);
            const deliveries = await env.action("__chimpbase.webhooks.listDeliveries", webhookId);
            return Response.json(deliveries);
          }

          // PATCH /_webhooks/:id
          if (matchPrefixWithId(segments, baseSegments) && request.method === "PATCH") {
            const webhookId = decodeURIComponent(segments[segments.length - 1]!);
            const body = await parseJsonBody(request);
            const result = await env.action("__chimpbase.webhooks.update", {
              id: webhookId,
              url: optionalString(body, "url"),
              events: optionalStringArray(body, "events"),
              active: optionalBoolean(body, "active"),
              label: optionalString(body, "label"),
            });
            return result === null
              ? jsonError(404, "webhook not found")
              : Response.json(result);
          }

          // DELETE /_webhooks/:id
          if (matchPrefixWithId(segments, baseSegments) && request.method === "DELETE") {
            const webhookId = decodeURIComponent(segments[segments.length - 1]!);
            const deleted = await env.action("__chimpbase.webhooks.delete", webhookId);
            return deleted === 0
              ? jsonError(404, "webhook not found")
              : new Response(null, { status: 204 });
          }

          return null;
        } catch (error) {
          if (error instanceof WebhooksRequestError) {
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
        name: options.name ?? "chimpbase-webhooks",
      },
      ...entries,
    );
  }

  return plugin({ name: "chimpbase-webhooks" }, ...entries);
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

function requireString(body: Record<string, unknown>, field: string): string {
  const value = body[field];
  if (typeof value !== "string" || !(value.length > 0)) {
    throw new WebhooksRequestError(400, `"${field}" is required and must be a non-empty string`);
  }
  return value;
}

function requireStringArray(body: Record<string, unknown>, field: string): string[] {
  const value = body[field];
  if (!isStringArray(value)) {
    throw new WebhooksRequestError(400, `"${field}" is required and must be an array of strings`);
  }
  if (value.length === 0) {
    throw new WebhooksRequestError(400, `"${field}" must not be empty`);
  }
  return value as string[];
}

function optionalString(body: Record<string, unknown>, field: string): string | undefined {
  const value = body[field];
  if (value === undefined || value === null) {
    return undefined;
  }
  if (typeof value !== "string") {
    throw new WebhooksRequestError(400, `"${field}" must be a string`);
  }
  return value;
}

function optionalStringArray(body: Record<string, unknown>, field: string): string[] | undefined {
  const value = body[field];
  if (value === undefined || value === null) {
    return undefined;
  }
  if (!isStringArray(value)) {
    throw new WebhooksRequestError(400, `"${field}" must be an array of strings`);
  }
  return value;
}

function optionalBoolean(body: Record<string, unknown>, field: string): boolean | undefined {
  const value = body[field];
  if (value === undefined || value === null) {
    return undefined;
  }
  if (typeof value !== "boolean") {
    throw new WebhooksRequestError(400, `"${field}" must be a boolean`);
  }
  return value;
}
