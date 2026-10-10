import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { chimpbaseAuth } from "../packages/auth/src/index.ts";
import {
  chimpbaseWebhooks,
  headerToken,
  type WebhookDeliveryLog,
  type WebhookRegistration,
} from "../packages/webhooks/src/index.ts";
import { action, subscription, v } from "../packages/runtime/index.ts";
import { readJsonResponse } from "./support/http.ts";

/** The webhook routes return the stored registration with `events` decoded back into a list. */
interface WebhookResponse extends Omit<WebhookRegistration, "events"> {
  events: string[];
}

const cleanupDirs: string[] = [];

for (const engine of ["memory", "sqlite", "postgres"] as const) {
  for (const scenario of ["HTTP retry", "network retry", "exhausted retries"] as const) {
    const pgUrl = process.env.CHIMPBASE_TEST_PG_URL;
    (engine === "postgres" && !pgUrl ? test.skip : test)(`webhook ${scenario} retains every attempt after rollback (${engine})`, async () => {
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-webhook-retry-"));
      cleanupDirs.push(projectDir);
      const host = await createChimpbase({
        projectDir, storage: engine === "postgres" ? { engine, url: pgUrl } : { engine },
        worker: { maxAttempts: 2, retryDelayMs: 0 },
        registrations: [
          chimpbaseWebhooks({ allowedEvents: ["order.created"], deliveryTimeoutMs: 50 }),
          action("publishRetry", async (ctx) => { ctx.pubsub.publish("order.created", { orderId: 1 }); }),
          action("inspectRetry", async (ctx, deliveryId: string) => await ctx.db.query(
            "SELECT queue_name, status, attempt_count, lease_expires_at_ms FROM _chimpbase_queue_jobs WHERE CAST(payload_json AS TEXT) LIKE ?1 ORDER BY id",
            [`%${deliveryId}%`], v.object({ queue_name: v.string(), status: v.string(), attempt_count: v.number(), lease_expires_at_ms: v.number().nullable() }),
          )),
          action("cleanupRetry", async (ctx, webhookId: string) => {
            await ctx.collection.delete("__chimpbase.webhooks.registrations", { id: webhookId });
            await ctx.collection.delete("__chimpbase.webhooks.delivery_log", { webhookId });
            await ctx.db.query("DELETE FROM _chimpbase_queue_jobs WHERE CAST(payload_json AS TEXT) LIKE ?1", [`%${webhookId}%`]);
          }),
        ],
      });
      const originalFetch = globalThis.fetch;
      const deliveryIds: string[] = [];
      let webhookId = "";
      let fetchCalls = 0;
      globalThis.fetch = Object.assign(async (_url: string | URL | Request, init?: RequestInit): Promise<Response> => {
        fetchCalls += 1;
        deliveryIds.push(new Headers(init?.headers).get("x-chimpbase-delivery-id") ?? "");
        if (scenario === "network retry" && fetchCalls === 1) throw new Error("connection failed");
        return new Response("response", { status: fetchCalls === 1 || scenario === "exhausted retries" ? 500 : 200 });
      }, { preconnect: originalFetch.preconnect });
      const failure = scenario === "network retry" ? "connection failed" : "HTTP 500";
      const history = async () => v.object({
        webhookId: v.string(), deliveryId: v.string(), event: v.string(), status: v.string(),
        statusCode: v.number().nullable(), attempt: v.number(), error: v.string().nullable(),
      }).array().parse((await host.executeAction("__chimpbase.webhooks.listDeliveries", webhookId)).result)
        .sort((a, b) => a.attempt - b.attempt);
      try {
        webhookId = v.object({ id: v.string() }).parse((await host.executeAction("__chimpbase.webhooks.register", {
          url: "https://example.invalid/hook", events: ["order.created"],
        })).result).id;
        await host.executeAction("publishRetry");
        await expect(host.processNextQueueJob()).rejects.toThrow(`webhook delivery failed: ${failure}`);
        const first = {
          webhookId, deliveryId: deliveryIds[0], event: "order.created", status: "failed",
          statusCode: scenario === "network retry" ? null : 500, attempt: 1, error: failure,
        };
        expect(await history()).toEqual([first]);
        expect((await host.executeAction("inspectRetry", deliveryIds[0])).result).toEqual([{
          queue_name: "__chimpbase.webhooks.deliver", status: "pending", attempt_count: 1, lease_expires_at_ms: null,
        }]);
        if (scenario === "exhausted retries") await expect(host.processNextQueueJob()).rejects.toThrow("webhook delivery failed: HTTP 500");
        else await host.processNextQueueJob();
        expect(fetchCalls).toBe(2);
        expect(deliveryIds[1]).toBe(deliveryIds[0]);
        expect(await history()).toEqual([first, {
          ...first, attempt: 2, status: scenario === "exhausted retries" ? "failed" : "delivered",
          statusCode: scenario === "exhausted retries" ? 500 : 200, error: scenario === "exhausted retries" ? "HTTP 500" : null,
        }]);
        expect((await host.executeAction("inspectRetry", deliveryIds[0])).result).toEqual(scenario === "exhausted retries" ? [
          { queue_name: "__chimpbase.webhooks.deliver", status: "dlq", attempt_count: 2, lease_expires_at_ms: null },
          { queue_name: "__chimpbase.webhooks.deliver.dlq", status: "pending", attempt_count: 0, lease_expires_at_ms: null },
        ] : [{ queue_name: "__chimpbase.webhooks.deliver", status: "completed", attempt_count: 2, lease_expires_at_ms: null }]);
        if (scenario === "exhausted retries") await host.processNextQueueJob();
        expect(await host.processNextQueueJob()).toBeNull();
      } finally {
        globalThis.fetch = originalFetch;
        try { if (webhookId) await host.executeAction("cleanupRetry", webhookId); }
        finally { await host.close(); }
      }
    });
  }
}

afterEach(async () => {
  while (cleanupDirs.length > 0) {
    const dir = cleanupDirs.pop();
    if ((dir !== undefined && dir.length > 0)) {
      await rm(dir, { recursive: true, force: true });
    }
  }
});

const INBOUND_SECRET = "test-inbound-secret";
const MANAGEMENT_KEY = "test-management-key";

async function createWebhooksHost(options?: { withInbound?: boolean; withDedup?: boolean; withAuth?: boolean; managementBasePath?: string }) {
  const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-webhooks-test-"));
  cleanupDirs.push(projectDir);

  const inbound = (options?.withInbound === true)
    ? {
        testSource: {
          path: "/webhooks/test",
          publishAs: "test.inbound",
          verify: headerToken({ header: "x-webhook-token", secretName: "INBOUND_SECRET" }),
          ...((options.withDedup === true)
            ? {
                deduplicationKey: (request: Request) => request.headers.get("x-idempotency-key"),
                deduplicationTtlSeconds: 3600,
              }
            : {}),
        },
      }
    : undefined;

  const host = await createChimpbase({
    project: { name: "webhooks-test" },
    projectDir,
    storage: { engine: "memory" },
    secrets: { get: (name: string) => name === "INBOUND_SECRET" ? INBOUND_SECRET : name === "MANAGEMENT_KEY" ? MANAGEMENT_KEY : null },
  });

  if (options?.withAuth === true) {
    host.register(chimpbaseAuth({
      bootstrapKeySecret: "MANAGEMENT_KEY",
      webhooksManagementPaths: [options.managementBasePath ?? "/_webhooks"],
    }));
  }

  host.register({
    webhooksPlugin: chimpbaseWebhooks({
      allowedEvents: ["order.created", "order.updated"],
      inbound,
      managementBasePath: options?.managementBasePath,
    }),
  });

  return host;
}

describe("@chimpbase/webhooks", () => {
  // ── Outbound management ─────────────────────────────────────────────────

  test("registers a webhook", async () => {
    const host = await createWebhooksHost();
    try {
      const outcome = await host.executeRoute(
        new Request("http://test.local/_webhooks", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            url: "https://example.com/hook",
            events: ["order.created"],
            label: "test",
          }),
        }),
      );
      expect(outcome.response?.status).toBe(201);
      const webhook = await readJsonResponse<WebhookResponse>(outcome.response);
      expect(webhook.url).toBe("https://example.com/hook");
      expect(webhook.events).toEqual(["order.created"]);
      expect(webhook.secret).toBeDefined();
      expect(webhook.secret.length).toBe(64);
      expect(webhook.active).toBe(true);
    } finally {
      await host.close();
    }
  });

  test("lists webhooks", async () => {
    const host = await createWebhooksHost();
    try {
      await host.executeRoute(
        new Request("http://test.local/_webhooks", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ url: "https://a.com/hook", events: ["order.created"] }),
        }),
      );

      const outcome = await host.executeRoute(
        new Request("http://test.local/_webhooks"),
      );
      expect(outcome.response?.status).toBe(200);
      const webhooks = await readJsonResponse<WebhookResponse[]>(outcome.response);
      expect(webhooks).toHaveLength(1);
      expect(webhooks[0].url).toBe("https://a.com/hook");
    } finally {
      await host.close();
    }
  });

  test("gets webhook by ID with secret", async () => {
    const host = await createWebhooksHost();
    try {
      const createOutcome = await host.executeRoute(
        new Request("http://test.local/_webhooks", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ url: "https://get.com/hook", events: ["order.created"] }),
        }),
      );
      const created = await readJsonResponse<WebhookResponse>(createOutcome.response);

      const getOutcome = await host.executeRoute(
        new Request(`http://test.local/_webhooks/${created.id}`),
      );
      expect(getOutcome.response?.status).toBe(200);
      const webhook = await readJsonResponse<WebhookResponse>(getOutcome.response);
      expect(webhook.secret).toBe(created.secret);
    } finally {
      await host.close();
    }
  });

  test("updates a webhook", async () => {
    const host = await createWebhooksHost();
    try {
      const createOutcome = await host.executeRoute(
        new Request("http://test.local/_webhooks", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ url: "https://old.com/hook", events: ["order.created"] }),
        }),
      );
      const created = await readJsonResponse<WebhookResponse>(createOutcome.response);

      const updateOutcome = await host.executeRoute(
        new Request(`http://test.local/_webhooks/${created.id}`, {
          method: "PATCH",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ active: false, label: "disabled" }),
        }),
      );
      expect(updateOutcome.response?.status).toBe(200);
    } finally {
      await host.close();
    }
  });

  test("gets delivery history (empty initially)", async () => {
    const host = await createWebhooksHost();
    try {
      const createOutcome = await host.executeRoute(
        new Request("http://test.local/_webhooks", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ url: "https://x.com/hook", events: ["order.created"] }),
        }),
      );
      const created = await readJsonResponse<WebhookResponse>(createOutcome.response);

      const outcome = await host.executeRoute(
        new Request(`http://test.local/_webhooks/${created.id}/deliveries`),
      );
      expect(outcome.response?.status).toBe(200);
      expect(await readJsonResponse<unknown[]>(outcome.response)).toEqual([]);
    } finally {
      await host.close();
    }
  });

  for (const managementBasePath of ["/_webhooks", "/admin/hooks"]) {
    for (const withAuth of [false, true]) {
      test(`delivery history requires the complete ${managementBasePath} prefix (auth: ${withAuth})`, async () => {
        const host = await createWebhooksHost({ managementBasePath, withAuth });
        try {
          const headers = { "x-api-key": MANAGEMENT_KEY };
          const created = await host.executeRoute(new Request(`http://test.local${managementBasePath}`, {
            method: "POST",
            headers: { "content-type": "application/json", ...headers },
            body: JSON.stringify({ url: "https://example.invalid/hook", events: ["order.created"] }),
          }));
          expect(created.response?.status).toBe(201);
          const webhook = await readJsonResponse<WebhookResponse>(created.response);
          host.register(action("test.seedDelivery", async (ctx) => {
            await ctx.collection.insert("__chimpbase.webhooks.delivery_log", {
              webhookId: webhook.id,
              event: "order.created",
              deliveryId: "test-delivery",
              status: "delivered",
              statusCode: 200,
              attempt: 1,
              error: null,
              createdAt: new Date().toISOString(),
            });
          }));
          await host.executeAction("test.seedDelivery");

          const historyPath = `${managementBasePath}/${webhook.id}/deliveries`;
          const unauthenticated = await host.executeRoute(new Request(`http://test.local${historyPath}`));
          expect(unauthenticated.response?.status).toBe(withAuth ? 401 : 200);

          for (const path of [historyPath, `${historyPath.replaceAll("/", "//")}//`]) {
            const authorized = await host.executeRoute(new Request(`http://test.local${path}`, { headers }));
            expect(authorized.response?.status).toBe(200);
            const records = await readJsonResponse<WebhookDeliveryLog[]>(authorized.response);
            expect(records.map(({ deliveryId, webhookId }) => ({ deliveryId, webhookId }))).toEqual([
              { deliveryId: "test-delivery", webhookId: webhook.id },
            ]);
          }

          const healthPrefix = managementBasePath === "/_webhooks" ? "/health" : "/health/status";
          const unrelatedPrefix = managementBasePath === "/_webhooks" ? "/unrelated" : "/other/hooks";
          for (const prefix of [healthPrefix, unrelatedPrefix, `${managementBasePath}-other`]) {
            const outcome = await host.executeRoute(new Request(`http://test.local${prefix}/${webhook.id}/deliveries`, { headers }));
            expect(outcome.response).toBeNull();
          }

          const health = await host.executeRoute(new Request(`http://test.local${healthPrefix}/${webhook.id}/deliveries`));
          expect(health.response).toBeNull();

          for (const path of [`${historyPath}/extra`, `${managementBasePath}/${webhook.id}/events`]) {
            const outcome = await host.executeRoute(new Request(`http://test.local${path}`, { headers }));
            expect(outcome.response).toBeNull();
          }
          const wrongMethod = await host.executeRoute(new Request(`http://test.local${historyPath}`, { method: "POST", headers }));
          expect(wrongMethod.response).toBeNull();
        } finally {
          await host.close();
        }
      });
    }
  }

  test("deletes a webhook", async () => {
    const host = await createWebhooksHost();
    try {
      const createOutcome = await host.executeRoute(
        new Request("http://test.local/_webhooks", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ url: "https://del.com/hook", events: ["order.created"] }),
        }),
      );
      const created = await readJsonResponse<WebhookResponse>(createOutcome.response);

      const deleteOutcome = await host.executeRoute(
        new Request(`http://test.local/_webhooks/${created.id}`, { method: "DELETE" }),
      );
      expect(deleteOutcome.response?.status).toBe(204);
    } finally {
      await host.close();
    }
  });

  test("delete non-existent webhook returns 404", async () => {
    const host = await createWebhooksHost();
    try {
      const outcome = await host.executeRoute(
        new Request("http://test.local/_webhooks/nonexistent", { method: "DELETE" }),
      );
      expect(outcome.response?.status).toBe(404);
    } finally {
      await host.close();
    }
  });

  test("get non-existent webhook returns 404", async () => {
    const host = await createWebhooksHost();
    try {
      const outcome = await host.executeRoute(
        new Request("http://test.local/_webhooks/nonexistent"),
      );
      expect(outcome.response?.status).toBe(404);
    } finally {
      await host.close();
    }
  });

  test("invalid body returns 400", async () => {
    const host = await createWebhooksHost();
    try {
      const outcome = await host.executeRoute(
        new Request("http://test.local/_webhooks", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ url: "https://x.com" }),
        }),
      );
      expect(outcome.response?.status).toBe(400);
    } finally {
      await host.close();
    }
  });

  // ── Inbound webhooks ────────────────────────────────────────────────────

  test("verified inbound POST is accepted", async () => {
    const host = await createWebhooksHost({ withInbound: true });
    try {
      const outcome = await host.executeRoute(
        new Request("http://test.local/webhooks/test", {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "x-webhook-token": INBOUND_SECRET,
          },
          body: JSON.stringify({ event: "test" }),
        }),
      );
      expect(outcome.response?.status).toBe(200);
      expect(await readJsonResponse<{ accepted: boolean }>(outcome.response)).toEqual({ accepted: true });
    } finally {
      await host.close();
    }
  });

  test("unverified inbound POST returns 401", async () => {
    const host = await createWebhooksHost({ withInbound: true });
    try {
      const outcome = await host.executeRoute(
        new Request("http://test.local/webhooks/test", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ event: "test" }),
        }),
      );
      expect(outcome.response?.status).toBe(401);
    } finally {
      await host.close();
    }
  });

  test("inbound deduplication ignores duplicate requests", async () => {
    const host = await createWebhooksHost({ withInbound: true, withDedup: true });
    let publishCount = 0;

    host.register({
      counter: subscription("test.inbound", async () => {
        publishCount++;
      }, { name: "dedup-counter" }),
    });

    try {
      const makeRequest = () =>
        host.executeRoute(
          new Request("http://test.local/webhooks/test", {
            method: "POST",
            headers: {
              "content-type": "application/json",
              "x-webhook-token": INBOUND_SECRET,
              "x-idempotency-key": "unique-123",
            },
            body: JSON.stringify({ event: "test" }),
          }),
        );

      const first = await makeRequest();
      expect(first.response?.status).toBe(200);

      const second = await makeRequest();
      expect(second.response?.status).toBe(200);

      // Only one event should have been published
      expect(publishCount).toBe(1);
    } finally {
      await host.close();
    }
  });

  test("non-POST to inbound path returns null", async () => {
    const host = await createWebhooksHost({ withInbound: true });
    try {
      const outcome = await host.executeRoute(
        new Request("http://test.local/webhooks/test", {
          headers: { "x-webhook-token": INBOUND_SECRET },
        }),
      );
      // GET request to inbound path — route returns null (not matched)
      expect(outcome.response).toBeNull();
    } finally {
      await host.close();
    }
  });
});
