import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { createChimpbase } from "../packages/bun/src/library.ts";
import {
  chimpbaseWebhooks,
  headerToken,
  type WebhookRegistration,
} from "../packages/webhooks/src/index.ts";
import { action, subscription } from "../packages/runtime/index.ts";
import { readJsonResponse } from "./support/http.ts";

/** The webhook routes return the stored registration with `events` decoded back into a list. */
interface WebhookResponse extends Omit<WebhookRegistration, "events"> {
  events: string[];
}

const cleanupDirs: string[] = [];

afterEach(async () => {
  while (cleanupDirs.length > 0) {
    const dir = cleanupDirs.pop();
    if ((dir !== undefined && dir.length > 0)) {
      await rm(dir, { recursive: true, force: true });
    }
  }
});

const INBOUND_SECRET = "test-inbound-secret";

async function createWebhooksHost(options?: { withInbound?: boolean; withDedup?: boolean }) {
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
    secrets: { get: (name: string) => name === "INBOUND_SECRET" ? INBOUND_SECRET : null },
  });

  host.register({
    webhooksPlugin: chimpbaseWebhooks({
      allowedEvents: ["order.created", "order.updated"],
      inbound,
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
