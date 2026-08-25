import { createChimpbase } from "@chimpbase/bun";
import { chimpbaseWebhooks, headerToken } from "@chimpbase/webhooks";
const INBOUND_SECRET = "test-secret";
const chimpbase = await createChimpbase({
  storage: { engine: "memory" },
  server: { port: 0 },
  secrets: { get: (name: string) => name === "WH_SECRET" ? INBOUND_SECRET : null },
});
chimpbase.register({ webhooks: chimpbaseWebhooks({
  allowedEvents: ["order.created"],
  inbound: {
    test: {
      path: "/webhooks/test",
      publishAs: "test.inbound",
      verify: headerToken({ header: "x-webhook-token", secretName: "WH_SECRET" }),
    },
  },
}) });
await chimpbase.start();

// Register outbound webhook
const r1 = await chimpbase.executeRoute(new Request("http://test.local/_webhooks", {
  method: "POST", headers: { "content-type": "application/json" },
  body: JSON.stringify({ url: "https://example.com/hook", events: ["order.created"], label: "test" }),
}));
if (r1.response?.status !== 201) throw new Error("should register webhook");
const webhook = await r1.response!.json() as { id: string; secret: string };
if (!(webhook.secret.length > 0) || webhook.secret.length !== 64) throw new Error("secret should be 64 chars");

// List webhooks
const r2 = await chimpbase.executeRoute(new Request("http://test.local/_webhooks"));
if (r2.response?.status !== 200) throw new Error("should list webhooks");
const webhooks = await r2.response!.json() as unknown[];
if (webhooks.length !== 1) throw new Error("should have 1 webhook");

// Get webhook
const r3 = await chimpbase.executeRoute(new Request(`http://test.local/_webhooks/${webhook.id}`));
if (r3.response?.status !== 200) throw new Error("should get webhook");

// Delete webhook
const r4 = await chimpbase.executeRoute(new Request(`http://test.local/_webhooks/${webhook.id}`, { method: "DELETE" }));
if (r4.response?.status !== 204) throw new Error("should delete webhook");

// Inbound: verified
const r5 = await chimpbase.executeRoute(new Request("http://test.local/webhooks/test", {
  method: "POST", headers: { "content-type": "application/json", "x-webhook-token": INBOUND_SECRET },
  body: JSON.stringify({ event: "test" }),
}));
if (r5.response?.status !== 200) throw new Error("verified inbound should be 200");

// Inbound: unverified
const r6 = await chimpbase.executeRoute(new Request("http://test.local/webhooks/test", {
  method: "POST", headers: { "content-type": "application/json" },
  body: JSON.stringify({ event: "test" }),
}));
if (r6.response?.status !== 401) throw new Error("unverified inbound should be 401");

console.log("webhooks: OK"); await chimpbase.close(); process.exit(0);
