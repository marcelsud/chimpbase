# @chimpbase/webhooks

Outbound and inbound webhook plugin for Chimpbase with pluggable verification.

## Installation

```bash
bun add @chimpbase/webhooks
```

## Quick Start

```ts
import { chimpbaseWebhooks, hmac, basicAuth } from "@chimpbase/webhooks";

const webhooks = chimpbaseWebhooks({
  // Outbound: deliver events to external URLs
  allowedEvents: ["order.created", "order.shipped"],

  // Inbound: receive webhooks from external services
  inbound: {
    stripe: {
      path: "/webhooks/stripe",
      publishAs: "stripe.event",
      verify: hmac({
        signatureHeader: "stripe-signature",
        secretName: "STRIPE_WEBHOOK_SECRET",
        extractSignature: (header) => {
          const parts = Object.fromEntries(
            header.split(",").map((p) => p.split("=") as [string, string]),
          );
          return { signature: parts.v1, timestamp: parts.t };
        },
        computePayload: (body, timestamp) => `${timestamp}.${body}`,
      }),
    },
  },
});
```

## Configuration

```ts
chimpbaseWebhooks({
  // Outbound: events that can trigger webhooks. Required.
  allowedEvents: ["order.created", "order.shipped"],

  // Inbound: receive webhooks from external services
  inbound: { /* see Inbound Webhooks section */ },

  // Base path for outbound management API. Default: "/_webhooks"
  managementBasePath: "/_webhooks",

  // Timeout for outbound HTTP delivery in ms. Default: 10000
  deliveryTimeoutMs: 10_000,
})
```

Worker retry behavior (max attempts, retry delay) is configured at the project level in the app definition under `worker`.

---

## Inbound Webhooks

Inbound webhooks let you receive HTTP calls from external services, verify their authenticity, and publish the payload into Chimpbase's pub/sub system so your subscriptions and workers can react.

### Defining Inbound Sources

Each inbound source has a `path`, a `publishAs` event name, and a `verify` function:

```ts
inbound: {
  sourceName: {
    path: "/webhooks/source",     // URL path to listen on
    publishAs: "source.event",    // event name published to pub/sub
    verify: /* verification fn */ // how to authenticate the request
  },
}
```

When a `POST` arrives at the path and verification passes, the request body is published as a pub/sub event. Your app handles it with a normal subscription:

```ts
subscription("stripe.event", async (ctx, payload) => {
  // payload is the verified webhook body from Stripe
  if (payload.type === "payment_intent.succeeded") {
    await ctx.action("fulfillOrder", payload.data.object);
  }
});
```

### Built-in Verification Helpers

All helpers are exported from `@chimpbase/webhooks` and return a verify function. Mix and match per source.

#### `hmac()` — HMAC Signature

For services that sign payloads with a shared secret (Stripe, GitHub, Shopify, etc.).

```ts
import { hmac } from "@chimpbase/webhooks";
```

**Standard format** (`sha256=<hex>` in a header):

```ts
// GitHub
hmac({
  signatureHeader: "x-hub-signature-256",
  secretName: "GITHUB_WEBHOOK_SECRET",
  prefix: "sha256=",
})
```

**Custom format** (Stripe's `t=timestamp,v1=signature`):

```ts
hmac({
  signatureHeader: "stripe-signature",
  secretName: "STRIPE_WEBHOOK_SECRET",
  extractSignature: (header) => {
    const parts = Object.fromEntries(
      header.split(",").map((p) => p.split("=") as [string, string]),
    );
    return { signature: parts.v1, timestamp: parts.t };
  },
  computePayload: (body, timestamp) => `${timestamp}.${body}`,
})
```

**Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `signatureHeader` | `string` | — | Header containing the signature |
| `secretName` | `string` | — | Secret name in Chimpbase secrets |
| `algorithm` | `string` | `"SHA-256"` | Hash algorithm |
| `prefix` | `string` | `""` | Prefix to strip from header value |
| `extractSignature` | `fn` | — | Custom extractor for signature + timestamp |
| `computePayload` | `fn` | body only | Custom function to build the signed payload |

#### `basicAuth()` — HTTP Basic Authentication

For services that authenticate with username/password.

```ts
import { basicAuth } from "@chimpbase/webhooks";

basicAuth({
  username: "webhook",
  passwordSecretName: "PARTNER_WEBHOOK_PASSWORD",
})
```

| Option | Type | Description |
|--------|------|-------------|
| `username` | `string` | Expected username |
| `passwordSecretName` | `string` | Secret name for the expected password |

#### `bearerToken()` — Bearer Token

For services that send a token in the `Authorization: Bearer` header.

```ts
import { bearerToken } from "@chimpbase/webhooks";

bearerToken({
  secretName: "INTERNAL_WEBHOOK_TOKEN",
})
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `secretName` | `string` | — | Secret name for the expected token |
| `header` | `string` | `"authorization"` | Header to read from |

#### `headerToken()` — Shared Token in Custom Header

For services that send a shared secret in a custom header.

```ts
import { headerToken } from "@chimpbase/webhooks";

headerToken({
  header: "x-webhook-token",
  secretName: "VENDOR_WEBHOOK_TOKEN",
})
```

| Option | Type | Description |
|--------|------|-------------|
| `header` | `string` | Header name to read the token from |
| `secretName` | `string` | Secret name for the expected token |

### Custom Verification

For any auth scheme not covered by the built-ins, pass an async function directly:

```ts
inbound: {
  custom: {
    path: "/webhooks/custom",
    publishAs: "custom.event",
    verify: async (request, body, secret) => {
      // request: the raw Request object
      // body: the request body as a string
      // secret: (name) => string | null — reads from Chimpbase secrets

      const token = request.headers.get("x-custom-token");
      const nonce = request.headers.get("x-nonce");
      const expectedToken = secret("CUSTOM_WEBHOOK_SECRET");

      if (!token || !expectedToken) return false;

      // Your custom logic here
      return token === expectedToken && nonce !== null;
    },
  },
}
```

The function signature is:

```ts
type InboundVerifyFn = (
  request: Request,
  body: string,
  secret: (name: string) => string | null,
) => boolean | Promise<boolean>;
```

### Full Example

```ts
import {
  chimpbaseWebhooks,
  hmac,
  basicAuth,
  bearerToken,
  headerToken,
} from "@chimpbase/webhooks";

chimpbaseWebhooks({
  allowedEvents: ["order.created"],

  inbound: {
    stripe: {
      path: "/webhooks/stripe",
      publishAs: "stripe.event",
      verify: hmac({
        signatureHeader: "stripe-signature",
        secretName: "STRIPE_WEBHOOK_SECRET",
        extractSignature: (header) => {
          const parts = Object.fromEntries(
            header.split(",").map((p) => p.split("=") as [string, string]),
          );
          return { signature: parts.v1, timestamp: parts.t };
        },
        computePayload: (body, timestamp) => `${timestamp}.${body}`,
      }),
    },

    github: {
      path: "/webhooks/github",
      publishAs: "github.event",
      verify: hmac({
        signatureHeader: "x-hub-signature-256",
        secretName: "GITHUB_WEBHOOK_SECRET",
        prefix: "sha256=",
      }),
    },

    partner: {
      path: "/webhooks/partner",
      publishAs: "partner.event",
      verify: basicAuth({
        username: "webhook",
        passwordSecretName: "PARTNER_WEBHOOK_PASSWORD",
      }),
    },

    internal: {
      path: "/webhooks/internal",
      publishAs: "internal.event",
      verify: bearerToken({ secretName: "INTERNAL_WEBHOOK_TOKEN" }),
    },

    vendor: {
      path: "/webhooks/vendor",
      publishAs: "vendor.event",
      verify: headerToken({
        header: "x-webhook-token",
        secretName: "VENDOR_WEBHOOK_TOKEN",
      }),
    },

    custom: {
      path: "/webhooks/custom",
      publishAs: "custom.event",
      verify: async (request, body, secret) => {
        const token = request.headers.get("x-my-token");
        return token === secret("MY_TOKEN");
      },
    },
  },
})
```

### Deduplication

External services often retry webhook deliveries. Without dedup, your app processes the same event multiple times. Add a `deduplicationKey` function to extract a unique ID from the request:

```ts
inbound: {
  stripe: {
    path: "/webhooks/stripe",
    publishAs: "stripe.event",
    verify: hmac({ ... }),
    // Stripe includes a unique event ID in the payload
    deduplicationKey: (_request, body) => {
      return JSON.parse(body).id; // e.g., "evt_1234"
    },
  },
  github: {
    path: "/webhooks/github",
    publishAs: "github.event",
    verify: hmac({ ... }),
    // GitHub sends a unique delivery ID in a header
    deduplicationKey: (request) => {
      return request.headers.get("x-github-delivery");
    },
  },
}
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `deduplicationKey` | `(request, body) => string \| null` | — | Extract a dedup key. Return `null` to skip dedup for this request. |
| `deduplicationTtlSeconds` | `number` | `86400` (24h) | How long to remember keys. Uses KV with TTL. |

Under the hood, dedup keys are reserved atomically in the KV store with automatic TTL expiration. Concurrent deliveries to hosts sharing a database publish once while the key is live. Failed acceptance rolls back the reservation so the delivery can be retried. No cleanup cron needed per webhook source.

**Outbound subscriptions** are also deduplicated — they use the framework's built-in subscription idempotency (`idempotent: true`), so replayed events don't trigger duplicate deliveries.

### Inbound + Auth

When using `@chimpbase/auth`, you likely want to **exclude** inbound webhook paths from API key authentication, since external services authenticate via their own mechanism:

```ts
chimpbaseAuth({
  bootstrapKeySecret: "CHIMPBASE_BOOTSTRAP_API_KEY",
  excludePaths: ["/health", "/webhooks/stripe", "/webhooks/github"],
}),
chimpbaseWebhooks({
  allowedEvents: ["order.created"],
  inbound: {
    stripe: { path: "/webhooks/stripe", publishAs: "stripe.event", verify: hmac({ ... }) },
    github: { path: "/webhooks/github", publishAs: "github.event", verify: hmac({ ... }) },
  },
}),
```

---

## Outbound Webhooks

### How It Works

1. Your app publishes an event via `ctx.pubsub.publish("order.created", payload)`
2. The plugin's subscription for `order.created` fires
3. It queries all active webhooks subscribed to that event
4. For each matching webhook, it enqueues a delivery job to the worker queue
5. The delivery worker loads the webhook, computes an HMAC-SHA256 signature, and sends an HTTP POST
6. On failure (non-2xx or network error), the framework's worker retry mechanism retries the delivery
7. After all retries are exhausted, the job moves to the dead letter queue (DLQ)

### Registering Outbound Webhooks

Register a webhook via the management API:

```bash
curl -X POST http://localhost:3000/_webhooks \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://example.com/webhook",
    "events": ["order.created", "order.shipped"],
    "label": "order notifications"
  }'
```

The response includes the webhook `secret` — store it to verify signatures on the receiving end.

### HMAC Signature on Outbound Deliveries

Every outbound delivery includes these headers:

| Header | Description |
|--------|-------------|
| `X-Chimpbase-Signature` | `sha256=<hex-encoded HMAC>` |
| `X-Chimpbase-Timestamp` | Unix epoch seconds when the signature was computed |
| `X-Chimpbase-Delivery-Id` | Unique UUID for this delivery attempt |
| `X-Chimpbase-Event` | The event name (e.g., `order.created`) |
| `Content-Type` | `application/json` |

The signed payload is `${timestamp}.${body}` where `body` is the JSON request body.

### Verifying Outbound Signatures (Receiver Side)

```ts
import { createHmac, timingSafeEqual } from "node:crypto";

function verifyWebhookSignature(
  secret: string,
  signatureHeader: string,
  timestampHeader: string,
  body: string,
): boolean {
  // Reject old timestamps to prevent replay attacks (e.g., 5 min window)
  const timestamp = parseInt(timestampHeader, 10);
  const age = Math.abs(Date.now() / 1000 - timestamp);
  if (age > 300) {
    return false;
  }

  const expected = createHmac("sha256", secret)
    .update(`${timestamp}.${body}`)
    .digest("hex");

  const received = signatureHeader.replace("sha256=", "");

  return timingSafeEqual(
    Buffer.from(expected, "hex"),
    Buffer.from(received, "hex"),
  );
}
```

### Outbound Request Body Format

```json
{
  "event": "order.created",
  "payload": { ... },
  "deliveryId": "550e8400-e29b-41d4-a716-446655440000"
}
```

### Wildcard Subscriptions

Webhooks can subscribe to all allowed events by including `"*"` in their events array:

```bash
curl -X POST http://localhost:3000/_webhooks \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://example.com/all-events",
    "events": ["*"]
  }'
```

---

## Management API

All endpoints are served under the configured `managementBasePath` (default: `/_webhooks`).

### Register Webhook

```
POST /_webhooks
```

```json
{
  "url": "https://example.com/webhook",
  "events": ["order.created", "order.shipped"],
  "label": "order notifications"
}
```

Returns the webhook registration including the `secret` with `201`.

### List Webhooks

```
GET /_webhooks
```

Returns an array of all webhook registrations (secrets are omitted in list view).

### Get Webhook

```
GET /_webhooks/:id
```

Returns a single webhook including its `secret`.

### Update Webhook

```
PATCH /_webhooks/:id
```

```json
{
  "url": "https://new-url.com/webhook",
  "events": ["order.created"],
  "active": false,
  "label": "updated label"
}
```

All fields are optional.

### Delete Webhook

```
DELETE /_webhooks/:id
```

Returns `204` on success, `404` if not found.

### List Deliveries

```
GET /_webhooks/:id/deliveries
```

Returns the delivery log for a specific webhook.

---

## Actions

| Action | Args | Description |
|--------|------|-------------|
| `__chimpbase.webhooks.register` | `{ url, events, label? }` | Register an outbound webhook |
| `__chimpbase.webhooks.list` | — | List all outbound webhooks |
| `__chimpbase.webhooks.get` | `id` | Get webhook with secret |
| `__chimpbase.webhooks.update` | `{ id, url?, events?, active?, label? }` | Update a webhook |
| `__chimpbase.webhooks.delete` | `id` | Delete a webhook |
| `__chimpbase.webhooks.deliver` | `{ webhookId, event, payload, deliveryId, attempt }` | Deliver (used internally) |
| `__chimpbase.webhooks.listDeliveries` | `webhookId` | List delivery history |

## Data Storage

The plugin uses two Chimpbase collections:

- `__chimpbase.webhooks.registrations` — outbound webhook configurations and secrets
- `__chimpbase.webhooks.delivery_log` — outbound delivery attempt history

It also uses the framework's worker queue system:

- `__chimpbase.webhooks.deliver` — delivery job queue
- `__chimpbase.webhooks.deliver.dlq` — dead letter queue for exhausted retries

No additional migrations are required.
