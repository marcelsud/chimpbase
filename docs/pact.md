# @chimpbase/pact

Consumer-driven contract testing for Chimpbase applications. Verify that services agree on the shape of actions, events, and queue payloads — without needing both services running at the same time.

## Installation

```bash
bun add @chimpbase/pact
```

## How It Works

Contract testing verifies that a **consumer** (the caller) and a **provider** (the service) agree on the shape of their interactions. The workflow:

1. The consumer defines a **pact** — a contract describing expected interactions (action args/results, event payloads, queue payloads)
2. The pact is shared with the provider (via filesystem, git, or serialized JSON)
3. The provider runs **verification** against their real, running app — calling real action handlers and checking that results match the contract
4. Both sides can deploy independently with confidence

No external broker is needed. Pacts are plain data structures or JSON files.

## Defining a Pact

A pact describes what one service expects from another:

```ts
import { pact, interaction } from "@chimpbase/pact";
import { v } from "@chimpbase/runtime";

const inventoryPact = pact({
  consumer: "order-service",
  provider: "inventory-service",
  interactions: [
    interaction.action("reserveStock", {
      states: ["product SKU-001 has 10 units in stock"],
      args: v.object({ sku: v.string(), quantity: v.number() }),
      result: v.object({ reservationId: v.string(), expiresAt: v.string() }),
      example: {
        args: [{ sku: "SKU-001", quantity: 2 }],
        result: { reservationId: "res-abc", expiresAt: "2026-04-12T00:00:00Z" },
      },
    }),
  ],
});
```

### Interaction Types

#### Actions

Verify that an action accepts the expected args and returns the expected result:

```ts
interaction.action("actionName", {
  states: ["precondition description"],
  args: v.object({ /* expected args shape */ }),
  result: v.object({ /* expected result shape */ }),
  example: {
    args: [{ /* example args to send during verification */ }],
    result: { /* example result for documentation */ },
  },
})
```

The `args` and `result` validators use the same `v.*` primitives from `@chimpbase/runtime`. During verification, the real action handler is called with the example args, and the real result is validated against the `result` validator using `.parse()`.

#### Events

Verify that an action emits an event with the expected payload:

```ts
interaction.event("order.created", {
  states: ["an order is placed"],
  payload: v.object({
    orderId: v.string(),
    total: v.number(),
  }),
})
```

During verification, the state setup function triggers the action that emits the event. The emitted events are captured and validated against the `payload` validator.

#### Workers

Verify the expected shape of queue job payloads:

```ts
interaction.worker("email.send", {
  payload: v.object({
    to: v.string(),
    subject: v.string(),
    body: v.string(),
  }),
  example: {
    to: "user@example.com",
    subject: "Welcome",
    body: "Hello!",
  },
})
```

### States

States are preconditions that must be true for an interaction to make sense. For example, "product SKU-001 has 10 units in stock" means the provider needs to seed that data before the interaction can be verified.

States are set up by the provider during verification — the consumer only declares their names.

## Provider Verification

The provider verifies the pact against their real application:

```ts
import { describe, expect, test } from "bun:test";
import { verifyPact } from "@chimpbase/pact";
import { createChimpbase } from "@chimpbase/bun";
import { defineChimpbaseApp } from "@chimpbase/core";
import app from "../chimpbase.app";

describe("pact verification", () => {
  test("satisfies order-service contract", async () => {
    const host = await createChimpbase({
      app,
      storage: { engine: "memory" },
    });

    try {
      const result = await verifyPact({
        host,
        pact: inventoryPact,
        states: {
          "product SKU-001 has 10 units in stock": async (host) => {
            await host.executeAction("seedProduct", [
              { sku: "SKU-001", stock: 10 },
            ]);
          },
        },
      });

      expect(result.failed).toBe(0);
    } finally {
      await host.close();
    }
  });
});
```

### How Verification Works

For each interaction in the pact:

1. **State setup** — runs the state handler functions to prepare preconditions (seed data, configure state)
2. **Execute** — for actions, calls `host.executeAction()` with the example args. For events, the state setup triggers actions that emit events.
3. **Validate** — checks the real result/payload against the contract's validator using `.parse()`
4. **Report** — records pass or fail with a structured failure reason

### The `PactVerificationHost` Interface

`verifyPact` accepts any object that implements:

```ts
interface PactVerificationHost {
  executeAction(name: string, args?: unknown[] | unknown): Promise<{
    emittedEvents: Array<{ name: string; payload: unknown }>;
    result: unknown;
  }>;
  close?(): void;
}
```

This matches `ChimpbaseHost` from any runtime (Bun, Node, Deno), so verification is not coupled to a specific platform.

### Verification Result

```ts
const result = await verifyPact({ host, pact: myPact, states });

result.consumer;   // "order-service"
result.provider;   // "inventory-service"
result.passed;     // number of passed interactions
result.failed;     // number of failed interactions
result.total;      // total interactions
result.results;    // per-interaction results
```

Each interaction result is either `{ status: "passed" }` or `{ status: "failed", failure }` where `failure` describes what went wrong:

| Failure Kind | Description |
|---|---|
| `missing_state` | No state handler provided for a required state |
| `action_threw` | The action handler threw an error |
| `result_mismatch` | The action result did not match the contract's validator |
| `event_not_emitted` | The expected event was not emitted during state setup |
| `event_payload_mismatch` | The emitted event payload did not match the contract's validator |
| `worker_payload_mismatch` | The worker payload example did not match the contract's validator |
| `no_example` | No example args provided for an action interaction |

### Lifecycle Hooks

Track verification progress with optional callbacks:

```ts
await verifyPact({
  host,
  pact: myPact,
  states,
  onInteractionStart(interaction) {
    console.log(`verifying ${interaction.kind}: ${interaction.kind === "action" ? interaction.name : interaction.kind === "event" ? interaction.eventName : interaction.queueName}`);
  },
  onInteractionPass(interaction) {
    console.log("  passed");
  },
  onInteractionFail(interaction, failure) {
    console.log(`  FAILED: ${failure.kind}`);
  },
});
```

## Sharing Pacts

Pacts can be serialized to JSON for sharing between repositories or services:

```ts
import { serializePactToJson, deserializePactJson } from "@chimpbase/pact";

// Serialize to JSON string
const json = serializePactToJson(myPact);
await Bun.write("pacts/inventory-service.pact.json", json);

// Deserialize (for inspection — not for verification)
const serialized = deserializePactJson(json);
```

The serialized format stores validator schemas as JSON Schema objects. Serialized pacts are for sharing and documentation — provider verification requires the original pact with live validators.

### Serialized Format

```json
{
  "consumer": "order-service",
  "provider": "inventory-service",
  "interactions": [
    {
      "kind": "action",
      "name": "reserveStock",
      "states": ["product SKU-001 has 10 units in stock"],
      "argsSchema": {
        "type": "object",
        "properties": { "sku": { "type": "string" }, "quantity": { "type": "number" } },
        "required": ["sku", "quantity"]
      },
      "resultSchema": {
        "type": "object",
        "properties": { "reservationId": { "type": "string" } },
        "required": ["reservationId"]
      },
      "example": {
        "args": [{ "sku": "SKU-001", "quantity": 2 }],
        "result": { "reservationId": "res-abc" }
      }
    }
  ]
}
```

## Full Example

A complete two-service contract test scenario.

### Consumer Side (Order Service)

Define what the order service expects from the inventory service:

```ts
// order-service/pacts/inventory.pact.ts
import { pact, interaction } from "@chimpbase/pact";
import { v } from "@chimpbase/runtime";

export const inventoryPact = pact({
  consumer: "order-service",
  provider: "inventory-service",
  interactions: [
    interaction.action("reserveStock", {
      states: ["product SKU-001 has 10 units in stock"],
      args: v.object({ sku: v.string(), quantity: v.number() }),
      result: v.object({ reservationId: v.string(), expiresAt: v.string() }),
      example: {
        args: [{ sku: "SKU-001", quantity: 2 }],
      },
    }),

    interaction.event("stock.reserved", {
      states: ["a reservation was just created"],
      payload: v.object({
        reservationId: v.string(),
        sku: v.string(),
        quantity: v.number(),
      }),
    }),
  ],
});
```

### Provider Side (Inventory Service)

Verify the real inventory service satisfies the contract:

```ts
// inventory-service/tests/pact.verify.test.ts
import { describe, expect, test } from "bun:test";
import { verifyPact } from "@chimpbase/pact";
import { createChimpbase } from "@chimpbase/bun";
import app from "../chimpbase.app";
import { inventoryPact } from "../../order-service/pacts/inventory.pact";

describe("inventory service pact verification", () => {
  test("satisfies order-service contract", async () => {
    const host = await createChimpbase({
      app,
      storage: { engine: "memory" },
    });

    try {
      const result = await verifyPact({
        host,
        pact: inventoryPact,
        states: {
          "product SKU-001 has 10 units in stock": async (host) => {
            await host.executeAction("seedProduct", [
              { sku: "SKU-001", stock: 10 },
            ]);
          },
          "a reservation was just created": async (host) => {
            await host.executeAction("reserveStock", [
              { sku: "SKU-001", quantity: 1 },
            ]);
          },
        },
      });

      expect(result.failed).toBe(0);
      expect(result.passed).toBe(2);
    } finally {
      await host.close();
    }
  });
});
```

## API Reference

### `pact(input)`

Creates a pact contract.

| Field | Type | Description |
|---|---|---|
| `consumer` | `string` | Name of the consuming service |
| `provider` | `string` | Name of the providing service |
| `interactions` | `ChimpbasePactInteraction[]` | List of interactions to verify |

### `interaction.action(name, options?)`

Defines an action interaction.

| Option | Type | Description |
|---|---|---|
| `states` | `string[]` | Preconditions required for this interaction |
| `args` | `ChimpbaseValidator` | Expected args shape |
| `result` | `ChimpbaseValidator` | Expected result shape |
| `example` | `{ args?, result? }` | Example data used during verification |

### `interaction.event(eventName, options?)`

Defines an event interaction.

| Option | Type | Description |
|---|---|---|
| `states` | `string[]` | Preconditions that trigger the event |
| `payload` | `ChimpbaseValidator` | Expected event payload shape |
| `example` | `unknown` | Example payload for documentation |

### `interaction.worker(queueName, options?)`

Defines a worker/queue interaction.

| Option | Type | Description |
|---|---|---|
| `states` | `string[]` | Preconditions for the queue job |
| `payload` | `ChimpbaseValidator` | Expected job payload shape |
| `example` | `unknown` | Example payload for documentation and validation |

### `verifyPact(options)`

Runs provider verification.

| Option | Type | Description |
|---|---|---|
| `host` | `PactVerificationHost` | A booted Chimpbase host |
| `pact` | `ChimpbasePact` | The pact to verify |
| `states` | `Record<string, (host) => void>` | State setup functions |
| `onInteractionStart` | `(interaction) => void` | Called before each interaction |
| `onInteractionPass` | `(interaction) => void` | Called when an interaction passes |
| `onInteractionFail` | `(interaction, failure) => void` | Called when an interaction fails |

### `serializePact(pact)`

Serializes a pact to a plain object with JSON Schema representations.

### `serializePactToJson(pact)`

Serializes a pact to a JSON string.

### `deserializePactJson(json)`

Parses a serialized pact JSON string back to a `SerializedPact` object.
