# Business modules

Chimpbase business modules provide architectural isolation inside one trusted process. They do not provide a security boundary for untrusted code, independent scaling, or failure isolation.

## Interface and implementation

Define a public interface without handlers:

```ts
import { defineChimpbaseModuleInterface } from "chimpbase/core";
import { v } from "chimpbase/runtime";

export const accounts = defineChimpbaseModuleInterface({
  name: "accounts",
  version: 1,
  calls: {
    get: {
      input: v.object({ id: v.string() }),
      output: v.object({ id: v.string() }).nullable(),
      errors: ["not_found"],
      guarantees: ["reads committed account state"],
    },
  },
  events: {
    created: { payload: v.object({ id: v.string() }), version: 1 },
  },
});
```

Bind every public call exactly once in a private implementation:

```ts
import { defineChimpbaseModuleImplementation } from "chimpbase/core";
import { accounts } from "./interface.ts";

export const accountsImplementation = defineChimpbaseModuleImplementation({
  interface: accounts,
  calls: {
    async get(ctx, input) {
      return await ctx.collection.findOne("accounts", { id: input.id });
    },
  },
  resources: { collections: ["accounts"] },
});
```

The composition root passes implementations through `modules`. Ordinary modules import only another module's `interface.ts` and list synchronous dependencies by stable module name. Plugins remain app-global infrastructure and are not module implementations.

The supported layout is:

```text
chimpbase.app.ts
src/modules/<module>/interface.ts
src/modules/<module>/implementation.ts
src/modules/<module>/<private files>.ts
```

The architecture check resolves TypeScript aliases, relative traversal, and re-exports. It rejects implementation imports, undeclared dependencies, imports from non-composition code outside the module layout, and complete synchronous dependency cycles.

## Communication

Use `ctx.call(contract, input)` when the caller needs an immediate result. Input and output validators run at the module seam. Nested synchronous calls share the runtime-managed transaction; a failure rolls back the outer operation.

Use `ctx.publish(contract, payload)` for a committed fact. A module can publish only an event owned by its interface. Raw topic publication and raw action-name calls cannot cross a module boundary. Event consumers use `defineChimpbaseModuleSubscription` with the publisher's versioned event contract.

Routes, actions, workers, subscriptions, crons, workflows, and lifecycle hooks receive the owning module identity through `ctx.module` or `env.module`. App-global legacy registrations remain callable from the application boundary but are not an implicit module-to-module bypass.

## State ownership

Collections, KV keys, streams, queues, workflow definitions, and workflow instance identifiers are prefixed with the module identity. Crafted names cannot escape that prefix. Module migrations are ordered by the synchronous dependency graph and receive owner-qualified identities.

PostgreSQL modules receive a stable `chimpbase_<module>` schema. Module Kysely queries are schema-scoped and reject compiled access to another schema. Raw SQL requires explicit references to the owning schema and rejects foreign or unqualified tables. These runtime guards cover all database access exposed through a module context; direct access to an injected pool is outside the module API and outside the trusted-process boundary.

SQLite and memory adapters enforce generic-storage namespaces, but do not provide PostgreSQL schemas or database privileges. Module raw SQL is therefore intentionally unavailable for owned tables on those adapters; use typed Kysely access or generic storage. Generated `database.d.ts` interfaces list only each module's declared tables and projections.

Framework tables remain framework-owned. Business modules use framework services such as queues and workflows through context clients rather than querying `_chimpbase_*` tables.

## Durable event delivery

Module events use the persisted event log and an outbox queue entry committed in the publisher transaction. PostgreSQL LISTEN/NOTIFY and polling buses are wake-up optimizations, not delivery sources.

Delivery is asynchronous and at least once:

- the queue lease prevents concurrent handling until its lease expires;
- the stable `<module>/<subscription>` identity and event ID form a durable inbox marker;
- consumer-owned state and its inbox marker commit in one transaction;
- progress is recorded only after the handler succeeds;
- a crash before completion safely redelivers the event;
- retries use the app worker's `maxAttempts` and `retryDelayMs` settings;
- exhausted deliveries move to `__chimpbase.subscription.run.dlq` with the original payload and error;
- one poison delivery does not block later queue jobs or other subscriptions.

Ordering follows persisted queue order among available jobs. A delayed retry allows later events to run. Exactly-once external side effects are not promised; use the event ID as an idempotency key. Event records currently have indefinite retention, so cleanup cannot overtake the slowest durable consumer.

## Tooling

Generate deterministic artifacts:

```sh
chimpbase modules sync
```

Check source boundaries, compatibility, ownership, cycles, and stale artifacts in CI:

```sh
chimpbase modules check
```

The generated manifest is the shared input for runtime registration, dependency and event graphs, compatibility diagnostics, and Pact-style action/event interactions. A removed call, narrowed input, incompatible output, changed payload at the same event version, or removed event version is breaking. Additive calls and a new event version can coexist during migration.

See `examples/modular-monolith` for composition, synchronous calls, a private action, durable event publication, and an event-owned projection.
