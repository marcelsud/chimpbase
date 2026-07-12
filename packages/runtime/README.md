# @chimpbase/runtime

Public runtime DSL for Chimpbase applications.

This is the package application code uses to declare:

- `action(...)`
- `cron(...)`
- `subscription(...)`
- `worker(...)`
- `workflow(...)`

It also exports the public context and durable workflow contracts that hosts consume.

## Typical usage

```ts
import { action, cron, v, worker } from "@chimpbase/runtime";

const createCustomer = action({
  args: v.object({
    email: v.string(),
    name: v.string(),
  }),
  async handler(ctx, input) {
    await ctx.enqueue("customer.sync", input);
    return { ok: true };
  },
  name: "createCustomer",
});

const seedCustomers = action({
  args: v.array(
    v.object({
      email: v.string(),
      name: v.string(),
    }),
  ),
  async handler(_ctx, input) {
    return await Promise.all(input.map((customer) => createCustomer(customer)));
  },
  name: "seedCustomers",
});

chimpbase.register(
  createCustomer,
  seedCustomers,
  cron("customer.rollup", "0 * * * *", async (ctx, invocation) => {
    await ctx.collection.insert("customer_rollups", {
      capturedAt: invocation.fireAt,
      schedule: invocation.name,
    });
  }),
  worker("customer.sync", async (ctx, payload) => {
    ctx.log.info("syncing customer", payload);
  }),
);
```

Inside an active chimpbase runtime scope, action refs are directly callable, so one action can invoke another with `await createCustomer(input)`.

When an unnamed action is exported from `chimpbase.app.ts`, the loader infers a durable id from `module path + export name`, for example `chimpbase.app.ts#createCustomer`. That keeps workflow, CLI and persistence references stable without requiring a manual `name`.

Outside app module loading, `host.register({ createCustomer })` also infers the action id from the object key and assigns `createCustomer` as the action name. Only the bare `host.register(createCustomer)` form still requires an explicit `name`.

## Per-handler telemetry persistence

The `action()`, `worker()`, `subscription()` and `cron()` factories accept an optional `telemetry` override:

```ts
action("createCustomer", handler, { telemetry: { log: true, metric: true } });
worker("sync", handler, undefined, { telemetry: false });
```

This overrides the global `telemetry.persist` setting from `createChimpbase`. See `@chimpbase/bun` docs for the full configuration shape.

## Distribution model

`@chimpbase/runtime` is published as TypeScript source for the alpha release.

That keeps the public DSL close to the implementation and avoids introducing a build pipeline before the package surface settles.
