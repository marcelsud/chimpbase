# Plugins

Plugins are composable groups of registrations (actions, routes, subscriptions, workers, crons, workflows). They provide a way to package and distribute reusable functionality.

## Creating a Plugin

```ts
import { action, plugin, route } from "chimpbase/runtime";

export function myPlugin() {
  return plugin(
    { name: "my-plugin" },
    action("myPlugin.doSomething", async (ctx) => {
      return { ok: true };
    }),
    route("myPlugin.api", async (request, env) => {
      if (new URL(request.url).pathname !== "/my-plugin") return null;
      return Response.json(await env.action("myPlugin.doSomething"));
    }),
  );
}
```

## Using Plugins

Register plugins in your app's `registrations` array:

```ts
import { myPlugin } from "./my-plugin";

export default {
  registrations: [
    myPlugin(),
    // ... other registrations
  ],
};
```

## Lifecycle Hooks

Plugins can run code on startup and shutdown:

```ts
import { onStart, onStop, plugin, action } from "chimpbase/runtime";

function myPlugin() {
  return plugin(
    { name: "my-plugin" },
    onStart("my-plugin.init", async (ctx) => {
      ctx.log.info("plugin starting");
      await ctx.kv.set("my-plugin.version", "1.0");
    }),
    onStop("my-plugin.cleanup", async () => {
      console.log("plugin shutting down");
    }),
    action("myAction", async (ctx) => { /* ... */ }),
  );
}
```

### `onStart(name, handler)`

Runs after the engine is initialized, before serving requests. The handler receives a full `ChimpbaseContext` with access to `db`, `kv`, `collection`, etc.

Use cases: seed data, validate config, warm caches, run health checks.

### `onStop(name, handler)`

Runs during graceful shutdown, before the server and worker stop.

Use cases: flush buffers, close connections, log final metrics.

Hooks run in registration order. If an `onStop` hook throws, the error is logged but shutdown continues.

## Plugin Dependencies

Plugins can declare dependencies on other plugins to ensure ordering:

```ts
plugin(
  {
    name: "webhooks",
    dependsOn: [{ name: "auth" }],
  },
  ...entries,
);
```

## Official Plugins

| Plugin | Package | Description |
|--------|---------|-------------|
| [Auth](/advanced/auth) | `chimpbase/auth` | API key authentication, user management, scopes, rate limiting |
| [Webhooks](/advanced/webhooks) | `chimpbase/webhooks` | Outbound + inbound webhooks with HMAC and dedup |
| [REST Collections](/advanced/rest-collections) | `chimpbase/rest-collections` | Expose collections as REST APIs |
| [Mesh](/advanced/mesh) | `chimpbase/mesh` | Services and distributed registry across multiple nodes |

## Middleware in Plugins

Plugins often register middleware — routes that set [request context](/routes#request-context) or short-circuit before downstream handlers run. Use `middleware()` instead of `route()` to signal this intent:

```ts
import { middleware, plugin, action } from "chimpbase/runtime";

function corsPlugin() {
  return plugin(
    { name: "cors" },
    middleware("cors.preflight", async (request, env) => {
      if (request.method === "OPTIONS") {
        return new Response(null, {
          headers: {
            "access-control-allow-origin": "*",
            "access-control-allow-methods": "GET, POST, PATCH, DELETE",
            "access-control-allow-headers": "content-type, x-api-key, authorization",
          },
        });
      }
      return null; // pass through
    }),
  );
}
```

Middleware runs in registration order. A plugin registered first in the `registrations` array runs before later plugins. This is how the auth plugin's guard route protects all other routes.

Middleware can also pass data to downstream handlers via request context:

```ts
middleware("requestLogger", async (request, env) => {
  env.set("request.startedAt", Date.now());
  return null;
});
```

See the [Routes](/routes#request-context) page for full request context documentation.

## Plugin Pattern

The standard plugin pattern used by all official plugins:

1. Export a function that accepts options and returns `ChimpbasePluginRegistration`
2. Build an array of registrations (actions, routes, subscriptions, workers, middleware, lifecycle hooks)
3. Wrap with `plugin()` to group and name them
4. Use internal collection/KV namespaces prefixed with `__chimpbase.{pluginName}.*`
5. Use internal action names prefixed with `__chimpbase.{pluginName}.*`

```ts
export function chimpbaseExample(options: ExampleOptions): ChimpbasePluginRegistration {
  const entries: ChimpbaseRegistrationSource[] = [];

  // Add actions, routes, workers, etc.
  entries.push(action("__chimpbase.example.doStuff", handler));

  return plugin({ name: "chimpbase-example" }, ...entries);
}
```
