# HTTP Routes

Use `route()` to handle HTTP requests with the standard `Request` and `Response` APIs. Call actions for database writes and other business logic.

## Define a Route

```ts
import { route } from "chimpbase/runtime";

const notesRoute = route("notes", async (request, env) => {
  if (new URL(request.url).pathname !== "/notes") return null;

  if (request.method === "GET") {
    return Response.json(await env.action("listNotes"));
  }

  if (request.method === "POST") {
    const note = await env.action("createNote", await request.json());
    return Response.json(note, { status: 201 });
  }

  return new Response("Method not allowed", { status: 405 });
});
```

Add `notesRoute` alongside its actions in your app's `registrations` array. See [Getting Started](/getting-started) for a complete app.

## Route Environment

The route's second argument is a `ChimpbaseRouteEnv`:

```ts
interface ChimpbaseRouteEnv {
  action(name: string, ...args: unknown[]): Promise<unknown>;
  action(reference: ActionRegistration, ...args): Promise<Result>;
  get<T = unknown>(key: string): T | undefined;
  set(key: string, value: unknown): void;
}
```

Use `env.action(name, ...args)` or pass an action reference. Routes do not have direct access to `db`, `kv`, or `collection`; those belong to the action context.

### Request Context

`get` and `set` allow routes and middleware to pass data to downstream handlers within the same request. Context is per-request — each `executeRoute()` call gets a fresh map.

```ts
// Middleware sets context
middleware("requestId", async (request, env) => {
  env.set("requestId", crypto.randomUUID());
  return null; // pass through
});

// A later route reads it
route("requestInfo", async (request, env) => {
  if (new URL(request.url).pathname !== "/request-info") return null;
  return Response.json({ requestId: env.get<string>("requestId") });
});
```

For authentication middleware and its context values, see [Authentication](/advanced/auth).

## Middleware

The `middleware()` function is an alias for `route()` that signals intent — a handler that sets context or short-circuits, then returns `null` to pass through:

```ts
import { middleware } from "chimpbase/runtime";

const cors = middleware("cors", async (request, env) => {
  if (request.method === "OPTIONS") {
    return new Response(null, {
      headers: {
        "access-control-allow-origin": "*",
        "access-control-allow-methods": "GET, POST, PATCH, DELETE",
        "access-control-allow-headers": "content-type, x-api-key, authorization",
      },
    });
  }
  return null;
});
```

Middleware and routes run in registration order. Put middleware before the routes it should affect.

## Handler Signature

```ts
(request: Request, env: ChimpbaseRouteEnv) => Response | null | Promise<Response | null>
```

- Return a `Response` to handle the request
- Return `null` to pass the request to the next route handler

Routes are tried in registration order. The first non-null response wins.

## Route Execution Order

1. Registered `route()` handlers run in order
2. If no route matches, an optional `httpHandler` runs
3. If nothing matches, the server returns `404`

To use a framework as the `httpHandler`, see [Hono](/advanced/hono) or the other [advanced integrations](/advanced/).

## Built-in Endpoints

The framework provides a `/health` endpoint automatically:

```
GET /health → { "ok": true }
```

This runs before any registered routes and cannot be overridden.
