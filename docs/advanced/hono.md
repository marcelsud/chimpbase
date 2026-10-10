# Hono

Hono is the recommended HTTP framework for Chimpbase. It uses the Web standard `Request`/`Response` API, which maps directly to Chimpbase's route handler interface.

## Setup

```bash
bun add hono
```

```ts
import { Hono } from "hono";
import { type ChimpbaseRouteEnv } from "chimpbase/runtime";

const app = new Hono<{ Bindings: ChimpbaseRouteEnv }>();
```

## Calling actions from routes

Use `c.env.action()` to invoke registered actions from your HTTP handlers:

```ts
import { listProjects, createProject } from "./modules/projects.ts";

// Type-safe with direct function reference
app.get("/projects", async (c) => {
  const projects = await c.env.action(listProjects);
  return c.json(projects);
});

app.post("/projects", async (c) => {
  const body = await c.req.json();
  const project = await c.env.action(createProject, body);
  return c.json(project, 201);
});
```

You can also call actions by string name:

```ts
app.get("/projects", async (c) => {
  const projects = await c.env.action("listProjects");
  return c.json(projects);
});
```

## Registering with Chimpbase

Pass the Hono app's `fetch` method as the `httpHandler`:

```ts
import type { ChimpbaseAppDefinitionInput } from "chimpbase/runtime/bun";

export default {
  project: { name: "my-app" },
  httpHandler: app.fetch,
  registrations: [createProject, listProjects],
} satisfies ChimpbaseAppDefinitionInput;
```

## Full example

```ts
import { Hono } from "hono";
import type { ChimpbaseAppDefinitionInput } from "chimpbase/runtime/bun";
import { action, v, type ChimpbaseRouteEnv } from "chimpbase/runtime";

const createTodo = action({
  args: v.object({ title: v.string() }),
  async handler(ctx, input) {
    const [todo] = await ctx.db.query(
      "insert into todos (title) values (?1) returning id",
      [input.title],
      v.object({ id: v.number() }),
    );
    return todo;
  },
});

const app = new Hono<{ Bindings: ChimpbaseRouteEnv }>();

app.post("/todos", async (c) => {
  const body = await c.req.json();
  const todo = await c.env.action(createTodo, body);
  return c.json(todo, 201);
});

export default {
  project: { name: "todo-app" },
  httpHandler: app.fetch,
  registrations: [createTodo],
} satisfies ChimpbaseAppDefinitionInput;
```
