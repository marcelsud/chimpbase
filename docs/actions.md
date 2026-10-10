# Actions

Actions are the core unit of business logic in Chimpbase. They can be called from HTTP routes, other actions, subscriptions, workers, workflows, and the CLI.

## Defining Actions

### Simple (tuple args)

```ts
import { action } from "chimpbase";

const greetUser = action("greetUser", async (ctx, name: string) => {
  ctx.log.info("greeting user", { name });
  return { message: `Hello, ${name}!` };
});
```

### With validation (object args)

```ts
import { action, v } from "chimpbase";

const createTodo = action({
  name: "createTodo",
  args: v.object({
    title: v.string(),
    projectSlug: v.string(),
    priority: v.optional(v.string()),
    assigneeEmail: v.optional(v.union(v.string(), v.null())),
  }),
  async handler(ctx, input) {
    const [todo] = await ctx.db.query(
      "INSERT INTO todos (title, project_slug, priority, assignee_email) VALUES (?1, ?2, ?3, ?4) RETURNING *",
      [input.title, input.projectSlug, input.priority ?? "medium", input.assigneeEmail ?? null],
    );

    ctx.pubsub.publish("todo.created", todo);
    return todo;
  },
});
```

## Handler Signature

Every action handler receives a `ChimpbaseContext` as its first argument:

```ts
(ctx: ChimpbaseContext, ...args) => TResult | Promise<TResult>
```

The context provides database access, collections, event publication and job queues. See [Context](/context) for the API reference.

## Calling Actions

### From another action

```ts
const getDashboard = action("getDashboard", async (ctx) => {
  const todos = await ctx.action("listTodos", { status: "backlog" });
  return { backlog: todos.length };
});
```

### From an HTTP route

```ts
import { route } from "chimpbase/runtime";

const todosRoute = route("todos", async (request, env) => {
  if (new URL(request.url).pathname !== "/todos" || request.method !== "POST") return null;
  const todo = await env.action(createTodo, await request.json());
  return Response.json(todo, { status: 201 });
});
```

### Using action references

When you store an action in a variable, you can pass the reference directly instead of using a string name:

```ts
const result = await ctx.action(createTodo, { title: "Ship it", projectSlug: "core" });
```

For per-action persistence and export options, see [Telemetry](/advanced/telemetry).

## Validators

The `v` namespace provides runtime input validation:

```ts chimpbase-check:validators
import { v } from "chimpbase";

v.string()                          // string
v.number()                          // number
v.boolean()                         // boolean
v.null()                            // null
v.unknown()                         // unknown
v.optional(v.string())              // string | undefined
v.string().nullable()               // string | null
v.array(v.string())                 // string[]
v.union(v.string(), v.null())       // string | null
v.enum(["low", "medium", "high"])   // "low" | "medium" | "high"
v.literal("active")                 // "active"
v.object({ name: v.string() })      // { name: string }
```

Invalid input throws a validation error before the handler runs.

## Registration

Actions are registered in the `registrations` array of your app definition:

```ts
export default {
  project: { name: "my-app" },
  registrations: [
    createTodo,
    listTodos,
    getDashboard,
  ],
} satisfies ChimpbaseAppDefinitionInput;
```
