# Next.js

Next.js serves your frontend and API routes. A local Chimpbase host executes registered actions; a separate worker process handles queued jobs through the same PostgreSQL database.

## Setup

```bash
npm install chimpbase
```

Set `DATABASE_URL` in both processes. Sharing the database shares durable state and queues; it does not send an action call to the worker process.

## Shared app definition

Keep registrations in a module that both processes can load:

```ts
// chimpbase.app.ts
import type { ChimpbaseAppDefinitionInput } from "chimpbase/runtime/node";
import { action, worker, v } from "chimpbase";

const createTodo = action({
  name: "createTodo",
  args: v.object({ title: v.string() }),
  async handler(ctx, input) {
    const id = await ctx.collection.insert("todos", { title: input.title });
    await ctx.enqueue("todo.created", { id, title: input.title });
    return { id, title: input.title };
  },
});

export default {
  project: { name: "my-nextjs-app" },
  registrations: [
    createTodo,
    worker("todo.created", async (ctx, payload: { id: string; title: string }) => {
      await ctx.stream.append("todo.activity", "todo.created", payload);
    }),
  ],
} satisfies ChimpbaseAppDefinitionInput;
```

## API route

Create one host per Next.js process and load that app definition. Keep it on the Node runtime; do not call `start()` in an API route:

```ts
// lib/chimpbase.ts
import { createChimpbase } from "chimpbase/runtime/node";
import app from "../chimpbase.app";

export const chimpbase = createChimpbase({
  ...app,
  storage: { engine: "postgres", url: process.env.DATABASE_URL },
});
```

```ts
// app/api/todos/route.ts
import { chimpbase } from "@/lib/chimpbase";

export const runtime = "nodejs";

export async function POST(request: Request) {
  const host = await chimpbase;
  const body = await request.json();
  const outcome = await host.executeAction("createTodo", body);
  return Response.json(outcome.result, { status: 201 });
}
```

## Worker process

Start the same app with background processing enabled and HTTP disabled:

```ts
// worker.ts
import { createChimpbase } from "chimpbase/runtime/node";
import app from "./chimpbase.app";

const chimpbase = await createChimpbase({
  ...app,
  storage: { engine: "postgres", url: process.env.DATABASE_URL },
});
await chimpbase.start({ serve: false, runWorker: true });
```

Run `worker.ts` as a persistent Node process using your TypeScript runner or compiled output. The worker polls the shared queue; the API host only executes requests. See [Workers & Queues](/workers) for retries and [Deployment](/advanced/deployment) for containers.
