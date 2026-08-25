# Next.js

Chimpbase can run alongside Next.js to handle background jobs, cron, workflows, and durable queues while Next.js handles your frontend and API routes.

## Architecture

Next.js and Chimpbase run as separate processes sharing the same PostgreSQL database:

```
┌─────────────┐     ┌─────────────┐
│   Next.js   │     │  Chimpbase  │
│  (frontend  │     │  (workers,  │
│  + API)     │     │  cron, etc) │
└──────┬──────┘     └──────┬──────┘
       │                   │
       └───────┬───────────┘
               │
        ┌──────┴──────┐
        │  PostgreSQL  │
        └─────────────┘
```

## Setup

```bash
npm install @chimpbase/node
```

## Enqueuing jobs from API routes

Use a shared Chimpbase client to enqueue work from Next.js API routes or server actions:

```ts
// lib/chimpbase.ts
import { createChimpbase } from "@chimpbase/node";

export const chimpbase = await createChimpbase({
  storage: { engine: "postgres", url: process.env.DATABASE_URL },
});
```

```ts
// app/api/todos/route.ts
import { chimpbase } from "@/lib/chimpbase";

export async function POST(request: Request) {
  const body = await request.json();

  const result = await chimpbase.run("createTodo", body);

  return Response.json(result, { status: 201 });
}
```

## Worker process

Run Chimpbase as a separate process that handles background work:

```ts
// chimpbase.app.ts
import type { ChimpbaseAppDefinitionInput } from "@chimpbase/node";
import { action, worker, cron, subscription, v } from "@chimpbase/runtime";

const createTodo = action({
  args: v.object({ title: v.string() }),
  async handler(ctx, input) {
    const [todo] = await ctx.db.query(
      "insert into todos (title) values (?1) returning id",
      [input.title],
      v.object({ id: v.number() }),
    );
    ctx.pubsub.publish("todo.created", { todoId: todo.id });
    return todo;
  },
});

export default {
  project: { name: "my-nextjs-app" },
  registrations: [
    createTodo,
    subscription("todo.created", async (ctx, event) => {
      await ctx.enqueue("todo.index", event);
    }, { idempotent: true, name: "enqueueTodoIndex" }),
    worker("todo.index", async (ctx, payload) => {
      // Update search index, send notifications, etc.
    }),
    cron("reports.daily", "0 9 * * *", async (ctx) => {
      // Generate daily report
    }),
  ],
} satisfies ChimpbaseAppDefinitionInput;
```

## Docker Compose

Run both processes together:

```yaml
services:
  postgres:
    image: postgres:17
    environment:
      POSTGRES_DB: myapp
      POSTGRES_USER: myapp
      POSTGRES_PASSWORD: secret
    volumes:
      - pgdata:/var/lib/postgresql/data

  nextjs:
    build:
      context: .
      dockerfile: Dockerfile.nextjs
    environment:
      DATABASE_URL: postgres://myapp:secret@postgres:5432/myapp
    ports:
      - "3000:3000"
    depends_on:
      - postgres

  chimpbase:
    build:
      context: .
      dockerfile: Dockerfile.chimpbase
    environment:
      DATABASE_URL: postgres://myapp:secret@postgres:5432/myapp
    depends_on:
      - postgres

volumes:
  pgdata:
```

## When to use this pattern

This works well when you already have a Next.js frontend and need durable background processing without adding a separate message broker or workflow engine. Chimpbase handles the async work while Next.js stays focused on serving pages and API responses.
