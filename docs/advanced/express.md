# Express

Keep Express as your HTTP server and call Chimpbase actions from its routes. Chimpbase runs background work in the same process without opening a second HTTP port.

## Setup

```bash
npm install express chimpbase
```

## Express server

Set `DATABASE_URL` to your PostgreSQL connection string. This example uses a Chimpbase collection, so it needs no application SQL migration:

```ts
import express from "express";
import { createChimpbase } from "chimpbase/runtime/node";
import { action, v } from "chimpbase";

const app = express();
app.use(express.json());

const createTodo = action({
  name: "createTodo",
  args: v.object({ title: v.string() }),
  async handler(ctx, input) {
    const id = await ctx.collection.insert("todos", { title: input.title });
    return { id, title: input.title };
  },
});

const chimpbase = await createChimpbase({
  registrations: [createTodo],
  storage: { engine: "postgres", url: process.env.DATABASE_URL },
});
await chimpbase.start({ serve: false, runWorker: true });

app.post("/todos", async (req, res) => {
  try {
    const outcome = await chimpbase.executeAction(createTodo, req.body);
    res.status(201).json(outcome.result);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Request failed" });
  }
});

app.listen(3000, () => {
  console.log("listening on :3000");
});
```

`executeAction()` runs a registered action locally and returns `{ result, emittedEvents }`. Return `outcome.result` to your HTTP client. Add [workers](/workers) to `registrations` when you need background jobs.

## When to use Express

If you have an existing Express application and want to add Chimpbase's background jobs, cron, and workflow capabilities without rewriting your HTTP layer, this approach lets you adopt Chimpbase incrementally.

For new projects, consider [Hono](/advanced/hono) instead — it uses the same Web standard API as Chimpbase with no adapter needed.
