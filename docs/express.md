# Express

Chimpbase's route handler uses the Web standard `Request`/`Response` API. Express uses its own `req`/`res` API, so you need a thin adapter to bridge them.

## Setup

```bash
npm install express @chimpbase/node
```

## Adapter

Convert between Express and Chimpbase's Web API handler:

```ts
import express from "express";
import { createChimpbase } from "@chimpbase/node";
import { action, v } from "@chimpbase/runtime";

const app = express();
app.use(express.json());

const chimpbase = await createChimpbase({
  storage: { engine: "postgres", url: process.env.DATABASE_URL },
});

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

chimpbase.register({ createTodo });
```

## Calling actions from routes

Use `chimpbase.run()` to execute actions from Express route handlers:

```ts
app.post("/todos", async (req, res) => {
  try {
    const result = await chimpbase.run("createTodo", req.body);
    res.status(201).json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(3000, () => {
  console.log("listening on :3000");
});

await chimpbase.start();
```

## When to use Express

If you have an existing Express application and want to add Chimpbase's background jobs, cron, and workflow capabilities without rewriting your HTTP layer, this approach lets you adopt Chimpbase incrementally.

For new projects, consider [Hono](/hono) instead — it uses the same Web standard API as Chimpbase with no adapter needed.
