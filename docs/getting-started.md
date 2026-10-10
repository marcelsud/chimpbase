# Getting Started

Build a small notes API with Bun. It uses SQLite locally, so you do not need a database server or HTTP framework.

## Install

In a new project directory, install the single public package:

```bash
bun add chimpbase
```

## Create the app

Save this as `chimpbase.app.ts`:

```ts chimpbase-check:getting-started
import type { ChimpbaseAppDefinitionInput } from "chimpbase/core";
import { action, route, v } from "chimpbase/runtime";

const createNote = action({
  name: "createNote",
  args: v.object({ body: v.string() }),
  async handler(ctx, input) {
    const id = await ctx.collection.insert("notes", input);
    return { id, ...input };
  },
});

const listNotes = action({
  name: "listNotes",
  async handler(ctx) {
    return await ctx.collection.find("notes");
  },
});

const notes = route("GET", "/notes", async (_request, env) => {
  return Response.json(await env.action(listNotes));
});

const addNote = route("POST", "/notes", async (request, env) => {
  const note = await env.action(createNote, await request.json());
  return Response.json(note, { status: 201 });
});

export default {
  project: { name: "my-app" },
  registrations: [createNote, listNotes, notes, addNote],
} satisfies ChimpbaseAppDefinitionInput;
```

Actions validate input and run inside a transaction. Routes call those actions over HTTP. Collections store JSON documents without application migrations.

## Run it

```bash
bunx chimpbase dev
```

The CLI loads `chimpbase.app.ts`, starts HTTP on port 3000 and runs the background worker. SQLite data is stored in `data/my-app.db`. The app file exports a definition; the CLI starts it.

For port, storage, and worker settings and defaults, see the [environment-variable reference](/configuration#environment-variables).

In another terminal:

```bash
curl http://localhost:3000/notes \
  -H 'Content-Type: application/json' \
  -d '{"body":"First note"}'

curl http://localhost:3000/notes
curl http://localhost:3000/health
```

The POST returns a note with a generated `id`; GET returns the stored notes. `/health` returns `{"ok":true}`.

You can also call an action from the CLI:

```bash
bunx chimpbase dev --action createNote --args '{"body":"From the CLI"}'
```

## Next steps

- [Actions](/actions), [HTTP routes](/routes) and [collections](/collections) explain the API above.
- [Workers and queues](/workers) add background jobs; [cron](/cron) adds schedules.
- [Configuration](/configuration) covers PostgreSQL, Node, Deno and CLI options.

When you need workflows, framework integrations, plugins or deployment recipes, use the separate [advanced guides](/advanced/).
