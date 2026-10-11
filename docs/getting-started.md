# Getting Started

Build a small notes API with Bun. It uses SQLite locally, so you do not need a database server or HTTP framework.

This walkthrough uses Bun 1.3 and `curl`. Install [Bun](https://bun.sh/docs/installation) before starting. For Node or Deno, follow the [runtime host instructions](/configuration#runtime-hosts) with the same app below.

## Install

Create a project directory and install the single public package:

```bash chimpbase-install:bun
mkdir my-app
cd my-app
bun add chimpbase
```

If the package is not available in your registry yet, [install from source](/configuration#install-from-source), then continue with the app below.

## Create the app

Save this as `chimpbase.app.ts` in `my-app`:

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

From the directory containing `chimpbase.app.ts`, run:

```bash chimpbase-run:bun
bunx chimpbase dev
```

Wait for `listening on http://127.0.0.1:3000` and keep this terminal open. The CLI loads `chimpbase.app.ts`, starts HTTP on port 3000 and runs the background worker. SQLite data is stored in `data/my-app.db`. The app file exports a definition; the CLI starts it.

For port, storage, and worker settings and defaults, see the [environment-variable reference](/configuration#environment-variables).

In another terminal, create your first note:

```bash chimpbase-request:create
curl --include http://localhost:3000/notes \
  -H 'Content-Type: application/json' \
  -d '{"body":"First note"}'
```

Expect `HTTP/1.1 201 Created` and a JSON body like this; your generated `id` will differ:

```json
{"id":"<generated id>","body":"First note"}
```

List the saved notes:

```bash chimpbase-request:list
curl http://localhost:3000/notes
```

The response is an array containing your note:

```json
[{"body":"First note","id":"<generated id>"}]
```

Check that the server is healthy:

```bash chimpbase-request:health
curl http://localhost:3000/health
```

Expect `{"ok":true}`. Press `Ctrl+C` in the server terminal when finished; SQLite keeps your notes for the next run.

You can also call an action from the CLI:

```bash chimpbase-action:bun
bunx chimpbase dev --action createNote --args '{"body":"From the CLI"}'
```

## Next steps

- [Actions](/actions), [HTTP routes](/routes) and [collections](/collections) explain the API above.
- [Workers and queues](/workers) add background jobs; [cron](/cron) adds schedules.
- [Configuration](/configuration) covers PostgreSQL, Node, Deno and CLI options.

When you need workflows, framework integrations, plugins or deployment recipes, use the separate [advanced guides](/advanced/).
