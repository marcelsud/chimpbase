import { Hono } from "hono";
import type { ChimpbaseAppDefinitionInput } from "@chimpbase/bun";
import { action, v, type ChimpbaseRouteEnv } from "@chimpbase/runtime";
const createTodo = action({ args: v.object({ title: v.string() }), async handler(_ctx, input) { return { id: 1, title: input.title }; } });
const app = new Hono<{ Bindings: ChimpbaseRouteEnv }>();
app.post("/todos", async (c) => { const body = await c.req.json<{ title: string }>(); const todo = await c.env.action(createTodo, body); return c.json(todo, 201); });
app.get("/todos", async (c) => { const todos = await c.env.action("createTodo", { title: "from-string" }); return c.json(todos); });
const appDef = { project: { name: "todo-app" }, httpHandler: app.fetch, registrations: [createTodo] } satisfies ChimpbaseAppDefinitionInput;
console.log("hono: OK (app created:", appDef != null, ")");
