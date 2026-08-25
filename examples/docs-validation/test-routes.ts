import { createChimpbase } from "@chimpbase/bun";
import { action, route, v } from "@chimpbase/runtime";
const chimpbase = await createChimpbase({ storage: { engine: "memory" }, server: { port: 0 } });
const createTodo = action({ name: "createTodo", args: v.object({ title: v.string() }), async handler(_ctx, input) { return { id: 1, title: input.title }; } });
const healthCheck = route("health", async () => new Response("ok", { status: 200 }));
const api = route("api", async (request, env) => { if (request.method === "POST" && new URL(request.url).pathname === "/todos") { const body = await request.json() as { title: string }; const todo = await env.action(createTodo, body); return new Response(JSON.stringify(todo), { status: 201, headers: { "Content-Type": "application/json" } }); } return null; });
chimpbase.register({ createTodo, healthCheck, api }); await chimpbase.start();
console.log("routes: OK"); await chimpbase.close(); process.exit(0);
