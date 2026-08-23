import { createChimpbase } from "@chimpbase/bun";
import { action, subscription, worker, v } from "@chimpbase/runtime";
import { Hono } from "hono";
import type { ChimpbaseRouteEnv } from "@chimpbase/runtime";

// ── Actions ─────────────────────────────────────────────────────────────

const createCustomer = action({
  name: "createCustomer",
  args: v.object({
    email: v.string(),
    name: v.string(),
  }),
  async handler(ctx, input) {
    const [customer] = await ctx.db.query<{ id: number }>(
      "INSERT INTO customers (email, name) VALUES (?1, ?2) RETURNING id",
      [input.email, input.name],
    );

    ctx.pubsub.publish("customer.created", {
      customerId: customer.id,
      email: input.email,
    });

    return customer;
  },
});

// ── Subscriptions ───────────────────────────────────────────────────────

const onCustomerCreated = subscription(
  "customer.created",
  async (ctx, payload) => {
    await ctx.enqueue("customer.welcome", payload);
  },
  { idempotent: true, name: "enqueueWelcome" },
);

// ── Workers ─────────────────────────────────────────────────────────────

let welcomeSent = false;
const sendWelcome = worker("customer.welcome", async (ctx, payload: { email: string }) => {
  ctx.log.info("sending welcome email", { email: payload.email });
  welcomeSent = true;
});

// ── HTTP Routes ─────────────────────────────────────────────────────────

const api = new Hono<{ Bindings: ChimpbaseRouteEnv }>();

api.post("/customers", async (c) => {
  const body = await c.req.json<{ email: string; name: string; plan: string }>();
  const customer = await c.env.action(createCustomer, body);
  return c.json(customer, 201);
});

// ── Boot ────────────────────────────────────────────────────────────────

const chimpbase = await createChimpbase({
  project: { name: "getting-started" },
  httpHandler: api,
  storage: { engine: "memory" },
  server: { port: 0 },
  migrationsSql: [
    "CREATE TABLE customers (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT NOT NULL, name TEXT NOT NULL)",
  ],
});

chimpbase.register({
  createCustomer,
  onCustomerCreated,
  sendWelcome,
});

await chimpbase.start();

// ── Test: POST /customers ───────────────────────────────────────────────

const r1 = await chimpbase.executeRoute(
  new Request("http://test.local/customers", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ email: "alice@example.com", name: "Alice" }),
  }),
);

if (r1.response?.status !== 201) throw new Error("POST /customers should return 201, got " + r1.response?.status);
const customer = await r1.response!.json() as { id: number };
if (typeof customer.id !== "number") throw new Error("customer should have numeric id");
console.log("getting-started (create customer):", JSON.stringify(customer));

// ── Test: subscription enqueued a worker job ────────────────────────────

if (r1.emittedEvents.length === 0) throw new Error("should have emitted customer.created event");
console.log("getting-started (events emitted):", r1.emittedEvents.length);

// ── Test: worker processes the job ──────────────────────────────────────

await chimpbase.processNextQueueJob();
if (!welcomeSent) throw new Error("welcome worker should have executed");
console.log("getting-started (worker executed): true");

console.log("getting-started-full: OK");
chimpbase.close();
process.exit(0);
