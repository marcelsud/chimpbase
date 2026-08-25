import type { ChimpbaseAppDefinitionInput } from "@chimpbase/bun";
import { action, route, v } from "@chimpbase/runtime";

import migrations from "./chimpbase.migrations.ts";

type OrderRow = {
  id: number;
  customer: string;
  amount: number;
  created_at: string;
};
const orderRowValidator = v.object({
  amount: v.number(),
  created_at: v.string(),
  customer: v.string(),
  id: v.number(),
});


const createOrder = action({
  name: "createOrder",
  args: v.object({
    customer: v.string(),
    amount: v.number(),
  }),
  async handler(ctx, input) {
    const [row] = await ctx.db.query(
      "INSERT INTO orders (customer, amount) VALUES (?1, ?2) RETURNING id, customer, amount, created_at",
      [input.customer, input.amount],
      orderRowValidator,
    );
    return row;
  },
});

const listOrders = action({
  name: "listOrders",
  async handler(ctx) {
    return await ctx.db.query(
      "SELECT id, customer, amount, created_at FROM orders ORDER BY id",
      undefined,
      orderRowValidator,
    );
  },
});

const ordersRoute = route("orders", async (request, env) => {
  const url = new URL(request.url);

  if (url.pathname !== "/orders") return null;

  if (request.method === "POST") {
    const body = (await request.json()) as { customer: string; amount: number };
    const order = await env.action("createOrder", body);
    return Response.json(order, { status: 201 });
  }

  if (request.method === "GET") {
    const orders = await env.action("listOrders", {});
    return Response.json(orders);
  }

  return null;
});

export default {
  migrations,
  project: { name: "bun-basic" },
  registrations: [createOrder, listOrders, ordersRoute],
} satisfies ChimpbaseAppDefinitionInput;
