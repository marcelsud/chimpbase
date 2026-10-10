import type { ChimpbaseRouteEnv } from "@chimpbase/runtime";
import { Hono } from "hono";

import {
  assignOrder,
  completeOrder,
  createOrder,
  listOrders,
  rejectOrder,
  startOrder,
} from "./order.actions.ts";
import {
  listOrderEvents,
  listOrderNotifications,
} from "./order.audit.actions.ts";
import { listOrderBacklogSnapshots } from "./order.cron.ts";

const app = new Hono<{ Bindings: ChimpbaseRouteEnv }>();

app.get("/orders", async (context) => {
  const orders = await context.env.action(listOrders);
  return context.json(orders);
});

app.post("/orders", async (context) => {
  const body = await context.req.json<{ customer: string; amount: number }>();
  const order = await context.env.action(createOrder, body);
  return context.json(order, 201);
});

app.post("/orders/:id/assign", async (context) => {
  const id = Number(context.req.param("id"));
  const { assignee } = await context.req.json<{ assignee: string }>();
  const order = await context.env.action(assignOrder, { id, assignee });
  return context.json(order);
});

app.post("/orders/:id/start", async (context) => {
  const id = Number(context.req.param("id"));
  const order = await context.env.action(startOrder, { id });
  return context.json(order);
});

app.post("/orders/:id/complete", async (context) => {
  const id = Number(context.req.param("id"));
  const order = await context.env.action(completeOrder, { id });
  return context.json(order);
});

app.post("/orders/:id/reject", async (context) => {
  const id = Number(context.req.param("id"));
  const { reason } = await context.req.json<{ reason: string }>();
  const order = await context.env.action(rejectOrder, { id, reason });
  return context.json(order);
});

app.get("/orders/:id/events", async (context) => {
  const orderId = Number(context.req.param("id"));
  const events = await context.env.action(listOrderEvents, { orderId });
  return context.json(events);
});

app.get("/notifications", async (context) => {
  const notifications = await context.env.action(listOrderNotifications);
  return context.json(notifications);
});

app.get("/backlog/snapshots", async (context) => {
  const snapshots = await context.env.action(listOrderBacklogSnapshots);
  return context.json(snapshots);
});

export const orderApiApp = app;
