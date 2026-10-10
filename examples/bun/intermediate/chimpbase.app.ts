import type { ChimpbaseAppDefinitionInput } from "@chimpbase/bun";
import { cron, subscription, worker } from "@chimpbase/runtime";

import migrations from "./chimpbase.migrations.ts";
import { orderApiApp } from "../../shared/orders/order.http.ts";
import {
  assignOrder,
  completeOrder,
  createOrder,
  listOrders,
  rejectOrder,
  startOrder,
} from "../../shared/orders/order.actions.ts";
import {
  listOrderEvents,
  listOrderNotifications,
} from "../../shared/orders/order.audit.actions.ts";
import {
  captureOrderBacklogSnapshot,
  listOrderBacklogSnapshots,
} from "../../shared/orders/order.cron.ts";
import {
  auditOrderAssigned,
  auditOrderCompleted,
  auditOrderCreated,
  auditOrderRejected,
  auditOrderStarted,
  enqueueOrderCompletedNotification,
} from "../../shared/orders/order.subscriptions.ts";
import {
  captureOrderCompletedDlq,
  notifyOrderCompleted,
} from "../../shared/orders/order.workers.ts";

export default {
  httpHandler: orderApiApp,
  migrations,
  project: { name: "bun-intermediate" },
  registrations: [
    createOrder,
    listOrders,
    assignOrder,
    startOrder,
    completeOrder,
    rejectOrder,
    listOrderEvents,
    listOrderNotifications,
    listOrderBacklogSnapshots,

    subscription("order.created", auditOrderCreated, {
      idempotent: true,
      name: "auditOrderCreated",
    }),
    subscription("order.assigned", auditOrderAssigned, {
      idempotent: true,
      name: "auditOrderAssigned",
    }),
    subscription("order.started", auditOrderStarted, {
      idempotent: true,
      name: "auditOrderStarted",
    }),
    subscription("order.completed", auditOrderCompleted, {
      idempotent: true,
      name: "auditOrderCompleted",
    }),
    subscription("order.completed", enqueueOrderCompletedNotification, {
      idempotent: true,
      name: "enqueueOrderCompletedNotification",
    }),
    subscription("order.rejected", auditOrderRejected, {
      idempotent: true,
      name: "auditOrderRejected",
    }),

    worker("order.completed.notify", notifyOrderCompleted),
    worker("order.completed.notify.dlq", captureOrderCompletedDlq, { dlq: false }),

    cron("orders.backlog.snapshot", "*/15 * * * *", captureOrderBacklogSnapshot),
  ],
} satisfies ChimpbaseAppDefinitionInput;
