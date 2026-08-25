import { v, type ChimpbaseContext } from "@chimpbase/runtime";

import {
  ORDER_STATUSES,
  type OrderAuditRow,
  type OrderBacklogSnapshotRow,
  type OrderNotificationRow,
  type OrderRecord,
  type OrderStatus,
} from "./order.types.ts";
const orderRecordValidator = v.object({
  amount: v.integer(),
  assignee: v.string().nullable(),
  created_at: v.string(),
  customer: v.string(),
  id: v.integer(),
  status: v.enum(ORDER_STATUSES),
  updated_at: v.string(),
});
const orderAuditRowValidator = v.object({
  created_at: v.string(),
  event: v.string(),
  id: v.integer(),
  order_id: v.integer(),
  payload: v.string(),
});
const orderNotificationRowValidator = v.object({
  channel: v.string(),
  created_at: v.string(),
  detail: v.string().nullable(),
  id: v.integer(),
  order_id: v.integer(),
  status: v.enum(["pending", "sent", "failed"] as const),
});
const orderBacklogSnapshotRowValidator = v.object({
  id: v.integer(),
  in_progress_count: v.integer(),
  pending_count: v.integer(),
  snapshot_at: v.string(),
  total_count: v.integer(),
});
const statusCountRowValidator = v.object({
  count: v.integer(),
  status: v.enum(ORDER_STATUSES),
});


export async function insertOrder(
  ctx: ChimpbaseContext,
  input: { customer: string; amount: number },
): Promise<OrderRecord> {
  const [row] = await ctx.db.query(
    `INSERT INTO orders (customer, amount, status) VALUES (?1, ?2, 'pending')
     RETURNING CAST(id AS DOUBLE PRECISION) AS id, customer, CAST(amount AS DOUBLE PRECISION) AS amount, status, assignee, CAST(created_at AS TEXT) AS created_at, CAST(updated_at AS TEXT) AS updated_at`,
    [input.customer, input.amount],
    orderRecordValidator,
  );
  return row;
}

export async function updateOrderStatus(
  ctx: ChimpbaseContext,
  id: number,
  status: OrderStatus,
  assignee: string | null = null,
): Promise<OrderRecord> {
  const [row] = await ctx.db.query(
    `UPDATE orders
     SET status = ?2,
         assignee = COALESCE(?3, assignee),
         updated_at = CURRENT_TIMESTAMP
     WHERE id = ?1
     RETURNING CAST(id AS DOUBLE PRECISION) AS id, customer, CAST(amount AS DOUBLE PRECISION) AS amount, status, assignee, CAST(created_at AS TEXT) AS created_at, CAST(updated_at AS TEXT) AS updated_at`,
    [id, status, assignee],
    orderRecordValidator,
  );
  if (!(row !== null && row !== undefined)) throw new Error(`order ${id} not found`);
  return row;
}

export async function getOrder(
  ctx: ChimpbaseContext,
  id: number,
): Promise<OrderRecord | null> {
  const rows = await ctx.db.query(
    `SELECT CAST(id AS DOUBLE PRECISION) AS id, customer, CAST(amount AS DOUBLE PRECISION) AS amount, status, assignee, CAST(created_at AS TEXT) AS created_at, CAST(updated_at AS TEXT) AS updated_at
     FROM orders WHERE id = ?1`,
    [id],
    orderRecordValidator,
  );
  return rows[0] ?? null;
}

export async function listOrders(
  ctx: ChimpbaseContext,
): Promise<OrderRecord[]> {
  return await ctx.db.query(
    `SELECT CAST(id AS DOUBLE PRECISION) AS id, customer, CAST(amount AS DOUBLE PRECISION) AS amount, status, assignee, CAST(created_at AS TEXT) AS created_at, CAST(updated_at AS TEXT) AS updated_at
     FROM orders ORDER BY id DESC`,
    undefined,
    orderRecordValidator,
  );
}

export async function insertAuditEntry(
  ctx: ChimpbaseContext,
  entry: { orderId: number; event: string; payload: unknown },
): Promise<void> {
  await ctx.db.query(
    `INSERT INTO order_audit_log (order_id, event, payload) VALUES (?1, ?2, ?3)`,
    [entry.orderId, entry.event, JSON.stringify(entry.payload)],
  );
}

export async function listOrderAudit(
  ctx: ChimpbaseContext,
  orderId: number,
): Promise<OrderAuditRow[]> {
  return await ctx.db.query(
    `SELECT CAST(id AS DOUBLE PRECISION) AS id, CAST(order_id AS DOUBLE PRECISION) AS order_id, event, CAST(payload AS TEXT) AS payload, CAST(created_at AS TEXT) AS created_at
     FROM order_audit_log WHERE order_id = ?1 ORDER BY id`,
    [orderId],
    orderAuditRowValidator,
  );
}

export async function insertNotification(
  ctx: ChimpbaseContext,
  entry: {
    orderId: number;
    channel: string;
    status: OrderNotificationRow["status"];
    detail: string | null;
  },
): Promise<void> {
  await ctx.db.query(
    `INSERT INTO order_notifications (order_id, channel, status, detail)
     VALUES (?1, ?2, ?3, ?4)`,
    [entry.orderId, entry.channel, entry.status, entry.detail],
  );
}

export async function listNotifications(
  ctx: ChimpbaseContext,
): Promise<OrderNotificationRow[]> {
  return await ctx.db.query(
    `SELECT CAST(id AS DOUBLE PRECISION) AS id, CAST(order_id AS DOUBLE PRECISION) AS order_id, channel, status, detail, CAST(created_at AS TEXT) AS created_at
     FROM order_notifications ORDER BY id DESC`,
    undefined,
    orderNotificationRowValidator,
  );
}

export async function countByStatus(
  ctx: ChimpbaseContext,
): Promise<{ pending: number; in_progress: number; total: number }> {
  const rows = await ctx.db.query(
    `SELECT status, CAST(COUNT(*) AS DOUBLE PRECISION) AS count FROM orders GROUP BY status`,
    undefined,
    statusCountRowValidator,
  );
  const pending = Number(rows.find((r) => r.status === "pending")?.count ?? 0);
  const in_progress = Number(rows.find((r) => r.status === "in_progress")?.count ?? 0);
  const total = rows.reduce((sum, r) => sum + Number(r.count ?? 0), 0);
  return { pending, in_progress, total };
}

export async function insertBacklogSnapshot(
  ctx: ChimpbaseContext,
  counts: { pending: number; in_progress: number; total: number },
): Promise<void> {
  await ctx.db.query(
    `INSERT INTO order_backlog_snapshots (pending_count, in_progress_count, total_count)
     VALUES (?1, ?2, ?3)`,
    [counts.pending, counts.in_progress, counts.total],
  );
}

export async function listBacklogSnapshots(
  ctx: ChimpbaseContext,
): Promise<OrderBacklogSnapshotRow[]> {
  return await ctx.db.query(
    `SELECT CAST(id AS DOUBLE PRECISION) AS id, pending_count, in_progress_count, total_count, CAST(snapshot_at AS TEXT) AS snapshot_at
     FROM order_backlog_snapshots ORDER BY id DESC LIMIT 50`,
    undefined,
    orderBacklogSnapshotRowValidator,
  );
}
