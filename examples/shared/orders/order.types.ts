export const ORDER_STATUSES = [
  "pending",
  "assigned",
  "in_progress",
  "completed",
  "rejected",
] as const;

export type OrderStatus = (typeof ORDER_STATUSES)[number];

export interface OrderRecord {
  id: number;
  customer: string;
  amount: number;
  status: OrderStatus;
  assignee: string | null;
  created_at: string;
  updated_at: string;
}

export interface OrderAuditRow {
  id: number;
  order_id: number;
  event: string;
  payload: string;
  created_at: string;
}

export interface OrderNotificationRow {
  id: number;
  order_id: number;
  channel: string;
  status: "pending" | "sent" | "failed";
  detail: string | null;
  created_at: string;
}

export interface OrderBacklogSnapshotRow {
  id: number;
  pending_count: number;
  in_progress_count: number;
  total_count: number;
  snapshot_at: string;
}
