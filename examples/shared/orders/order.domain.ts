import type { OrderStatus } from "./order.types.ts";

const TRANSITIONS: Record<OrderStatus, readonly OrderStatus[]> = {
  pending: ["assigned", "rejected"],
  assigned: ["in_progress", "rejected"],
  in_progress: ["completed", "rejected"],
  completed: [],
  rejected: [],
};

export function normalizeCustomer(input: string): string {
  const trimmed = input.trim().toLowerCase();
  if (trimmed.length === 0) throw new Error("customer cannot be empty");
  return trimmed;
}

export function normalizeAmount(input: number): number {
  if (!Number.isInteger(input) || input <= 0) {
    throw new Error("amount must be a positive integer (cents)");
  }
  return input;
}

export function assertTransition(from: OrderStatus, to: OrderStatus): void {
  if (!TRANSITIONS[from].includes(to)) {
    throw new Error(`illegal order transition ${from} -> ${to}`);
  }
}

export function isTerminal(status: OrderStatus): boolean {
  return status === "completed" || status === "rejected";
}
