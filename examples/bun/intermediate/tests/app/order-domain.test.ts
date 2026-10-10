import { describe, expect, test } from "bun:test";

import {
  assertTransition,
  isTerminal,
  normalizeAmount,
  normalizeCustomer,
} from "../../../../shared/orders/order.domain.ts";

describe("order domain", () => {
  test("normalizes customer", () => {
    expect(normalizeCustomer("  Alice@Example.com  ")).toBe("alice@example.com");
    expect(() => normalizeCustomer("   ")).toThrow("customer cannot be empty");
  });

  test("normalizes amount", () => {
    expect(normalizeAmount(4200)).toBe(4200);
    expect(() => normalizeAmount(0)).toThrow("positive integer");
    expect(() => normalizeAmount(1.5)).toThrow("positive integer");
    expect(() => normalizeAmount(-1)).toThrow("positive integer");
  });

  test("legal transitions", () => {
    expect(() => assertTransition("pending", "assigned")).not.toThrow();
    expect(() => assertTransition("assigned", "in_progress")).not.toThrow();
    expect(() => assertTransition("in_progress", "completed")).not.toThrow();
    expect(() => assertTransition("pending", "rejected")).not.toThrow();
  });

  test("illegal transitions", () => {
    expect(() => assertTransition("completed", "pending")).toThrow();
    expect(() => assertTransition("pending", "completed")).toThrow();
    expect(() => assertTransition("rejected", "assigned")).toThrow();
  });

  test("terminal states", () => {
    expect(isTerminal("completed")).toBe(true);
    expect(isTerminal("rejected")).toBe(true);
    expect(isTerminal("pending")).toBe(false);
    expect(isTerminal("in_progress")).toBe(false);
  });
});
