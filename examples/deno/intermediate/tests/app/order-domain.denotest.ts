import { assertEquals, assertThrows } from "jsr:@std/assert@1";

import {
  assertTransition,
  isTerminal,
  normalizeAmount,
  normalizeCustomer,
} from "../../../../shared/orders/order.domain.ts";

Deno.test("order domain — normalizes customer", () => {
  assertEquals(normalizeCustomer("  Alice@Example.com  "), "alice@example.com");
  assertThrows(() => normalizeCustomer("   "), Error, "customer cannot be empty");
});

Deno.test("order domain — normalizes amount", () => {
  assertEquals(normalizeAmount(4200), 4200);
  assertThrows(() => normalizeAmount(0), Error, "positive integer");
  assertThrows(() => normalizeAmount(1.5), Error, "positive integer");
  assertThrows(() => normalizeAmount(-1), Error, "positive integer");
});

Deno.test("order domain — legal transitions", () => {
  assertTransition("pending", "assigned");
  assertTransition("assigned", "in_progress");
  assertTransition("in_progress", "completed");
  assertTransition("pending", "rejected");
});

Deno.test("order domain — illegal transitions", () => {
  assertThrows(() => assertTransition("completed", "pending"));
  assertThrows(() => assertTransition("pending", "completed"));
  assertThrows(() => assertTransition("rejected", "assigned"));
});

Deno.test("order domain — terminal states", () => {
  assertEquals(isTerminal("completed"), true);
  assertEquals(isTerminal("rejected"), true);
  assertEquals(isTerminal("pending"), false);
  assertEquals(isTerminal("in_progress"), false);
});
