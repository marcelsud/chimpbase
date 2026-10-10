import { describe, test } from "node:test";
import { strict as assert } from "node:assert";

import {
  assertTransition,
  isTerminal,
  normalizeAmount,
  normalizeCustomer,
} from "../../../../shared/orders/order.domain.ts";

describe("order domain", () => {
  test("normalizes customer", () => {
    assert.equal(normalizeCustomer("  Alice@Example.com  "), "alice@example.com");
    assert.throws(() => normalizeCustomer("   "), /customer cannot be empty/);
  });

  test("normalizes amount", () => {
    assert.equal(normalizeAmount(4200), 4200);
    assert.throws(() => normalizeAmount(0), /positive integer/);
    assert.throws(() => normalizeAmount(1.5), /positive integer/);
    assert.throws(() => normalizeAmount(-1), /positive integer/);
  });

  test("legal transitions", () => {
    assert.doesNotThrow(() => assertTransition("pending", "assigned"));
    assert.doesNotThrow(() => assertTransition("assigned", "in_progress"));
    assert.doesNotThrow(() => assertTransition("in_progress", "completed"));
    assert.doesNotThrow(() => assertTransition("pending", "rejected"));
  });

  test("illegal transitions", () => {
    assert.throws(() => assertTransition("completed", "pending"));
    assert.throws(() => assertTransition("pending", "completed"));
    assert.throws(() => assertTransition("rejected", "assigned"));
  });

  test("terminal states", () => {
    assert.equal(isTerminal("completed"), true);
    assert.equal(isTerminal("rejected"), true);
    assert.equal(isTerminal("pending"), false);
    assert.equal(isTerminal("in_progress"), false);
  });
});
