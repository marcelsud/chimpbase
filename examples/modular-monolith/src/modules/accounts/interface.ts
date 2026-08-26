import { defineChimpbaseModuleInterface } from "@chimpbase/core";
import { v } from "@chimpbase/runtime";

export const accounts = defineChimpbaseModuleInterface({
  name: "accounts",
  version: 1,
  calls: {
    create: {
      input: v.object({ email: v.string(), id: v.string() }),
      output: v.object({ email: v.string(), id: v.string() }),
      errors: ["account_already_exists"],
      guarantees: ["account is persisted before AccountCreated is published"],
    },
    get: {
      input: v.object({ id: v.string() }),
      output: v.object({ email: v.string(), id: v.string() }).nullable(),
      errors: [],
      guarantees: ["reads committed account state"],
    },
  },
  events: {
    created: {
      payload: v.object({ email: v.string(), id: v.string() }),
      version: 1,
    },
  },
});
