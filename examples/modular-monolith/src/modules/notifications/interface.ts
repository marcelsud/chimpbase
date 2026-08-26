import { defineChimpbaseModuleInterface } from "@chimpbase/core";
import { v } from "@chimpbase/runtime";

export const notifications = defineChimpbaseModuleInterface({
  name: "notifications",
  version: 1,
  dependencies: ["accounts"],
  calls: {
    welcomeMessage: {
      input: v.object({ accountId: v.string() }),
      output: v.string(),
      errors: ["account_not_found"],
      guarantees: ["message reflects committed account data"],
    },
  },
  events: {},
});
