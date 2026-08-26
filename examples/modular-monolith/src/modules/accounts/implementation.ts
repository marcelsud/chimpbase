import { defineChimpbaseModuleImplementation } from "@chimpbase/core";

import { accounts } from "./interface.ts";

export const accountsImplementation = defineChimpbaseModuleImplementation({
  interface: accounts,
  calls: {
    async create(ctx, input) {
      const existing = await ctx.collection.findOne("accounts", { id: input.id });
      if (existing !== null) throw new Error("account_already_exists");
      await ctx.collection.insert("accounts", input);
      ctx.publish(accounts.events.created, input);
      return input;
    },
    async get(ctx, input) {
      return await ctx.collection.findOne("accounts", { id: input.id }, accounts.calls.create.output);
    },
  },
  resources: {
    collections: ["accounts"],
  },
});
