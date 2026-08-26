import {
  defineChimpbaseModuleImplementation,
  defineChimpbaseModuleSubscription,
} from "@chimpbase/core";
import { action, v } from "@chimpbase/runtime";

import { accounts } from "../accounts/interface.ts";
import { notifications } from "./interface.ts";

const formatPrivate = action({
  name: "notifications.private.format",
  args: v.object({ email: v.string() }),
  result: v.string(),
  handler: (_ctx, input) => `Welcome ${input.email}`,
});

export const notificationsImplementation = defineChimpbaseModuleImplementation({
  interface: notifications,
  calls: {
    async welcomeMessage(ctx, input) {
      const account = await ctx.call(accounts.calls.get, { id: input.accountId });
      if (account === null) throw new Error("account_not_found");
      return await ctx.action(formatPrivate, { email: account.email });
    },
  },
  registrations: [formatPrivate],
  subscriptions: [
    defineChimpbaseModuleSubscription(accounts.events.created, "record-welcome", async (ctx, account) => {
      await ctx.kv.set(`welcomed:${account.id}`, true);
    }),
  ],
  resources: {
    kvPrefixes: ["welcomed:"],
  },
});
