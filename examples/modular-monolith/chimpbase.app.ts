import { defineChimpbaseApp } from "@chimpbase/core";

import { accountsImplementation } from "./src/modules/accounts/implementation.ts";
import { notificationsImplementation } from "./src/modules/notifications/implementation.ts";

export default defineChimpbaseApp({
  project: { name: "modular-monolith-example" },
  modules: [accountsImplementation, notificationsImplementation],
});
