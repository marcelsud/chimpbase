import type { ChimpbaseModuleInterface } from "@chimpbase/core";

import { interaction, pact, type ChimpbasePact } from "./contract.ts";

export function pactFromChimpbaseModuleInterface(
  consumer: string,
  provider: ChimpbaseModuleInterface,
): ChimpbasePact {
  return pact({
    consumer,
    provider: provider.name,
    interactions: [
      ...Object.values(provider.calls)
        .sort((left, right) => left.id.localeCompare(right.id))
        .map((call) => interaction.action(call.id, {
          args: call.input,
          result: call.output,
        })),
      ...Object.values(provider.events)
        .sort((left, right) => left.id.localeCompare(right.id))
        .map((event) => interaction.event(event.id, { payload: event.payload })),
    ],
  });
}
