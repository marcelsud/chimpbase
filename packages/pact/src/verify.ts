import type { ChimpbaseActionExecutionResult, ChimpbaseEventRecord } from "@chimpbase/core";

import type {
  ChimpbasePact,
  ChimpbasePactActionInteraction,
  ChimpbasePactEventInteraction,
  ChimpbasePactInteraction,
  ChimpbasePactWorkerInteraction,
} from "./contract.ts";

export interface PactVerificationHost {
  executeAction(name: string, args?: unknown[] | unknown): Promise<ChimpbaseActionExecutionResult>;
  close?(): void;
}

export type PactStateSetupFn = (host: PactVerificationHost) => Promise<void> | void;

export interface VerifyPactOptions {
  host: PactVerificationHost;
  pact: ChimpbasePact;
  states?: Record<string, PactStateSetupFn>;
  onInteractionStart?: (interaction: ChimpbasePactInteraction) => void;
  onInteractionPass?: (interaction: ChimpbasePactInteraction) => void;
  onInteractionFail?: (interaction: ChimpbasePactInteraction, error: PactVerificationFailure) => void;
}

export interface PactVerificationResult {
  consumer: string;
  provider: string;
  passed: number;
  failed: number;
  total: number;
  results: PactInteractionResult[];
}

export type PactInteractionResult =
  | PactInteractionPass
  | PactInteractionFail;

export interface PactInteractionPass {
  interaction: ChimpbasePactInteraction;
  status: "passed";
}

export interface PactInteractionFail {
  interaction: ChimpbasePactInteraction;
  status: "failed";
  failure: PactVerificationFailure;
}

export type PactVerificationFailure =
  | { kind: "missing_state"; state: string }
  | { kind: "action_threw"; name: string; error: string }
  | { kind: "result_mismatch"; name: string; expected: unknown; actual: unknown; error: string }
  | { kind: "event_not_emitted"; eventName: string }
  | { kind: "event_payload_mismatch"; eventName: string; expected: unknown; actual: unknown; error: string }
  | { kind: "worker_payload_mismatch"; queueName: string; expected: unknown; actual: unknown; error: string }
  | { kind: "no_example"; interactionKind: string; name: string };

export async function verifyPact(options: VerifyPactOptions): Promise<PactVerificationResult> {
  const { host, states = {} } = options;
  const pactData = options.pact;
  const results: PactInteractionResult[] = [];

  for (const interaction of pactData.interactions) {
    options.onInteractionStart?.(interaction);

    const result = await verifyInteraction(host, interaction, states);
    results.push(result);

    if (result.status === "passed") {
      options.onInteractionPass?.(interaction);
    } else {
      options.onInteractionFail?.(interaction, result.failure);
    }
  }

  return {
    consumer: pactData.consumer,
    provider: pactData.provider,
    passed: results.filter((r) => r.status === "passed").length,
    failed: results.filter((r) => r.status === "failed").length,
    total: results.length,
    results,
  };
}

async function verifyInteraction(
  host: PactVerificationHost,
  interaction: ChimpbasePactInteraction,
  states: Record<string, PactStateSetupFn>,
): Promise<PactInteractionResult> {
  switch (interaction.kind) {
    case "action":
      return await verifyActionInteraction(host, interaction, states);
    case "event":
      return await verifyEventInteraction(host, interaction, states);
    case "worker":
      return await verifyWorkerInteraction(host, interaction, states);
  }
}

async function setupStates(
  host: PactVerificationHost,
  stateNames: string[],
  stateHandlers: Record<string, PactStateSetupFn>,
): Promise<{ failure: PactVerificationFailure } | null> {
  for (const state of stateNames) {
    const handler = stateHandlers[state];
    if (!(handler !== null && handler !== undefined)) {
      return { failure: { kind: "missing_state", state } };
    }

    await handler(host);
  }

  return null;
}

async function verifyActionInteraction(
  host: PactVerificationHost,
  interaction: ChimpbasePactActionInteraction,
  states: Record<string, PactStateSetupFn>,
): Promise<PactInteractionResult> {
  const stateError = await setupStates(host, interaction.states, states);
  if ((stateError !== null)) {
    return { interaction, status: "failed", failure: stateError.failure };
  }

  if (!(interaction.example?.args !== undefined)) {
    return {
      interaction,
      status: "failed",
      failure: { kind: "no_example", interactionKind: "action", name: interaction.name },
    };
  }

  let outcome: ChimpbaseActionExecutionResult;
  try {
    outcome = await host.executeAction(interaction.name, interaction.example.args);
  } catch (error) {
    return {
      interaction,
      status: "failed",
      failure: {
        kind: "action_threw",
        name: interaction.name,
        error: error instanceof Error ? error.message : String(error),
      },
    };
  }

  if ((interaction.result !== undefined)) {
    try {
      interaction.result.parse(outcome.result);
    } catch (error) {
      return {
        interaction,
        status: "failed",
        failure: {
          kind: "result_mismatch",
          name: interaction.name,
          expected: interaction.result.schema,
          actual: outcome.result,
          error: error instanceof Error ? error.message : String(error),
        },
      };
    }
  }

  return { interaction, status: "passed" };
}

async function verifyEventInteraction(
  host: PactVerificationHost,
  interaction: ChimpbasePactEventInteraction,
  states: Record<string, PactStateSetupFn>,
): Promise<PactInteractionResult> {
  // For event verification, the state setup should trigger actions that
  // cause the expected event to be emitted. We wrap the state handlers
  // to collect all emitted events, then check that the expected event
  // was emitted with a valid payload.

  const collectedEvents: ChimpbaseEventRecord[] = [];

  const collectingHost: PactVerificationHost = {
    async executeAction(name, args) {
      const result = await host.executeAction(name, args);
      collectedEvents.push(...result.emittedEvents);
      return result;
    },
    close: host.close?.bind(host),
  };

  const stateError = await setupStates(collectingHost, interaction.states, states);
  if ((stateError !== null)) {
    return { interaction, status: "failed", failure: stateError.failure };
  }

  const matchingEvents = collectedEvents.filter((e) => e.name === interaction.eventName);

  if (matchingEvents.length === 0) {
    return {
      interaction,
      status: "failed",
      failure: { kind: "event_not_emitted", eventName: interaction.eventName },
    };
  }

  if ((interaction.payload !== undefined)) {
    const validator = interaction.payload;
    for (const event of matchingEvents) {
      try {
        validator.parse(event.payload);
        return { interaction, status: "passed" };
      } catch {
        // Try the next matching event
      }
    }

    // None of the matching events passed validation
    const lastEvent = matchingEvents[matchingEvents.length - 1];
    try {
      validator.parse(lastEvent.payload);
    } catch (error) {
      return {
        interaction,
        status: "failed",
        failure: {
          kind: "event_payload_mismatch",
          eventName: interaction.eventName,
          expected: validator.schema,
          actual: lastEvent.payload,
          error: error instanceof Error ? error.message : String(error),
        },
      };
    }
  }

  return { interaction, status: "passed" };
}

async function verifyWorkerInteraction(
  host: PactVerificationHost,
  interaction: ChimpbasePactWorkerInteraction,
  states: Record<string, PactStateSetupFn>,
): Promise<PactInteractionResult> {
  // Worker interactions verify that the payload schema is valid.
  // The state setup should enqueue a job. We validate the example payload
  // against the validator if both are provided.

  const stateError = await setupStates(host, interaction.states, states);
  if ((stateError !== null)) {
    return { interaction, status: "failed", failure: stateError.failure };
  }

  if ((interaction.payload !== undefined) && interaction.example !== undefined) {
    try {
      interaction.payload.parse(interaction.example);
    } catch (error) {
      return {
        interaction,
        status: "failed",
        failure: {
          kind: "worker_payload_mismatch",
          queueName: interaction.queueName,
          expected: interaction.payload.schema,
          actual: interaction.example,
          error: error instanceof Error ? error.message : String(error),
        },
      };
    }
  }

  return { interaction, status: "passed" };
}

