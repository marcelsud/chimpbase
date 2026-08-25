import type { ChimpbaseValidator, Infer } from "@chimpbase/runtime";

export interface ChimpbasePact {
  consumer: string;
  provider: string;
  interactions: ChimpbasePactInteraction[];
}

export type ChimpbasePactInteraction =
  | ChimpbasePactActionInteraction
  | ChimpbasePactEventInteraction
  | ChimpbasePactWorkerInteraction;

export interface ChimpbasePactActionInteraction<
  TArgs = unknown,
  TResult = unknown,
> {
  kind: "action";
  name: string;
  states: string[];
  args?: ChimpbaseValidator<TArgs>;
  result?: ChimpbaseValidator<TResult>;
  example?: {
    args?: unknown[];
    result?: TResult;
  };
}

export interface ChimpbasePactEventInteraction<TPayload = unknown> {
  kind: "event";
  eventName: string;
  states: string[];
  payload?: ChimpbaseValidator<TPayload>;
  example?: TPayload;
}

export interface ChimpbasePactWorkerInteraction<TPayload = unknown> {
  kind: "worker";
  queueName: string;
  states: string[];
  payload?: ChimpbaseValidator<TPayload>;
  example?: TPayload;
}

export interface ChimpbasePactInput {
  consumer: string;
  provider: string;
  interactions: ChimpbasePactInteraction[];
}

export function pact(input: ChimpbasePactInput): ChimpbasePact {
  return {
    consumer: input.consumer,
    provider: input.provider,
    interactions: input.interactions,
  };
}

export const interaction = {
  action<
    TArgsValidator extends ChimpbaseValidator<unknown> | undefined = undefined,
    TResultValidator extends ChimpbaseValidator<unknown> | undefined = undefined,
  >(
    name: string,
    options: {
      states?: string[];
      args?: TArgsValidator;
      result?: TResultValidator;
      example?: {
        args?: unknown[];
        result?: TResultValidator extends ChimpbaseValidator<infer T> ? T : undefined;
      };
    } = {},
  ): ChimpbasePactActionInteraction {
    return {
      kind: "action",
      name,
      states: options.states ?? [],
      args: options.args,
      result: options.result,
      example: options.example,
    };
  },

  event<TValidator extends ChimpbaseValidator<unknown> | undefined = undefined>(
    eventName: string,
    options: {
      states?: string[];
      payload?: TValidator;
      example?: TValidator extends ChimpbaseValidator<infer T> ? T : undefined;
    } = {},
  ): ChimpbasePactEventInteraction {
    return {
      kind: "event",
      eventName,
      states: options.states ?? [],
      payload: options.payload,
      example: options.example,
    };
  },

  worker<TValidator extends ChimpbaseValidator<unknown> | undefined = undefined>(
    queueName: string,
    options: {
      states?: string[];
      payload?: TValidator;
      example?: TValidator extends ChimpbaseValidator<infer T> ? T : undefined;
    } = {},
  ): ChimpbasePactWorkerInteraction {
    return {
      kind: "worker",
      queueName,
      states: options.states ?? [],
      payload: options.payload,
      example: options.example,
    };
  },
};

export interface SerializedPact {
  consumer: string;
  provider: string;
  interactions: SerializedPactInteraction[];
}

export type SerializedPactInteraction =
  | SerializedPactActionInteraction
  | SerializedPactEventInteraction
  | SerializedPactWorkerInteraction;

export interface SerializedPactActionInteraction {
  kind: "action";
  name: string;
  states: string[];
  argsSchema?: unknown;
  resultSchema?: unknown;
  example?: {
    args?: unknown;
    result?: unknown;
  };
}

export interface SerializedPactEventInteraction {
  kind: "event";
  eventName: string;
  states: string[];
  payloadSchema?: unknown;
  example?: unknown;
}

export interface SerializedPactWorkerInteraction {
  kind: "worker";
  queueName: string;
  states: string[];
  payloadSchema?: unknown;
  example?: unknown;
}

export function serializePact(pact: ChimpbasePact): SerializedPact {
  return {
    consumer: pact.consumer,
    provider: pact.provider,
    interactions: pact.interactions.map(serializeInteraction),
  };
}

function serializeInteraction(interaction: ChimpbasePactInteraction): SerializedPactInteraction {
  switch (interaction.kind) {
    case "action":
      return {
        kind: "action",
        name: interaction.name,
        states: interaction.states,
        ...((interaction.args !== undefined) ? { argsSchema: interaction.args.schema } : {}),
        ...((interaction.result !== undefined) ? { resultSchema: interaction.result.schema } : {}),
        ...((interaction.example !== undefined) ? { example: interaction.example } : {}),
      };
    case "event":
      return {
        kind: "event",
        eventName: interaction.eventName,
        states: interaction.states,
        ...((interaction.payload !== undefined) ? { payloadSchema: interaction.payload.schema } : {}),
        ...(interaction.example !== undefined ? { example: interaction.example } : {}),
      };
    case "worker":
      return {
        kind: "worker",
        queueName: interaction.queueName,
        states: interaction.states,
        ...((interaction.payload !== undefined) ? { payloadSchema: interaction.payload.schema } : {}),
        ...(interaction.example !== undefined ? { example: interaction.example } : {}),
      };
  }
}

export function serializePactToJson(pact: ChimpbasePact): string {
  return JSON.stringify(serializePact(pact), null, 2);
}

export function deserializePactJson(json: string): SerializedPact {
  return JSON.parse(json) as SerializedPact;
}
