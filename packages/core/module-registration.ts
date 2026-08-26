import {
  action,
  register,
  subscription,
  type ChimpbaseActionHandler,
  type ChimpbaseActionRegistrationLike,
  type ChimpbaseContext,
  type ChimpbaseContextExtensionRegistration,
  type ChimpbaseObjectActionHandler,
  type ChimpbaseCronHandler,
  type ChimpbaseRegistrationTarget,
  type ChimpbaseRouteHandler,
  type ChimpbaseSubscriptionHandler,
  type ChimpbaseSubscriptionOptions,
  type ChimpbaseValidator,
  type ChimpbaseWorkerDefinition,
  type ChimpbaseWorkerHandler,
  type ChimpbaseWorkflowDefinition,
} from "@chimpbase/runtime";

import type { ChimpbaseRegistry } from "./index.ts";
import { chimpbaseModuleResourceName, type ChimpbaseModuleImplementation } from "./modules.ts";

export function registerChimpbaseModuleImplementations(
  target: ChimpbaseRegistrationTarget,
  registry: ChimpbaseRegistry,
  implementations: readonly ChimpbaseModuleImplementation[],
): void {
  for (const implementation of implementations) {
    const moduleInterface = implementation.interface;
    registry.moduleInterfaces.set(moduleInterface.name, moduleInterface);
    for (const event of Object.values(moduleInterface.events)) {
      const existing = registry.eventContracts.get(event.id);
      if (existing !== undefined && existing !== event) {
        throw new Error(`duplicate module event contract: ${event.id}`);
      }
      registry.eventContracts.set(event.id, event);
    }
  }

  for (const implementation of implementations) {
    const moduleName = implementation.interface.name;
    for (const [callName, contract] of Object.entries(implementation.interface.calls)) {
      const handler = implementation.calls[callName];
      if (handler === undefined) {
        throw new Error(`module ${moduleName} has no handler for public call ${contract.id}`);
      }
      ensureActionAvailable(registry, contract.id, moduleName);
      register(target, action({
        args: contract.input,
        handler: handler as unknown as ChimpbaseObjectActionHandler<unknown, unknown>,
        name: contract.id,
        result: contract.output,
      }));
      registry.actionOwnership.set(contract.id, { module: moduleName, visibility: "public" });
    }

    const ownedTarget = createOwnedRegistrationTarget(target, registry, moduleName);
    for (const entry of implementation.registrations) register(ownedTarget, entry);
    for (const contractSubscription of implementation.subscriptions) {
      const eventName = contractSubscription.event.id;
      const subscriptionName = `${moduleName}/${contractSubscription.name}`;
      const before = registry.subscriptions.get(eventName)?.length ?? 0;
      target.registerSubscription(eventName, contractSubscription.handler, {
        idempotent: true,
        name: subscriptionName,
      });
      const entry = registry.subscriptions.get(eventName)?.[before];
      if (entry === undefined) throw new Error(`subscription registration failed: ${eventName}`);
      entry.module = moduleName;
      registry.registrationOwnership.set(`subscription:${eventName}:${subscriptionName}`, moduleName);
    }
  }
}

function createOwnedRegistrationTarget(
  target: ChimpbaseRegistrationTarget,
  registry: ChimpbaseRegistry,
  moduleName: string,
): ChimpbaseRegistrationTarget {
  return {
    bindActionInvoker(reference: ChimpbaseActionRegistrationLike): void {
      target.bindActionInvoker?.(reference);
    },
    registerAction(
      name: string,
      handler: ChimpbaseActionHandler<unknown, unknown>,
      definition?: { args?: ChimpbaseValidator<unknown>; result?: ChimpbaseValidator<unknown> },
    ): ChimpbaseActionHandler<unknown, unknown> {
      ensureActionAvailable(registry, name, moduleName);
      const registered = definition?.args !== undefined
        ? target.registerAction(
          name,
          handler as (ctx: ChimpbaseContext, input: unknown) => unknown,
          { args: definition.args, result: definition.result },
        )
        : target.registerAction(name, handler as (ctx: ChimpbaseContext, ...args: unknown[]) => unknown, {
          result: definition?.result,
        });
      registry.actionOwnership.set(name, { module: moduleName, visibility: "internal" });
      return registered;
    },
    registerSubscription<TPayload = unknown, TResult = unknown>(
      eventName: string,
      _handler: ChimpbaseSubscriptionHandler<TPayload, TResult>,
      _options?: ChimpbaseSubscriptionOptions,
    ): ChimpbaseSubscriptionHandler<TPayload, TResult> {
      throw new Error(
        `module ${moduleName} cannot register raw subscription ${eventName}; use defineChimpbaseModuleSubscription`,
      );
    },
    registerWorker<TPayload = unknown, TResult = unknown>(
      name: string,
      handler: ChimpbaseWorkerHandler<TPayload, TResult>,
      definition?: ChimpbaseWorkerDefinition,
    ): ChimpbaseWorkerHandler<TPayload, TResult> {
      const registeredName = chimpbaseModuleResourceName(moduleName, "queue", name);
      ensureRegistrationAvailable(registry, "worker", registeredName, moduleName);
      const registered = target.registerWorker(registeredName, handler, definition);
      const entry = registry.workers.get(registeredName);
      if (entry === undefined) throw new Error(`worker registration failed: ${registeredName}`);
      entry.module = moduleName;
      registry.registrationOwnership.set(`worker:${registeredName}`, moduleName);
      return registered;
    },
    registerCron<TResult = unknown>(
      name: string,
      schedule: string,
      handler: ChimpbaseCronHandler<TResult>,
    ): ChimpbaseCronHandler<TResult> {
      if (target.registerCron === undefined) throw new Error(`registration target does not support cron entries: ${name}`);
      const registeredName = chimpbaseModuleResourceName(moduleName, "cron", name);
      ensureRegistrationAvailable(registry, "cron", registeredName, moduleName);
      const registered = target.registerCron(registeredName, schedule, handler);
      const entry = registry.crons.get(registeredName);
      if (entry === undefined) throw new Error(`cron registration failed: ${registeredName}`);
      entry.module = moduleName;
      registry.registrationOwnership.set(`cron:${registeredName}`, moduleName);
      return registered;
    },
    registerRoute(name: string, handler: ChimpbaseRouteHandler): ChimpbaseRouteHandler {
      if (target.registerRoute === undefined) throw new Error(`registration target does not support route entries: ${name}`);
      const registeredName = chimpbaseModuleResourceName(moduleName, "route", name);
      ensureRegistrationAvailable(registry, "route", registeredName, moduleName);
      const registered = target.registerRoute(registeredName, handler);
      registry.registrationOwnership.set(`route:${registeredName}`, moduleName);
      return registered;
    },
    registerOnStart(name: string, handler: (ctx: ChimpbaseContext) => Promise<void> | void): void {
      if (target.registerOnStart === undefined) throw new Error(`registration target does not support onStart entries: ${name}`);
      ensureRegistrationAvailable(registry, "onStart", name, moduleName);
      target.registerOnStart(name, handler);
      const entry = registry.onStartHooks.at(-1);
      if (entry === undefined || entry.name !== name) throw new Error(`onStart registration failed: ${name}`);
      entry.module = moduleName;
      registry.registrationOwnership.set(`onStart:${name}`, moduleName);
    },
    registerOnStop(name: string, handler: (ctx: ChimpbaseContext) => Promise<void> | void): void {
      if (target.registerOnStop === undefined) throw new Error(`registration target does not support onStop entries: ${name}`);
      ensureRegistrationAvailable(registry, "onStop", name, moduleName);
      target.registerOnStop(name, handler);
      const entry = registry.onStopHooks.at(-1);
      if (entry === undefined || entry.name !== name) throw new Error(`onStop registration failed: ${name}`);
      entry.module = moduleName;
      registry.registrationOwnership.set(`onStop:${name}`, moduleName);
    },
    registerContextExtension(registration: ChimpbaseContextExtensionRegistration): void {
      if (target.registerContextExtension === undefined) {
        throw new Error(`registration target does not support context extensions: ${registration.key}`);
      }
      ensureRegistrationAvailable(registry, "contextExtension", registration.key, moduleName);
      target.registerContextExtension(registration);
      registry.registrationOwnership.set(`contextExtension:${registration.key}`, moduleName);
    },
    registerWorkflow<TInput = unknown, TState = unknown>(
      definition: ChimpbaseWorkflowDefinition<TInput, TState>,
    ): ChimpbaseWorkflowDefinition<TInput, TState> {
      const registeredName = chimpbaseModuleResourceName(moduleName, "workflow", definition.name);
      ensureRegistrationAvailable(registry, "workflow", registeredName, moduleName);
      const registered = target.registerWorkflow({ ...definition, name: registeredName });
      registry.workflowOwnership.set(registeredName, moduleName);
      registry.registrationOwnership.set(`workflow:${registeredName}`, moduleName);
      return registered;
    },
    setTelemetryOverride(key, value): void {
      target.setTelemetryOverride?.(key, value);
    },
  };
}

function ensureActionAvailable(registry: ChimpbaseRegistry, name: string, moduleName: string): void {
  if (registry.actions.has(name) || registry.actionOwnership.has(name)) {
    const owner = registry.actionOwnership.get(name)?.module ?? "app-global infrastructure";
    throw new Error(`module ${moduleName} cannot register action ${name}; already owned by ${owner}`);
  }
}

function ensureRegistrationAvailable(
  registry: ChimpbaseRegistry,
  kind: string,
  name: string,
  moduleName: string,
): void {
  const owner = registry.registrationOwnership.get(`${kind}:${name}`);
  if (owner !== undefined) {
    throw new Error(`module ${moduleName} cannot register ${kind} ${name}; already owned by ${owner}`);
  }
}
