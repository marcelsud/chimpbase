import { isJsonObject } from "@chimpbase/runtime";

import type {
  AnyServiceActionHandler,
  AnyServiceDefinition,
  AnyServiceEventDefinition,
  AnyServiceEventHandler,
  ServiceDefinition,
  ServiceMethods,
} from "./types.ts";

export function service<
  TSettings = unknown,
  TMethods extends ServiceMethods = ServiceMethods,
>(
  def: ServiceDefinition<TSettings, TMethods>,
): ServiceDefinition<TSettings, TMethods> {
  if (!(def.name.length > 0)) {
    throw new Error("service definition requires a name");
  }

  return def;
}

export interface ResolvedService {
  actions: Record<string, AnyServiceActionHandler>;
  events: Record<string, AnyServiceEventDefinition>;
  methods: ServiceMethods;
  name: string;
  settings: Record<string, unknown>;
  started?: AnyServiceDefinition["started"];
  stopped?: AnyServiceDefinition["stopped"];
  version: number;
}

export function resolveService(
  def: AnyServiceDefinition,
  seen: Set<AnyServiceDefinition> = new Set(),
): ResolvedService {
  if (seen.has(def)) {
    throw new Error(`service "${def.name}" has a circular mixin reference`);
  }

  seen.add(def);

  const merged: ResolvedService = {
    actions: {},
    events: {},
    methods: {},
    name: def.name,
    settings: {},
    version: def.version ?? 1,
  };

  for (const mixin of def.mixins ?? []) {
    const resolved = resolveService(mixin, new Set(seen));
    Object.assign(merged.actions, resolved.actions);
    Object.assign(merged.events, resolved.events);
    Object.assign(merged.methods, resolved.methods);
    Object.assign(merged.settings, resolved.settings);
  }

  if (isJsonObject(def.settings)) {
    Object.assign(merged.settings, def.settings);
  }

  if ((def.methods !== undefined)) {
    Object.assign(merged.methods, def.methods as Record<string, unknown>);
  }

  for (const [actionName, handler] of Object.entries(def.actions ?? {})) {
    merged.actions[actionName] = handler;
  }

  for (const [eventName, entry] of Object.entries(def.events ?? {})) {
    merged.events[eventName] = normalizeEvent(entry);
  }

  merged.started = def.started ?? merged.started;
  merged.stopped = def.stopped ?? merged.stopped;

  return merged;
}

function normalizeEvent(
  entry: AnyServiceEventHandler | AnyServiceEventDefinition,
): AnyServiceEventDefinition {
  if (typeof entry === "function") {
    return { balanced: false, handler: entry };
  }

  return { balanced: entry.balanced ?? false, handler: entry.handler };
}

export function prefixedActionName(serviceName: string, version: number, actionName: string): string {
  return `v${version}.${serviceName}.${actionName}`;
}
