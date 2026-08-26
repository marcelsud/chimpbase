import type { Kysely } from "kysely";

import type {
  ChimpbaseContext,
  ChimpbaseDbClient,
  ChimpbaseModuleCallReference,
  ChimpbaseModuleEventReference,
  ChimpbaseRegistration,
  ChimpbaseSubscriptionHandler,
  ChimpbaseValidator,
  Infer,
} from "@chimpbase/runtime";

import {
  defineChimpbaseMigrations,
  type ChimpbaseMigration,
  type ChimpbaseMigrationsDefinition,
  type ChimpbaseMigrationsDefinitionInput,
} from "./host.ts";

export interface ChimpbaseModuleCallDefinition<TInput = unknown, TOutput = unknown> {
  readonly errors?: readonly string[];
  readonly guarantees?: readonly string[];
  readonly input: ChimpbaseValidator<TInput>;
  readonly output: ChimpbaseValidator<TOutput>;
}

export interface ChimpbaseModuleEventDefinition<TPayload = unknown> {
  readonly name?: string;
  readonly payload: ChimpbaseValidator<TPayload>;
  readonly version: number;
}

type ChimpbaseModuleCallDefinitions = Record<string, ChimpbaseModuleCallDefinition>;
type ChimpbaseModuleEventDefinitions = Record<string, ChimpbaseModuleEventDefinition>;

export interface ChimpbaseModuleCallContract<TInput = unknown, TOutput = unknown>
  extends ChimpbaseModuleCallReference<TInput, TOutput> {
  readonly errors: readonly string[];
  readonly guarantees: readonly string[];
}

export interface ChimpbaseModuleEventContract<TPayload = unknown>
  extends ChimpbaseModuleEventReference<TPayload> {}

type ChimpbaseModuleCallContracts<TDefinitions extends ChimpbaseModuleCallDefinitions> = {
  readonly [TName in keyof TDefinitions]: ChimpbaseModuleCallContract<
    Infer<TDefinitions[TName]["input"]>,
    Infer<TDefinitions[TName]["output"]>
  >;
};

type ChimpbaseModuleEventContracts<TDefinitions extends ChimpbaseModuleEventDefinitions> = {
  readonly [TName in keyof TDefinitions]: ChimpbaseModuleEventContract<
    Infer<TDefinitions[TName]["payload"]>
  >;
};

export interface ChimpbaseModuleInterface<
  TCalls extends Record<string, ChimpbaseModuleCallContract> = Record<string, ChimpbaseModuleCallContract>,
  TEvents extends Record<string, ChimpbaseModuleEventContract> = Record<string, ChimpbaseModuleEventContract>,
  TDatabase = Record<string, never>,
> {
  readonly calls: TCalls;
  readonly databaseType?: (database: TDatabase) => TDatabase;
  readonly dependencies: readonly string[];
  readonly events: TEvents;
  readonly kind: "module-interface";
  readonly name: string;
  readonly version: number;
}

export interface ChimpbaseModuleInterfaceInput<
  TCalls extends ChimpbaseModuleCallDefinitions,
  TEvents extends ChimpbaseModuleEventDefinitions,
> {
  readonly calls: TCalls;
  readonly dependencies?: readonly string[];
  readonly events: TEvents;
  readonly name: string;
  readonly version: number;
}

type ChimpbaseModuleDatabase<TInterface> = TInterface extends ChimpbaseModuleInterface<
  Record<string, ChimpbaseModuleCallContract>,
  Record<string, ChimpbaseModuleEventContract>,
  infer TDatabase
> ? TDatabase : Record<string, never>;

export type ChimpbaseModuleDbClient<TDatabase> = Omit<ChimpbaseDbClient, "kysely"> & {
  readonly schema: string;
  kysely(): Kysely<TDatabase>;
};

export type ChimpbaseModuleContext<TDatabase = Record<string, never>> = Omit<
  ChimpbaseContext,
  "db" | "module"
> & {
  db: ChimpbaseModuleDbClient<TDatabase>;
  readonly module: { readonly name: string };
};

type ChimpbaseModuleCallInput<TContract> = TContract extends ChimpbaseModuleCallContract<infer TInput, unknown>
  ? TInput
  : never;

type ChimpbaseModuleCallOutput<TContract> = TContract extends ChimpbaseModuleCallContract<unknown, infer TOutput>
  ? TOutput
  : never;

export type ChimpbaseModuleCallHandlers<TInterface extends ChimpbaseModuleInterface> = {
  readonly [TName in keyof TInterface["calls"]]: (
    ctx: ChimpbaseModuleContext<ChimpbaseModuleDatabase<TInterface>>,
    input: ChimpbaseModuleCallInput<TInterface["calls"][TName]>,
  ) => ChimpbaseModuleCallOutput<TInterface["calls"][TName]>
    | Promise<ChimpbaseModuleCallOutput<TInterface["calls"][TName]>>;
};

export interface ChimpbaseModuleSubscription<TPayload = unknown> {
  readonly event: ChimpbaseModuleEventContract<TPayload>;
  readonly handler: {
    bivarianceHack(ctx: ChimpbaseContext, payload: TPayload): unknown | Promise<unknown>;
  }["bivarianceHack"];
  readonly name: string;
}

export interface ChimpbaseModuleResources {
  readonly collections?: readonly string[];
  readonly kvPrefixes?: readonly string[];
  readonly projections?: readonly string[];
  readonly queues?: readonly string[];
  readonly streams?: readonly string[];
  readonly tables?: readonly string[];
  readonly workflows?: readonly string[];
}

export interface ChimpbaseModuleImplementation<TInterface extends ChimpbaseModuleInterface = ChimpbaseModuleInterface> {
  readonly calls: ChimpbaseModuleCallHandlers<TInterface>;
  readonly interface: TInterface;
  readonly kind: "module-implementation";
  readonly migrations: ChimpbaseMigrationsDefinition;
  readonly registrations: readonly ChimpbaseRegistration[];
  readonly resources: ChimpbaseModuleResources;
  readonly subscriptions: readonly ChimpbaseModuleSubscription[];
}

export interface ChimpbaseModuleImplementationInput<TInterface extends ChimpbaseModuleInterface> {
  readonly calls: ChimpbaseModuleCallHandlers<TInterface>;
  readonly interface: TInterface;
  readonly migrations?: ChimpbaseMigrationsDefinitionInput;
  readonly registrations?: readonly ChimpbaseRegistration[];
  readonly resources?: ChimpbaseModuleResources;
  readonly subscriptions?: readonly ChimpbaseModuleSubscription[];
}

export function defineChimpbaseModuleInterface<
  TDatabase = Record<string, never>,
  const TCalls extends ChimpbaseModuleCallDefinitions = ChimpbaseModuleCallDefinitions,
  const TEvents extends ChimpbaseModuleEventDefinitions = ChimpbaseModuleEventDefinitions,
>(
  input: ChimpbaseModuleInterfaceInput<TCalls, TEvents>,
): ChimpbaseModuleInterface<
  ChimpbaseModuleCallContracts<TCalls>,
  ChimpbaseModuleEventContracts<TEvents>,
  TDatabase
> {
  assertModuleName(input.name);
  assertVersion(input.version, `module ${input.name}`);
  const dependencies = [...new Set(input.dependencies ?? [])];
  if (dependencies.includes(input.name)) {
    throw new Error(`module ${input.name} cannot depend on itself`);
  }
  for (const dependency of dependencies) assertModuleName(dependency);

  const calls = Object.fromEntries(Object.entries(input.calls).map(([name, definition]) => {
    assertContractName(name, `module ${input.name} call`);
    return [name, Object.freeze({
      errors: Object.freeze([...(definition.errors ?? [])]),
      guarantees: Object.freeze([...(definition.guarantees ?? [])]),
      id: `${input.name}/${name}@v${input.version}`,
      input: definition.input,
      kind: "module-call" as const,
      module: input.name,
      name,
      output: definition.output,
      version: input.version,
    })];
  })) as ChimpbaseModuleCallContracts<TCalls>;

  const eventIds = new Set<string>();
  const events = Object.fromEntries(Object.entries(input.events).map(([key, definition]) => {
    const name = definition.name ?? key;
    assertContractName(name, `module ${input.name} event`);
    assertVersion(definition.version, `module ${input.name} event ${name}`);
    const id = `${input.name}/${name}@v${definition.version}`;
    if (eventIds.has(id)) throw new Error(`duplicate module event identity: ${id}`);
    eventIds.add(id);
    return [key, Object.freeze({
      id,
      kind: "module-event" as const,
      module: input.name,
      name,
      payload: definition.payload,
      version: definition.version,
    })];
  })) as ChimpbaseModuleEventContracts<TEvents>;

  return Object.freeze({
    calls: Object.freeze(calls),
    dependencies: Object.freeze(dependencies.sort()),
    events: Object.freeze(events),
    kind: "module-interface" as const,
    name: input.name,
    version: input.version,
  });
}

export function defineChimpbaseModuleImplementation<TInterface extends ChimpbaseModuleInterface>(
  input: ChimpbaseModuleImplementationInput<TInterface>,
): ChimpbaseModuleImplementation<TInterface> {
  const expectedCalls = Object.keys(input.interface.calls).sort();
  const implementedCalls = Object.keys(input.calls).sort();
  if (expectedCalls.join("\0") !== implementedCalls.join("\0")) {
    throw new Error(
      `module ${input.interface.name} must implement each public call exactly once; expected [${expectedCalls.join(", ")}], received [${implementedCalls.join(", ")}]`,
    );
  }

  const subscriptions = [...(input.subscriptions ?? [])];
  const names = new Set<string>();
  for (const entry of subscriptions) {
    assertContractName(entry.name, `module ${input.interface.name} subscription`);
    if (names.has(entry.name)) {
      throw new Error(`duplicate subscription identity: ${input.interface.name}/${entry.name}`);
    }
    names.add(entry.name);
  }

  return Object.freeze({
    calls: input.calls,
    interface: input.interface,
    kind: "module-implementation" as const,
    migrations: defineChimpbaseMigrations(input.migrations),
    registrations: Object.freeze([...(input.registrations ?? [])]),
    resources: Object.freeze({ ...(input.resources ?? {}) }),
    subscriptions: Object.freeze(subscriptions),
  });
}

export function defineChimpbaseModuleSubscription<TPayload>(
  event: ChimpbaseModuleEventContract<TPayload>,
  name: string,
  handler: ChimpbaseSubscriptionHandler<TPayload, unknown>,
): ChimpbaseModuleSubscription<TPayload> {
  return Object.freeze({ event, handler, name });
}

export function validateChimpbaseModules(
  implementations: readonly ChimpbaseModuleImplementation[],
): readonly ChimpbaseModuleImplementation[] {
  const byName = new Map<string, ChimpbaseModuleImplementation>();
  for (const implementation of implementations) {
    const name = implementation.interface.name;
    if (byName.has(name)) throw new Error(`duplicate module identity: ${name}`);
    byName.set(name, implementation);
  }

  for (const implementation of implementations) {
    for (const dependency of implementation.interface.dependencies) {
      if (!byName.has(dependency)) {
        throw new Error(`module ${implementation.interface.name} declares missing dependency ${dependency}`);
      }
    }
    for (const subscription of implementation.subscriptions) {
      const publisher = byName.get(subscription.event.module);
      const declared = publisher !== undefined
        && Object.values(publisher.interface.events).some((event) => event.id === subscription.event.id);
      if (!declared) {
        throw new Error(
          `module ${implementation.interface.name} subscribes to undeclared event ${subscription.event.id}`,
        );
      }
    }
  }

  return Object.freeze(topologicallySortModules(byName));
}

export function composeChimpbaseModuleMigrations(
  implementations: readonly ChimpbaseModuleImplementation[],
): ChimpbaseMigrationsDefinition {
  const ordered = validateChimpbaseModules(implementations);
  return {
    postgres: composeMigrationsForEngine(ordered, "postgres"),
    sqlite: composeMigrationsForEngine(ordered, "sqlite"),
  };
}

export function chimpbaseModuleSchemaName(moduleName: string): string {
  assertModuleName(moduleName);
  return `chimpbase_${moduleName.replaceAll("-", "_")}`;
}

export function chimpbaseModuleResourceName(moduleName: string, kind: string, name: string): string {
  assertModuleName(moduleName);
  assertContractName(kind, "module resource kind");
  if (name.length === 0 || name.includes("\0")) throw new Error("module resource name must not be empty");
  return `module:${moduleName}:${kind}:${name}`;
}

function composeMigrationsForEngine(
  implementations: readonly ChimpbaseModuleImplementation[],
  engine: keyof ChimpbaseMigrationsDefinition,
): ChimpbaseMigration[] {
  const seen = new Set<string>();
  const migrations: ChimpbaseMigration[] = [];
  for (const implementation of implementations) {
    const owner = implementation.interface.name;
    if (engine === "postgres") {
      const name = `${owner}:__schema`;
      seen.add(name);
      migrations.push({
        name,
        owner,
        sql: `CREATE SCHEMA IF NOT EXISTS ${chimpbaseModuleSchemaName(owner)}`,
      });
    }
    for (const migration of [...implementation.migrations[engine]].sort((left, right) => left.name.localeCompare(right.name))) {
      const name = `${owner}:${migration.name}`;
      if (seen.has(name)) throw new Error(`duplicate module migration identity: ${name}`);
      seen.add(name);
      if (engine === "postgres") assertPostgresMigrationOwnership(owner, migration.sql);
      migrations.push({ name, owner, sql: migration.sql });
    }
  }
  return migrations;
}

function topologicallySortModules(
  byName: ReadonlyMap<string, ChimpbaseModuleImplementation>,
): ChimpbaseModuleImplementation[] {
  const ordered: ChimpbaseModuleImplementation[] = [];
  const visiting: string[] = [];
  const visited = new Set<string>();

  const visit = (name: string): void => {
    if (visited.has(name)) return;
    const cycleStart = visiting.indexOf(name);
    if (cycleStart >= 0) {
      throw new Error(`synchronous module dependency cycle: ${[...visiting.slice(cycleStart), name].join(" -> ")}`);
    }
    const implementation = byName.get(name);
    if (implementation === undefined) throw new Error(`module not found: ${name}`);
    visiting.push(name);
    for (const dependency of [...implementation.interface.dependencies].sort()) visit(dependency);
    visiting.pop();
    visited.add(name);
    ordered.push(implementation);
  };

  for (const name of [...byName.keys()].sort()) visit(name);
  return ordered;
}

function assertPostgresMigrationOwnership(owner: string, sql: string): void {
  const ownSchema = chimpbaseModuleSchemaName(owner);
  for (const match of sql.matchAll(
    /\b(?:ALTER\s+TABLE|CREATE\s+TABLE|DROP\s+TABLE|REFERENCES)\s+"?([a-z_][a-z0-9_]*)"?(?:\s*\.\s*"?([a-z_][a-z0-9_]*)"?)?/gi,
  )) {
    const schema = match[1]?.toLowerCase();
    const table = match[2]?.toLowerCase();
    if (schema === undefined) continue;
    if (table === undefined) {
      throw new Error(`module ${owner} migration must qualify owned tables with schema ${ownSchema}`);
    }
    if (schema !== ownSchema) {
      throw new Error(`module ${owner} migration references foreign schema ${schema}`);
    }
  }
}

function assertModuleName(name: string): void {
  if (!/^[a-z][a-z0-9-]{0,30}$/.test(name)) {
    throw new Error(`invalid module name ${JSON.stringify(name)}; use lowercase letters, digits, and hyphens`);
  }
}

function assertContractName(name: string, label: string): void {
  if (!/^[a-z][a-zA-Z0-9._-]{0,62}$/.test(name)) {
    throw new Error(`invalid ${label} name ${JSON.stringify(name)}`);
  }
}

function assertVersion(version: number, label: string): void {
  if (!Number.isSafeInteger(version) || version < 1) {
    throw new Error(`${label} version must be a positive integer`);
  }
}
