import { access, mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";

import * as ts from "typescript";

import type {
  ChimpbaseAppDefinition,
  ChimpbaseModuleImplementation,
  ChimpbaseModuleInterface,
} from "@chimpbase/core";

export type ChimpbaseModuleCompatibility = "breaking" | "compatible" | "migration-required";

export interface ChimpbaseModuleArchitectureDiagnostic {
  readonly file: string;
  readonly rule: "composition-root-only" | "cycle" | "deep-import" | "undeclared-dependency";
  readonly sourceModule: string | null;
  readonly target: string;
  readonly targetModule: string;
}

export interface CheckChimpbaseModuleArchitectureOptions {
  readonly compositionRoot?: string;
  readonly modulesDir?: string;
  readonly projectDir: string;
}

export interface ChimpbaseModuleManifestCall {
  readonly errors: readonly string[];
  readonly guarantees: readonly string[];
  readonly id: string;
  readonly inputSchema: unknown;
  readonly name: string;
  readonly outputSchema: unknown;
  readonly version: number;
}

export interface ChimpbaseModuleManifestEvent {
  readonly id: string;
  readonly name: string;
  readonly payloadSchema: unknown;
  readonly version: number;
}

export interface ChimpbaseModuleManifestEntry {
  readonly calls: readonly ChimpbaseModuleManifestCall[];
  readonly dependencies: readonly string[];
  readonly events: readonly ChimpbaseModuleManifestEvent[];
  readonly migrations: {
    readonly postgres: readonly string[];
    readonly sqlite: readonly string[];
  };
  readonly name: string;
  readonly resources: Readonly<Record<string, readonly string[]>>;
  readonly subscriptions: readonly { event: string; name: string }[];
  readonly version: number;
}

export interface ChimpbaseModuleManifest {
  readonly eventGraph: readonly { consumer: string; event: string; publisher: string; subscription: string }[];
  readonly modules: readonly ChimpbaseModuleManifestEntry[];
  readonly pactInteractions: readonly ChimpbaseModulePactInteraction[];
  readonly schemaVersion: 1;
  readonly synchronousGraph: readonly { source: string; target: string }[];
}

export type ChimpbaseModulePactInteraction =
  | {
      readonly inputSchema: unknown;
      readonly kind: "action";
      readonly name: string;
      readonly outputSchema: unknown;
      readonly provider: string;
    }
  | {
      readonly kind: "event";
      readonly name: string;
      readonly payloadSchema: unknown;
      readonly provider: string;
      readonly version: number;
    };

export interface ChimpbaseModuleCompatibilityDiagnostic {
  readonly classification: Exclude<ChimpbaseModuleCompatibility, "compatible">;
  readonly contract: string;
  readonly module: string;
  readonly next: unknown;
  readonly previous: unknown;
  readonly remediation: string;
}

export interface ChimpbaseModuleCompatibilityResult {
  readonly classification: ChimpbaseModuleCompatibility;
  readonly diagnostics: readonly ChimpbaseModuleCompatibilityDiagnostic[];
}

export interface SyncChimpbaseModuleArtifactsOptions {
  readonly artifactsDir?: string;
  readonly check?: boolean;
  readonly compositionRoot?: string;
  readonly modulesDir?: string;
}

export interface SyncChimpbaseModuleArtifactsResult {
  readonly architecture: readonly ChimpbaseModuleArchitectureDiagnostic[];
  readonly compatibility: ChimpbaseModuleCompatibilityResult;
  readonly manifest: ChimpbaseModuleManifest;
  readonly paths: { readonly databaseTypes: string; readonly manifest: string; readonly summary: string };
  readonly status: "unchanged" | "written";
}

export async function checkChimpbaseModuleArchitecture(
  interfaces: readonly ChimpbaseModuleInterface[],
  options: CheckChimpbaseModuleArchitectureOptions,
): Promise<ChimpbaseModuleArchitectureDiagnostic[]> {
  const projectDir = resolve(options.projectDir);
  const modulesDir = resolve(projectDir, options.modulesDir ?? "src/modules");
  const compositionRoot = resolve(projectDir, options.compositionRoot ?? "chimpbase.app.ts");
  if (!(await fileExists(modulesDir))) return [];

  const interfacesByName = new Map(interfaces.map((entry) => [entry.name, entry]));
  const compilerOptions = readCompilerOptions(projectDir);
  const diagnostics: ChimpbaseModuleArchitectureDiagnostic[] = [];
  const sourceRoot = resolve(projectDir, "src");
  const files = await listTypeScriptFiles(await fileExists(sourceRoot) ? sourceRoot : modulesDir);
  if (await fileExists(compositionRoot)) files.push(compositionRoot);

  for (const file of files) {
    const sourceModule = moduleForFile(modulesDir, file, interfacesByName);
    const source = await readFile(file, "utf8");
    const imports = ts.preProcessFile(source, true, true).importedFiles;
    for (const imported of imports) {
      const resolvedImport = ts.resolveModuleName(imported.fileName, file, compilerOptions, ts.sys)
        .resolvedModule?.resolvedFileName;
      if (resolvedImport === undefined) continue;
      const targetFile = normalizeResolvedFile(resolvedImport);
      const targetModule = moduleForFile(modulesDir, targetFile, interfacesByName);
      if (targetModule === null) continue;
      if (targetModule === sourceModule) {
        const sourceIsInterface = resolve(file) === join(modulesDir, sourceModule, "interface.ts");
        if (sourceIsInterface && resolve(targetFile) !== resolve(file)) {
          diagnostics.push({
            file: relative(projectDir, file),
            rule: "deep-import",
            sourceModule,
            target: imported.fileName,
            targetModule,
          });
        }
        continue;
      }

      const isCompositionRoot = resolve(file) === compositionRoot;
      if (isCompositionRoot) continue;
      if (sourceModule === null) {
        diagnostics.push({
          file: relative(projectDir, file),
          rule: "composition-root-only",
          sourceModule,
          target: imported.fileName,
          targetModule,
        });
        continue;
      }

      const isPublicInterface = resolve(targetFile) === join(modulesDir, targetModule, "interface.ts");
      if (!isPublicInterface) {
        diagnostics.push({
          file: relative(projectDir, file),
          rule: "deep-import",
          sourceModule,
          target: imported.fileName,
          targetModule,
        });
        continue;
      }

      const sourceInterface = interfacesByName.get(sourceModule);
      if (sourceInterface === undefined || !sourceInterface.dependencies.includes(targetModule)) {
        diagnostics.push({
          file: relative(projectDir, file),
          rule: "undeclared-dependency",
          sourceModule,
          target: imported.fileName,
          targetModule,
        });
      }
    }
  }

  diagnostics.push(...findDependencyCycleDiagnostics(interfaces));
  return diagnostics.sort((left, right) =>
    left.file.localeCompare(right.file)
    || left.rule.localeCompare(right.rule)
    || left.target.localeCompare(right.target)
  );
}

export function generateChimpbaseModuleManifest(
  implementations: readonly ChimpbaseModuleImplementation[],
): ChimpbaseModuleManifest {
  const ordered = [...implementations].sort((left, right) =>
    left.interface.name.localeCompare(right.interface.name)
  );
  const modules: ChimpbaseModuleManifestEntry[] = ordered.map((implementation) => ({
    calls: Object.values(implementation.interface.calls)
      .map((call) => ({
        errors: [...call.errors],
        guarantees: [...call.guarantees],
        id: call.id,
        inputSchema: sortSerializableValue(call.input.schema),
        name: call.name,
        outputSchema: sortSerializableValue(call.output.schema),
        version: call.version,
      }))
      .sort((left, right) => left.id.localeCompare(right.id)),
    dependencies: [...implementation.interface.dependencies].sort(),
    events: Object.values(implementation.interface.events)
      .map((event) => ({
        id: event.id,
        name: event.name,
        payloadSchema: sortSerializableValue(event.payload.schema),
        version: event.version,
      }))
      .sort((left, right) => left.id.localeCompare(right.id)),
    migrations: {
      postgres: implementation.migrations.postgres.map((entry) => entry.name).sort(),
      sqlite: implementation.migrations.sqlite.map((entry) => entry.name).sort(),
    },
    name: implementation.interface.name,
    resources: normalizeResources(implementation.resources),
    subscriptions: implementation.subscriptions
      .map((entry) => ({ event: entry.event.id, name: entry.name }))
      .sort((left, right) => left.name.localeCompare(right.name)),
    version: implementation.interface.version,
  }));

  return {
    eventGraph: ordered.flatMap((implementation) => implementation.subscriptions.map((entry) => ({
      consumer: implementation.interface.name,
      event: entry.event.id,
      publisher: entry.event.module,
      subscription: entry.name,
    }))).sort((left, right) =>
      left.publisher.localeCompare(right.publisher)
      || left.consumer.localeCompare(right.consumer)
      || left.subscription.localeCompare(right.subscription)
    ),
    modules,
    pactInteractions: modules.flatMap((module) => [
      ...module.calls.map((call): ChimpbaseModulePactInteraction => ({
        inputSchema: call.inputSchema,
        kind: "action",
        name: call.id,
        outputSchema: call.outputSchema,
        provider: module.name,
      })),
      ...module.events.map((event): ChimpbaseModulePactInteraction => ({
        kind: "event",
        name: event.name,
        payloadSchema: event.payloadSchema,
        provider: module.name,
        version: event.version,
      })),
    ]),
    schemaVersion: 1,
    synchronousGraph: modules.flatMap((module) => module.dependencies.map((target) => ({
      source: module.name,
      target,
    }))),
  };
}

export function compareChimpbaseModuleManifests(
  previous: ChimpbaseModuleManifest | null,
  next: ChimpbaseModuleManifest,
): ChimpbaseModuleCompatibilityResult {
  if (previous === null) return { classification: "compatible", diagnostics: [] };
  const diagnostics: ChimpbaseModuleCompatibilityDiagnostic[] = [];
  const previousModules = new Map(previous.modules.map((entry) => [entry.name, entry]));
  const nextModules = new Map(next.modules.map((entry) => [entry.name, entry]));

  for (const [moduleName, previousModule] of previousModules) {
    const nextModule = nextModules.get(moduleName);
    if (nextModule === undefined) {
      diagnostics.push(breaking(moduleName, "module", previousModule, null, "restore the module or approve a breaking removal"));
      continue;
    }
    const nextCalls = new Map(nextModule.calls.map((entry) => [entry.id, entry]));
    for (const previousCall of previousModule.calls) {
      const nextCall = nextCalls.get(previousCall.id);
      if (nextCall === undefined) {
        diagnostics.push(nextModule.version > previousModule.version
          ? {
              classification: "migration-required",
              contract: previousCall.id,
              module: moduleName,
              next: null,
              previous: previousCall,
              remediation: `migrate consumers to module version ${nextModule.version}`,
            }
          : breaking(moduleName, previousCall.id, previousCall, null, "restore the call or increment the module version"));
        continue;
      }
      if (!schemaAccepts(previousCall.inputSchema, nextCall.inputSchema)) {
        diagnostics.push(breaking(moduleName, previousCall.id, previousCall.inputSchema, nextCall.inputSchema, "widen accepted input or version the call"));
      }
      if (!schemaPreserves(previousCall.outputSchema, nextCall.outputSchema)) {
        diagnostics.push(breaking(moduleName, previousCall.id, previousCall.outputSchema, nextCall.outputSchema, "preserve the previous output shape or version the call"));
      }
      if (nextCall.errors.some((error) => !previousCall.errors.includes(error))) {
        diagnostics.push({
          classification: "migration-required",
          contract: previousCall.id,
          module: moduleName,
          next: nextCall.errors,
          previous: previousCall.errors,
          remediation: "document the new error and increment the module version",
        });
      }
    }

    const nextEvents = new Map(nextModule.events.map((entry) => [entry.id, entry]));
    for (const previousEvent of previousModule.events) {
      const nextEvent = nextEvents.get(previousEvent.id);
      if (nextEvent === undefined) {
        diagnostics.push(breaking(moduleName, previousEvent.id, previousEvent, null, "keep the old event version during consumer migration"));
      } else if (!deepEqual(previousEvent.payloadSchema, nextEvent.payloadSchema)) {
        diagnostics.push(breaking(moduleName, previousEvent.id, previousEvent.payloadSchema, nextEvent.payloadSchema, "publish a new event version and retain the old version"));
      }
    }
  }

  return {
    classification: diagnostics.some((entry) => entry.classification === "breaking")
      ? "breaking"
      : diagnostics.length > 0
        ? "migration-required"
        : "compatible",
    diagnostics,
  };
}

export async function syncChimpbaseModuleArtifacts(
  app: ChimpbaseAppDefinition,
  projectDir: string,
  options: SyncChimpbaseModuleArtifactsOptions = {},
): Promise<SyncChimpbaseModuleArtifactsResult> {
  const implementations = app.modules;
  const architecture = await checkChimpbaseModuleArchitecture(
    implementations.map((entry) => entry.interface),
    {
      compositionRoot: options.compositionRoot,
      modulesDir: options.modulesDir,
      projectDir,
    },
  );
  if (architecture.length > 0) {
    throw new Error(formatArchitectureDiagnostics(architecture));
  }

  const artifactsDir = resolve(projectDir, options.artifactsDir ?? "module-contracts");
  const databaseTypesPath = join(artifactsDir, "database.d.ts");
  const manifestPath = join(artifactsDir, "manifest.json");
  const summaryPath = join(artifactsDir, "architecture.txt");
  const previous = await readManifest(manifestPath);
  const manifest = generateChimpbaseModuleManifest(implementations);
  const compatibility = compareChimpbaseModuleManifests(previous, manifest);
  if (compatibility.classification === "breaking") {
    throw new Error(formatCompatibilityDiagnostics(compatibility.diagnostics));
  }

  const databaseTypesText = renderChimpbaseModuleDatabaseTypes(manifest);
  const manifestText = `${stableSerialize(manifest)}\n`;
  const summaryText = renderChimpbaseModuleSummary(manifest);
  const unchanged = await fileContentsEqual(databaseTypesPath, databaseTypesText)
    && await fileContentsEqual(manifestPath, manifestText)
    && await fileContentsEqual(summaryPath, summaryText);
  if (options.check === true) {
    if (!unchanged) throw new Error(`stale module artifacts in ${relative(projectDir, artifactsDir)}; run modules sync`);
    return {
      architecture,
      compatibility,
      manifest,
      paths: { databaseTypes: databaseTypesPath, manifest: manifestPath, summary: summaryPath },
      status: "unchanged",
    };
  }
  if (!unchanged) {
    await mkdir(artifactsDir, { recursive: true });
    await writeFile(databaseTypesPath, databaseTypesText, "utf8");
    await writeFile(manifestPath, manifestText, "utf8");
    await writeFile(summaryPath, summaryText, "utf8");
  }
  return {
    architecture,
    compatibility,
    manifest,
    paths: { databaseTypes: databaseTypesPath, manifest: manifestPath, summary: summaryPath },
    status: unchanged ? "unchanged" : "written",
  };
}

export function renderChimpbaseModuleDatabaseTypes(manifest: ChimpbaseModuleManifest): string {
  const lines = [
    "// Generated by `chimpbase modules sync`; do not edit.",
    "",
  ];
  for (const module of manifest.modules) {
    const typeName = `${module.name.split("-").map((part) =>
      `${part[0]?.toUpperCase() ?? ""}${part.slice(1)}`
    ).join("")}Database`;
    lines.push(`export interface ${typeName} {`);
    const names = [...(module.resources.tables ?? []), ...(module.resources.projections ?? [])].sort();
    for (const name of names) lines.push(`  ${JSON.stringify(name)}: Record<string, unknown>;`);
    lines.push("}", "");
  }
  return `${lines.join("\n")}\n`;
}

export function renderChimpbaseModuleSummary(manifest: ChimpbaseModuleManifest): string {
  const lines = ["Chimpbase module architecture", ""];
  for (const module of manifest.modules) {
    lines.push(`${module.name} v${module.version}`);
    lines.push(`  depends: ${module.dependencies.join(", ") || "none"}`);
    for (const call of module.calls) lines.push(`  call: ${call.id}`);
    for (const event of module.events) lines.push(`  event: ${event.id}`);
    for (const subscription of module.subscriptions) {
      lines.push(`  consumes: ${subscription.event} as ${subscription.name}`);
    }
  }
  return `${lines.join("\n")}\n`;
}

function readCompilerOptions(projectDir: string): ts.CompilerOptions {
  const configPath = ts.findConfigFile(projectDir, ts.sys.fileExists);
  if (configPath === undefined) {
    return { allowImportingTsExtensions: true, moduleResolution: ts.ModuleResolutionKind.Bundler };
  }
  const config = ts.readConfigFile(configPath, ts.sys.readFile);
  if (config.error !== undefined) throw new Error(ts.flattenDiagnosticMessageText(config.error.messageText, "\n"));
  return ts.parseJsonConfigFileContent(config.config, ts.sys, dirname(configPath)).options;
}

async function listTypeScriptFiles(root: string): Promise<string[]> {
  const files: string[] = [];
  for (const entry of await readdir(root, { withFileTypes: true })) {
    const path = join(root, entry.name);
    if (entry.isDirectory()) files.push(...await listTypeScriptFiles(path));
    else if (/\.[cm]?tsx?$/.test(entry.name) && !entry.name.endsWith(".d.ts")) files.push(path);
  }
  return files.sort();
}

function moduleForFile(
  modulesDir: string,
  file: string,
  interfacesByName: ReadonlyMap<string, ChimpbaseModuleInterface>,
): string | null {
  const path = relative(modulesDir, resolve(file));
  if (path.startsWith(`..${sep}`) || isAbsolute(path)) return null;
  const [moduleName] = path.split(sep);
  return moduleName !== undefined && interfacesByName.has(moduleName) ? moduleName : null;
}

function normalizeResolvedFile(file: string): string {
  return file.replace(/\.d\.[cm]?ts$/, ".ts");
}

function findDependencyCycleDiagnostics(
  interfaces: readonly ChimpbaseModuleInterface[],
): ChimpbaseModuleArchitectureDiagnostic[] {
  const byName = new Map(interfaces.map((entry) => [entry.name, entry]));
  const visiting: string[] = [];
  const visited = new Set<string>();
  const diagnostics: ChimpbaseModuleArchitectureDiagnostic[] = [];
  const visit = (name: string): void => {
    if (visited.has(name)) return;
    const cycleStart = visiting.indexOf(name);
    if (cycleStart >= 0) {
      const cycle = [...visiting.slice(cycleStart), name].join(" -> ");
      diagnostics.push({ file: "<module definitions>", rule: "cycle", sourceModule: name, target: cycle, targetModule: name });
      return;
    }
    visiting.push(name);
    for (const dependency of byName.get(name)?.dependencies ?? []) visit(dependency);
    visiting.pop();
    visited.add(name);
  };
  for (const name of [...byName.keys()].sort()) visit(name);
  return diagnostics;
}

function normalizeResources(resources: ChimpbaseModuleImplementation["resources"]): Record<string, readonly string[]> {
  const entries: Array<[string, readonly string[]]> = Object.entries(resources)
    .filter((entry): entry is [string, readonly string[]] => Array.isArray(entry[1]))
    .map(([key, values]) => [key, [...values].sort()]);
  entries.sort((left, right) => left[0].localeCompare(right[0]));
  return Object.fromEntries(entries);
}

function schemaAccepts(previous: unknown, next: unknown): boolean {
  if (deepEqual(previous, next)) return true;
  if (!isRecord(previous) || !isRecord(next)) return false;
  if (previous.type === "object" && next.type === "object") {
    const previousProperties = isRecord(previous.properties) ? previous.properties : {};
    const nextProperties = isRecord(next.properties) ? next.properties : {};
    const nextRequired = new Set(Array.isArray(next.required) ? next.required.filter((value): value is string => typeof value === "string") : []);
    for (const [name, schema] of Object.entries(previousProperties)) {
      const nextSchema = nextProperties[name];
      if (nextSchema === undefined || !schemaAccepts(schema, nextSchema)) return false;
    }
    for (const name of nextRequired) {
      if (!(name in previousProperties)) return false;
    }
    return true;
  }
  return false;
}

function schemaPreserves(previous: unknown, next: unknown): boolean {
  if (deepEqual(previous, next)) return true;
  if (!isRecord(previous) || !isRecord(next)) return false;
  if (previous.type !== "object" || next.type !== "object") return false;
  const previousProperties = isRecord(previous.properties) ? previous.properties : {};
  const nextProperties = isRecord(next.properties) ? next.properties : {};
  const previousRequired = new Set(
    Array.isArray(previous.required)
      ? previous.required.filter((value): value is string => typeof value === "string")
      : [],
  );
  const nextRequired = new Set(
    Array.isArray(next.required)
      ? next.required.filter((value): value is string => typeof value === "string")
      : [],
  );
  for (const [name, schema] of Object.entries(previousProperties)) {
    const nextSchema = nextProperties[name];
    if (nextSchema === undefined || !schemaPreserves(schema, nextSchema)) return false;
    if (previousRequired.has(name) && !nextRequired.has(name)) return false;
  }
  return true;
}

function breaking(
  module: string,
  contract: string,
  previous: unknown,
  next: unknown,
  remediation: string,
): ChimpbaseModuleCompatibilityDiagnostic {
  return { classification: "breaking", contract, module, next, previous, remediation };
}

function formatArchitectureDiagnostics(diagnostics: readonly ChimpbaseModuleArchitectureDiagnostic[]): string {
  return diagnostics.map((entry) =>
    `${entry.file}: ${entry.rule}: source ${entry.sourceModule ?? "<composition>"} -> target ${entry.targetModule} (${entry.target})`
  ).join("\n");
}

function formatCompatibilityDiagnostics(diagnostics: readonly ChimpbaseModuleCompatibilityDiagnostic[]): string {
  return diagnostics.map((entry) =>
    `${entry.module} ${entry.contract}: ${entry.classification}; previous=${stableSerialize(entry.previous)} next=${stableSerialize(entry.next)}; ${entry.remediation}`
  ).join("\n");
}

async function readManifest(path: string): Promise<ChimpbaseModuleManifest | null> {
  try {
    return JSON.parse(await readFile(path, "utf8")) as ChimpbaseModuleManifest;
  } catch (error) {
    if (isMissingFileError(error)) return null;
    throw error;
  }
}

async function fileContentsEqual(path: string, expected: string): Promise<boolean> {
  try {
    return await readFile(path, "utf8") === expected;
  } catch (error) {
    if (isMissingFileError(error)) return false;
    throw error;
  }
}

async function fileExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

function isMissingFileError(error: unknown): boolean {
  return error instanceof Error && "code" in error && error.code === "ENOENT";
}

function stableSerialize(value: unknown): string {
  return JSON.stringify(sortSerializableValue(value), null, 2);
}

function sortSerializableValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map((entry) => sortSerializableValue(entry));
  if (!isRecord(value)) return value;
  return Object.fromEntries(Object.entries(value)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, entry]) => [key, sortSerializableValue(entry)]));
}

function deepEqual(left: unknown, right: unknown): boolean {
  return stableSerialize(left) === stableSerialize(right);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
