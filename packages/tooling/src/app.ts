import { access } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, relative, resolve } from "node:path";
import { pathToFileURL } from "node:url";

import {
  defineChimpbaseApp,
  type ChimpbaseAppDefinition,
  type ChimpbaseAppDefinitionInput,
} from "@chimpbase/core";
import {
  hasChimpbaseActionRegistrationName,
  isChimpbaseActionRegistration,
  isJsonObject,
  setChimpbaseActionRegistrationName,
} from "@chimpbase/runtime";

const PROJECT_APP_MODULE_FILE = "chimpbase.app.ts";
const require = createRequire(import.meta.url);

export async function loadProjectAppDefinition(projectDir: string): Promise<ChimpbaseAppDefinition | null> {
  const resolvedProjectDir = resolve(projectDir);
  const appModulePath = await resolveProjectAppModulePath(resolvedProjectDir);
  if (!(appModulePath !== null && appModulePath.length > 0)) {
    return null;
  }

  return await loadChimpbaseAppDefinitionModule(appModulePath, resolvedProjectDir);
}

export async function resolveProjectAppModulePath(projectDir: string): Promise<string | null> {
  const path = resolve(projectDir, PROJECT_APP_MODULE_FILE);
  if (await fileExists(path)) {
    return path;
  }

  return null;
}

export async function loadChimpbaseAppDefinitionModule(
  modulePath: string,
  projectDir = dirname(modulePath),
): Promise<ChimpbaseAppDefinition> {
  const resolvedModulePath = resolve(modulePath);
  // Bun shares the require/import cache and ignores query strings on file imports.
  delete require.cache[resolvedModulePath];
  const moduleUrl = pathToFileURL(resolvedModulePath);
  moduleUrl.searchParams.set("chimpbase_load", globalThis.crypto.randomUUID());
  const moduleExports: unknown = await import(moduleUrl.href);
  if (!isJsonObject(moduleExports)) {
    throw new Error(`project app module did not resolve to a module namespace: ${modulePath}`);
  }

  return coerceChimpbaseAppDefinition(moduleExports, modulePath, projectDir);
}

function coerceChimpbaseAppDefinition(
  moduleExports: Record<string, unknown>,
  modulePath: string,
  projectDir: string,
): ChimpbaseAppDefinition {
  inferUnnamedActionExportNames(moduleExports, modulePath, projectDir);

  const candidate = moduleExports.default ?? moduleExports.app;
  if (!(candidate !== null && candidate !== undefined) || typeof candidate !== "object") {
    throw new Error(`project app module must export a default object or named "app": ${modulePath}`);
  }

  return defineChimpbaseApp(candidate as ChimpbaseAppDefinitionInput);
}

function inferUnnamedActionExportNames(
  moduleExports: Record<string, unknown>,
  modulePath: string,
  projectDir: string,
): void {
  const relativePath = relative(projectDir, modulePath);
  const relativeModulePath = toPosixPath(relativePath.length > 0 ? relativePath : modulePath);

  for (const [exportName, value] of Object.entries(moduleExports)) {
    if (exportName === "default" || exportName === "app") {
      continue;
    }

    if (!isChimpbaseActionRegistration(value) || hasChimpbaseActionRegistrationName(value)) {
      continue;
    }

    setChimpbaseActionRegistrationName(value, `${relativeModulePath}#${exportName}`);
  }
}

function toPosixPath(path: string): string {
  return path.replaceAll("\\", "/");
}

async function fileExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}
