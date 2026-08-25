import type { Dirent } from "node:fs";
import { readdir, readFile } from "node:fs/promises";
import { isAbsolute, resolve } from "node:path";

import type { ChimpbaseProjectConfig, ChimpbaseSecretsSource } from "@chimpbase/core";
import { tryParseJson } from "@chimpbase/runtime";

const DEFAULT_ENV_FILE = ".env";
const DEFAULT_SECRETS_DIR = "/run/secrets";

export interface SecretStore extends ChimpbaseSecretsSource {}

export interface LoadLocalSecretStoreOptions {
  env?: Record<string, string | undefined>;
  envFileDefault?: string | null;
  secretsDirDefault?: string | null;
}

export async function loadLocalSecretStore(
  projectDir: string,
  config: ChimpbaseProjectConfig,
  options: LoadLocalSecretStoreOptions = {},
): Promise<SecretStore> {
  const values = new Map<string, string>();

  const envFilePath = resolveLocalPath(
    projectDir,
    config.secrets.envFile ?? options.envFileDefault ?? DEFAULT_ENV_FILE,
  );
  if ((envFilePath !== null && envFilePath.length > 0)) {
    await preloadDotenvFile(envFilePath, values);
  }

  for (const [name, value] of Object.entries(options.env ?? process.env)) {
    if (typeof value === "string") {
      values.set(name, value);
    }
  }

  const secretsDirPath = resolveLocalPath(
    projectDir,
    config.secrets.dir ?? options.secretsDirDefault ?? DEFAULT_SECRETS_DIR,
  );
  if ((secretsDirPath !== null && secretsDirPath.length > 0)) {
    await preloadSecretDirectory(secretsDirPath, values);
  }

  return {
    get(name: string): string | null {
      return values.get(name) ?? null;
    },
  };
}

export function parseDotenv(raw: string): Map<string, string> {
  const values = new Map<string, string>();

  for (const line of raw.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!(trimmed.length > 0) || trimmed.startsWith("#")) {
      continue;
    }

    const normalized = trimmed.startsWith("export ")
      ? trimmed.slice("export ".length).trimStart()
      : trimmed;
    const separatorIndex = normalized.indexOf("=");
    if (separatorIndex < 1) {
      continue;
    }

    const name = normalized.slice(0, separatorIndex).trim();
    const rawValue = normalized.slice(separatorIndex + 1);
    values.set(name, parseDotenvValue(rawValue));
  }

  return values;
}

async function preloadDotenvFile(path: string, values: Map<string, string>): Promise<void> {
  let raw: string;
  try {
    raw = await readFile(path, "utf8");
  } catch {
    return;
  }

  for (const [name, value] of parseDotenv(raw)) {
    values.set(name, value);
  }
}

async function preloadSecretDirectory(path: string, values: Map<string, string>): Promise<void> {
  let entries: Dirent[];
  try {
    entries = await readdir(path, { withFileTypes: true });
  } catch {
    return;
  }

  for (const entry of entries) {
    if (!entry.isFile()) {
      continue;
    }

    values.set(entry.name, await readFile(resolve(path, entry.name), "utf8"));
  }
}

function resolveLocalPath(projectDir: string, path: string | null): string | null {
  if (!(path !== null && path.length > 0)) {
    return null;
  }

  return isAbsolute(path) ? path : resolve(projectDir, path);
}

function parseDotenvValue(raw: string): string {
  const trimmed = raw.trim();

  if (trimmed.startsWith('"') && trimmed.endsWith('"')) {
    const decoded = tryParseJson(trimmed);
    return typeof decoded === "string" ? decoded : trimmed.slice(1, -1);
  }

  if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
    return trimmed.slice(1, -1);
  }

  const commentIndex = trimmed.indexOf(" #");
  return commentIndex >= 0 ? trimmed.slice(0, commentIndex).trimEnd() : trimmed;
}
