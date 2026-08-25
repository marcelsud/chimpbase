import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import {
  normalizeProjectConfig,
  type ChimpbaseProjectConfig,
} from "@chimpbase/core";

export {
  normalizeProjectConfig,
  type ChimpbaseProjectConfig,
  type ChimpbaseProjectConfigInput,
} from "@chimpbase/core";

type TomlPrimitive = boolean | number | string;
interface TomlTable {
  [key: string]: TomlPrimitive | TomlTable;
}

export async function loadProjectConfig(
  projectDir: string,
  configFile = "chimpbase.toml",
): Promise<ChimpbaseProjectConfig> {
  const configPath = resolve(projectDir, configFile);
  const raw = await readFile(configPath, "utf8");
  const parsed = parseTomlDocument(raw);
  const project = getTable(parsed, "project");
  const server = getTable(parsed, "server");
  const storage = getTable(parsed, "storage");
  const worker = getTable(parsed, "worker");
  const secrets = getTable(parsed, "secrets");
  const subscriptions = getTable(parsed, "subscriptions");
  const subscriptionsIdempotency = getTable(subscriptions, "idempotency");
  const subscriptionsIdempotencyRetention = getTable(subscriptionsIdempotency, "retention");
  const kv = getTable(parsed, "kv");
  const kvRetention = getTable(kv, "retention");
  const workflows = getTable(parsed, "workflows");
  const telemetry = getTable(parsed, "telemetry");
  const telemetryPersist = getTable(telemetry, "persist");
  const telemetryRetention = getTable(telemetry, "retention");

  return normalizeProjectConfig({
    project: {
      name: readString(project, "name"),
    },
    server: {
      port: readNumber(server, "port"),
    },
    storage: {
      engine: readStorageEngine(storage),
      path: readNullableString(storage, "path"),
      url: readNullableString(storage, "url"),
    },
    subscriptions: {
      dispatch: readSubscriptionDispatchMode(subscriptions, "dispatch"),
      idempotency: {
        retention: {
          enabled: readBoolean(subscriptionsIdempotencyRetention, "enabled"),
          maxAgeDays: readNumber(subscriptionsIdempotencyRetention, "max_age_days"),
          schedule: readString(subscriptionsIdempotencyRetention, "schedule"),
        },
      },
    },
    worker: {
      leaseMs: readNumber(worker, "lease_ms"),
      maxAttempts: readNumber(worker, "max_attempts"),
      pollIntervalMs: readNumber(worker, "poll_interval_ms"),
      retryDelayMs: readNumber(worker, "retry_delay_ms"),
    },
    secrets: {
      dir: readNullableString(secrets, "dir"),
      envFile: readNullableString(secrets, "env_file"),
    },
    telemetry: {
      minLevel: readLogLevel(telemetry, "min_level"),
      persist: {
        log: readBoolean(telemetryPersist, "log"),
        metric: readBoolean(telemetryPersist, "metric"),
        trace: readBoolean(telemetryPersist, "trace"),
      },
      retention: {
        enabled: readBoolean(telemetryRetention, "enabled"),
        maxAgeDays: readNumber(telemetryRetention, "max_age_days"),
        schedule: readString(telemetryRetention, "schedule"),
      },
    },
    kv: {
      retention: {
        enabled: readBoolean(kvRetention, "enabled"),
        schedule: readString(kvRetention, "schedule"),
      },
    },
    workflows: {
      contractsDir: readNullableString(workflows, "contracts_dir"),
    },
  });
}

function parseTomlDocument(raw: string): TomlTable {
  const root: TomlTable = {};
  let currentTable = root;

  for (const line of raw.split(/\r?\n/)) {
    const trimmed = stripTomlComment(line).trim();
    if (!(trimmed.length > 0)) {
      continue;
    }

    if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
      const sectionName = trimmed.slice(1, -1).trim();
      const sectionPath = sectionName
        .split(".")
        .map((entry) => entry.trim())
        .filter(Boolean);

      currentTable = root;
      for (const segment of sectionPath) {
        const existing = currentTable[segment];
        if (isTomlTable(existing)) {
          currentTable = existing;
          continue;
        }

        const nextTable: TomlTable = {};
        currentTable[segment] = nextTable;
        currentTable = nextTable;
      }

      continue;
    }

    const separatorIndex = trimmed.indexOf("=");
    if (separatorIndex < 1) {
      continue;
    }

    const key = trimmed.slice(0, separatorIndex).trim();
    const value = trimmed.slice(separatorIndex + 1).trim();
    currentTable[key] = parseTomlValue(value);
  }

  return root;
}

function stripTomlComment(line: string): string {
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let escaped = false;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];

    if (escaped) {
      escaped = false;
      continue;
    }

    if (char === "\\" && inDoubleQuote) {
      escaped = true;
      continue;
    }

    if (char === '"' && !inSingleQuote) {
      inDoubleQuote = !inDoubleQuote;
      continue;
    }

    if (char === "'" && !inDoubleQuote) {
      inSingleQuote = !inSingleQuote;
      continue;
    }

    if (char === "#" && !inSingleQuote && !inDoubleQuote) {
      return line.slice(0, index);
    }
  }

  return line;
}

function parseTomlValue(rawValue: string): TomlPrimitive {
  if (rawValue.startsWith('"') && rawValue.endsWith('"')) {
    return JSON.parse(rawValue) as string;
  }

  if (rawValue.startsWith("'") && rawValue.endsWith("'")) {
    return rawValue.slice(1, -1);
  }

  if (rawValue === "true") {
    return true;
  }

  if (rawValue === "false") {
    return false;
  }

  if (/^[+-]?\d+$/.test(rawValue)) {
    return Number(rawValue);
  }

  if (/^[+-]?\d+\.\d+$/.test(rawValue)) {
    return Number(rawValue);
  }

  return rawValue;
}

function readStorageEngine(table: TomlTable | undefined): "memory" | "postgres" | "sqlite" | undefined {
  const value = readString(table, "engine");
  return value === "memory" || value === "postgres" || value === "sqlite"
    ? value
    : undefined;
}

function readLogLevel(table: TomlTable | undefined, key: string): "debug" | "error" | "info" | "warn" | undefined {
  const value = readString(table, key);
  return value === "debug" || value === "info" || value === "warn" || value === "error"
    ? value
    : undefined;
}

function readSubscriptionDispatchMode(table: TomlTable | undefined, key: string): "async" | "sync" | undefined {
  const value = readString(table, key);
  return value === "async" || value === "sync" ? value : undefined;
}

function readString(table: TomlTable | undefined, key: string): string | undefined {
  const value = table?.[key];
  return typeof value === "string" ? value : undefined;
}

function readNullableString(table: TomlTable | undefined, key: string): string | null | undefined {
  const value = table?.[key];
  return typeof value === "string" ? value : undefined;
}

function readNumber(table: TomlTable | undefined, key: string): number | undefined {
  const value = table?.[key];
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function readBoolean(table: TomlTable | undefined, key: string): boolean | undefined {
  const value = table?.[key];
  return typeof value === "boolean" ? value : undefined;
}

function getTable(table: TomlTable | undefined, key: string): TomlTable | undefined {
  const value = table?.[key];
  return isTomlTable(value) ? value : undefined;
}

function isTomlTable(value: TomlPrimitive | TomlTable | undefined): value is TomlTable {
  return Boolean((value !== null && value !== undefined) && typeof value === "object" && !Array.isArray(value));
}
