import { execFile } from "node:child_process";
import { cp, mkdir, mkdtemp, readFile, readdir, rm, stat, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "..");
const packageJsonPath = resolve(repoRoot, "package.json");

const expectedExports = {
  ".": "./packages/runtime/dist/index.js",
  "./auth": "./packages/auth/dist/src/index.js",
  "./blobs": "./packages/blobs/dist/src/index.js",
  "./core": "./packages/core/dist/index.js",
  "./host": "./packages/host/dist/src/index.js",
  "./mesh": "./packages/mesh/dist/src/index.js",
  "./otel": "./packages/otel/dist/src/index.js",
  "./pact": "./packages/pact/dist/src/index.js",
  "./postgres": "./packages/postgres/dist/src/index.js",
  "./rest-collections": "./packages/rest-collections/dist/src/index.js",
  "./runtime": "./packages/runtime/dist/index.js",
  "./runtime/bun": "./packages/bun/dist/src/library.js",
  "./runtime/bun/cli": "./packages/bun/dist/src/cli.js",
  "./runtime/deno": "./packages/deno/dist/src/library.js",
  "./runtime/deno/cli": "./packages/deno/dist/src/cli.js",
  "./runtime/node": "./packages/node/dist/src/library.js",
  "./runtime/node/cli": "./packages/node/dist/src/cli.js",
  "./tooling": "./packages/tooling/dist/src/index.js",
  "./tooling/app": "./packages/tooling/dist/src/app.js",
  "./tooling/cli": "./packages/tooling/dist/src/cli.js",
  "./tooling/config": "./packages/tooling/dist/src/config.js",
  "./tooling/migrations": "./packages/tooling/dist/src/migrations.js",
  "./tooling/modules": "./packages/tooling/dist/src/modules.js",
  "./tooling/postgres_docker": "./packages/tooling/dist/src/postgres_docker.js",
  "./tooling/schema": "./packages/tooling/dist/src/schema.js",
  "./tooling/secrets": "./packages/tooling/dist/src/secrets.js",
  "./tooling/workflow_contracts": "./packages/tooling/dist/src/workflow_contracts.js",
  "./webhooks": "./packages/webhooks/dist/src/index.js",
};

const expectedFiles = [
  "LICENSE",
  "NOTICE",
  "packages/auth/dist",
  "packages/blobs/dist",
  "packages/bun/dist",
  "packages/core/dist",
  "packages/deno/dist",
  "packages/host/dist",
  "packages/mesh/dist",
  "packages/node/dist",
  "packages/otel/dist",
  "packages/pact/dist",
  "packages/postgres/dist",
  "packages/rest-collections/dist",
  "packages/runtime/dist",
  "packages/tooling/dist",
  "packages/webhooks/dist",
];

const expectedDependencies = {
  "@opentelemetry/api": "^1.9.0",
  "@opentelemetry/api-logs": "^0.200.0",
  "@opentelemetry/context-async-hooks": "^2.0.0",
  "@opentelemetry/exporter-logs-otlp-http": "^0.200.0",
  "@opentelemetry/exporter-metrics-otlp-http": "^0.200.0",
  "@opentelemetry/exporter-trace-otlp-http": "^0.200.0",
  "@opentelemetry/resources": "^2.0.0",
  "@opentelemetry/sdk-logs": "^0.200.0",
  "@opentelemetry/sdk-metrics": "^2.0.0",
  "@opentelemetry/sdk-trace-base": "^2.0.0",
  "@types/pg": "^8.15.6",
  kysely: "^0.28.11",
  pg: "^8.16.3",
  typescript: "^5.9.3",
};
const expectedScripts = {
  "build:publish:packages": "node ./scripts/build-package.mjs --publish runtime core tooling postgres host deno node bun rest-collections otel auth blobs webhooks pact mesh",
  "build:check": "node ./scripts/build-package.mjs --check",
  prepack: "bun run build:publish:packages && bun run smoke:package",
  "publish:dry-run": "npm pack --dry-run",
  "release:publish": "npm publish",
};
const cliSmokeCommand = "__chimpbase_package_smoke__";
const cliSmokeError = `unsupported command: ${cliSmokeCommand}`;


const args = process.argv.slice(2);
if (args.length > 1 || (args.length === 1 && args[0] !== "--check-config")) {
  throw new Error(`unsupported package smoke option: ${args.join(" ")}`);
}
const checkConfigOnly = args[0] === "--check-config";

let packageJson;
try {
  packageJson = JSON.parse(await readFile(packageJsonPath, "utf8"));
} catch (error) {
  throw new Error(`invalid root package.json: ${error.message}`, { cause: error });
}

validatePackageConfig(packageJson);

if (checkConfigOnly) {
  console.log("chimpbase package config smoke passed");
} else {
  await validateBuiltExports(packageJson.exports);
  await rejectInternalSpecifiers();
  await validateStagedPackage(packageJson.exports);
  console.log("chimpbase package smoke passed");
}

function validatePackageConfig(config) {
  if (config.name !== "chimpbase") {
    throw new Error(`root package name must be chimpbase, got ${JSON.stringify(config.name)}`);
  }
  if (config.private !== false) {
    throw new Error("root package must set private to false");
  }
  if (config.type !== "module") {
    throw new Error("root package must set type to module");
  }
  assertExactObject(config.dependencies, expectedDependencies, "root dependencies");
  for (const [scriptName, command] of Object.entries(expectedScripts)) {
    if (config.scripts?.[scriptName] !== command) {
      throw new Error(`root script ${scriptName} must be exactly ${command}`);
    }
  }
  if (JSON.stringify(config.files) !== JSON.stringify(expectedFiles)) {
    throw new Error(`root package files must be exactly ${JSON.stringify(expectedFiles)}`);
  }

  const actualExportKeys = Object.keys(config.exports ?? {}).sort();
  const expectedExportKeys = Object.keys(expectedExports).sort();
  if (JSON.stringify(actualExportKeys) !== JSON.stringify(expectedExportKeys)) {
    throw new Error(`root package exports must be exactly ${JSON.stringify(expectedExportKeys)}`);
  }

  for (const [exportName, defaultTarget] of Object.entries(expectedExports)) {
    const conditions = config.exports[exportName];
    if (conditions === null || typeof conditions !== "object" || Array.isArray(conditions)) {
      throw new Error(`export ${exportName} must define types and default conditions`);
    }
    const conditionNames = Object.keys(conditions);
    if (JSON.stringify(conditionNames) !== JSON.stringify(["types", "import", "default"])) {
      throw new Error(`export ${exportName} conditions must be exactly types, import, and default`);
    }

    const typesTarget = defaultTarget.replace(/\.js$/, ".d.ts");
    if (
      conditions.types !== typesTarget
      || conditions.import !== defaultTarget
      || conditions.default !== defaultTarget
    ) {
      throw new Error(
        `export ${exportName} must map types to ${typesTarget} and import/default to ${defaultTarget}`,
      );
    }
  }
}

async function validateStagedPackage(exports) {
  const stageRoot = await mkdtemp(join(tmpdir(), "chimpbase-package-smoke-"));

  try {
    await cp(packageJsonPath, resolve(stageRoot, "package.json"));
    const distRoots = new Set(
      Object.values(expectedExports).map((target) =>
        resolve(repoRoot, target.slice(0, target.indexOf("/dist/") + "/dist".length)),
      ),
    );

    for (const distRoot of distRoots) {
      const stagedDistRoot = resolve(stageRoot, relative(repoRoot, distRoot));
      await mkdir(dirname(stagedDistRoot), { recursive: true });
      await cp(distRoot, stagedDistRoot, { recursive: true });
    }

    await symlink(resolve(repoRoot, "node_modules"), resolve(stageRoot, "node_modules"), "dir");
    const importsByRuntime = {
      node: [],
      bun: [],
      deno: [],
    };
    for (const [exportName, conditions] of Object.entries(exports)) {
      if (exportName.startsWith("./runtime/") && exportName.endsWith("/cli")) {
        continue;
      }
      const runtime = exportName === "./runtime/bun"
        ? "bun"
        : exportName === "./runtime/deno"
          ? "deno"
          : "node";
      importsByRuntime[runtime].push(pathToFileURL(resolve(stageRoot, conditions.default)).href);
    }

    for (const [runtime, command, commandArgs] of [
      ["node", process.execPath, ["--input-type=module", "--eval"]],
      ["bun", "bun", ["--eval"]],
      ["deno", "deno", ["eval"]],
    ]) {
      const source = `for (const specifier of ${JSON.stringify(importsByRuntime[runtime])}) await import(specifier);`;
      await runSubprocess(`${runtime} library exports`, command, [...commandArgs, source], stageRoot);
    }

    for (const [exportName, command, commandArgs] of [
      ["./runtime/node/cli", process.execPath, []],
      ["./runtime/bun/cli", "bun", []],
      ["./runtime/deno/cli", "deno", ["run", "--allow-env"]],
    ]) {
      const target = resolve(stageRoot, exports[exportName].default);
      await runExpectedFailure(
        `${exportName} sentinel command`,
        command,
        [...commandArgs, target, cliSmokeCommand],
        stageRoot,
        cliSmokeError,
      );
    }
  } finally {
    await rm(stageRoot, { force: true, recursive: true });
  }
}

async function runSubprocess(label, command, args, cwd) {
  try {
    await new Promise((resolve, reject) => {
      execFile(command, args, { cwd }, (error, stdout, stderr) => {
        if (error) {
          error.stdout = stdout;
          error.stderr = stderr;
          reject(error);
        } else {
          resolve();
        }
      });
    });
  } catch (error) {
    throw new Error(
      `${label} failed: stdout=${conciseOutput(error.stdout)}; stderr=${conciseOutput(error.stderr)}`,
      { cause: error },
    );
  }
}

async function runExpectedFailure(label, command, args, cwd, expectedMessage) {
  const result = await new Promise((resolve) => {
    execFile(command, args, { cwd }, (error, stdout, stderr) => {
      resolve({ error, stdout, stderr });
    });
  });
  const lines = `${result.stdout}\n${result.stderr}`.split(/\r?\n/);

  if (result.error && lines.some((line) => line.trim().endsWith(expectedMessage))) {
    return;
  }

  throw new Error(
    `${label} did not fail with ${JSON.stringify(expectedMessage)}: `
      + `stdout=${conciseOutput(result.stdout)}; stderr=${conciseOutput(result.stderr)}`,
    { cause: result.error },
  );
}

function conciseOutput(output) {
  const text = output?.trim() ?? "";
  return JSON.stringify(text.length > 1_000 ? `${text.slice(0, 1_000)}…` : text);
}

async function validateBuiltExports(exports) {
  for (const [exportName, conditions] of Object.entries(exports)) {
    for (const condition of ["types", "default"]) {
      const target = resolveExportTarget(exportName, condition, conditions[condition]);
      let targetStat;
      try {
        targetStat = await stat(target);
      } catch (error) {
        throw new Error(`missing ${condition} target for export ${exportName}: ${conditions[condition]}`, {
          cause: error,
        });
      }
      if (!targetStat.isFile()) {
        throw new Error(`${condition} target for export ${exportName} is not a file: ${conditions[condition]}`);
      }
    }
  }
}

function resolveExportTarget(exportName, condition, target) {
  if (typeof target !== "string" || !target.startsWith("./")) {
    throw new Error(`${condition} target for export ${exportName} must be a relative package path`);
  }
  const resolved = resolve(repoRoot, target);
  if (resolved === repoRoot || !resolved.startsWith(`${repoRoot}/`)) {
    throw new Error(`${condition} target for export ${exportName} escapes the package root: ${target}`);
  }
  return resolved;
}

async function rejectInternalSpecifiers() {
  const distRoots = new Set(
    Object.values(expectedExports).map((target) =>
      resolve(repoRoot, target.slice(0, target.indexOf("/dist/") + "/dist".length)),
    ),
  );

  for (const distRoot of distRoots) {
    for (const file of await listBuiltFiles(distRoot)) {
      const source = await readFile(file, "utf8");
      if (/(\b(?:from|module)\s+|\b(?:import|require)\s*(?:\(\s*)?|<reference\s+types\s*=\s*)(["'])@chimpbase\/[^"'\\\r\n]+\2/.test(source)) {
        throw new Error(`built file retains an internal @chimpbase specifier: ${file}`);
      }
    }
  }
}

async function listBuiltFiles(dir) {
  const entries = await readdir(dir, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...await listBuiltFiles(path));
    } else if (entry.isFile() && (entry.name.endsWith(".js") || entry.name.endsWith(".d.ts"))) {
      files.push(path);
    }
  }

  return files;
}

function assertExactObject(actual, expected, label) {
  if (actual === null || typeof actual !== "object" || Array.isArray(actual)) {
    throw new Error(`${label} must be an object`);
  }
  const actualEntries = Object.entries(actual).sort(([left], [right]) => left.localeCompare(right));
  const expectedEntries = Object.entries(expected).sort(([left], [right]) => left.localeCompare(right));
  if (JSON.stringify(actualEntries) !== JSON.stringify(expectedEntries)) {
    throw new Error(`${label} do not match the publishable package contract`);
  }
}
