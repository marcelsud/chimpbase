import { spawnSync } from "node:child_process";
import { readFile, readdir, rm, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "..");
const tscEntrypoint = resolve(repoRoot, "node_modules/typescript/bin/tsc");
const packageJsonPath = resolve(repoRoot, "package.json");
const tsconfigPath = resolve(repoRoot, "tsconfig.json");

const packageConfigs = {
  auth: {
    dir: "packages/auth",
    types: ["node"],
  },
  blobs: {
    dir: "packages/blobs",
    types: ["node"],
  },
  bun: {
    dir: "packages/bun",
    types: ["bun-types", "node"],
  },
  core: {
    dir: "packages/core",
    types: ["node"],
  },
  host: {
    dir: "packages/host",
    types: ["node"],
  },
  mesh: {
    dir: "packages/mesh",
    types: ["node"],
  },
  deno: {
    dir: "packages/deno",
    types: ["node"],
  },
  node: {
    dir: "packages/node",
    types: ["node"],
  },
  otel: {
    dir: "packages/otel",
    types: ["node"],
  },
  pact: {
    dir: "packages/pact",
    types: ["node"],
  },
  postgres: {
    dir: "packages/postgres",
    types: ["node"],
  },
  "rest-collections": {
    dir: "packages/rest-collections",
    types: ["node"],
  },
  runtime: {
    dir: "packages/runtime",
    types: ["node"],
  },
  tooling: {
    dir: "packages/tooling",
    types: ["node"],
  },
  webhooks: {
    dir: "packages/webhooks",
    types: ["node"],
  },
};
const packageJson = await readJson(packageJsonPath, "root package.json");
const tsconfig = await readJson(tsconfigPath, "root tsconfig.json");
const resolveInternalSpecifier = createInternalSpecifierResolver(packageJson, tsconfig);

const args = process.argv.slice(2);
const option = args[0]?.startsWith("--") ? args.shift() : null;
if (option && option !== "--check" && option !== "--publish") {
  throw new Error(`unsupported package build option: ${option}`);
}
const checkOnly = option === "--check";
const publishBuild = option === "--publish";
const packageNames = args.length > 0
  ? args
  : checkOnly
    ? []
    : Object.keys(packageConfigs);

for (const packageName of packageNames) {
  if (!(packageName in packageConfigs)) {
    throw new Error(`unsupported package build target: ${packageName}`);
  }
}

if (checkOnly && packageNames.length === 0) {
  const result = spawnSync(
    process.execPath,
    [tscEntrypoint, "--noEmit", "--project", resolve(repoRoot, "tsconfig.json")],
    {
      cwd: repoRoot,
      stdio: "inherit",
    },
  );

  if (result.status !== 0) {
    throw new Error("failed to check package build");
  }
}

for (const packageName of packageNames) {
  const config = packageConfigs[packageName];
  const packageDir = resolve(repoRoot, config.dir);
  const outDir = resolve(packageDir, "dist");
  const sourceFiles = await listTypescriptFiles(packageDir);

  if (sourceFiles.length === 0) {
    throw new Error(`no TypeScript files found for ${packageName}`);
  }

  if (!checkOnly) {
    await rm(outDir, { force: true, recursive: true });
  }

  const result = spawnSync(
    process.execPath,
    [
      tscEntrypoint,
      "--allowImportingTsExtensions",
      "--lib",
      "ES2022,DOM",
      "--module",
      "NodeNext",
      "--moduleResolution",
      "NodeNext",
      "--pretty",
      "false",
      "--rewriteRelativeImportExtensions",
      "--rootDir",
      packageDir,
      "--skipLibCheck",
      "--strict",
      "--target",
      "ES2022",
      "--types",
      config.types.join(","),
      ...(checkOnly
        ? ["--noEmit"]
        : [
            "--declaration",
            "--declarationMap",
            "false",
            "--outDir",
            outDir,
            "--sourceMap",
            "false",
          ]),
      ...sourceFiles,
    ],
    {
      cwd: repoRoot,
      stdio: "inherit",
    },
  );

  if (result.status !== 0) {
    throw new Error(`failed to ${checkOnly ? "check" : "build"} ${packageName}`);
  }

}
if (publishBuild) {
  for (const packageName of packageNames) {
    await rewriteInternalSpecifiers(resolve(repoRoot, packageConfigs[packageName].dir, "dist"));
  }
}

async function listTypescriptFiles(dir) {
  const entries = await readdir(dir, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    if (entry.name === "dist" || entry.name === "node_modules") {
      continue;
    }

    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...await listTypescriptFiles(path));
      continue;
    }

    if (entry.isFile() && entry.name.endsWith(".ts")) {
      files.push(path);
    }
  }

  return files.sort();
}

async function rewriteInternalSpecifiers(dir) {
  const emittedFiles = await listEmittedFiles(dir);

  for (const file of emittedFiles) {
    const source = await readFile(file, "utf8");
    const rewritten = source.replace(
      /(\b(?:from|module)\s+|\b(?:import|require)\s*(?:\(\s*)?|<reference\s+types\s*=\s*)(["'])(@chimpbase\/[^"'\\\r\n]+)\2/g,
      (_, prefix, quote, specifier) =>
        `${prefix}${quote}${resolveInternalSpecifier(specifier)}${quote}`,
    );

    if (rewritten !== source) {
      await writeFile(file, rewritten);
    }
  }
}

async function listEmittedFiles(dir) {
  const entries = await readdir(dir, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...await listEmittedFiles(path));
    } else if (entry.isFile() && (entry.name.endsWith(".js") || entry.name.endsWith(".d.ts"))) {
      files.push(path);
    }
  }

  return files;
}

async function readJson(path, label) {
  try {
    return JSON.parse(await readFile(path, "utf8"));
  } catch (error) {
    throw new Error(`invalid ${label}: ${error.message}`, { cause: error });
  }
}

function createInternalSpecifierResolver(packageJson, tsconfig) {
  const exports = packageJson.exports;
  const paths = tsconfig.compilerOptions?.paths;
  if (exports === null || typeof exports !== "object" || Array.isArray(exports)) {
    throw new Error("root package exports must be an object");
  }
  if (paths === null || typeof paths !== "object" || Array.isArray(paths)) {
    throw new Error("root tsconfig paths must be an object");
  }

  const publicAliases = Object.entries(paths).filter(
    ([specifier]) => specifier === "chimpbase" || specifier.startsWith("chimpbase/"),
  );
  const expectedExportKeys = publicAliases.map(([specifier]) =>
    specifier === "chimpbase" ? "." : `./${specifier.slice("chimpbase/".length)}`
  ).sort();
  const actualExportKeys = Object.keys(exports).sort();
  if (JSON.stringify(actualExportKeys) !== JSON.stringify(expectedExportKeys)) {
    throw new Error("root package exports and public tsconfig aliases must declare identical subpaths");
  }

  const publicSpecifiersBySource = new Map();
  for (const [specifier, targets] of publicAliases) {
    const sourceTarget = singlePathTarget(specifier, targets, false);
    const exportKey = specifier === "chimpbase" ? "." : `./${specifier.slice("chimpbase/".length)}`;
    const conditions = exports[exportKey];
    const expectedDefault = emittedTarget(sourceTarget, ".js");
    const expectedTypes = emittedTarget(sourceTarget, ".d.ts");
    if (
      conditions === null
      || typeof conditions !== "object"
      || Array.isArray(conditions)
      || JSON.stringify(Object.keys(conditions)) !== JSON.stringify(["types", "import", "default"])
      || conditions.types !== expectedTypes
      || conditions.import !== expectedDefault
      || conditions.default !== expectedDefault
    ) {
      throw new Error(
        `export ${exportKey} must match tsconfig alias ${specifier} with types, import, and default targets`,
      );
    }

    const canonicalTarget = canonicalSourceTarget(sourceTarget);
    const specifiers = publicSpecifiersBySource.get(canonicalTarget) ?? [];
    specifiers.push(specifier);
    publicSpecifiersBySource.set(canonicalTarget, specifiers);
  }

  const internalAliases = Object.entries(paths).filter(([specifier]) =>
    specifier.startsWith("@chimpbase/")
  );
  for (const [specifier, targets] of internalAliases) {
    singlePathTarget(specifier, targets, specifier.endsWith("/*"));
  }

  const resolveSpecifier = (specifier) => {
    const matches = internalAliases.filter(([alias]) =>
      alias.endsWith("/*")
        ? specifier.startsWith(alias.slice(0, -1))
        : specifier === alias
    );
    if (matches.length !== 1) {
      throw new Error(`internal specifier ${specifier} does not match exactly one tsconfig alias`);
    }

    const [alias, targets] = matches[0];
    const suffix = alias.endsWith("/*") ? specifier.slice(alias.length - 1) : "";
    const sourceTarget = singlePathTarget(alias, targets, alias.endsWith("/*")).replace("*", suffix);
    const candidates = publicSpecifiersBySource.get(canonicalSourceTarget(sourceTarget)) ?? [];
    const preferred = internalPublicRoot(specifier);
    const publicSpecifier = candidates.includes(preferred)
      ? preferred
      : candidates.length === 1
        ? candidates[0]
        : null;
    if (publicSpecifier === null) {
      throw new Error(
        `internal specifier ${specifier} resolves to ${sourceTarget}, which has no exact declared chimpbase export`,
      );
    }
    return publicSpecifier;
  };

  for (const [alias] of internalAliases) {
    if (!alias.endsWith("/*")) {
      resolveSpecifier(alias);
    }
  }
  return resolveSpecifier;
}

function singlePathTarget(specifier, targets, wildcard) {
  if (
    !Array.isArray(targets)
    || targets.length !== 1
    || typeof targets[0] !== "string"
    || (wildcard ? (specifier.match(/\*/g)?.length !== 1 || targets[0].match(/\*/g)?.length !== 1) : targets[0].includes("*"))
  ) {
    throw new Error(`tsconfig alias ${specifier} must have exactly one matching path target`);
  }
  return targets[0];
}

function canonicalSourceTarget(target) {
  return target.replace(/^\.\//, "").replace(/\.(?:[cm]?[jt]sx?)$/, "");
}

function emittedTarget(sourceTarget, extension) {
  const match = sourceTarget.match(/^\.\/packages\/([^/]+)\/(.+)\.(?:[cm]?tsx?)$/);
  if (match === null || sourceTarget.includes("*")) {
    throw new Error(`public tsconfig alias has unsupported source target: ${sourceTarget}`);
  }
  return `./packages/${match[1]}/dist/${match[2]}${extension}`;
}

function internalPublicRoot(specifier) {
  const packageName = specifier.slice("@chimpbase/".length).split("/", 1)[0];
  if (packageName === "runtime") {
    return "chimpbase/runtime";
  }
  if (packageName === "node" || packageName === "bun" || packageName === "deno") {
    return `chimpbase/runtime/${packageName}`;
  }
  return `chimpbase/${packageName}`;
}
