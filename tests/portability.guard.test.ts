import { describe, expect, test } from "bun:test";
import { readdir, readFile } from "node:fs/promises";
import { join, relative, resolve } from "node:path";

const repoRoot = resolve(import.meta.dir, "..");

const bannedChecks = [
  {
    label: "node builtins",
    pattern: /\bfrom\s+["']node:|\bimport\s*\(\s*["']node:|\brequire\s*\(\s*["']node:/,
  },
  {
    label: "Bun global",
    pattern: /\bBun\b/,
  },
  {
    label: "Deno global",
    pattern: /\bDeno\b/,
  },
  {
    label: "process global",
    pattern: /\bprocess\b/,
  },
] as const;

const allowedPortableImports = [
  {
    file: "packages/runtime/index.ts",
    label: "node builtins",
    pattern: /\bfrom\s+["']node:async_hooks["']/,
  },
] as const;

describe("portable package guards", () => {
  test("importing the host leaves the TypeScript compiler unloaded", () => {
    const result = Bun.spawnSync([
      process.execPath,
      "--eval",
      `await import("./packages/host/src/library.ts");
       if (Object.keys(require.cache).some(path => path.endsWith("/typescript/lib/typescript.js"))) {
         throw new Error("host import loaded the TypeScript compiler");
       }`,
    ], { cwd: repoRoot, stdout: "pipe", stderr: "pipe" });
    expect(result.stderr.toString()).toBe("");
    expect(result.exitCode).toBe(0);
  });

  for (const packageName of ["core", "runtime", "postgres", "rest-collections"] as const) {
    test(`@chimpbase/${packageName} stays free of host-specific APIs`, async () => {
      const packageDir = resolve(repoRoot, "packages", packageName);
      const files = await listTypescriptFiles(packageDir);
      const violations: string[] = [];

      for (const file of files) {
        const source = stripComments(await readFile(file, "utf8"));

        for (const check of bannedChecks) {
          if (
            check.pattern.test(source)
            && !isAllowedPortableImport(relative(repoRoot, file), check.label, source)
          ) {
            violations.push(`${relative(repoRoot, file)}: found ${check.label}`);
          }
        }
      }

      expect(violations).toEqual([]);
    });
  }

  test("@chimpbase/deno stays free of Bun globals", async () => {
    const packageDir = resolve(repoRoot, "packages", "deno");
    const files = await listTypescriptFiles(packageDir);
    const violations: string[] = [];

    for (const file of files) {
      const source = stripComments(await readFile(file, "utf8"));
      if (/\bBun\b/.test(source)) {
        violations.push(`${relative(repoRoot, file)}: found Bun global`);
      }
    }

    expect(violations).toEqual([]);
  });

  test("@chimpbase/core publishes the internal files its entrypoint imports", async () => {
    const manifest = JSON.parse(
      await readFile(resolve(repoRoot, "packages/core/package.json"), "utf8"),
    ) as { files?: string[] };

    expect(manifest.files).toEqual([
      "dist",
      "README.md",
      "LICENSE",
      "NOTICE",
    ]);
  });

  test("published packages do not use workspace protocol for internal runtime dependencies", async () => {
    const packageDirs = ["bun", "core", "deno", "postgres", "rest-collections", "runtime", "tooling"] as const;
    const violations: string[] = [];

    for (const packageName of packageDirs) {
      const manifest = JSON.parse(
        await readFile(resolve(repoRoot, `packages/${packageName}/package.json`), "utf8"),
      ) as { dependencies?: Record<string, string> };

      for (const [dependency, version] of Object.entries(manifest.dependencies ?? {})) {
        if (dependency.startsWith("@chimpbase/") && version.startsWith("workspace:")) {
          violations.push(`packages/${packageName}/package.json: ${dependency} -> ${version}`);
        }
      }
    }

    expect(violations).toEqual([]);
  });

  test("packages consumed by the Deno host publish JavaScript runtime exports", async () => {
    const packageDirs = ["runtime", "core", "tooling", "postgres", "rest-collections", "deno"] as const;
    const violations: string[] = [];

    for (const packageName of packageDirs) {
      const manifest = JSON.parse(
        await readFile(resolve(repoRoot, `packages/${packageName}/package.json`), "utf8"),
      ) as {
        exports?: Record<string, string | { default?: string; import?: string; types?: string }>;
        files?: string[];
      };

      if (!(manifest.files?.includes("dist") === true)) {
        violations.push(`packages/${packageName}/package.json: missing dist in files`);
      }

      for (const [subpath, entry] of Object.entries(manifest.exports ?? {})) {
        if (typeof entry === "string") {
          violations.push(`packages/${packageName}/package.json: ${subpath} must use object exports`);
          continue;
        }

        if (!(entry.import?.endsWith(".js") === true)) {
          violations.push(`packages/${packageName}/package.json: ${subpath} import must point to .js`);
        }

        if (!(entry.default?.endsWith(".js") === true)) {
          violations.push(`packages/${packageName}/package.json: ${subpath} default must point to .js`);
        }

        if (!(entry.types?.endsWith(".d.ts") === true)) {
          violations.push(`packages/${packageName}/package.json: ${subpath} types must point to .d.ts`);
        }
      }
    }

    expect(violations).toEqual([]);
  });
});

function stripComments(source: string): string {
  return source
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/\/\/.*/g, "");
}

function isAllowedPortableImport(relativePath: string, label: string, source: string): boolean {
  return allowedPortableImports.some((entry) =>
    entry.file === relativePath
    && entry.label === label
    && entry.pattern.test(source)
  );
}

async function listTypescriptFiles(dir: string): Promise<string[]> {
  const entries = await readdir(dir, { withFileTypes: true });
  const files: string[] = [];

  for (const entry of entries) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...await listTypescriptFiles(path));
      continue;
    }

    if (entry.isFile() && path.endsWith(".ts")) {
      files.push(path);
    }
  }

  return files.sort();
}
