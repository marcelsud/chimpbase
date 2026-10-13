import { createRequire } from "node:module";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const require = createRequire(import.meta.url);

export async function importProjectModule(modulePath: string): Promise<unknown> {
  const resolvedModulePath = resolve(modulePath);
  // Bun shares the require/import cache and ignores query strings on file imports.
  delete require.cache[resolvedModulePath];
  const moduleUrl = pathToFileURL(resolvedModulePath);
  moduleUrl.searchParams.set("chimpbase_load", globalThis.crypto.randomUUID());
  return await import(moduleUrl.href);
}
