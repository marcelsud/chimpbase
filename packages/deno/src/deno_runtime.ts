export interface DenoEnvApi {
  get(name: string): string | undefined;
  toObject?(): Record<string, string>;
}

export interface DenoServeHandle {
  finished?: Promise<void>;
  port: number;
  shutdown?(): void;
}

export interface DenoRuntimeLike {
  args?: readonly string[];
  env?: DenoEnvApi;
  serve?(
    options: { hostname?: string; port?: number },
    handler: (request: Request) => Response | Promise<Response>,
  ): Omit<DenoServeHandle, "port">;
}

export function getDenoEnv(name: string): string | undefined {
  return getOptionalDenoRuntime()?.env?.get(name);
}

export function getDenoEnvObject(): Record<string, string> {
  const env = getOptionalDenoRuntime()?.env;
  if (!env || typeof env.toObject !== "function") {
    return {};
  }

  return env.toObject();
}

export function getDenoArgs(): readonly string[] {
  return getOptionalDenoRuntime()?.args ?? [];
}

export function requireDenoServe(): NonNullable<DenoRuntimeLike["serve"]> {
  const serve = getOptionalDenoRuntime()?.serve;
  if (typeof serve !== "function") {
    throw new Error("Deno.serve is unavailable");
  }

  return serve;
}

// `Reflect.get` is declared as returning `any`; the interop is typed once here.
const reflectGet: (target: object, key: PropertyKey) => unknown = Reflect.get;

function getOptionalDenoRuntime(): DenoRuntimeLike | null {
  const runtime = reflectGet(globalThis, "Deno");
  if (!runtime || typeof runtime !== "object") {
    return null;
  }

  return runtime as DenoRuntimeLike;
}
