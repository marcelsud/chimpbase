import {
  createChimpbaseRuntimeLibrary,
  type CreateChimpbaseAppFieldsOptions,
  type CreateChimpbaseFromAppOptions,
  type CreateChimpbaseOptions,
  type CreateChimpbaseRuntimeOptions,
  type CreateChimpbaseWithDefaultsOptions,
  type LoadChimpbaseAppOptions,
  type StartChimpbaseProjectOptions,
  type StartedChimpbaseProject,
  type SyncChimpbaseSchemaOptions,
  type SyncChimpbaseModulesOptions,
  type SyncChimpbaseWorkflowContractsOptions,
} from "@chimpbase/host";
import type { ChimpbaseAppDefinition, ChimpbaseAppDefinitionInput } from "@chimpbase/core";
import { runChimpbaseCli } from "@chimpbase/tooling/cli";

import { getDenoArgs } from "./deno_runtime.ts";
import { denoRuntimeShim, ChimpbaseDenoHost, type ActionExecutionResult, type DenoServeHandle } from "./runtime.ts";

const runtimeLibrary = createChimpbaseRuntimeLibrary(ChimpbaseDenoHost, denoRuntimeShim);

export type {
  CreateChimpbaseAppFieldsOptions,
  CreateChimpbaseFromAppOptions,
  CreateChimpbaseOptions,
  CreateChimpbaseRuntimeOptions,
  CreateChimpbaseWithDefaultsOptions,
  DenoServeHandle,
  LoadChimpbaseAppOptions,
  StartChimpbaseProjectOptions,
  StartedChimpbaseProject,
  SyncChimpbaseSchemaOptions,
  SyncChimpbaseModulesOptions,
  SyncChimpbaseWorkflowContractsOptions,
};

export const createChimpbase = runtimeLibrary.createChimpbase;
export const createChimpbaseDeno = createChimpbase;
export const loadChimpbaseApp = runtimeLibrary.loadChimpbaseApp;
export const loadChimpbaseProject = runtimeLibrary.loadChimpbaseProject;
export const runChimpbaseAction = runtimeLibrary.runChimpbaseAction;
export const runChimpbaseAppAction = runtimeLibrary.runChimpbaseAppAction;
export const startChimpbaseApp = runtimeLibrary.startChimpbaseApp;
export const startChimpbaseProject = runtimeLibrary.startChimpbaseProject;
export const syncChimpbaseModules = runtimeLibrary.syncChimpbaseModules;
export const syncChimpbaseSchema = runtimeLibrary.syncChimpbaseSchema;
export const syncChimpbaseWorkflowContracts = runtimeLibrary.syncChimpbaseWorkflowContracts;

export {
  type ActionExecutionResult,
  type ChimpbaseAppDefinition,
  type ChimpbaseAppDefinitionInput,
};

export async function runDenoCli(argv = getDenoArgs()): Promise<void> {
  await runChimpbaseCli(argv, {
    runAction: runChimpbaseAction,
    startProject: startChimpbaseProject,
    syncModules: syncChimpbaseModules,
    syncSchema: syncChimpbaseSchema,
    syncWorkflowContracts: syncChimpbaseWorkflowContracts,
  });
}
