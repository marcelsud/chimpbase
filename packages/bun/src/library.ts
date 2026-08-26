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

import { bunRuntimeShim, ChimpbaseBunHost, type ActionExecutionResult } from "./runtime.ts";

const runtimeLibrary = createChimpbaseRuntimeLibrary(ChimpbaseBunHost, bunRuntimeShim);

export type {
  CreateChimpbaseAppFieldsOptions,
  CreateChimpbaseFromAppOptions,
  CreateChimpbaseOptions,
  CreateChimpbaseRuntimeOptions,
  CreateChimpbaseWithDefaultsOptions,
  LoadChimpbaseAppOptions,
  StartChimpbaseProjectOptions,
  StartedChimpbaseProject,
  SyncChimpbaseSchemaOptions,
  SyncChimpbaseModulesOptions,
  SyncChimpbaseWorkflowContractsOptions,
};

export const createChimpbase = runtimeLibrary.createChimpbase;
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
