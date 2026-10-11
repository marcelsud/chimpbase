#!/usr/bin/env bun

import { installCliShutdownHandler, runChimpbaseCli } from "@chimpbase/tooling/cli";

import {
  loadChimpbaseProject,
  runChimpbaseAction,
  startChimpbaseProject,
  syncChimpbaseModules,
  syncChimpbaseSchema,
  syncChimpbaseWorkflowContracts,
} from "./library.ts";

await runChimpbaseCli(Bun.argv.slice(2), {
  loadProject: loadChimpbaseProject,
  installShutdownHandler: installCliShutdownHandler,
  runAction: runChimpbaseAction,
  startProject: startChimpbaseProject,
  syncModules: syncChimpbaseModules,
  syncSchema: syncChimpbaseSchema,
  syncWorkflowContracts: syncChimpbaseWorkflowContracts,
});
