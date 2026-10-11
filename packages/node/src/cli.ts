#!/usr/bin/env node

import { installCliShutdownHandler, runChimpbaseCli } from "@chimpbase/tooling/cli";

import {
  loadChimpbaseProject,
  runChimpbaseAction,
  startChimpbaseProject,
  syncChimpbaseModules,
  syncChimpbaseSchema,
  syncChimpbaseWorkflowContracts,
} from "./library.ts";

await runChimpbaseCli(process.argv.slice(2), {
  loadProject: loadChimpbaseProject,
  installShutdownHandler: installCliShutdownHandler,
  runAction: runChimpbaseAction,
  startProject: startChimpbaseProject,
  syncModules: syncChimpbaseModules,
  syncSchema: syncChimpbaseSchema,
  syncWorkflowContracts: syncChimpbaseWorkflowContracts,
});
