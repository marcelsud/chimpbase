#!/usr/bin/env node

import { installCliShutdownHandler, runChimpbaseCli } from "@chimpbase/tooling/cli";

import {
  runChimpbaseAction,
  startChimpbaseProject,
  syncChimpbaseModules,
  syncChimpbaseSchema,
  syncChimpbaseWorkflowContracts,
} from "./library.ts";

await runChimpbaseCli(process.argv.slice(2), {
  installShutdownHandler: installCliShutdownHandler,
  runAction: runChimpbaseAction,
  startProject: startChimpbaseProject,
  syncModules: syncChimpbaseModules,
  syncSchema: syncChimpbaseSchema,
  syncWorkflowContracts: syncChimpbaseWorkflowContracts,
});
