#!/usr/bin/env bun

import { installCliShutdownHandler, runChimpbaseCli } from "@chimpbase/tooling/cli";

import {
  runChimpbaseAction,
  startChimpbaseProject,
  syncChimpbaseModules,
  syncChimpbaseSchema,
  syncChimpbaseWorkflowContracts,
} from "./library.ts";

await runChimpbaseCli(Bun.argv.slice(2), {
  installShutdownHandler: installCliShutdownHandler,
  runAction: runChimpbaseAction,
  startProject: startChimpbaseProject,
  syncModules: syncChimpbaseModules,
  syncSchema: syncChimpbaseSchema,
  syncWorkflowContracts: syncChimpbaseWorkflowContracts,
});
