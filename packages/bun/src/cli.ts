#!/usr/bin/env bun

import { runChimpbaseCli } from "@chimpbase/tooling/cli";

import {
  runChimpbaseAction,
  startChimpbaseProject,
  syncChimpbaseModules,
  syncChimpbaseSchema,
  syncChimpbaseWorkflowContracts,
} from "./library.ts";

await runChimpbaseCli(Bun.argv.slice(2), {
  runAction: runChimpbaseAction,
  startProject: startChimpbaseProject,
  syncModules: syncChimpbaseModules,
  syncSchema: syncChimpbaseSchema,
  syncWorkflowContracts: syncChimpbaseWorkflowContracts,
});
