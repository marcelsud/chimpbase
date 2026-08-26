import { resolve } from "node:path";

import { parseJson } from "@chimpbase/runtime";

import type { ChimpbaseSchemaSyncResult } from "./schema.ts";
import type { SyncChimpbaseModuleArtifactsResult } from "./modules.ts";
import type { WorkflowContractSyncResult } from "./workflow_contracts.ts";

interface CliHost {
  close(): Promise<void>;
  config: {
    project: {
      name: string;
    };
  };
}

interface ActionOutcome {
  emittedEvents: unknown[];
  result: unknown;
}

interface StartedProject {
  server: { port?: number } | null;
  stop(): Promise<void>;
}

export interface RunChimpbaseCliDependencies {
  runAction(
    actionName: string,
    args?: unknown[] | unknown,
    options?: { projectDir?: string },
  ): Promise<{ host: CliHost; outcome: ActionOutcome }>;
  startProject(
    options?: { projectDir?: string; runWorker?: boolean; serve?: boolean },
  ): Promise<StartedProject>;
  syncModules(options?: {
    artifactsDir?: string;
    check?: boolean;
    compositionRoot?: string;
    modulesDir?: string;
    projectDir?: string;
  }): Promise<SyncChimpbaseModuleArtifactsResult>;
  syncSchema(options?: {
    check?: boolean;
    dockerImage?: string;
    outputDir?: string;
    projectDir?: string;
  }): Promise<ChimpbaseSchemaSyncResult>;
  syncWorkflowContracts(options?: {
    allowBreaking?: boolean;
    check?: boolean;
    contractsDir?: string | null;
    projectDir?: string;
  }): Promise<{ host: CliHost; result: WorkflowContractSyncResult }>;
  writeLine?(line: string): void;
}

export async function runChimpbaseCli(
  argv: readonly string[],
  dependencies: RunChimpbaseCliDependencies,
): Promise<void> {
  const [command = "dev", maybeSubcommand, ...rawRest] = argv;
  const subcommand = maybeSubcommand !== undefined
    && maybeSubcommand.length > 0
    && !maybeSubcommand.startsWith("--")
    ? maybeSubcommand
    : null;
  const rest = (subcommand !== null && subcommand.length > 0) ? rawRest : [maybeSubcommand, ...rawRest].filter((token): token is string => Boolean(token));

  const args = parseArgs(rest);
  const projectDirArg = args["project-dir"];
  const projectDir = resolve(typeof projectDirArg === "string" ? projectDirArg : ".");
  const writeLine = dependencies.writeLine ?? ((line: string) => console.log(line));

  if (command === "contracts") {
    const { host, result } = await dependencies.syncWorkflowContracts({
      allowBreaking: args["allow-breaking"] === true,
      check: args.check === true,
      projectDir,
    });

    try {
      if (result.entries.length === 0) {
        writeLine(`no workflow contracts registered for project ${host.config.project.name}`);
      } else {
        for (const entry of result.entries) {
          writeLine(`${entry.status} workflow contract ${entry.contract.name} v${entry.contract.version} (${entry.compatibility})`);
        }
      }

      writeLine(`contracts directory ${result.contractsDir}`);
      return;
    } finally {
      await host.close();
    }
  }

  if (command === "modules") {
    if (subcommand !== "sync" && subcommand !== "check") {
      throw new Error("modules command expects `sync` or `check`");
    }
    const result = await dependencies.syncModules({
      artifactsDir: typeof args["artifacts-dir"] === "string" ? args["artifacts-dir"] : undefined,
      check: subcommand === "check",
      compositionRoot: typeof args["composition-root"] === "string" ? args["composition-root"] : undefined,
      modulesDir: typeof args["modules-dir"] === "string" ? args["modules-dir"] : undefined,
      projectDir,
    });
    writeLine(subcommand === "check" ? "module architecture check passed" : `${result.status} module artifacts`);
    writeLine(`module manifest ${result.paths.manifest}`);
    writeLine(`module summary ${result.paths.summary}`);
    writeLine(`module database types ${result.paths.databaseTypes}`);
    return;
  }

  if (command === "schema") {
    if (subcommand !== "generate" && subcommand !== "check") {
      throw new Error("schema command expects `generate` or `check`");
    }

    const result = await dependencies.syncSchema({
      check: subcommand === "check",
      dockerImage: typeof args["docker-image"] === "string" ? args["docker-image"] : undefined,
      outputDir: typeof args["output-dir"] === "string" ? args["output-dir"] : undefined,
      projectDir,
    });

    writeLine(
      subcommand === "check"
        ? `schema check passed for project ${result.projectName}`
        : `${result.status} schema artifacts for project ${result.projectName}`,
    );
    writeLine(`schema snapshot ${result.snapshotPath}`);
    writeLine(`schema types ${result.typesPath}`);
    return;
  }

  if (command !== "dev") {
    throw new Error(`unsupported command: ${command}`);
  }

  if (typeof args.action === "string") {
    const actionArgs = typeof args.args === "string" ? parseJson(args.args) : [];

    const { host, outcome } = await dependencies.runAction(args.action, actionArgs, {
      projectDir,
    });
    writeLine(`executed action ${args.action} for project ${host.config.project.name}`);
    writeLine(JSON.stringify(outcome.result, null, 2));
    writeLine(`emitted ${outcome.emittedEvents.length} event(s)`);
    await host.close();
    return;
  }

  const serveFlag = args.serve === true;
  const workerFlag = args.worker === true;
  const started = await dependencies.startProject({
    projectDir,
    runWorker: workerFlag ? true : undefined,
    serve: serveFlag ? true : undefined,
  });

  if ((started.server !== null && started.server !== undefined)) {
    writeLine(`listening on http://127.0.0.1:${started.server.port}`);
    if (workerFlag || !serveFlag) {
      writeLine("queue worker started");
    }
    return;
  }

  if (workerFlag) {
    writeLine("queue worker started");
    return;
  }

  await started.stop();
  throw new Error("host currently supports default mode, --serve, --worker or --action");
}

function parseArgs(rawArgs: readonly string[]): Record<string, string | boolean> {
  const parsed: Record<string, string | boolean> = {};

  for (let index = 0; index < rawArgs.length; index += 1) {
    const token = rawArgs[index];
    if (!token.startsWith("--")) {
      continue;
    }

    const key = token.slice(2);
    const next = rawArgs[index + 1];
    if (next === undefined || next.length === 0 || next.startsWith("--")) {
      parsed[key] = true;
      continue;
    }

    parsed[key] = next;
    index += 1;
  }

  return parsed;
}
