import { resolve } from "node:path";
import { spawn } from "node:child_process";
import process from "node:process";
import { parseArgs } from "node:util";

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
  installShutdownHandler?(stop: () => Promise<void>): void;
  writeLine?(line: string): void;
}

export async function runChimpbaseCli(
  argv: readonly string[],
  dependencies: RunChimpbaseCliDependencies,
): Promise<void> {
  const command = argv[0] !== undefined && !argv[0].startsWith("-") ? argv[0] : "dev";
  let rest = command === argv[0] ? argv.slice(1) : argv;
  const writeLine = dependencies.writeLine ?? ((line: string) => console.log(line));
  const commonOptions = {
    help: { type: "boolean", short: "h" },
    "project-dir": { type: "string" },
  } as const;
  const options: Record<string, { type: "boolean" | "string"; short?: string }> | null = command === "dev" ? {
    ...commonOptions,
    action: { type: "string" }, args: { type: "string" },
    serve: { type: "boolean" }, worker: { type: "boolean" }, watch: { type: "boolean" },
  } as const : command === "contracts" ? {
    ...commonOptions,
    "allow-breaking": { type: "boolean" }, check: { type: "boolean" },
  } as const : command === "modules" ? {
    ...commonOptions,
    "artifacts-dir": { type: "string" }, "composition-root": { type: "string" }, "modules-dir": { type: "string" },
  } as const : command === "schema" ? {
    ...commonOptions,
    "docker-image": { type: "string" }, "output-dir": { type: "string" },
  } as const : null;
  if (options === null) throw new Error(`unsupported command: ${command}`);

  let subcommand: string | null = null;
  if ((command === "modules" || command === "schema") && rest[0] !== undefined && !rest[0].startsWith("-")) {
    subcommand = rest[0];
    rest = rest.slice(1);
  }
  const { values: args } = parseArgs({ args: [...rest], options, strict: true, allowPositionals: false });
  if (args.help) {
    writeLine(CLI_HELP);
    return;
  }
  for (const [name, value] of Object.entries(args)) {
    if (typeof value === "string" && value.trim().length === 0) {
      throw new Error(`--${name} requires a nonempty value`);
    }
  }
  const projectDirArg = args["project-dir"];
  const projectDir = resolve(typeof projectDirArg === "string" ? projectDirArg : ".");

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

  if (args.args !== undefined && args.action === undefined) {
    throw new Error("--args requires --action NAME");
  }
  if (args.action !== undefined && (args.serve || args.worker || args.watch)) {
    throw new Error("--action cannot be combined with --serve, --worker or --watch");
  }
  if (args.watch) {
    await runNativeWatchProcess(argv);
    return;
  }

  if (typeof args.action === "string") {
    const actionArgs = typeof args.args === "string" ? parseJson(args.args) : [];

    const { host, outcome } = await dependencies.runAction(args.action, actionArgs, {
      projectDir,
    });
    try {
      writeLine(`executed action ${args.action} for project ${host.config.project.name}`);
      writeLine(JSON.stringify(outcome.result, null, 2));
      writeLine(`emitted ${outcome.emittedEvents.length} event(s)`);
    } finally {
      await host.close();
    }
    return;
  }

  const serveFlag = args.serve === true;
  const workerFlag = args.worker === true;
  const started = await dependencies.startProject({
    projectDir,
    runWorker: workerFlag ? true : undefined,
    serve: serveFlag ? true : undefined,
  });
  dependencies.installShutdownHandler?.(() => started.stop());

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

const CLI_HELP = `Usage: chimpbase [command] [options]

dev                    Start HTTP and the background worker (default command)
  --serve              Start HTTP only
  --worker             Start the worker only
  --watch              Restart with the runtime's native watcher on source edits
  --action NAME        Run one action and exit
  --args JSON          Input for --action
contracts              Sync workflow contracts (--check, --allow-breaking)
modules sync|check     Sync or check module artifacts
  --modules-dir PATH --artifacts-dir PATH --composition-root PATH
schema generate|check  Generate or check database schema artifacts
  --docker-image IMAGE --output-dir PATH

--project-dir PATH     Project directory (default: current directory)
--help, -h             Show this help without starting services`;

export function installCliShutdownHandler(stop: () => Promise<void>): void {
  let stopping = false;
  const shutdown = async () => {
    if (stopping) return;
    stopping = true;
    try {
      await stop();
      process.exit(0);
    } catch (error) {
      console.error(error);
      process.exit(1);
    }
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

async function runNativeWatchProcess(argv: readonly string[]): Promise<void> {
  const entrypoint = process.argv[1];
  if (!entrypoint) throw new Error("--watch requires a CLI entry point");
  const runtimeArgs = [...process.execArgv, "--watch"];
  const child = spawn(process.execPath, [...runtimeArgs, entrypoint, ...argv.filter((arg) => arg !== "--watch")], {
    stdio: "inherit",
  });
  const interrupt = () => { child.kill("SIGINT"); };
  const terminate = () => { child.kill("SIGTERM"); };
  process.once("SIGINT", interrupt);
  process.once("SIGTERM", terminate);
  try {
    await new Promise<void>((resolveExit, rejectExit) => {
      child.once("error", rejectExit);
      child.once("exit", (code) => {
        process.exitCode = code ?? 0;
        resolveExit();
      });
    });
  } finally {
    process.removeListener("SIGINT", interrupt);
    process.removeListener("SIGTERM", terminate);
  }
}
