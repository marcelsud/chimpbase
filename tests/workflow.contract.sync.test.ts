import { afterAll, describe, expect, test } from "bun:test";
import { cp, mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";

import { syncChimpbaseWorkflowContracts } from "../packages/bun/src/library.ts";
import type { ChimpbaseWorkflowContract } from "../packages/runtime/index.ts";
import { installLocalPackage } from "./support/local_package.ts";

const runtimeRoot = resolve(import.meta.dir, "..");
const cleanupDirs: string[] = [];

afterAll(async () => {
  for (const dir of cleanupDirs.splice(0)) {
    await rm(dir, { force: true, recursive: true });
  }
});

describe("workflow contract sync", () => {
  test("writes workflow contracts and reuses unchanged versions in check mode", async () => {
    const projectDir = await createWorkflowFixture("sync-v1", createWorkflowIndex({
      stateSchema: `
        {
          properties: {
            customerId: { type: "string" },
          },
          required: ["customerId"],
          type: "object",
        }
      `,
      steps: `
        workflowActionStep("create-account", "createCustomerRecord"),
        workflowSleepStep("wait-7-days", 7 * 24 * 60 * 60 * 1000),
        workflowActionStep("activate-account", "activateCustomer"),
      `,
      version: 1,
    }));

    const first = await runContractSync(projectDir);
    expect(first.result.entries).toEqual([
      expect.objectContaining({
        compatibility: "additive",
        status: "written",
      }),
    ]);

    const contractPath = resolve(projectDir, "workflow-contracts", "customer.onboarding.v1.contract.json");
    const stored = await Bun.file(contractPath).json() as ChimpbaseWorkflowContract;
    expect(stored).toEqual(
      expect.objectContaining({
        name: "customer.onboarding",
        version: 1,
      }) as ChimpbaseWorkflowContract,
    );

    const second = await runContractSync(projectDir, { check: true });
    expect(second.result.entries).toEqual([
      expect.objectContaining({
        compatibility: "additive",
        status: "unchanged",
      }),
    ]);
  });

  test("writes additive workflow versions as new contract snapshots", async () => {
    const projectDir = await createWorkflowFixture("sync-v2-additive", createWorkflowIndex({
      stateSchema: `
        {
          properties: {
            customerId: { type: "string" },
          },
          required: ["customerId"],
          type: "object",
        }
      `,
      steps: `
        workflowActionStep("create-account", "createCustomerRecord"),
        workflowSleepStep("wait-7-days", 7 * 24 * 60 * 60 * 1000),
        workflowActionStep("activate-account", "activateCustomer"),
      `,
      version: 1,
    }));

    await runContractSync(projectDir);

    await writeProjectFile(projectDir, "chimpbase.app.ts", createWorkflowIndex({
      stateSchema: `
        {
          properties: {
            customerId: { type: "string" },
            activationRate30d: { type: "number" },
          },
          required: ["customerId"],
          type: "object",
        }
      `,
      steps: `
        workflowActionStep("create-account", "createCustomerRecord"),
        workflowSleepStep("wait-7-days", 7 * 24 * 60 * 60 * 1000),
        workflowActionStep("activate-account", "activateCustomer"),
        workflowActionStep("measure-30-day-adoption", "evaluateActivation"),
      `,
      version: 2,
    }));

    const synced = await runContractSync(projectDir);
    expect(synced.result.entries).toEqual([
      expect.objectContaining({
        compatibility: "additive",
        status: "written",
      }),
    ]);

    const contractPath = resolve(projectDir, "workflow-contracts", "customer.onboarding.v2.contract.json");
    const stored = await Bun.file(contractPath).json() as ChimpbaseWorkflowContract;
    expect(stored).toEqual(
      expect.objectContaining({
        name: "customer.onboarding",
        version: 2,
      }) as ChimpbaseWorkflowContract,
    );
  });

  test("fails when a new workflow version is incompatible with the latest contract", async () => {
    const projectDir = await createWorkflowFixture("sync-v2-breaking", createWorkflowIndex({
      stateSchema: `
        {
          properties: {
            customerId: { type: "string" },
          },
          required: ["customerId"],
          type: "object",
        }
      `,
      steps: `
        workflowActionStep("create-account", "createCustomerRecord"),
        workflowSleepStep("wait-7-days", 7 * 24 * 60 * 60 * 1000),
        workflowActionStep("activate-account", "activateCustomer"),
      `,
      version: 1,
    }));

    await runContractSync(projectDir);

    await writeProjectFile(projectDir, "chimpbase.app.ts", createWorkflowIndex({
      stateSchema: `
        {
          properties: {
            customerId: { type: "string" },
          },
          required: ["customerId"],
          type: "object",
        }
      `,
      steps: `
        workflowActionStep("create-account", "createCustomerRecord"),
        workflowActionStep("send-welcome-email", "sendWelcomeEmail"),
        workflowSleepStep("wait-7-days", 7 * 24 * 60 * 60 * 1000),
        workflowActionStep("activate-account", "activateCustomer"),
      `,
      version: 2,
    }));

    await expect(runContractSync(projectDir)).rejects.toThrow("requires_migration");
  });
});

async function runContractSync(
  projectDir: string,
  options: { check?: boolean } = {},
) {
  const synced = await syncChimpbaseWorkflowContracts({
    check: options.check,
    projectDir,
  });

  try {
    return synced;
  } finally {
    await synced.host.close();
  }
}

async function createWorkflowFixture(label: string, indexSource: string): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), `chimpbase-bun-workflow-contract-${label}-`));
  cleanupDirs.push(dir);

  await installLocalPackage(dir, "@chimpbase/runtime", resolve(runtimeRoot, "packages/runtime"));
  await cp(resolve(runtimeRoot, "node_modules/kysely"), resolve(dir, "node_modules/kysely"), {
    recursive: true,
  });

  await writeProjectFile(
    dir,
    "package.json",
    JSON.stringify(
      {
        dependencies: {
          "@chimpbase/runtime": "file:./packages/runtime",
          kysely: "^0.28.11",
        },
      },
      null,
      2,
    ),
  );
  await writeProjectFile(
    dir,
    "tsconfig.json",
    JSON.stringify(
      {
        compilerOptions: {
          allowImportingTsExtensions: true,
          lib: ["ES2022", "DOM"],
          module: "ESNext",
          moduleResolution: "Bundler",
          noEmit: true,
          skipLibCheck: true,
          strict: true,
          target: "ES2022",
        },
        include: ["**/*.ts"],
      },
      null,
      2,
    ),
  );
  await writeProjectFile(dir, "chimpbase.app.ts", indexSource);

  return dir;
}

function createWorkflowIndex(options: {
  stateSchema: string;
  steps: string;
  version: number;
}): string {
  return [
    'import { workflow, workflowActionStep, workflowSleepStep } from "@chimpbase/runtime";',
    "",
    "const onboardingWorkflow = workflow({",
    '  name: "customer.onboarding",',
    `  version: ${options.version},`,
    "  inputSchema: {",
    "    properties: { customerId: { type: \"string\" } },",
    "    required: [\"customerId\"],",
    '    type: "object",',
    "  },",
    `  stateSchema: ${options.stateSchema.trim()},`,
    "  initialState(input) {",
    "    return { customerId: input.customerId };",
    "  },",
    "  steps: [",
    options.steps.trim(),
    "  ],",
    "});",
    "",
    "export default {",
    '  project: { name: "workflow-contract-sync" },',
    "  registrations: [onboardingWorkflow],",
    "};",
    "",
  ].join("\n");
}

async function writeProjectFile(projectDir: string, relativePath: string, contents: string): Promise<void> {
  const path = resolve(projectDir, relativePath);
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, contents);
}
