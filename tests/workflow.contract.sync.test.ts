import { afterAll, describe, expect, test } from "bun:test";
import { cp, mkdtemp, mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";

import { syncChimpbaseWorkflowContracts } from "../packages/bun/src/library.ts";
import { defineWorkflow, describeWorkflow, type ChimpbaseWorkflowContract } from "../packages/runtime/index.ts";
import { createChimpbaseRegistry } from "../packages/core/index.ts";
import { syncRegisteredWorkflowContracts } from "../packages/tooling/src/workflow_contracts.ts";
import { installLocalPackage } from "./support/local_package.ts";

const runtimeRoot = resolve(import.meta.dir, "..");
const cleanupDirs: string[] = [];

afterAll(async () => {
  for (const dir of cleanupDirs.splice(0)) {
    await rm(dir, { force: true, recursive: true });
  }
});

describe("workflow contract sync", () => {
  test("keeps slash and literal escape-looking workflow names in distinct snapshots", async () => {
    const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-workflow-filenames-"));
    cleanupDirs.push(projectDir);
    const registry = createChimpbaseRegistry();
    const names = ["order/a", "order_2Fa", "order%2Fa", "order/a b", "order_2Fa_20b", "customer.onboarding"];
    for (const name of names) {
      registry.workflows.set(name, new Map([[1, defineWorkflow({ name, version: 1, initialState: () => ({}), steps: [] })]]));
    }
    const result = await syncRegisteredWorkflowContracts(registry, projectDir);
    expect(new Set(result.entries.map((entry) => entry.path)).size).toBe(names.length);
    expect(result.entries.find((entry) => entry.contract.name === "customer.onboarding")?.path)
      .toBe(join(result.contractsDir, "customer.onboarding.v1.contract.json"));
    const stored = await Promise.all((await readdir(result.contractsDir)).map(async (file) =>
      (JSON.parse(await readFile(join(result.contractsDir, file), "utf8")) as ChimpbaseWorkflowContract).name));
    expect(stored.sort()).toEqual(names.sort());
    expect((await syncRegisteredWorkflowContracts(registry, projectDir, { check: true })).entries)
      .toHaveLength(names.length);
  });

  test("reuses existing legacy snapshot paths without overwriting them when a colliding name is added", async () => {
    const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-workflow-legacy-"));
    cleanupDirs.push(projectDir);
    const contractsDir = join(projectDir, "workflow-contracts");
    await mkdir(contractsDir);
    const workflow = defineWorkflow({ name: "order/a", version: 1, initialState: () => ({}), steps: [] });
    const legacyPath = join(contractsDir, "order_2Fa.v1.contract.json");
    const original = `${JSON.stringify(describeWorkflow(workflow), null, 2)}\n`;
    await writeFile(legacyPath, original);
    const registry = createChimpbaseRegistry();
    registry.workflows.set(workflow.name, new Map([[1, workflow]]));
    const checked = await syncRegisteredWorkflowContracts(registry, projectDir, { check: true });
    expect(checked.entries[0]).toMatchObject({ path: legacyPath, status: "unchanged" });
    const added = defineWorkflow({ name: "order_2Fa", version: 1, initialState: () => ({}), steps: [] });
    registry.workflows.set(added.name, new Map([[1, added]]));
    const synced = await syncRegisteredWorkflowContracts(registry, projectDir);
    expect(synced.entries.find((entry) => entry.contract.name === added.name)?.path).not.toBe(legacyPath);
    expect(await readFile(legacyPath, "utf8")).toBe(original);
    expect(await readdir(contractsDir)).toHaveLength(2);
    expect((await syncRegisteredWorkflowContracts(registry, projectDir, { check: true })).entries)
      .toHaveLength(2);
  });

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

  test("rejects same-version timing changes and preserves snapshots after an explicit breaking bump", async () => {
    const index = (delayMs: number, version: number) => createWorkflowIndex({
      stateSchema: '{ type: "object", properties: { customerId: { type: "string" } }, required: ["customerId"] }',
      steps: `workflowSleepStep("wait", ${delayMs}),`,
      version,
    });
    const projectDir = await createWorkflowFixture("timing", index(1_000, 1));
    await runContractSync(projectDir);
    const previousPath = resolve(projectDir, "workflow-contracts", "customer.onboarding.v1.contract.json");
    const previous = await Bun.file(previousPath).text();

    await writeProjectFile(projectDir, "chimpbase.app.ts", index(86_400_000, 1));
    await expect(runContractSync(projectDir, { allowBreaking: true }))
      .rejects.toThrow("changed without a version bump");
    expect(await Bun.file(previousPath).text()).toBe(previous);

    await writeProjectFile(projectDir, "chimpbase.app.ts", index(86_400_000, 2));
    await expect(runContractSync(projectDir)).rejects.toThrow("breaking relative to v1");
    const synced = await runContractSync(projectDir, { allowBreaking: true });
    expect(synced.result.entries).toEqual([
      expect.objectContaining({ compatibility: "breaking", status: "written" }),
    ]);
    expect(synced.result.entries[0]?.contract.steps[0]?.delayMs).toBe(86_400_000);
    expect(await Bun.file(previousPath).text()).toBe(previous);
  });
});

async function runContractSync(
  projectDir: string,
  options: { allowBreaking?: boolean; check?: boolean } = {},
) {
  const synced = await syncChimpbaseWorkflowContracts({
    allowBreaking: options.allowBreaking,
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
