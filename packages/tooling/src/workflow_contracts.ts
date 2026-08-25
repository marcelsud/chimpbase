import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";

import type { ChimpbaseRegistry } from "@chimpbase/core";
import {
  compareWorkflowContracts,
  describeWorkflow,
  isChimpbaseWorkflowDefinition,
  type ChimpbaseWorkflowCompatibility,
  type ChimpbaseWorkflowContract,
} from "@chimpbase/runtime";

export interface WorkflowContractSyncOptions {
  allowBreaking?: boolean;
  check?: boolean;
  contractsDir?: string | null;
}

export interface WorkflowContractSyncEntry {
  compatibility: ChimpbaseWorkflowCompatibility;
  contract: ChimpbaseWorkflowContract;
  path: string;
  status: "missing" | "unchanged" | "written";
}

export interface WorkflowContractSyncResult {
  contractsDir: string;
  entries: WorkflowContractSyncEntry[];
}

export async function syncRegisteredWorkflowContracts(
  registry: ChimpbaseRegistry,
  projectDir: string,
  options: WorkflowContractSyncOptions = {},
): Promise<WorkflowContractSyncResult> {
  const contractsDir = resolve(projectDir, options.contractsDir ?? "workflow-contracts");
  const existing = await readWorkflowContracts(contractsDir);
  const entries: WorkflowContractSyncEntry[] = [];
  const issues: string[] = [];
  const latestByName = new Map<string, ChimpbaseWorkflowContract>();

  for (const contract of flattenRegisteredWorkflowContracts(registry)) {
    const knownVersions = existing.get(contract.name) ?? [];
    const storedSameVersion = knownVersions.find((entry) => entry.version === contract.version) ?? null;
    const previousVersion = latestByName.get(contract.name)
      ?? [...knownVersions]
        .filter((entry) => entry.version < contract.version)
        .sort((left, right) => right.version - left.version)[0]
      ?? null;
    const compatibility = (previousVersion !== null && previousVersion !== undefined)
      ? compareWorkflowContracts(previousVersion, contract)
      : "additive";
    const path = join(contractsDir, workflowContractFileName(contract.name, contract.version));

    if ((storedSameVersion !== null)) {
      if (storedSameVersion.hash !== contract.hash) {
        issues.push(`workflow ${contract.name} v${contract.version} changed without a version bump`);
      } else {
        entries.push({
          compatibility,
          contract,
          path,
          status: "unchanged",
        });
      }
      latestByName.set(contract.name, contract);
      continue;
    }

    const expectedVersion = (previousVersion !== null && previousVersion !== undefined) ? previousVersion.version + 1 : 1;
    if (contract.version !== expectedVersion) {
      issues.push(`workflow ${contract.name} expected version ${expectedVersion}, received ${contract.version}`);
      latestByName.set(contract.name, contract);
      continue;
    }

    if (!(options.allowBreaking === true) && (compatibility === "breaking" || compatibility === "requires_migration")) {
      issues.push(`workflow ${contract.name} v${contract.version} is ${compatibility} relative to v${previousVersion?.version}`);
      latestByName.set(contract.name, contract);
      continue;
    }

    entries.push({
      compatibility,
      contract,
      path,
      status: (options.check === true) ? "missing" : "written",
    });
    latestByName.set(contract.name, contract);
  }

  if (issues.length > 0) {
    throw new Error(issues.join("\n"));
  }

  if ((options.check === true)) {
    const missing = entries.filter((entry) => entry.status === "missing");
    if (missing.length > 0) {
      throw new Error(
        missing
          .map((entry) => `missing workflow contract ${entry.contract.name} v${entry.contract.version}`)
          .join("\n"),
      );
    }

    return {
      contractsDir,
      entries,
    };
  }

  if (entries.some((entry) => entry.status === "written")) {
    await mkdir(contractsDir, { recursive: true });
  }

  for (const entry of entries) {
    if (entry.status !== "written") {
      continue;
    }

    await writeFile(entry.path, `${JSON.stringify(entry.contract, null, 2)}\n`);
  }

  return {
    contractsDir,
    entries,
  };
}

function flattenRegisteredWorkflowContracts(registry: ChimpbaseRegistry): ChimpbaseWorkflowContract[] {
  const contracts: ChimpbaseWorkflowContract[] = [];

  for (const [name, versions] of registry.workflows) {
    for (const definition of versions.values()) {
      if (!isChimpbaseWorkflowDefinition(definition)) {
        throw new TypeError(`workflow ${name} has an invalid definition`);
      }
      const contract = describeWorkflow(definition);
      if (contract.name !== name) {
        throw new Error(`workflow registry mismatch: expected ${name}, received ${contract.name}`);
      }

      contracts.push(contract);
    }
  }

  contracts.sort((left, right) => {
    const byName = left.name.localeCompare(right.name);
    if (byName !== 0) {
      return byName;
    }

    return left.version - right.version;
  });

  return contracts;
}

async function readWorkflowContracts(contractsDir: string): Promise<Map<string, ChimpbaseWorkflowContract[]>> {
  const contracts = new Map<string, ChimpbaseWorkflowContract[]>();
  let files: string[] = [];

  try {
    files = await readdir(contractsDir);
  } catch (error) {
    if ((error !== null && error !== undefined) && typeof error === "object" && "code" in error && error.code === "ENOENT") {
      return contracts;
    }

    throw error;
  }

  for (const fileName of files) {
    if (!fileName.endsWith(".contract.json")) {
      continue;
    }

    const contract = JSON.parse(await readFile(join(contractsDir, fileName), "utf8")) as ChimpbaseWorkflowContract;
    const versions = contracts.get(contract.name) ?? [];
    versions.push(contract);
    versions.sort((left, right) => left.version - right.version);
    contracts.set(contract.name, versions);
  }

  return contracts;
}

function workflowContractFileName(name: string, version: number): string {
  return `${encodeWorkflowName(name)}.v${version}.contract.json`;
}

function encodeWorkflowName(name: string): string {
  return encodeURIComponent(name).replaceAll("%", "_");
}
