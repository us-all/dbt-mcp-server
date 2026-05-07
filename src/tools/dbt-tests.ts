import { z } from "zod";
import { loadManifest, loadRunResults, type DbtNode } from "../clients/dbt-artifacts.js";

function isTest(node: DbtNode): boolean {
  return node.resource_type === "test";
}

export const dbtListTestsSchema = z.object({
  attachedTo: z
    .string()
    .optional()
    .describe("Filter to tests attached to a specific model unique_id or name"),
  testKind: z.enum(["generic", "singular", "all"]).default("all"),
  search: z.string().optional().describe("Substring match against test name"),
  limit: z.coerce.number().int().min(1).max(2000).default(500),
});

export async function dbtListTests(args: z.infer<typeof dbtListTestsSchema>): Promise<unknown> {
  const manifest = loadManifest();
  let attachedId: string | undefined = args.attachedTo;
  if (attachedId && !manifest.nodes[attachedId]) {
    const found = Object.values(manifest.nodes).find((n) => n.name === attachedId);
    attachedId = found?.unique_id;
  }
  const out: Array<Record<string, unknown>> = [];
  const search = args.search?.toLowerCase();
  for (const node of Object.values(manifest.nodes)) {
    if (!isTest(node)) continue;
    const isGeneric = !!node.test_metadata;
    if (args.testKind === "generic" && !isGeneric) continue;
    if (args.testKind === "singular" && isGeneric) continue;
    if (search && !node.name.toLowerCase().includes(search)) continue;
    if (attachedId) {
      const dependsNodes = node.depends_on?.nodes ?? [];
      if (!dependsNodes.includes(attachedId) && node.attached_node !== attachedId) continue;
    }
    out.push({
      uniqueId: node.unique_id,
      name: node.name,
      package: node.package_name,
      kind: isGeneric ? "generic" : "singular",
      definition: node.test_metadata?.name,
      column: node.column_name,
      attachedTo: node.depends_on?.nodes?.[0],
      severity: node.severity,
      tags: node.tags ?? node.config?.tags ?? [],
    });
    if (out.length >= args.limit) break;
  }
  return { count: out.length, tests: out };
}

export const dbtGetTestSchema = z.object({
  uniqueId: z.string().describe("dbt unique_id of the test (e.g. 'test.proj.unique_users_id')"),
});

export async function dbtGetTest(args: z.infer<typeof dbtGetTestSchema>): Promise<unknown> {
  const manifest = loadManifest();
  const node = manifest.nodes[args.uniqueId];
  if (!node || !isTest(node)) throw new Error(`Test not found: ${args.uniqueId}`);

  let latestResult: unknown = null;
  try {
    const runResults = loadRunResults();
    const result = runResults.results.find((r) => r.unique_id === args.uniqueId);
    if (result) {
      latestResult = {
        status: result.status,
        executionTime: result.execution_time,
        message: result.message,
        failures: result.failures,
        generatedAt: runResults.metadata.generated_at,
        invocationId: runResults.metadata.invocation_id,
      };
    }
  } catch {
    // run_results.json may be absent
  }

  return {
    uniqueId: node.unique_id,
    name: node.name,
    package: node.package_name,
    kind: node.test_metadata ? "generic" : "singular",
    testDefinition: node.test_metadata?.name,
    testKwargs: node.test_metadata?.kwargs,
    column: node.column_name,
    severity: node.severity,
    attachedNodes: node.depends_on?.nodes ?? [],
    tags: node.tags ?? node.config?.tags ?? [],
    rawCode: node.raw_code,
    latestResult,
  };
}
