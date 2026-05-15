import { z } from "zod";
import { extractFieldsDescription } from "@us-all/mcp-toolkit";
import {
  loadManifest,
  loadCatalog,
  manifestSchemaSupported,
  type DbtNode,
} from "../clients/dbt-artifacts.js";

const ef = z.string().optional().describe(extractFieldsDescription);

function isModel(node: DbtNode): boolean {
  return node.resource_type === "model";
}

function refKey(ref: string[] | { name: string; package?: string | null }): string {
  if (Array.isArray(ref)) return ref[ref.length - 1]!;
  return ref.name;
}

export const dbtListModelsSchema = z.object({
  package: z.string().optional().describe("Filter by dbt package name (e.g. project name)"),
  tag: z.string().optional().describe("Filter by tag (a model is included if it has this tag)"),
  materialized: z.string().optional().describe("Filter by materialization (table | view | incremental | ...)"),
  schema: z.string().optional().describe("Filter by destination schema/dataset"),
  search: z.string().optional().describe("Substring match against model name (case-insensitive)"),
  limit: z.coerce.number().int().min(1).max(2000).default(200).describe("Max rows to return"),
  extractFields: ef,
});

export async function dbtListModels(args: z.infer<typeof dbtListModelsSchema>): Promise<unknown> {
  const manifest = loadManifest();
  const out: Array<Record<string, unknown>> = [];
  const search = args.search?.toLowerCase();
  for (const node of Object.values(manifest.nodes)) {
    if (!isModel(node)) continue;
    if (args.package && node.package_name !== args.package) continue;
    if (args.tag && !(node.tags ?? node.config?.tags ?? []).includes(args.tag)) continue;
    if (args.materialized && node.config?.materialized !== args.materialized) continue;
    if (args.schema && node.schema !== args.schema) continue;
    if (search && !node.name.toLowerCase().includes(search)) continue;
    out.push({
      uniqueId: node.unique_id,
      name: node.name,
      package: node.package_name,
      schema: node.schema,
      database: node.database,
      materialized: node.config?.materialized,
      tags: node.tags ?? node.config?.tags ?? [],
      path: node.original_file_path,
      description: node.description,
    });
    if (out.length >= args.limit) break;
  }
  return {
    count: out.length,
    models: out,
    caveats: manifestSchemaSupported(manifest)
      ? []
      : [`manifest schema_version '${manifest.metadata.dbt_schema_version}' is outside tested versions (v11–v14) — fields may shift`],
  };
}

export const dbtGetModelSchema = z.object({
  uniqueId: z.string().optional().describe("dbt unique_id (e.g. 'model.us_dbt.users_dim')"),
  name: z.string().optional().describe("Model name (resolved if uniqueId not provided)"),
  includeCompiledSql: z.boolean().default(false).describe("Include compiled_code in response"),
  extractFields: ef,
});

export async function dbtGetModel(args: z.infer<typeof dbtGetModelSchema>): Promise<unknown> {
  const manifest = loadManifest();
  const catalog = loadCatalog();
  let node: DbtNode | undefined;
  if (args.uniqueId) {
    node = manifest.nodes[args.uniqueId];
  } else if (args.name) {
    node = Object.values(manifest.nodes).find((n) => isModel(n) && n.name === args.name);
  } else {
    throw new Error("Provide uniqueId or name");
  }
  if (!node) throw new Error(`Model not found: ${args.uniqueId ?? args.name}`);

  const refs = (node.refs ?? []).map(refKey);
  const sources = (node.sources ?? []).map((s) => s.join("."));
  const dependsOn = node.depends_on?.nodes ?? [];

  const catalogEntry = catalog?.nodes[node.unique_id];
  const columns = node.columns
    ? Object.values(node.columns).map((c) => ({
        name: c.name,
        dataType:
          catalogEntry?.columns?.[c.name]?.type ?? c.data_type ?? null,
        description: c.description,
        tags: c.tags ?? [],
      }))
    : [];

  // Tests attached to this model
  const tests: Array<{ uniqueId: string; name: string; column?: string; severity?: string }> = [];
  for (const t of Object.values(manifest.nodes)) {
    if (t.resource_type !== "test") continue;
    const dependsNodes = t.depends_on?.nodes ?? [];
    if (dependsNodes.includes(node.unique_id) || t.attached_node === node.unique_id) {
      tests.push({
        uniqueId: t.unique_id,
        name: t.name,
        column: t.column_name,
        severity: t.severity ?? t.config?.meta?.severity as string | undefined,
      });
    }
  }

  return {
    uniqueId: node.unique_id,
    name: node.name,
    package: node.package_name,
    schema: node.schema,
    database: node.database,
    alias: node.alias,
    materialized: node.config?.materialized,
    tags: node.tags ?? node.config?.tags ?? [],
    path: node.original_file_path,
    description: node.description,
    refs,
    sources,
    dependsOn,
    columns,
    tests,
    rawCode: node.raw_code,
    compiledCode: args.includeCompiledSql ? node.compiled_code : undefined,
  };
}

export const dbtGraphSchema = z.object({
  uniqueId: z.string().optional().describe("dbt unique_id"),
  name: z.string().optional().describe("Model name (resolved if uniqueId not provided)"),
  upstreamDepth: z.coerce.number().int().min(0).max(10).default(2),
  downstreamDepth: z.coerce.number().int().min(0).max(10).default(2),
  extractFields: ef,
});

export async function dbtGraph(args: z.infer<typeof dbtGraphSchema>): Promise<unknown> {
  const manifest = loadManifest();
  let startId = args.uniqueId;
  if (!startId && args.name) {
    const found = Object.values(manifest.nodes).find((n) => n.name === args.name);
    startId = found?.unique_id;
  }
  if (!startId) throw new Error(`Node not found: ${args.uniqueId ?? args.name}`);

  const parent = manifest.parent_map ?? {};
  const child = manifest.child_map ?? {};

  function walk(map: Record<string, string[]>, id: string, depth: number): Map<string, number> {
    const out = new Map<string, number>();
    const queue: Array<[string, number]> = [[id, 0]];
    while (queue.length) {
      const [cur, d] = queue.shift()!;
      if (d >= depth) continue;
      for (const next of map[cur] ?? []) {
        if (out.has(next)) continue;
        out.set(next, d + 1);
        queue.push([next, d + 1]);
      }
    }
    return out;
  }

  function describe(id: string): { uniqueId: string; name: string; resourceType: string } {
    const node = manifest.nodes[id] ?? manifest.sources[id] ?? manifest.macros[id];
    return {
      uniqueId: id,
      name: node?.name ?? id.split(".").pop() ?? id,
      resourceType: node?.resource_type ?? "unknown",
    };
  }

  const upstream = walk(parent, startId, args.upstreamDepth);
  const downstream = walk(child, startId, args.downstreamDepth);
  return {
    start: describe(startId),
    upstream: Array.from(upstream).map(([id, depth]) => ({ ...describe(id), depth })),
    downstream: Array.from(downstream).map(([id, depth]) => ({ ...describe(id), depth })),
  };
}

export const dbtCoverageSchema = z.object({
  uniqueId: z.string().optional().describe("dbt unique_id"),
  name: z.string().optional().describe("Model name (resolved if uniqueId not provided)"),
  extractFields: ef,
});

export async function dbtCoverage(args: z.infer<typeof dbtCoverageSchema>): Promise<unknown> {
  const manifest = loadManifest();
  let model: DbtNode | undefined;
  if (args.uniqueId) model = manifest.nodes[args.uniqueId];
  else if (args.name) model = Object.values(manifest.nodes).find((n) => isModel(n) && n.name === args.name);
  if (!model) throw new Error(`Model not found: ${args.uniqueId ?? args.name}`);

  const columns = Object.values(model.columns ?? {});
  const tests = Object.values(manifest.nodes).filter(
    (n) => n.resource_type === "test" && (n.depends_on?.nodes ?? []).includes(model.unique_id),
  );

  const columnsCovered = new Set<string>();
  for (const t of tests) {
    if (t.column_name) columnsCovered.add(t.column_name);
  }

  const tableLevelTests = tests.filter((t) => !t.column_name).map((t) => t.name);

  return {
    model: { uniqueId: model.unique_id, name: model.name },
    totalColumns: columns.length,
    columnsWithTests: columnsCovered.size,
    coveragePct: columns.length === 0 ? 0 : Math.round((columnsCovered.size / columns.length) * 1000) / 10,
    columns: columns.map((c) => ({
      name: c.name,
      hasTests: columnsCovered.has(c.name),
      testCount: tests.filter((t) => t.column_name === c.name).length,
    })),
    tableLevelTests,
    totalTests: tests.length,
  };
}
