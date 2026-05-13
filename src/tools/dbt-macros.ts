import { z } from "zod";
import { extractFieldsDescription } from "@us-all/mcp-toolkit";
import { loadManifest } from "../clients/dbt-artifacts.js";

const ef = z.string().optional().describe(extractFieldsDescription);

export const dbtListMacrosSchema = z.object({
  package: z.string().optional().describe("Filter by package name"),
  search: z.string().optional().describe("Substring match against macro name"),
  limit: z.coerce.number().int().min(1).max(2000).default(500),
  extractFields: ef,
});

export async function dbtListMacros(args: z.infer<typeof dbtListMacrosSchema>): Promise<unknown> {
  const manifest = loadManifest();
  const out: Array<Record<string, unknown>> = [];
  const search = args.search?.toLowerCase();
  for (const m of Object.values(manifest.macros)) {
    if (args.package && m.package_name !== args.package) continue;
    if (search && !m.name.toLowerCase().includes(search)) continue;
    out.push({
      uniqueId: m.unique_id,
      name: m.name,
      package: m.package_name,
      path: m.original_file_path,
      arguments: m.arguments?.map((a) => ({ name: a.name, type: a.type })) ?? [],
      description: m.description,
    });
    if (out.length >= args.limit) break;
  }
  return { count: out.length, macros: out };
}

export const dbtGetMacroSchema = z.object({
  uniqueId: z.string().optional().describe("dbt unique_id (e.g. 'macro.proj.my_macro')"),
  name: z.string().optional().describe("Macro name (resolved if uniqueId not provided)"),
  extractFields: ef,
});

export async function dbtGetMacro(args: z.infer<typeof dbtGetMacroSchema>): Promise<unknown> {
  const manifest = loadManifest();
  let macro = args.uniqueId ? manifest.macros[args.uniqueId] : undefined;
  if (!macro && args.name) {
    macro = Object.values(manifest.macros).find((m) => m.name === args.name);
  }
  if (!macro) throw new Error(`Macro not found: ${args.uniqueId ?? args.name}`);

  // Find usages: any node whose depends_on.macros includes this macro
  const usages: string[] = [];
  for (const n of [...Object.values(manifest.nodes), ...Object.values(manifest.macros)]) {
    if ((n.depends_on?.macros ?? []).includes(macro.unique_id)) {
      usages.push(n.unique_id);
    }
  }

  return {
    uniqueId: macro.unique_id,
    name: macro.name,
    package: macro.package_name,
    path: macro.original_file_path,
    arguments: macro.arguments ?? [],
    description: macro.description,
    rawCode: macro.raw_code,
    dependsOnMacros: macro.depends_on?.macros ?? [],
    usages,
    usageCount: usages.length,
  };
}
