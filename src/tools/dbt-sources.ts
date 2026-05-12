import { z } from "zod";
import { loadManifest, loadSources } from "../clients/dbt-artifacts.js";

export const dbtListSourcesSchema = z.object({
  sourceName: z.string().optional().describe("Filter by source group name"),
  search: z.string().optional().describe("Substring match against source table name"),
  limit: z.coerce.number().int().min(1).max(2000).default(500),
});

export async function dbtListSources(args: z.infer<typeof dbtListSourcesSchema>): Promise<unknown> {
  const manifest = loadManifest();
  const out: Array<Record<string, unknown>> = [];
  const search = args.search?.toLowerCase();
  for (const src of Object.values(manifest.sources)) {
    if (args.sourceName && src.source_name !== args.sourceName) continue;
    if (search && !src.name.toLowerCase().includes(search)) continue;
    out.push({
      uniqueId: src.unique_id,
      sourceName: src.source_name,
      tableName: src.name,
      identifier: src.identifier ?? src.name,
      database: src.database,
      schema: src.schema,
      loader: src.loader,
      loadedAtField: src.loaded_at_field,
      hasFreshness: !!src.freshness?.error_after || !!src.freshness?.warn_after,
      meta: src.meta ?? {},
      tags: src.tags ?? [],
    });
    if (out.length >= args.limit) break;
  }
  return { count: out.length, sources: out };
}

export const dbtGetSourceSchema = z.object({
  uniqueId: z.string().optional().describe("dbt unique_id (e.g. 'source.proj.raw.users')"),
  sourceName: z.string().optional().describe("Source group name (with tableName)"),
  tableName: z.string().optional().describe("Source table name (with sourceName)"),
});

export async function dbtGetSource(args: z.infer<typeof dbtGetSourceSchema>): Promise<unknown> {
  const manifest = loadManifest();
  let id = args.uniqueId;
  if (!id) {
    if (!args.sourceName || !args.tableName) {
      throw new Error("Provide uniqueId, or both sourceName and tableName");
    }
    const found = Object.values(manifest.sources).find(
      (s) => s.source_name === args.sourceName && s.name === args.tableName,
    );
    id = found?.unique_id;
  }
  if (!id) throw new Error(`Source not found: ${args.uniqueId ?? `${args.sourceName}.${args.tableName}`}`);
  const src = manifest.sources[id];
  if (!src) throw new Error(`Source not found in manifest: ${id}`);

  let freshnessResult: unknown = null;
  try {
    const sources = loadSources();
    const r = sources.results.find((res) => res.unique_id === id);
    if (r) {
      freshnessResult = {
        status: r.status,
        maxLoadedAt: r.max_loaded_at,
        snapshottedAt: r.snapshotted_at,
        ageInSeconds: r.max_loaded_at_time_ago_in_s,
        criteria: r.criteria,
        generatedAt: sources.metadata.generated_at,
      };
    }
  } catch {
    // sources.json may be absent
  }

  return {
    uniqueId: src.unique_id,
    sourceName: src.source_name,
    tableName: src.name,
    identifier: src.identifier ?? src.name,
    database: src.database,
    schema: src.schema,
    loader: src.loader,
    loadedAtField: src.loaded_at_field,
    description: src.description,
    sourceDescription: src.source_description,
    freshness: src.freshness,
    columns: src.columns ? Object.values(src.columns) : [],
    meta: src.meta ?? {},
    tags: src.tags ?? [],
    freshnessResult,
  };
}

export const dbtListExposuresSchema = z.object({
  exposureType: z.string().optional().describe("Filter by type (dashboard | application | ml | analysis | notebook)"),
  search: z.string().optional().describe("Substring match against exposure name"),
  limit: z.coerce.number().int().min(1).max(1000).default(200),
});

export async function dbtListExposures(args: z.infer<typeof dbtListExposuresSchema>): Promise<unknown> {
  const manifest = loadManifest();
  if (!manifest.exposures) return { count: 0, exposures: [] };
  const out: Array<Record<string, unknown>> = [];
  const search = args.search?.toLowerCase();
  for (const exp of Object.values(manifest.exposures)) {
    if (args.exposureType && exp.exposure_type !== args.exposureType) continue;
    if (search && !exp.name.toLowerCase().includes(search)) continue;
    out.push({
      uniqueId: exp.unique_id,
      name: exp.name,
      type: exp.exposure_type,
      owner: exp.owner,
      description: exp.description,
      dependsOn: exp.depends_on?.nodes ?? [],
      tags: exp.tags ?? [],
    });
    if (out.length >= args.limit) break;
  }
  return { count: out.length, exposures: out };
}
