import { existsSync, readFileSync, statSync, readdirSync } from "node:fs";
import { join, basename } from "node:path";
import { config } from "../config.js";

// dbt manifest.json schema_version we explicitly target. Other minor versions
// usually parse cleanly but we surface a caveat if it differs.
export const SUPPORTED_MANIFEST_SCHEMA_PREFIXES = [
  "https://schemas.getdbt.com/dbt/manifest/v11",
  "https://schemas.getdbt.com/dbt/manifest/v12",
  "https://schemas.getdbt.com/dbt/manifest/v13",
  "https://schemas.getdbt.com/dbt/manifest/v14",
];

export interface DbtNode {
  unique_id: string;
  name: string;
  resource_type: string;
  package_name: string;
  path?: string;
  original_file_path?: string;
  schema?: string;
  database?: string;
  alias?: string;
  config?: { materialized?: string; tags?: string[]; meta?: Record<string, unknown> };
  description?: string;
  columns?: Record<
    string,
    { name: string; description?: string; data_type?: string; meta?: Record<string, unknown>; tags?: string[] }
  >;
  refs?: Array<string[] | { name: string; package?: string | null; version?: string | null }>;
  sources?: string[][];
  depends_on?: { nodes?: string[]; macros?: string[] };
  raw_code?: string;
  compiled_code?: string;
  tags?: string[];
  meta?: Record<string, unknown>;
  test_metadata?: { name: string; kwargs?: Record<string, unknown>; namespace?: string | null };
  column_name?: string;
  attached_node?: string;
  severity?: string;
  exposure_type?: string;
  owner?: { name?: string; email?: string };
  freshness?: {
    warn_after?: { count: number; period: string };
    error_after?: { count: number; period: string };
    filter?: string;
  };
  loader?: string;
  loaded_at_field?: string;
  identifier?: string;
  arguments?: Array<{ name: string; type?: string; description?: string }>;
}

export interface DbtSource extends DbtNode {
  source_name: string;
  source_description?: string;
  fqn?: string[];
}

export interface DbtManifest {
  metadata: {
    dbt_schema_version: string;
    dbt_version: string;
    generated_at: string;
    project_name?: string;
    adapter_type?: string;
  };
  nodes: Record<string, DbtNode>;
  sources: Record<string, DbtSource>;
  macros: Record<string, DbtNode>;
  exposures?: Record<string, DbtNode>;
  parent_map?: Record<string, string[]>;
  child_map?: Record<string, string[]>;
}

export interface DbtRunResult {
  unique_id: string;
  status: string;
  execution_time?: number;
  message?: string | null;
  failures?: number | null;
  thread_id?: string;
  timing?: Array<{ name: string; started_at: string; completed_at: string }>;
  adapter_response?: { rows_affected?: number; bytes_processed?: number };
}

export interface DbtRunResultsFile {
  metadata: {
    dbt_schema_version: string;
    generated_at: string;
    invocation_id?: string;
  };
  results: DbtRunResult[];
  elapsed_time?: number;
  args?: Record<string, unknown>;
}

export interface DbtSourcesFile {
  metadata: { generated_at: string };
  results: Array<{
    unique_id: string;
    status: string;
    max_loaded_at?: string;
    snapshotted_at?: string;
    max_loaded_at_time_ago_in_s?: number;
    criteria?: { warn_after?: { count: number; period: string }; error_after?: { count: number; period: string } };
  }>;
  elapsed_time?: number;
}

export interface DbtCatalog {
  metadata: { generated_at: string };
  nodes: Record<
    string,
    {
      unique_id: string;
      metadata: { schema?: string; name?: string; type?: string };
      columns: Record<string, { name: string; type: string; index?: number; comment?: string | null }>;
      stats?: Record<string, { id: string; label?: string; value?: unknown }>;
    }
  >;
  sources: Record<string, unknown>;
}

interface CacheEntry<T> {
  data: T;
  mtime: number;
}

const cache: {
  manifest?: CacheEntry<DbtManifest>;
  runResults?: CacheEntry<DbtRunResultsFile>;
  sources?: CacheEntry<DbtSourcesFile>;
  catalog?: CacheEntry<DbtCatalog>;
} = {};

function readArtifact<T>(path: string): T {
  if (!existsSync(path)) {
    throw new Error(`dbt artifact not found: ${path}`);
  }
  return JSON.parse(readFileSync(path, "utf-8")) as T;
}

function readWithCache<T>(
  key: keyof typeof cache,
  path: string,
): T {
  if (!existsSync(path)) {
    throw new Error(`dbt artifact not found: ${path}`);
  }
  const mtime = statSync(path).mtimeMs;
  const cached = cache[key] as CacheEntry<T> | undefined;
  if (cached && cached.mtime === mtime) {
    return cached.data;
  }
  const data = readArtifact<T>(path);
  cache[key] = { data, mtime } as never;
  return data;
}

function targetPath(filename: string): string {
  if (!config.dbt.targetDir) {
    throw new Error("DBT target dir not configured");
  }
  return join(config.dbt.targetDir, filename);
}

export function loadManifest(): DbtManifest {
  return readWithCache<DbtManifest>("manifest", targetPath("manifest.json"));
}

export function loadRunResults(): DbtRunResultsFile {
  return readWithCache<DbtRunResultsFile>("runResults", targetPath("run_results.json"));
}

export function loadSources(): DbtSourcesFile {
  return readWithCache<DbtSourcesFile>("sources", targetPath("sources.json"));
}

export function loadCatalog(): DbtCatalog | null {
  try {
    return readWithCache<DbtCatalog>("catalog", targetPath("catalog.json"));
  } catch {
    return null;
  }
}

export interface ArchivedRun {
  filePath: string;
  fileName: string;
  generatedAt: string;
  invocationId?: string;
  results: DbtRunResult[];
}

export function listRunHistory(limit = 20): ArchivedRun[] {
  const out: ArchivedRun[] = [];
  const dirs: string[] = [];
  if (config.dbt.runHistoryDir && existsSync(config.dbt.runHistoryDir)) {
    dirs.push(config.dbt.runHistoryDir);
  }
  if (config.dbt.targetDir && existsSync(config.dbt.targetDir)) {
    dirs.push(config.dbt.targetDir);
  }
  const seenInvocationIds = new Set<string>();
  for (const dir of dirs) {
    let entries: string[];
    try {
      entries = readdirSync(dir);
    } catch {
      continue;
    }
    for (const entry of entries) {
      if (!entry.endsWith(".json")) continue;
      if (!entry.includes("run_results")) continue;
      const filePath = join(dir, entry);
      try {
        const data = readArtifact<DbtRunResultsFile>(filePath);
        const invocationId = data.metadata?.invocation_id;
        if (invocationId && seenInvocationIds.has(invocationId)) continue;
        if (invocationId) seenInvocationIds.add(invocationId);
        out.push({
          filePath,
          fileName: basename(entry),
          generatedAt: data.metadata?.generated_at ?? "",
          invocationId,
          results: data.results ?? [],
        });
      } catch {
        // skip unreadable
      }
    }
  }
  out.sort((a, b) => (b.generatedAt > a.generatedAt ? 1 : -1));
  return out.slice(0, limit);
}

export function manifestSchemaSupported(manifest: DbtManifest): boolean {
  const schemaUrl = manifest.metadata?.dbt_schema_version ?? "";
  return SUPPORTED_MANIFEST_SCHEMA_PREFIXES.some((p) => schemaUrl.startsWith(p));
}

export function clearArtifactCache(): void {
  delete cache.manifest;
  delete cache.runResults;
  delete cache.sources;
  delete cache.catalog;
}
