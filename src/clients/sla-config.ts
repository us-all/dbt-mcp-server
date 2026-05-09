/**
 * SLA config reader.
 *
 * Reads a YAML file (path: `DBT_SLA_CONFIG_PATH` env) that captures the
 * project's tier thresholds + DAG SLAs. Optional — when unset, the quality
 * tools fall back to hardcoded defaults (Tier 1=99.5 / 2=99.0 / 3=95.0,
 * matches us-all's actual values but is a sensible default everywhere).
 *
 * Supported shape (additional keys ignored):
 *
 *   tier_sla:
 *     1: 99.5
 *     2: 99.0
 *     3: 95.0
 *   dbt_sla:
 *     test_pass_pct: 99.0
 *     freshness_pass_pct: 99.5
 *
 * Mtime cached, so editing the yaml between tool calls picks up the new
 * values without a server restart.
 */
import { existsSync, readFileSync, statSync } from "node:fs";
import { parse as parseYaml } from "yaml";

export interface SlaConfig {
  tierSla: Record<string, number>; // key is the tier as a string (matches DQ score table tier column type)
  dbtSla?: {
    testPassPct?: number;
    freshnessPassPct?: number;
  };
}

const DEFAULT_TIER_SLA: Record<string, number> = {
  "1": 99.5,
  "2": 99.0,
  "3": 95.0,
};

interface CacheEntry {
  mtimeMs: number;
  config: SlaConfig;
}

let cache: CacheEntry | null = null;
let cachedPath: string | null = null;

function configPath(): string {
  return (process.env.DBT_SLA_CONFIG_PATH ?? "").trim();
}

/**
 * Load SLA config. Returns null when DBT_SLA_CONFIG_PATH is unset; throws if
 * the path is set but the file is missing or unparseable.
 */
export function loadSlaConfig(): SlaConfig | null {
  const path = configPath();
  if (!path) {
    cache = null;
    cachedPath = null;
    return null;
  }
  if (!existsSync(path)) {
    throw new Error(`DBT_SLA_CONFIG_PATH does not exist: ${path}`);
  }
  const mtimeMs = statSync(path).mtimeMs;
  if (cache && cachedPath === path && cache.mtimeMs === mtimeMs) {
    return cache.config;
  }
  const raw = readFileSync(path, "utf8");
  let parsed: unknown;
  try {
    parsed = parseYaml(raw);
  } catch (err) {
    throw new Error(`Failed to parse DBT_SLA_CONFIG_PATH as YAML: ${path}: ${(err as Error).message}`);
  }

  const tierSlaRaw = (parsed as { tier_sla?: Record<string | number, number> })?.tier_sla ?? {};
  const tierSla: Record<string, number> = {};
  for (const [k, v] of Object.entries(tierSlaRaw)) {
    if (typeof v === "number" && Number.isFinite(v)) {
      tierSla[String(k)] = v;
    }
  }

  const dbtSlaRaw = (parsed as { dbt_sla?: Record<string, number> })?.dbt_sla;
  const dbtSla =
    dbtSlaRaw && typeof dbtSlaRaw === "object"
      ? {
          testPassPct: typeof dbtSlaRaw.test_pass_pct === "number" ? dbtSlaRaw.test_pass_pct : undefined,
          freshnessPassPct:
            typeof dbtSlaRaw.freshness_pass_pct === "number" ? dbtSlaRaw.freshness_pass_pct : undefined,
        }
      : undefined;

  const config: SlaConfig = { tierSla, dbtSla };
  cache = { mtimeMs, config };
  cachedPath = path;
  return config;
}

/**
 * Returns per-tier overall_score targets. Reads `tier_sla` from
 * `DBT_SLA_CONFIG_PATH` if configured; otherwise returns defaults
 * (1=99.5, 2=99.0, 3=95.0).
 */
export function getTierTargets(): Record<string, number> {
  const cfg = loadSlaConfig();
  if (cfg && Object.keys(cfg.tierSla).length > 0) return { ...cfg.tierSla };
  return { ...DEFAULT_TIER_SLA };
}

/**
 * Tier 1 target — used by `dq-tier-status` no-tier-column path. Resolution:
 *   1. SLA config `tier_sla.1` (if present)
 *   2. DQ_TIER1_TARGET_PCT env var
 *   3. 99.5
 */
export function getTier1Target(): number {
  const cfg = loadSlaConfig();
  if (cfg && typeof cfg.tierSla["1"] === "number") return cfg.tierSla["1"];
  const raw = parseFloat(process.env.DQ_TIER1_TARGET_PCT ?? "");
  if (Number.isFinite(raw)) return raw;
  return 99.5;
}

// Test-only cache reset.
export function _resetSlaConfigCacheForTest(): void {
  cache = null;
  cachedPath = null;
}
