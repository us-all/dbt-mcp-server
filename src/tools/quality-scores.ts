import { z } from "zod";
import { dqQuery, scoreTable, resultsTable } from "../clients/dq-store.js";
import { getDqColumns, getDqFlavor, hasScope, defaultTier1TargetPct, tableTimeWindowSql } from "../clients/dq-schema.js";
import { getTierTargets } from "../clients/sla-config.js";
import { loadManifest } from "../clients/dbt-artifacts.js";
import { config } from "../config.js";

export const dqScoreTrendSchema = z.object({
  days: z.coerce.number().int().min(1).max(365).default(14),
  scope: z.string().optional().describe("Scope filter (only honored when DQ_SCHEMA=generic)"),
});

export async function dqScoreTrend(args: z.infer<typeof dqScoreTrendSchema>): Promise<unknown> {
  const flavor = getDqFlavor();
  const cols = getDqColumns(flavor);
  const backend = config.dq.backend;
  const filters: string[] = [];
  const params: unknown[] = [];
  filters.push(tableTimeWindowSql(flavor, backend, "DAY"));
  params.push(args.days);
  if (args.scope && hasScope(flavor)) {
    filters.push(`${cols.scope} = ?`);
    params.push(args.scope);
  }
  const where = "WHERE " + filters.join(" AND ");

  const scopeSelect = cols.scope ? `${cols.scope} AS scope, ` : "";
  const orderBy = cols.scope ? `${cols.scoreDate} DESC, ${cols.scope}` : `${cols.scoreDate} DESC`;

  const sql = `
    SELECT ${cols.scoreDate} AS score_date, ${scopeSelect}
           completeness_pct, freshness_pct, validity_pct, anomaly_free_pct,
           overall_score
    FROM ${scoreTable()}
    ${where}
    ORDER BY ${orderBy}`;
  const result = await dqQuery(sql, params);
  const caveats: string[] = [];
  if (args.scope && !hasScope(flavor)) {
    caveats.push(`DQ_SCHEMA=${flavor} does not have a scope column — scope filter ignored`);
  }
  return { ...result, schema: flavor, caveats };
}

export const dqTierStatusSchema = z.object({
  date: z
    .string()
    .optional()
    .describe("ISO date (YYYY-MM-DD) to check, default = today"),
});

export async function dqTierStatus(args: z.infer<typeof dqTierStatusSchema>): Promise<unknown> {
  const flavor = getDqFlavor();
  const cols = getDqColumns(flavor);
  const backend = config.dq.backend;

  if (cols.tier) {
    // Generic schema: original Tier 1/2/3 rollup against per-scope rows.
    const useDate = args.date ?? null;
    const todayClause =
      backend === "bigquery" ? "CURRENT_DATE()" : "CURRENT_DATE";
    const sql = useDate
      ? `
        SELECT ${cols.tier} AS tier, ${cols.scope} AS scope, overall_score
        FROM ${scoreTable()}
        WHERE ${cols.scoreDate} = ?
        ORDER BY ${cols.tier}, ${cols.scope}`
      : `
        SELECT ${cols.tier} AS tier, ${cols.scope} AS scope, overall_score
        FROM ${scoreTable()}
        WHERE ${cols.scoreDate} = ${todayClause}
        ORDER BY ${cols.tier}, ${cols.scope}`;
    const result = await dqQuery(sql, useDate ? [useDate] : []);
    const targets = getTierTargets();
    const summary: Record<string, { target: number; observations: number; meeting: number; missing: number }> = {};
    for (const row of result.rows) {
      const tier = String(row.tier ?? "");
      const score = Number(row.overall_score ?? 0);
      if (!summary[tier]) {
        summary[tier] = { target: targets[tier] ?? 0, observations: 0, meeting: 0, missing: 0 };
      }
      summary[tier].observations += 1;
      if (targets[tier] != null && score >= targets[tier]) summary[tier].meeting += 1;
      else summary[tier].missing += 1;
    }
    return { backend: result.backend, schema: flavor, rowsExamined: result.rowCount, tiers: summary, rows: result.rows };
  }

  // us-all flavor: no per-scope tier column. Fetch latest single row and
  // compare overall_score vs DQ_TIER1_TARGET_PCT (default 99.5).
  const useDate = args.date ?? null;
  const todayClause = backend === "bigquery" ? "CURRENT_DATE()" : "CURRENT_DATE";
  const sql = useDate
    ? `
      SELECT ${cols.scoreDate} AS score_date, overall_score, total_checks, failed_checks
      FROM ${scoreTable()}
      WHERE ${cols.scoreDate} = ?`
    : `
      SELECT ${cols.scoreDate} AS score_date, overall_score, total_checks, failed_checks
      FROM ${scoreTable()}
      WHERE ${cols.scoreDate} = ${todayClause}`;
  const result = await dqQuery(sql, useDate ? [useDate] : []);
  const target = defaultTier1TargetPct();
  const row = result.rows[0];
  const score = row ? Number(row.overall_score ?? 0) : null;
  const meeting = score == null ? null : score >= target;
  return {
    backend: result.backend,
    schema: flavor,
    target,
    score,
    meeting,
    totalChecks: row?.total_checks ?? null,
    failedChecks: row?.failed_checks ?? null,
    scoreDate: row?.score_date ?? null,
    notes: [
      `${flavor} schema has a single overall_score per day (no per-scope tiers). Comparing against tier-1 target ${target}% (DBT_SLA_CONFIG_PATH > DQ_TIER1_TARGET_PCT > 99.5).`,
    ],
  };
}

export const dqTierBySourceSchema = z.object({
  date: z
    .string()
    .optional()
    .describe("ISO date (YYYY-MM-DD) for the rollup, default = today"),
  sinceHours: z
    .coerce.number().int().min(1).max(720)
    .optional()
    .describe("Alternative window: rollup over the last N hours instead of a single date"),
});

export async function dqTierBySource(args: z.infer<typeof dqTierBySourceSchema>): Promise<unknown> {
  const flavor = getDqFlavor();
  const cols = getDqColumns(flavor);
  const backend = config.dq.backend;

  // 1. Build source_name -> tier map from the dbt manifest. Source-level
  //    meta.tier is repeated on each table source entry; take the first
  //    non-undefined value per source_name.
  const manifest = loadManifest();
  const sourceTier: Record<string, number> = {};
  for (const src of Object.values(manifest.sources)) {
    if (sourceTier[src.source_name] !== undefined) continue;
    const tier = (src.meta as { tier?: unknown } | undefined)?.tier;
    if (typeof tier === "number") {
      sourceTier[src.source_name] = tier;
    } else if (typeof tier === "string" && /^\d+$/.test(tier)) {
      sourceTier[src.source_name] = Number(tier);
    }
  }

  // 2. Query per-source pass-rate over the time window.
  const filters: string[] = [];
  const params: unknown[] = [];
  if (args.date) {
    filters.push(`${cols.runAt} = ?`);
    params.push(args.date);
  } else if (args.sinceHours) {
    filters.push(tableTimeWindowSql(flavor, backend, "HOUR"));
    params.push(args.sinceHours);
  } else {
    const todayClause = backend === "bigquery" ? "CURRENT_DATE()" : "CURRENT_DATE";
    filters.push(`${cols.runAt} = ${todayClause}`);
  }
  const where = "WHERE " + filters.join(" AND ");

  const passExpr =
    backend === "bigquery"
      ? `SUM(CASE WHEN LOWER(${cols.status}) = 'pass' THEN 1 ELSE 0 END)`
      : `SUM(CASE WHEN LOWER(${cols.status}) = 'pass' THEN 1 ELSE 0 END)`;

  const sql = `
    SELECT ${cols.dataset} AS source,
           COUNT(*) AS total_checks,
           ${passExpr} AS passed_checks
    FROM ${resultsTable()}
    ${where}
    GROUP BY ${cols.dataset}
    ORDER BY ${cols.dataset}`;
  const result = await dqQuery(sql, params);

  // 3. Per-source compute pass rate, tier lookup, meeting/missing.
  const targets = getTierTargets();
  const sources: Array<{
    source: string;
    tier: number | null;
    target: number | null;
    totalChecks: number;
    passedChecks: number;
    passPct: number;
    meeting: boolean | null;
  }> = [];
  for (const row of result.rows) {
    const source = String(row.source ?? "");
    const total = Number(row.total_checks ?? 0);
    const passed = Number(row.passed_checks ?? 0);
    const passPct = total > 0 ? (passed / total) * 100 : 0;
    const tier = sourceTier[source] ?? null;
    const target = tier != null ? (targets[String(tier)] ?? null) : null;
    const meeting = target == null ? null : passPct >= target;
    sources.push({ source, tier, target, totalChecks: total, passedChecks: passed, passPct, meeting });
  }

  // 4. Per-tier rollup.
  const tierRollup: Record<string, {
    target: number;
    sources: number;
    meeting: number;
    missing: number;
    avgPassPct: number;
    sourcesEvaluated: string[];
  }> = {};
  for (const s of sources) {
    if (s.tier == null || s.target == null) continue;
    const key = String(s.tier);
    if (!tierRollup[key]) {
      tierRollup[key] = {
        target: s.target,
        sources: 0,
        meeting: 0,
        missing: 0,
        avgPassPct: 0,
        sourcesEvaluated: [],
      };
    }
    const t = tierRollup[key];
    t.sources += 1;
    t.avgPassPct += s.passPct;
    t.sourcesEvaluated.push(s.source);
    if (s.meeting) t.meeting += 1;
    else t.missing += 1;
  }
  for (const t of Object.values(tierRollup)) {
    t.avgPassPct = t.sources > 0 ? t.avgPassPct / t.sources : 0;
  }

  const caveats: string[] = [];
  const untieredSources = sources.filter((s) => s.tier == null).map((s) => s.source);
  if (untieredSources.length > 0) {
    caveats.push(
      `${untieredSources.length} source(s) have no tier in dbt manifest meta.tier — excluded from per-tier rollup: ${untieredSources.slice(0, 10).join(", ")}${untieredSources.length > 10 ? ", ..." : ""}`,
    );
  }
  if (Object.keys(sourceTier).length === 0) {
    caveats.push(
      "No sources in the dbt manifest carry meta.tier — set tier on each source group in sources.yml to enable per-tier rollup.",
    );
  }

  return {
    backend: result.backend,
    schema: flavor,
    targets,
    sources,
    tierRollup,
    caveats,
    sourcesWithTier: Object.keys(sourceTier).length,
  };
}
