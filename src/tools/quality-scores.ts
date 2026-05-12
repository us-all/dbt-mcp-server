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

  // us-all flavor: no per-scope tier column. Fetch the most recent row at or
  // before the requested date (today if unspecified) and compare its
  // overall_score against DQ_TIER1_TARGET_PCT (default 99.5). The "<= cutoff
  // ORDER BY DESC LIMIT 1" form silently falls back to the previous run when
  // today's score row hasn't landed yet — keeps `meeting` answerable instead
  // of stalling at null whenever the daily aggregator is mid-window.
  const useDate = args.date ?? null;
  const todayClause = backend === "bigquery" ? "CURRENT_DATE()" : "CURRENT_DATE";
  const cutoffExpr = useDate ? "?" : todayClause;
  const sql = `
    SELECT ${cols.scoreDate} AS score_date, overall_score, total_checks, failed_checks
    FROM ${scoreTable()}
    WHERE ${cols.scoreDate} <= ${cutoffExpr}
    ORDER BY ${cols.scoreDate} DESC
    LIMIT 1`;
  const result = await dqQuery(sql, useDate ? [useDate] : []);
  const target = defaultTier1TargetPct();
  const row = result.rows[0];
  const score = row?.overall_score != null ? Number(row.overall_score) : null;
  const meeting = score == null ? null : score >= target;
  const actualDate = row?.score_date ?? null;
  const requestedDate = useDate ?? "today";
  const fellBack =
    row != null && useDate != null && String(actualDate).slice(0, 10) !== useDate;
  const notes: string[] = [];
  if (fellBack) {
    notes.push(
      `No row for ${requestedDate}; using most recent prior row (${actualDate}).`,
    );
  }
  notes.push(
    `${flavor} schema has a single overall_score per day (no per-scope tiers). Comparing against tier-1 target ${target}% (DBT_SLA_CONFIG_PATH > DQ_TIER1_TARGET_PCT > 99.5).`,
  );
  return {
    backend: result.backend,
    schema: flavor,
    target,
    score,
    meeting,
    totalChecks: row?.total_checks ?? null,
    failedChecks: row?.failed_checks ?? null,
    scoreDate: actualDate,
    fellBack,
    notes,
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
  mode: z
    .enum(["source", "table"])
    .default("source")
    .describe(
      "How to roll up. 'source' (default) groups by the dataset/source column and looks up tier from each source group's first table-level meta.tier. 'table' groups by table_name (assumed format '<source_group>.<table>'), parses the prefix, and looks up the table-level meta.tier — useful when meta.tier varies per table inside a source group.",
    ),
  sourceFilter: z
    .string()
    .optional()
    .describe(
      "Optional pre-filter on the dataset/source column. Useful in mode='table' when only some source rows have target_name in '<source_group>.<table>' format (e.g. sourceFilter='bq' to keep only the BigQuery-shaped rows).",
    ),
});

export async function dqTierBySource(args: z.infer<typeof dqTierBySourceSchema>): Promise<unknown> {
  const flavor = getDqFlavor();
  const cols = getDqColumns(flavor);
  const backend = config.dq.backend;
  const mode = args.mode ?? "source";

  // 1. Build the manifest -> tier maps.
  //    sourceTier: source_name -> tier (first table's meta.tier per source group; mode='source')
  //    tableTier:  "source_name.table_name" -> tier (per-table meta.tier;        mode='table')
  const manifest = loadManifest();
  const sourceTier: Record<string, number> = {};
  const tableTier: Record<string, number> = {};
  const coerceTier = (raw: unknown): number | null => {
    if (typeof raw === "number" && Number.isFinite(raw)) return raw;
    if (typeof raw === "string" && /^\d+$/.test(raw)) return Number(raw);
    return null;
  };
  for (const src of Object.values(manifest.sources)) {
    const tier = coerceTier((src.meta as { tier?: unknown } | undefined)?.tier);
    if (tier == null) continue;
    if (sourceTier[src.source_name] === undefined) sourceTier[src.source_name] = tier;
    tableTier[`${src.source_name}.${src.name}`] = tier;
  }

  // 2. Query.
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
  if (args.sourceFilter) {
    filters.push(`${cols.dataset} = ?`);
    params.push(args.sourceFilter);
  }
  const where = "WHERE " + filters.join(" AND ");

  const passExpr = `SUM(CASE WHEN LOWER(${cols.status}) = 'pass' THEN 1 ELSE 0 END)`;
  const groupCol = mode === "table" ? cols.tableName : cols.dataset;

  const sql = `
    SELECT ${groupCol} AS rollup_key,
           COUNT(*) AS total_checks,
           ${passExpr} AS passed_checks
    FROM ${resultsTable()}
    ${where}
    GROUP BY ${groupCol}
    ORDER BY ${groupCol}`;
  const result = await dqQuery(sql, params);

  // 3. Per-row tier lookup + pass-rate.
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
    const key = String(row.rollup_key ?? "");
    const total = Number(row.total_checks ?? 0);
    const passed = Number(row.passed_checks ?? 0);
    const passPct = total > 0 ? (passed / total) * 100 : 0;
    let tier: number | null;
    if (mode === "table") {
      const dot = key.indexOf(".");
      tier = dot > 0 && dot < key.length - 1 ? (tableTier[key] ?? null) : null;
    } else {
      tier = sourceTier[key] ?? null;
    }
    const target = tier != null ? (targets[String(tier)] ?? null) : null;
    const meeting = target == null ? null : passPct >= target;
    sources.push({ source: key, tier, target, totalChecks: total, passedChecks: passed, passPct, meeting });
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
    const subject = mode === "table" ? "table(s)" : "source(s)";
    caveats.push(
      `${untieredSources.length} ${subject} have no tier in dbt manifest meta.tier — excluded from per-tier rollup: ${untieredSources.slice(0, 10).join(", ")}${untieredSources.length > 10 ? ", ..." : ""}`,
    );
  }
  const tierMapSize = mode === "table" ? Object.keys(tableTier).length : Object.keys(sourceTier).length;
  if (tierMapSize === 0) {
    const subject = mode === "table" ? "table" : "source group";
    caveats.push(
      `No ${subject} in the dbt manifest carries meta.tier — set tier on each ${subject} in sources.yml to enable per-tier rollup.`,
    );
  }

  // `sourcesWithTier` reports the response itself: how many rows in `sources`
  // came back with a tier match. Pre-v0.4.2 this returned the manifest source
  // group count instead, which silently disagreed with the visible payload
  // (e.g. mode='source' could surface 4 tier=null rows while the counter read
  // 2). The manifest-level totals are still useful for ops debugging and are
  // kept on `manifestSourceGroupsWithTier` / `manifestTablesWithTier`.
  const sourcesWithTier = sources.filter((s) => s.tier != null).length;
  return {
    backend: result.backend,
    schema: flavor,
    mode,
    targets,
    sources,
    tierRollup,
    caveats,
    sourcesWithTier,
    tablesWithTier: Object.keys(tableTier).length,
    manifestSourceGroupsWithTier: Object.keys(sourceTier).length,
    manifestTablesWithTier: Object.keys(tableTier).length,
  };
}
