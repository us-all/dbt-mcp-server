import { z } from "zod";
import { dqQuery, scoreTable } from "../clients/dq-store.js";
import { getDqColumns, getDqFlavor, hasScope, defaultTier1TargetPct, tableTimeWindowSql } from "../clients/dq-schema.js";
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
    const targets: Record<string, number> = { "1": 99.5, "2": 99.0, "3": 95.0 };
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
      `${flavor} schema has a single overall_score per day (no per-scope tiers). Comparing against DQ_TIER1_TARGET_PCT=${target}.`,
    ],
  };
}
