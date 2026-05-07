import { z } from "zod";
import { dqQuery, scoreTable } from "../clients/dq-store.js";

export const dqScoreTrendSchema = z.object({
  days: z.coerce.number().int().min(1).max(365).default(14),
  scope: z.string().optional().describe("Scope filter (domain or dataset, when score table supports it)"),
});

export async function dqScoreTrend(args: z.infer<typeof dqScoreTrendSchema>): Promise<unknown> {
  const filters: string[] = [];
  const params: unknown[] = [];
  filters.push(`score_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ? DAY)`);
  params.push(args.days);
  if (args.scope) {
    filters.push("scope = ?");
    params.push(args.scope);
  }
  const where = "WHERE " + filters.join(" AND ");
  const sql = `
    SELECT score_date, scope,
           completeness_pct, freshness_pct, validity_pct, anomaly_free_pct,
           overall_score
    FROM ${scoreTable()}
    ${where}
    ORDER BY score_date DESC, scope`;
  return dqQuery(sql, params);
}

export const dqTierStatusSchema = z.object({
  date: z
    .string()
    .optional()
    .describe("ISO date (YYYY-MM-DD) to check, default = today"),
});

export async function dqTierStatus(args: z.infer<typeof dqTierStatusSchema>): Promise<unknown> {
  const useDate = args.date ?? null;
  const sql = useDate
    ? `
      SELECT tier, scope, overall_score
      FROM ${scoreTable()}
      WHERE score_date = ?
      ORDER BY tier, scope`
    : `
      SELECT tier, scope, overall_score
      FROM ${scoreTable()}
      WHERE score_date = CURRENT_DATE()
      ORDER BY tier, scope`;
  const result = await dqQuery(sql, useDate ? [useDate] : []);

  // SLA targets per Tier (us-all standard).
  const targets: Record<string, number> = { "1": 99.5, "2": 99.0, "3": 95.0 };
  const summary: Record<string, { target: number; observations: number; meeting: number; missing: number }> = {};

  for (const row of result.rows) {
    const tier = String(row.tier ?? "");
    const score = Number(row.overall_score ?? 0);
    if (!summary[tier]) {
      summary[tier] = {
        target: targets[tier] ?? 0,
        observations: 0,
        meeting: 0,
        missing: 0,
      };
    }
    summary[tier].observations += 1;
    if (targets[tier] != null && score >= targets[tier]) {
      summary[tier].meeting += 1;
    } else {
      summary[tier].missing += 1;
    }
  }

  return {
    backend: result.backend,
    rowsExamined: result.rowCount,
    tiers: summary,
    rows: result.rows,
  };
}
