import { z } from "zod";
import { dqQuery, resultsTable } from "../clients/dq-store.js";

export const dqListChecksSchema = z.object({
  dataset: z.string().optional().describe("Filter by dataset / schema"),
  status: z.enum(["pass", "fail", "warn", "error"]).optional().describe("Filter by status"),
  type: z.string().optional().describe("Filter by check type (dbt_test | freshness | anomaly | reconciliation | ...)"),
  sinceHours: z.coerce.number().int().min(1).max(720).default(24),
  limit: z.coerce.number().int().min(1).max(500).default(100),
});

export async function dqListChecks(args: z.infer<typeof dqListChecksSchema>): Promise<unknown> {
  const filters: string[] = [];
  const params: unknown[] = [];
  if (args.dataset) {
    filters.push("dataset = ?");
    params.push(args.dataset);
  }
  if (args.status) {
    filters.push("LOWER(status) = ?");
    params.push(args.status);
  }
  if (args.type) {
    filters.push("check_type = ?");
    params.push(args.type);
  }
  filters.push(`run_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ? HOUR)`);
  params.push(args.sinceHours);
  const where = filters.length ? "WHERE " + filters.join(" AND ") : "";
  const sql = `
    SELECT check_name, check_type, dataset, table_name, status, severity,
           failure_count, run_at, message
    FROM ${resultsTable()}
    ${where}
    ORDER BY run_at DESC
    LIMIT ?`;
  params.push(args.limit);
  return dqQuery(sql, params);
}

export const dqGetCheckHistorySchema = z.object({
  checkName: z.string().describe("Exact check_name as stored in the results table"),
  days: z.coerce.number().int().min(1).max(365).default(30),
  limit: z.coerce.number().int().min(1).max(2000).default(500),
});

export async function dqGetCheckHistory(
  args: z.infer<typeof dqGetCheckHistorySchema>,
): Promise<unknown> {
  const sql = `
    SELECT check_name, status, severity, failure_count, run_at, message
    FROM ${resultsTable()}
    WHERE check_name = ?
      AND run_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ? DAY)
    ORDER BY run_at DESC
    LIMIT ?`;
  return dqQuery(sql, [args.checkName, args.days, args.limit]);
}

export const dqFailedChecksByDatasetSchema = z.object({
  sinceHours: z.coerce.number().int().min(1).max(720).default(24),
  topN: z.coerce.number().int().min(1).max(100).default(20),
});

export async function dqFailedChecksByDataset(
  args: z.infer<typeof dqFailedChecksByDatasetSchema>,
): Promise<unknown> {
  const sql = `
    SELECT dataset,
           COUNT(*) AS failures,
           ARRAY_AGG(STRUCT(check_name AS name, severity AS severity, run_at AS at, message AS message)
                     ORDER BY run_at DESC LIMIT 5) AS recent
    FROM ${resultsTable()}
    WHERE LOWER(status) IN ('fail', 'error')
      AND run_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ? HOUR)
    GROUP BY dataset
    ORDER BY failures DESC
    LIMIT ?`;
  return dqQuery(sql, [args.sinceHours, args.topN]);
}
