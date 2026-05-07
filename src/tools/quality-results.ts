import { z } from "zod";
import { dqQuery, resultsTable } from "../clients/dq-store.js";
import { getDqColumns, getDqFlavor, tableTimeWindowSql } from "../clients/dq-schema.js";
import { config } from "../config.js";

export const dqListChecksSchema = z.object({
  dataset: z.string().optional().describe("Filter by dataset / source"),
  status: z.enum(["pass", "fail", "warn", "error"]).optional().describe("Filter by status"),
  type: z.string().optional().describe("Filter by check type (dbt_test | freshness | anomaly | reconciliation | ...)"),
  sinceHours: z.coerce.number().int().min(1).max(720).default(24),
  limit: z.coerce.number().int().min(1).max(500).default(100),
});

export async function dqListChecks(args: z.infer<typeof dqListChecksSchema>): Promise<unknown> {
  const flavor = getDqFlavor();
  const cols = getDqColumns(flavor);
  const backend = config.dq.backend;
  const filters: string[] = [];
  const params: unknown[] = [];
  if (args.dataset) {
    filters.push(`${cols.dataset} = ?`);
    params.push(args.dataset);
  }
  if (args.status) {
    filters.push(`LOWER(${cols.status}) = ?`);
    params.push(args.status);
  }
  if (args.type) {
    filters.push(`${cols.checkType} = ?`);
    params.push(args.type);
  }
  filters.push(tableTimeWindowSql(flavor, backend, "HOUR"));
  params.push(args.sinceHours);
  const where = "WHERE " + filters.join(" AND ");

  const checkNameSelect = cols.checkName
    ? cols.checkName + " AS check_name"
    : `(${cols.checkType} || ':' || COALESCE(${cols.tableName}, '')) AS check_name`;

  const sql = `
    SELECT ${checkNameSelect},
           ${cols.checkType} AS check_type,
           ${cols.dataset} AS dataset,
           ${cols.tableName} AS table_name,
           ${cols.status} AS status,
           ${cols.severity} AS severity,
           ${cols.failureCount} AS failure_count,
           ${cols.runAt} AS run_at,
           ${cols.message} AS message
    FROM ${resultsTable()}
    ${where}
    ORDER BY ${cols.runAt} DESC
    LIMIT ?`;
  params.push(args.limit);
  return { ...(await dqQuery(sql, params)), schema: flavor };
}

export const dqGetCheckHistorySchema = z.object({
  checkName: z.string().describe("Generic schema: exact check_name. us-all schema: 'check_type:target_name' (concat)"),
  days: z.coerce.number().int().min(1).max(365).default(30),
  limit: z.coerce.number().int().min(1).max(2000).default(500),
});

export async function dqGetCheckHistory(
  args: z.infer<typeof dqGetCheckHistorySchema>,
): Promise<unknown> {
  const flavor = getDqFlavor();
  const cols = getDqColumns(flavor);
  const backend = config.dq.backend;

  let where: string;
  const params: unknown[] = [];
  if (cols.checkName) {
    where = `${cols.checkName} = ? AND ${tableTimeWindowSql(flavor, backend, "DAY")}`;
    params.push(args.checkName, args.days);
  } else {
    // us-all flavor: synthesize check_name from check_type+target_name
    const [checkType, targetName] = args.checkName.split(":");
    if (!checkType) throw new Error("us-all schema requires checkName as 'check_type:target_name'");
    where = `${cols.checkType} = ? AND ${cols.tableName} = ? AND ${tableTimeWindowSql(flavor, backend, "DAY")}`;
    params.push(checkType, targetName ?? "", args.days);
  }

  const checkNameSelect = cols.checkName
    ? cols.checkName + " AS check_name"
    : `(${cols.checkType} || ':' || COALESCE(${cols.tableName}, '')) AS check_name`;

  const sql = `
    SELECT ${checkNameSelect},
           ${cols.status} AS status,
           ${cols.severity} AS severity,
           ${cols.failureCount} AS failure_count,
           ${cols.runAt} AS run_at,
           ${cols.message} AS message
    FROM ${resultsTable()}
    WHERE ${where}
    ORDER BY ${cols.runAt} DESC
    LIMIT ?`;
  params.push(args.limit);
  return { ...(await dqQuery(sql, params)), schema: flavor };
}

export const dqFailedChecksByDatasetSchema = z.object({
  sinceHours: z.coerce.number().int().min(1).max(720).default(24),
  topN: z.coerce.number().int().min(1).max(100).default(20),
});

export async function dqFailedChecksByDataset(
  args: z.infer<typeof dqFailedChecksByDatasetSchema>,
): Promise<unknown> {
  const flavor = getDqFlavor();
  const cols = getDqColumns(flavor);
  const backend = config.dq.backend;

  const sql = `
    SELECT ${cols.dataset} AS dataset,
           COUNT(*) AS failures
    FROM ${resultsTable()}
    WHERE LOWER(${cols.status}) IN ('fail', 'error')
      AND ${tableTimeWindowSql(flavor, backend, "HOUR")}
    GROUP BY ${cols.dataset}
    ORDER BY failures DESC
    LIMIT ?`;
  const params: unknown[] = [args.sinceHours, args.topN];
  return { ...(await dqQuery(sql, params)), schema: flavor };
}
