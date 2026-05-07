/**
 * DQ result-table schema flavors.
 *
 * v0.1 supports two presets via the `DQ_SCHEMA` env var:
 *
 * - `generic` (default) — generic-shaped DQ tables:
 *   `quality_checks(run_at, check_name, check_type, dataset, table_name, status, severity, failure_count, message)`
 *   `quality_score_daily(score_date, scope, tier, completeness_pct, freshness_pct, validity_pct, anomaly_free_pct, overall_score)`
 *
 * - `us-all` — schema used internally at us-all (Postgres `data_ops`):
 *   `quality_checks(run_date, check_type, dimension, source, target_name, status, metric_value, threshold, details)`
 *   `quality_score_daily(run_date, completeness_pct, freshness_pct, validity_pct, anomaly_free_pct, overall_score, total_checks, failed_checks)`
 *
 * Pick the flavor that matches the table you're pointing at. v0.2 will introduce
 * per-column env vars (`DQ_COL_*`) for arbitrary schemas.
 */
import { config } from "../config.js";

export type DqFlavor = "generic" | "us-all";

export function getDqFlavor(): DqFlavor {
  const raw = (process.env.DQ_SCHEMA ?? "generic").toLowerCase();
  if (raw === "us-all" || raw === "us_all") return "us-all";
  return "generic";
}

/**
 * Column mapping returned by `getDqColumns()`. Each property is the actual
 * column name in the configured table for the corresponding logical concept.
 */
export interface DqColumnMap {
  // quality_checks
  runAt: string;          // generic: run_at  | us-all: run_date
  checkType: string;      // both: check_type
  status: string;         // both: status
  dataset: string;        // generic: dataset | us-all: source
  tableName: string;      // generic: table_name | us-all: target_name
  severity: string;       // generic: severity | us-all: dimension
  failureCount: string;   // generic: failure_count | us-all: metric_value
  message: string;        // generic: message | us-all: details::text
  checkName: string | null; // generic: check_name | us-all: NULL (no equivalent — use check_type+target_name)

  // quality_score_daily
  scoreDate: string;      // generic: score_date | us-all: run_date
  scope: string | null;   // generic: scope     | us-all: NULL (no scope dimension)
  tier: string | null;    // generic: tier      | us-all: NULL
}

export function getDqColumns(flavor: DqFlavor = getDqFlavor()): DqColumnMap {
  if (flavor === "us-all") {
    return {
      runAt: "run_date",
      checkType: "check_type",
      status: "status",
      dataset: "source",
      tableName: "target_name",
      severity: "dimension",
      failureCount: "metric_value",
      message: "details::text",
      checkName: null,
      scoreDate: "run_date",
      scope: null,
      tier: null,
    };
  }
  return {
    runAt: "run_at",
    checkType: "check_type",
    status: "status",
    dataset: "dataset",
    tableName: "table_name",
    severity: "severity",
    failureCount: "failure_count",
    message: "message",
    checkName: "check_name",
    scoreDate: "score_date",
    scope: "scope",
    tier: "tier",
  };
}

/**
 * Returns true if the configured DQ_SCHEMA flavor exposes a `scope` dimension
 * on the score table (i.e. one row per (date, scope)). When false, the score
 * table has one row per date — the whole org rolled up — and `scope` filters /
 * tier-by-scope rollups must be skipped.
 */
export function hasScope(flavor: DqFlavor = getDqFlavor()): boolean {
  return getDqColumns(flavor).scope !== null;
}

/**
 * BigQuery FQNs need backticks; Postgres + DuckDB don't. The dq-store
 * `qualifyTable()` already handles BQ backticks; we mostly use the env var
 * value verbatim for PG.
 */
export function tableTimeWindowSql(
  flavor: DqFlavor,
  backend: "bigquery" | "postgres",
  intervalUnit: "HOUR" | "DAY",
): string {
  const cols = getDqColumns(flavor);
  const dateCol = intervalUnit === "DAY" ? cols.scoreDate : cols.runAt;
  if (backend === "bigquery") {
    return intervalUnit === "DAY"
      ? `${dateCol} >= DATE_SUB(CURRENT_DATE(), INTERVAL ? DAY)`
      : `${dateCol} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ? HOUR)`;
  }
  // Postgres
  return intervalUnit === "DAY"
    ? `${dateCol} >= CURRENT_DATE - (? || ' days')::interval`
    : `${dateCol} >= NOW() - (? || ' hours')::interval`;
}

/**
 * Tier 1 SLA target — used by `dq-tier-status`. v0.1 hardcodes 99.5 (Tier 1).
 * v0.2 may surface a per-tier env var or read from a config table.
 */
export function defaultTier1TargetPct(): number {
  const raw = parseFloat(process.env.DQ_TIER1_TARGET_PCT ?? "99.5");
  return Number.isFinite(raw) ? raw : 99.5;
}

// keep config import alive (lint)
export const _configBackend = config.dq.backend;
