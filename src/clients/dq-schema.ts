/**
 * DQ result-table schema mapping.
 *
 * Two layers compose the column map:
 *
 * 1. **Preset** — picked by `DQ_SCHEMA`:
 *    - `generic` (default) — `quality_checks(run_at, check_name, check_type,
 *      dataset, table_name, status, severity, failure_count, message)` +
 *      `quality_score_daily(score_date, scope, tier, ...)`
 *    - `us-all` — `quality_checks(run_date, check_type, dimension, source,
 *      target_name, status, metric_value, threshold, details)` +
 *      `quality_score_daily(run_date, ..., overall_score, total_checks,
 *      failed_checks)` (no scope, no tier)
 *
 * 2. **Per-column override** — any `DQ_COL_*` env var, when set, replaces
 *    the preset value for that single column. Lets users point the server
 *    at an arbitrary DQ schema without writing a SQL view.
 *
 *    Nullable columns (`check_name`, `scope`, `tier`) accept the sentinels
 *    `none` / `null` / `-` to declare "no native column" — the tools then
 *    fall back to synthesized values where possible (e.g. check_name is
 *    built from `check_type || ':' || table_name`).
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

const GENERIC_PRESET: DqColumnMap = {
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

const US_ALL_PRESET: DqColumnMap = {
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

const NULL_SENTINELS = new Set(["none", "null", "-"]);

function envCol(name: string, fallback: string): string {
  const raw = process.env[name];
  if (!raw) return fallback;
  const trimmed = raw.trim();
  return trimmed || fallback;
}

function envColNullable(name: string, fallback: string | null): string | null {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  const trimmed = raw.trim();
  if (!trimmed) return fallback;
  if (NULL_SENTINELS.has(trimmed.toLowerCase())) return null;
  return trimmed;
}

export function getDqColumns(flavor: DqFlavor = getDqFlavor()): DqColumnMap {
  const preset = flavor === "us-all" ? US_ALL_PRESET : GENERIC_PRESET;
  return {
    runAt: envCol("DQ_COL_RUN_AT", preset.runAt),
    checkType: envCol("DQ_COL_CHECK_TYPE", preset.checkType),
    status: envCol("DQ_COL_STATUS", preset.status),
    dataset: envCol("DQ_COL_DATASET", preset.dataset),
    tableName: envCol("DQ_COL_TABLE_NAME", preset.tableName),
    severity: envCol("DQ_COL_SEVERITY", preset.severity),
    failureCount: envCol("DQ_COL_FAILURE_COUNT", preset.failureCount),
    message: envCol("DQ_COL_MESSAGE", preset.message),
    checkName: envColNullable("DQ_COL_CHECK_NAME", preset.checkName),
    scoreDate: envCol("DQ_COL_SCORE_DATE", preset.scoreDate),
    scope: envColNullable("DQ_COL_SCOPE", preset.scope),
    tier: envColNullable("DQ_COL_TIER", preset.tier),
  };
}

/**
 * Returns true if the resolved schema exposes a `scope` dimension on the score
 * table (i.e. one row per (date, scope)). When false, the score table has one
 * row per date — the whole org rolled up — and `scope` filters / tier-by-scope
 * rollups must be skipped.
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
