import { config } from "../config.js";
import { ConfigMissingError, DqStoreError } from "../tools/utils.js";

export interface DqRow {
  [key: string]: unknown;
}

export interface DqQueryResult {
  rows: DqRow[];
  rowCount: number;
  backend: "bigquery" | "postgres";
  query: string;
}

interface BackendDriver {
  query(sql: string, params: unknown[]): Promise<DqRow[]>;
}

let cachedDriver: BackendDriver | null = null;

async function loadBigqueryDriver(): Promise<BackendDriver> {
  let mod: typeof import("@google-cloud/bigquery");
  try {
    mod = await import("@google-cloud/bigquery");
  } catch (err) {
    throw new DqStoreError(
      "DQ_BACKEND=bigquery requires the optional '@google-cloud/bigquery' peer dependency. " +
        "Install with: pnpm add @google-cloud/bigquery",
      err,
    );
  }
  const BQ = (mod as unknown as { BigQuery: new (opts?: Record<string, unknown>) => unknown }).BigQuery;
  const opts: Record<string, unknown> = {};
  if (config.dq.bqProjectId) opts.projectId = config.dq.bqProjectId;
  // googleapis auto-discovers GOOGLE_APPLICATION_CREDENTIALS / ADC.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const client = new BQ(opts) as any;
  return {
    async query(sql, params) {
      try {
        // BigQuery uses positional params via @p0, @p1, ... — we keep callers using
        // ?-style placeholders and rewrite here.
        let i = 0;
        const namedSql = sql.replace(/\?/g, () => `@p${i++}`);
        const namedParams: Record<string, unknown> = {};
        params.forEach((v, idx) => {
          namedParams[`p${idx}`] = v;
        });
        const [rows] = await client.query({ query: namedSql, params: namedParams, location: undefined });
        return rows as DqRow[];
      } catch (err) {
        throw new DqStoreError("BigQuery query failed", err);
      }
    },
  };
}

async function loadPostgresDriver(): Promise<BackendDriver> {
  if (!config.dq.pgConnectionString) {
    throw new ConfigMissingError("PG_CONNECTION_STRING", "Postgres DQ backend");
  }
  let mod: typeof import("pg");
  try {
    mod = await import("pg");
  } catch (err) {
    throw new DqStoreError(
      "DQ_BACKEND=postgres requires the optional 'pg' peer dependency. " + "Install with: pnpm add pg",
      err,
    );
  }
  // pg's CJS default export is the namespace object.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const pgPkg = (mod as any).default ?? mod;
  const client = new pgPkg.Pool({ connectionString: config.dq.pgConnectionString });
  return {
    async query(sql, params) {
      try {
        // Rewrite ? placeholders to $1, $2, ...
        let i = 0;
        const pgSql = sql.replace(/\?/g, () => `$${++i}`);
        const result = await client.query(pgSql, params);
        return result.rows as DqRow[];
      } catch (err) {
        throw new DqStoreError("Postgres query failed", err);
      }
    },
  };
}

async function getDriver(): Promise<BackendDriver> {
  if (cachedDriver) return cachedDriver;
  cachedDriver =
    config.dq.backend === "postgres" ? await loadPostgresDriver() : await loadBigqueryDriver();
  return cachedDriver;
}

export async function dqQuery(sql: string, params: unknown[] = []): Promise<DqQueryResult> {
  const driver = await getDriver();
  const rows = await driver.query(sql, params);
  return { rows, rowCount: rows.length, backend: config.dq.backend, query: sql };
}

function assertSafeTableName(envValue: string): void {
  const parts = envValue.split(".");
  const bqPart = /^[A-Za-z0-9_-]+$/;
  const pgPart = /^[A-Za-z_][A-Za-z0-9_$]*$/;
  const valid =
    config.dq.backend === "bigquery"
      ? parts.length >= 1 && parts.length <= 3 && parts.every((part) => bqPart.test(part))
      : parts.length >= 1 && parts.length <= 3 && parts.every((part) => pgPart.test(part));
  if (!valid) {
    throw new DqStoreError(`Invalid DQ table identifier for ${config.dq.backend}: ${envValue}`, null);
  }
}

export function qualifyTable(envValue: string): string {
  assertSafeTableName(envValue);
  if (config.dq.backend === "bigquery") {
    return "`" + envValue + "`";
  }
  return envValue.split(".").map((part) => `"${part.replace(/"/g, "\"\"")}"`).join(".");
}

export function resultsTable(): string {
  if (!config.dq.resultsTable) {
    throw new ConfigMissingError("DQ_RESULTS_TABLE", "Quality category tools");
  }
  return qualifyTable(config.dq.resultsTable);
}

export function scoreTable(): string {
  if (!config.dq.scoreTable) {
    throw new ConfigMissingError("DQ_SCORE_TABLE", "Quality score-trend tools");
  }
  return qualifyTable(config.dq.scoreTable);
}

// Test-only injection.
export function _setDriverForTest(driver: BackendDriver | null): void {
  cachedDriver = driver;
}
