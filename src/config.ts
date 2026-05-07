import { existsSync } from "node:fs";
import { join } from "node:path";
import dotenv from "dotenv";

dotenv.config({ quiet: true });

function parseList(raw: string | undefined): string[] | null {
  if (!raw) return null;
  return raw.split(",").map((s) => s.trim().toLowerCase()).filter(Boolean);
}

const dbtProjectDir = (process.env.DBT_PROJECT_DIR ?? "").trim();
const dbtTargetDir =
  (process.env.DBT_TARGET_DIR ?? "").trim() ||
  (dbtProjectDir ? join(dbtProjectDir, "target") : "");

export const config = {
  dbt: {
    projectDir: dbtProjectDir,
    targetDir: dbtTargetDir,
    runHistoryDir: (process.env.DBT_RUN_HISTORY_DIR ?? "").trim() || null,
  },
  dq: {
    backend: (process.env.DQ_BACKEND ?? "bigquery").toLowerCase() as "bigquery" | "postgres",
    resultsTable: (process.env.DQ_RESULTS_TABLE ?? "").trim(),
    scoreTable: (process.env.DQ_SCORE_TABLE ?? "").trim(),
    bqProjectId: (process.env.BQ_PROJECT_ID ?? "").trim() || null,
    pgConnectionString: process.env.PG_CONNECTION_STRING ?? "",
  },
  allowWrite: process.env.DBT_ALLOW_WRITE === "true",
  enabledCategories: parseList(process.env.DBT_TOOLS),
  disabledCategories: parseList(process.env.DBT_DISABLE),
};

export function validateConfig(): void {
  if (!config.dbt.projectDir) {
    throw new Error("DBT_PROJECT_DIR environment variable is required");
  }
  if (!existsSync(config.dbt.projectDir)) {
    throw new Error(`DBT_PROJECT_DIR does not exist: ${config.dbt.projectDir}`);
  }
  if (config.dbt.targetDir && !existsSync(config.dbt.targetDir)) {
    process.stderr.write(
      `[dbt-mcp] WARN: DBT target dir not found yet: ${config.dbt.targetDir} ` +
        "(will be checked at tool-call time)\n",
    );
  }
  if (config.dq.backend !== "bigquery" && config.dq.backend !== "postgres") {
    throw new Error(`DQ_BACKEND must be 'bigquery' or 'postgres' (got '${config.dq.backend}')`);
  }
  if (!config.dq.resultsTable) {
    process.stderr.write(
      "[dbt-mcp] WARN: DQ_RESULTS_TABLE not set — quality category tools will return errors when called\n",
    );
  }
}

export function dqConfigured(): boolean {
  return !!config.dq.resultsTable;
}
