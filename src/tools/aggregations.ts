import { z } from "zod";
import { aggregate } from "@us-all/mcp-toolkit";
import { dbtFailedTests } from "./dbt-runs.js";
import { dqFailedChecksByDataset, dqListChecks } from "./quality-results.js";
import { dqScoreTrend, dqTierStatus } from "./quality-scores.js";
import { dbtGetSource } from "./dbt-sources.js";
import { dbtGetModel } from "./dbt-models.js";
import { loadManifest, loadRunResults, loadSources } from "../clients/dbt-artifacts.js";
import { dqConfigured } from "../config.js";
import { loadSlaConfig } from "../clients/sla-config.js";

export const failedTestsSummarySchema = z.object({
  recentRuns: z.coerce.number().int().min(1).max(20).default(3).describe("Look at last N dbt runs"),
  sinceHours: z.coerce.number().int().min(1).max(720).default(24).describe("Recent window for DQ checks"),
});

export async function failedTestsSummary(
  args: z.infer<typeof failedTestsSummarySchema>,
): Promise<unknown> {
  const caveats: string[] = [];
  const { dbt, dqByDataset, dqLatest } = await aggregate(
    {
      dbt: () => dbtFailedTests({ recentRuns: args.recentRuns }),
      dqByDataset: () =>
        dqConfigured()
          ? dqFailedChecksByDataset({ sinceHours: args.sinceHours, topN: 20 })
          : Promise.resolve(null),
      dqLatest: () =>
        dqConfigured()
          ? dqListChecks({ sinceHours: args.sinceHours, status: "fail", limit: 50 })
          : Promise.resolve(null),
    },
    caveats,
  );
  if (!dqConfigured()) caveats.push("DQ_RESULTS_TABLE not configured — quality category skipped");
  return {
    window: { recentRuns: args.recentRuns, sinceHours: args.sinceHours },
    dbtFailures: dbt,
    dqFailuresByDataset: dqByDataset,
    dqRecentFailures: dqLatest,
    caveats,
  };
}

export const freshnessStatusSchema = z.object({
  failingOnly: z.boolean().default(false).describe("Only return sources where freshness is warn/error"),
});

export async function freshnessStatus(args: z.infer<typeof freshnessStatusSchema>): Promise<unknown> {
  const manifest = loadManifest();
  let sourcesFile;
  try {
    sourcesFile = loadSources();
  } catch {
    sourcesFile = null;
  }
  const out: Array<Record<string, unknown>> = [];
  for (const src of Object.values(manifest.sources)) {
    if (!src.freshness?.error_after && !src.freshness?.warn_after) continue;
    const result = sourcesFile?.results.find((r) => r.unique_id === src.unique_id);
    if (args.failingOnly && (!result || result.status === "pass")) continue;
    out.push({
      sourceName: src.source_name,
      tableName: src.name,
      schema: src.schema,
      database: src.database,
      criteria: src.freshness,
      status: result?.status ?? "unknown",
      maxLoadedAt: result?.max_loaded_at,
      ageInSeconds: result?.max_loaded_at_time_ago_in_s,
    });
  }
  return {
    generatedAt: sourcesFile?.metadata.generated_at,
    sourcesFileAvailable: !!sourcesFile,
    count: out.length,
    sources: out,
    notes: [
      "If you also have @us-all/airflow-mcp installed, call airflow-list-runs with the corresponding source-loading DAG to correlate freshness with run-time.",
    ],
  };
}

export const dqScoreSnapshotSchema = z.object({
  days: z.coerce.number().int().min(1).max(90).default(7),
  includeFailing: z.boolean().default(true).describe("Also include the most recent failing checks"),
});

export async function dqScoreSnapshot(args: z.infer<typeof dqScoreSnapshotSchema>): Promise<unknown> {
  const caveats: string[] = [];
  const { trend, tier, failing } = await aggregate(
    {
      trend: () => dqScoreTrend({ days: args.days }),
      tier: () => dqTierStatus({}),
      failing: () =>
        args.includeFailing
          ? dqListChecks({ sinceHours: args.days * 24, status: "fail", limit: 10 })
          : Promise.resolve(null),
    },
    caveats,
  );
  return { days: args.days, trend, tier, failingTop: failing, caveats };
}

export const dbtSlaStatusSchema = z.object({});

export async function dbtSlaStatus(_args: z.infer<typeof dbtSlaStatusSchema>): Promise<unknown> {
  const caveats: string[] = [];
  const sla = loadSlaConfig();
  const testTarget = sla?.dbtSla?.testPassPct;
  const freshnessTarget = sla?.dbtSla?.freshnessPassPct;
  if (!sla) {
    caveats.push(
      "DBT_SLA_CONFIG_PATH not set — pass rates are reported but no SLA target/meeting comparison is available.",
    );
  } else {
    if (testTarget === undefined) caveats.push("dbt_sla.test_pass_pct not configured — test target/meeting null");
    if (freshnessTarget === undefined) {
      caveats.push("dbt_sla.freshness_pass_pct not configured — freshness target/meeting null");
    }
  }

  let testStatus: Record<string, unknown> | null = null;
  try {
    const run = loadRunResults();
    let total = 0;
    let passed = 0;
    let failed = 0;
    let skipped = 0;
    for (const r of run.results) {
      if (!r.unique_id.startsWith("test.")) continue;
      if (r.status === "skipped") {
        skipped++;
        continue;
      }
      total++;
      if (r.status === "pass" || r.status === "success") passed++;
      else failed++;
    }
    if (total === 0) {
      caveats.push("Latest run_results.json contains no test executions — testStatus pass% null");
      testStatus = {
        invocationId: run.metadata.invocation_id,
        generatedAt: run.metadata.generated_at,
        totalTests: 0,
        passedTests: 0,
        failedTests: 0,
        skippedTests: skipped,
        passPct: null,
        target: testTarget ?? null,
        meeting: null,
      };
    } else {
      const passPct = Math.round((passed / total) * 1000) / 10;
      testStatus = {
        invocationId: run.metadata.invocation_id,
        generatedAt: run.metadata.generated_at,
        totalTests: total,
        passedTests: passed,
        failedTests: failed,
        skippedTests: skipped,
        passPct,
        target: testTarget ?? null,
        meeting: testTarget !== undefined ? passPct >= testTarget : null,
      };
    }
  } catch (err) {
    caveats.push(`run_results.json unavailable: ${(err as Error).message}`);
  }

  let freshnessStatus: Record<string, unknown> | null = null;
  try {
    const sources = loadSources();
    let total = 0;
    let passed = 0;
    let failed = 0;
    for (const r of sources.results) {
      total++;
      if (r.status === "pass") passed++;
      else failed++;
    }
    if (total === 0) {
      caveats.push("sources.json contains no freshness results — freshnessStatus pass% null");
      freshnessStatus = {
        generatedAt: sources.metadata.generated_at,
        totalSources: 0,
        passedSources: 0,
        failedSources: 0,
        passPct: null,
        target: freshnessTarget ?? null,
        meeting: null,
      };
    } else {
      const passPct = Math.round((passed / total) * 1000) / 10;
      freshnessStatus = {
        generatedAt: sources.metadata.generated_at,
        totalSources: total,
        passedSources: passed,
        failedSources: failed,
        passPct,
        target: freshnessTarget ?? null,
        meeting: freshnessTarget !== undefined ? passPct >= freshnessTarget : null,
      };
    }
  } catch (err) {
    caveats.push(`sources.json unavailable: ${(err as Error).message}`);
  }

  return {
    slaConfigPath: process.env.DBT_SLA_CONFIG_PATH ?? null,
    testStatus,
    freshnessStatus,
    caveats,
  };
}

export const incidentContextSchema = z.object({
  modelName: z.string().optional().describe("dbt model name to anchor on (provide modelName OR sourceFqn)"),
  sourceFqn: z.string().optional().describe("'source_name.table_name' to anchor on a source instead of a model"),
  sinceHours: z.coerce.number().int().min(1).max(168).default(48),
});

export async function incidentContext(args: z.infer<typeof incidentContextSchema>): Promise<unknown> {
  if (!args.modelName && !args.sourceFqn) {
    throw new Error("Provide modelName or sourceFqn");
  }
  const caveats: string[] = [];
  const dataset = args.modelName
    ? args.modelName.split("__")[0]
    : args.sourceFqn?.split(".")[0];
  const sourceParts = args.sourceFqn ? args.sourceFqn.split(".") : null;

  const { model, source, failedTests, dqChecks } = await aggregate(
    {
      model: () =>
        args.modelName
          ? dbtGetModel({ name: args.modelName, includeCompiledSql: false })
          : Promise.resolve(null),
      source: () =>
        sourceParts && sourceParts.length === 2
          ? dbtGetSource({ sourceName: sourceParts[0], tableName: sourceParts[1] })
          : Promise.resolve(null),
      failedTests: () => dbtFailedTests({ recentRuns: 3 }),
      dqChecks: () =>
        dqConfigured() && dataset
          ? dqListChecks({ sinceHours: args.sinceHours, dataset, limit: 50 })
          : Promise.resolve(null),
    },
    caveats,
  );

  if (!dqConfigured()) caveats.push("DQ_RESULTS_TABLE not configured — quality category skipped");

  return {
    anchor: args.modelName ? { kind: "model", name: args.modelName } : { kind: "source", fqn: args.sourceFqn },
    window: { sinceHours: args.sinceHours },
    model,
    source,
    failedTests,
    dqChecks,
    caveats,
    notes: [
      "If you also have @us-all/airflow-mcp installed, call airflow-list-runs for the loading DAG to add run-time context.",
    ],
  };
}
