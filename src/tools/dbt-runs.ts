import { z } from "zod";
import { loadRunResults, listRunHistory } from "../clients/dbt-artifacts.js";

export const dbtListRunsSchema = z.object({
  limit: z.coerce.number().int().min(1).max(200).default(20),
});

export async function dbtListRuns(args: z.infer<typeof dbtListRunsSchema>): Promise<unknown> {
  const runs = listRunHistory(args.limit);
  return {
    count: runs.length,
    runs: runs.map((r) => {
      const total = r.results.length;
      const errored = r.results.filter((x) => x.status === "error" || x.status === "fail").length;
      const passed = r.results.filter((x) => x.status === "pass" || x.status === "success").length;
      return {
        invocationId: r.invocationId,
        generatedAt: r.generatedAt,
        filePath: r.filePath,
        totalNodes: total,
        passed,
        errored,
        successRate: total === 0 ? null : Math.round(((total - errored) / total) * 1000) / 10,
      };
    }),
  };
}

export const dbtGetRunResultsSchema = z.object({
  invocationId: z
    .string()
    .optional()
    .describe("invocation_id from a run; if omitted, the latest run_results.json in target/ is used"),
  status: z
    .string()
    .optional()
    .describe("Filter results by status (pass | error | fail | skipped | runtime error | success)"),
  limit: z.coerce.number().int().min(1).max(5000).default(500),
});

export async function dbtGetRunResults(
  args: z.infer<typeof dbtGetRunResultsSchema>,
): Promise<unknown> {
  let runFile;
  if (args.invocationId) {
    const all = listRunHistory(200);
    const match = all.find((r) => r.invocationId === args.invocationId);
    if (!match) throw new Error(`Run not found for invocation_id=${args.invocationId}`);
    runFile = {
      metadata: { generated_at: match.generatedAt, invocation_id: match.invocationId, dbt_schema_version: "" },
      results: match.results,
    };
  } else {
    runFile = loadRunResults();
  }
  let results = runFile.results;
  if (args.status) results = results.filter((r) => r.status === args.status);
  results = results.slice(0, args.limit);
  return {
    metadata: runFile.metadata,
    count: results.length,
    results: results.map((r) => ({
      uniqueId: r.unique_id,
      status: r.status,
      executionTime: r.execution_time,
      failures: r.failures,
      message: r.message,
      adapterResponse: r.adapter_response,
    })),
  };
}

export const dbtFailedTestsSchema = z.object({
  recentRuns: z.coerce.number().int().min(1).max(50).default(5).describe("Look at last N runs"),
});

export async function dbtFailedTests(args: z.infer<typeof dbtFailedTestsSchema>): Promise<unknown> {
  const runs = listRunHistory(args.recentRuns);
  const failures: Array<Record<string, unknown>> = [];
  for (const run of runs) {
    for (const r of run.results) {
      if (!r.unique_id.startsWith("test.")) continue;
      if (r.status === "pass" || r.status === "success") continue;
      failures.push({
        invocationId: run.invocationId,
        runGeneratedAt: run.generatedAt,
        testUniqueId: r.unique_id,
        status: r.status,
        failures: r.failures,
        message: r.message,
        executionTime: r.execution_time,
      });
    }
  }
  // Group by testUniqueId to see chronic failures
  const byTest = new Map<string, Array<Record<string, unknown>>>();
  for (const f of failures) {
    const key = f.testUniqueId as string;
    if (!byTest.has(key)) byTest.set(key, []);
    byTest.get(key)!.push(f);
  }
  return {
    runsScanned: runs.length,
    totalFailures: failures.length,
    uniqueTests: byTest.size,
    failuresByTest: Array.from(byTest.entries())
      .map(([uniqueId, occurrences]) => ({
        testUniqueId: uniqueId,
        occurrenceCount: occurrences.length,
        latest: occurrences[0],
        history: occurrences,
      }))
      .sort((a, b) => b.occurrenceCount - a.occurrenceCount),
  };
}

export const dbtSlowModelsSchema = z.object({
  topN: z.coerce.number().int().min(1).max(100).default(20),
  invocationId: z.string().optional().describe("Use a specific run; default is latest"),
});

export async function dbtSlowModels(args: z.infer<typeof dbtSlowModelsSchema>): Promise<unknown> {
  let runFile;
  if (args.invocationId) {
    const all = listRunHistory(200);
    const match = all.find((r) => r.invocationId === args.invocationId);
    if (!match) throw new Error(`Run not found for invocation_id=${args.invocationId}`);
    runFile = { metadata: { generated_at: match.generatedAt, invocation_id: match.invocationId }, results: match.results };
  } else {
    const data = loadRunResults();
    runFile = { metadata: data.metadata, results: data.results };
  }
  const models = runFile.results
    .filter((r) => r.unique_id.startsWith("model."))
    .filter((r) => typeof r.execution_time === "number")
    .sort((a, b) => (b.execution_time ?? 0) - (a.execution_time ?? 0))
    .slice(0, args.topN);
  return {
    invocation: runFile.metadata,
    count: models.length,
    models: models.map((r) => ({
      uniqueId: r.unique_id,
      status: r.status,
      executionTimeSec: r.execution_time,
      bytesProcessed: r.adapter_response?.bytes_processed,
      rowsAffected: r.adapter_response?.rows_affected,
    })),
  };
}
