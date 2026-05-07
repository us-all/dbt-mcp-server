import { describe, it, expect, beforeAll } from "vitest";
import { resolve } from "node:path";

beforeAll(() => {
  process.env.DBT_PROJECT_DIR = resolve(__dirname, "fixtures");
  process.env.DBT_TARGET_DIR = resolve(__dirname, "fixtures");
});

describe("dbt-runs", () => {
  it("dbt-list-runs surfaces run history with success rate", async () => {
    const { dbtListRuns } = await import("../src/tools/dbt-runs.js");
    const r = (await dbtListRuns({ limit: 5 })) as {
      count: number;
      runs: { invocationId: string; passed: number; errored: number; successRate: number }[];
    };
    expect(r.count).toBeGreaterThanOrEqual(1);
    const inv = r.runs.find((x) => x.invocationId === "inv-abc-001")!;
    expect(inv.errored).toBe(1);
  });

  it("dbt-failed-tests groups failures across runs by test", async () => {
    const { dbtFailedTests } = await import("../src/tools/dbt-runs.js");
    const r = (await dbtFailedTests({ recentRuns: 5 })) as {
      totalFailures: number;
      uniqueTests: number;
      failuresByTest: Array<{ testUniqueId: string; occurrenceCount: number }>;
    };
    expect(r.totalFailures).toBe(1);
    expect(r.failuresByTest[0]?.testUniqueId).toContain("not_null_users_dim_email");
  });

  it("dbt-slow-models orders models by execution_time desc", async () => {
    const { dbtSlowModels } = await import("../src/tools/dbt-runs.js");
    const r = (await dbtSlowModels({ topN: 5 })) as {
      models: { uniqueId: string; executionTimeSec: number }[];
    };
    expect(r.models[0]?.uniqueId).toBe("model.us_dbt.users_dim");
    expect(r.models[0]?.executionTimeSec).toBeGreaterThan(r.models[1]!.executionTimeSec);
  });

  it("dbt-get-run-results filters by status", async () => {
    const { dbtGetRunResults } = await import("../src/tools/dbt-runs.js");
    const r = (await dbtGetRunResults({ status: "fail", limit: 100 })) as {
      count: number;
      results: { uniqueId: string; status: string }[];
    };
    expect(r.count).toBe(1);
    expect(r.results[0]?.status).toBe("fail");
  });
});
