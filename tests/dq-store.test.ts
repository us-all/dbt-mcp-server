import { describe, it, expect, beforeAll, beforeEach } from "vitest";
import { resolve } from "node:path";

beforeAll(() => {
  process.env.DBT_PROJECT_DIR = resolve(__dirname, "fixtures");
  process.env.DBT_TARGET_DIR = resolve(__dirname, "fixtures");
  process.env.DQ_BACKEND = "bigquery";
  process.env.DQ_RESULTS_TABLE = "example-project.data_ops.quality_checks";
  process.env.DQ_SCORE_TABLE = "example-project.data_ops.quality_score_daily";
});

describe("dq-store driver injection", () => {
  beforeEach(async () => {
    const mod = await import("../src/clients/dq-store.js");
    mod._setDriverForTest(null);
  });

  it("dq-list-checks builds parameterized SQL and returns rows from injected driver", async () => {
    const calls: Array<{ sql: string; params: unknown[] }> = [];
    const { _setDriverForTest } = await import("../src/clients/dq-store.js");
    _setDriverForTest({
      query: async (sql, params) => {
        calls.push({ sql, params });
        return [
          { check_name: "users_not_null", status: "fail", run_at: "2026-05-06T01:00:00Z" },
          { check_name: "users_unique", status: "pass", run_at: "2026-05-06T00:30:00Z" },
        ];
      },
    });

    const { dqListChecks } = await import("../src/tools/quality-results.js");
    const r = (await dqListChecks({ dataset: "us_summary", sinceHours: 24, limit: 50 })) as {
      rows: { check_name: string }[];
      backend: string;
    };
    expect(r.rows.length).toBe(2);
    expect(r.backend).toBe("bigquery");
    const c = calls[0]!;
    expect(c.sql).toContain("FROM `example-project.data_ops.quality_checks`");
    expect(c.params[0]).toBe("us_summary");
    expect(c.params).toContain(24);
  });

  it("dq-failed-checks-by-dataset groups + orders by failures", async () => {
    const { _setDriverForTest } = await import("../src/clients/dq-store.js");
    _setDriverForTest({
      query: async () => [
        { dataset: "us_summary", failures: 5, recent: [] },
        { dataset: "us_crm", failures: 2, recent: [] },
      ],
    });
    const { dqFailedChecksByDataset } = await import("../src/tools/quality-results.js");
    const r = (await dqFailedChecksByDataset({ sinceHours: 24, topN: 10 })) as {
      rows: { dataset: string; failures: number }[];
    };
    expect(r.rows[0]?.dataset).toBe("us_summary");
  });

  it("dq-tier-status compares overall_score against Tier targets", async () => {
    const { _setDriverForTest } = await import("../src/clients/dq-store.js");
    _setDriverForTest({
      query: async () => [
        { tier: "1", scope: "us_summary", overall_score: 99.7 },
        { tier: "1", scope: "us_crm", overall_score: 98.2 },
        { tier: "2", scope: "us_plus_next", overall_score: 99.4 },
      ],
    });
    const { dqTierStatus } = await import("../src/tools/quality-scores.js");
    const r = (await dqTierStatus({})) as {
      tiers: Record<string, { target: number; meeting: number; missing: number }>;
    };
    expect(r.tiers["1"]?.target).toBe(99.5);
    expect(r.tiers["1"]?.meeting).toBe(1);
    expect(r.tiers["1"]?.missing).toBe(1);
    expect(r.tiers["2"]?.meeting).toBe(1);
  });
});
