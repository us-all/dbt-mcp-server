import { describe, it, expect, beforeAll, beforeEach, vi } from "vitest";
import { resolve } from "node:path";

beforeAll(() => {
  process.env.DBT_PROJECT_DIR = resolve(__dirname, "fixtures");
  process.env.DBT_TARGET_DIR = resolve(__dirname, "fixtures");
  process.env.DQ_BACKEND = "bigquery";
  process.env.DQ_SCHEMA = "generic";
  process.env.DQ_RESULTS_TABLE = "example-project.data_ops.quality_checks";
  process.env.DQ_SCORE_TABLE = "example-project.data_ops.quality_score_daily";
});

describe("dq-store driver injection (generic schema)", () => {
  beforeEach(async () => {
    process.env.DQ_BACKEND = "bigquery";
    process.env.DQ_SCHEMA = "generic";
    vi.resetModules();
    const mod = await import("../src/clients/dq-store.js");
    mod._setDriverForTest(null);
  });

  it("dq-list-checks (generic) selects with check_name + builds parameterized SQL", async () => {
    const calls: Array<{ sql: string; params: unknown[] }> = [];
    const { _setDriverForTest } = await import("../src/clients/dq-store.js");
    _setDriverForTest({
      query: async (sql, params) => {
        calls.push({ sql, params });
        return [{ check_name: "users_not_null", status: "fail", run_at: "2026-05-06T01:00:00Z" }];
      },
    });
    const { dqListChecks } = await import("../src/tools/quality-results.js");
    const r = (await dqListChecks({ dataset: "us_summary", sinceHours: 24, limit: 50 })) as {
      rows: unknown[]; backend: string; schema: string;
    };
    expect(r.rows.length).toBe(1);
    expect(r.backend).toBe("bigquery");
    expect(r.schema).toBe("generic");
    const c = calls[0]!;
    expect(c.sql).toContain("FROM `example-project.data_ops.quality_checks`");
    expect(c.sql).toContain("check_name AS check_name");
    expect(c.sql).toContain("dataset =");
    expect(c.sql).toContain("run_at >=");
    expect(c.params[0]).toBe("us_summary");
  });

  it("dq-failed-checks-by-dataset groups by dataset", async () => {
    const calls: Array<{ sql: string }> = [];
    const { _setDriverForTest } = await import("../src/clients/dq-store.js");
    _setDriverForTest({
      query: async (sql) => {
        calls.push({ sql });
        return [
          { dataset: "us_summary", failures: 5 },
          { dataset: "us_crm", failures: 2 },
        ];
      },
    });
    const { dqFailedChecksByDataset } = await import("../src/tools/quality-results.js");
    const r = (await dqFailedChecksByDataset({ sinceHours: 24, topN: 10 })) as {
      rows: { dataset: string; failures: number }[];
    };
    expect(r.rows[0]?.dataset).toBe("us_summary");
    expect(calls[0]!.sql).toContain("GROUP BY dataset");
  });

  it("dq-tier-status (generic) compares overall_score against Tier 1/2/3 targets", async () => {
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

describe("dq-store driver injection (us-all schema, postgres)", () => {
  beforeEach(async () => {
    process.env.DQ_BACKEND = "postgres";
    process.env.DQ_SCHEMA = "us-all";
    process.env.PG_CONNECTION_STRING = "postgres://u:p@localhost/data_ops";
    process.env.DQ_RESULTS_TABLE = "quality_checks";
    process.env.DQ_SCORE_TABLE = "quality_score_daily";
    vi.resetModules();
    const mod = await import("../src/clients/dq-store.js");
    mod._setDriverForTest(null);
  });

  it("dq-list-checks (us-all) maps to run_date / source / target_name / dimension / metric_value", async () => {
    const calls: Array<{ sql: string; params: unknown[] }> = [];
    const { _setDriverForTest } = await import("../src/clients/dq-store.js");
    _setDriverForTest({
      query: async (sql, params) => {
        calls.push({ sql, params });
        return [
          {
            check_name: "dbt_test:users_dim",
            check_type: "dbt_test",
            dataset: "us_summary",
            table_name: "users_dim",
            status: "fail",
            severity: "completeness",
            failure_count: 17,
            run_at: "2026-05-06",
            message: '{"failures":17}',
          },
        ];
      },
    });
    const { dqListChecks } = await import("../src/tools/quality-results.js");
    const r = (await dqListChecks({ dataset: "us_summary", sinceHours: 24, limit: 50 })) as {
      rows: unknown[]; schema: string; backend: string;
    };
    expect(r.schema).toBe("us-all");
    expect(r.backend).toBe("postgres");
    const c = calls[0]!;
    // Selects via PG quoted FQN, no backticks
    expect(c.sql).not.toContain("`");
    // Real columns mapped to virtual names
    expect(c.sql).toContain("source AS dataset");
    expect(c.sql).toContain("target_name AS table_name");
    expect(c.sql).toContain("dimension AS severity");
    expect(c.sql).toContain("metric_value AS failure_count");
    expect(c.sql).toContain("run_date AS run_at");
    // Synthesized check_name (no native check_name in us-all)
    expect(c.sql).toContain("check_type || ':'");
    // PG date arithmetic
    expect(c.sql).toContain("run_date >= NOW() - (");
  });

  it("dq-tier-status (us-all) returns single overall_score vs DQ_TIER1_TARGET_PCT", async () => {
    const { _setDriverForTest } = await import("../src/clients/dq-store.js");
    _setDriverForTest({
      query: async () => [
        { score_date: "2026-05-07", overall_score: 99.6, total_checks: 219, failed_checks: 1 },
      ],
    });
    const { dqTierStatus } = await import("../src/tools/quality-scores.js");
    const r = (await dqTierStatus({})) as {
      target: number; score: number; meeting: boolean; totalChecks: number; failedChecks: number; schema: string;
    };
    expect(r.schema).toBe("us-all");
    expect(r.target).toBe(99.5);
    expect(r.score).toBe(99.6);
    expect(r.meeting).toBe(true);
    expect(r.totalChecks).toBe(219);
    expect(r.failedChecks).toBe(1);
  });

  it("dq-tier-status (us-all) flags missing target when below threshold", async () => {
    const { _setDriverForTest } = await import("../src/clients/dq-store.js");
    _setDriverForTest({
      query: async () => [
        { score_date: "2026-05-07", overall_score: 98.4, total_checks: 219, failed_checks: 9 },
      ],
    });
    const { dqTierStatus } = await import("../src/tools/quality-scores.js");
    const r = (await dqTierStatus({})) as { meeting: boolean; score: number };
    expect(r.score).toBe(98.4);
    expect(r.meeting).toBe(false);
  });

  it("dq-score-trend (us-all) ignores scope filter with caveat", async () => {
    const { _setDriverForTest } = await import("../src/clients/dq-store.js");
    _setDriverForTest({
      query: async () => [
        { score_date: "2026-05-07", overall_score: 99.6 },
        { score_date: "2026-05-06", overall_score: 99.4 },
      ],
    });
    const { dqScoreTrend } = await import("../src/tools/quality-scores.js");
    const r = (await dqScoreTrend({ days: 14, scope: "ignored" })) as {
      rows: unknown[]; caveats: string[]; schema: string;
    };
    expect(r.schema).toBe("us-all");
    expect(r.caveats.some((c) => c.includes("scope"))).toBe(true);
    expect(r.rows.length).toBe(2);
  });
});

describe("dq-store DQ_COL_* per-column overrides (generic preset baseline)", () => {
  beforeEach(async () => {
    process.env.DQ_BACKEND = "postgres";
    process.env.DQ_SCHEMA = "generic";
    process.env.PG_CONNECTION_STRING = "postgres://u:p@localhost/data_ops";
    process.env.DQ_RESULTS_TABLE = "dq.checks";
    process.env.DQ_SCORE_TABLE = "dq.score_daily";
    // Custom column mapping — none of these match the generic preset.
    process.env.DQ_COL_RUN_AT = "checked_at";
    process.env.DQ_COL_CHECK_TYPE = "type";
    process.env.DQ_COL_STATUS = "result";
    process.env.DQ_COL_DATASET = "schema_name";
    process.env.DQ_COL_TABLE_NAME = "tbl";
    process.env.DQ_COL_SEVERITY = "level";
    process.env.DQ_COL_FAILURE_COUNT = "fail_n";
    process.env.DQ_COL_MESSAGE = "note";
    process.env.DQ_COL_CHECK_NAME = "check_id";
    process.env.DQ_COL_SCORE_DATE = "as_of";
    process.env.DQ_COL_SCOPE = "team";
    process.env.DQ_COL_TIER = "sla_tier";
    vi.resetModules();
    const mod = await import("../src/clients/dq-store.js");
    mod._setDriverForTest(null);
  });

  it("dq-list-checks rewrites every column to the DQ_COL_* override", async () => {
    const calls: Array<{ sql: string }> = [];
    const { _setDriverForTest } = await import("../src/clients/dq-store.js");
    _setDriverForTest({
      query: async (sql) => {
        calls.push({ sql });
        return [];
      },
    });
    const { dqListChecks } = await import("../src/tools/quality-results.js");
    await dqListChecks({ dataset: "us_summary", sinceHours: 24, limit: 50 });
    const sql = calls[0]!.sql;
    expect(sql).toContain("check_id AS check_name");
    expect(sql).toContain("type AS check_type");
    expect(sql).toContain("schema_name AS dataset");
    expect(sql).toContain("tbl AS table_name");
    expect(sql).toContain("result AS status");
    expect(sql).toContain("level AS severity");
    expect(sql).toContain("fail_n AS failure_count");
    expect(sql).toContain("checked_at AS run_at");
    expect(sql).toContain("note AS message");
    expect(sql).toContain("checked_at >= NOW() -");
    expect(sql).toContain("ORDER BY checked_at DESC");
  });

  it("dq-tier-status uses overridden tier/scope columns when both are set", async () => {
    const { _setDriverForTest } = await import("../src/clients/dq-store.js");
    _setDriverForTest({
      query: async () => [
        { tier: "1", scope: "team_a", overall_score: 99.7 },
      ],
    });
    const { dqTierStatus } = await import("../src/tools/quality-scores.js");
    const r = (await dqTierStatus({})) as {
      tiers: Record<string, { observations: number }>;
    };
    expect(r.tiers["1"]?.observations).toBe(1);
  });
});

describe("dq-store DQ_COL_* nullable sentinels (mixed preset)", () => {
  beforeEach(async () => {
    process.env.DQ_BACKEND = "postgres";
    process.env.DQ_SCHEMA = "generic";
    process.env.PG_CONNECTION_STRING = "postgres://u:p@localhost/data_ops";
    process.env.DQ_RESULTS_TABLE = "dq.checks";
    process.env.DQ_SCORE_TABLE = "dq.score_daily";
    process.env.DQ_COL_RUN_AT = "ts";
    process.env.DQ_COL_DATASET = "schema_name";
    // Explicitly declare no native check_name / scope / tier even though the
    // generic preset has them.
    process.env.DQ_COL_CHECK_NAME = "none";
    process.env.DQ_COL_SCOPE = "-";
    process.env.DQ_COL_TIER = "null";
    delete process.env.DQ_COL_CHECK_TYPE;
    delete process.env.DQ_COL_STATUS;
    delete process.env.DQ_COL_TABLE_NAME;
    delete process.env.DQ_COL_SEVERITY;
    delete process.env.DQ_COL_FAILURE_COUNT;
    delete process.env.DQ_COL_MESSAGE;
    delete process.env.DQ_COL_SCORE_DATE;
    vi.resetModules();
    const mod = await import("../src/clients/dq-store.js");
    mod._setDriverForTest(null);
  });

  it("dq-list-checks synthesizes check_name when DQ_COL_CHECK_NAME=none", async () => {
    const calls: Array<{ sql: string }> = [];
    const { _setDriverForTest } = await import("../src/clients/dq-store.js");
    _setDriverForTest({
      query: async (sql) => {
        calls.push({ sql });
        return [];
      },
    });
    const { dqListChecks } = await import("../src/tools/quality-results.js");
    await dqListChecks({ sinceHours: 24, limit: 10 });
    const sql = calls[0]!.sql;
    // No native check_name — synthesized from check_type+table_name (preset
    // values still used for non-overridden columns).
    expect(sql).toContain("check_type || ':'");
    expect(sql).toContain("ts >= NOW() -");
    expect(sql).toContain("schema_name AS dataset");
    // Score-date column kept at preset since DQ_COL_SCORE_DATE not set.
  });

  it("dq-score-trend skips scope filter and ORDERs by date only when DQ_COL_SCOPE=-", async () => {
    const { _setDriverForTest } = await import("../src/clients/dq-store.js");
    _setDriverForTest({
      query: async (sql) => {
        // capture for assertion
        (globalThis as unknown as { __lastSql?: string }).__lastSql = sql;
        return [{ score_date: "2026-05-08", overall_score: 99.4 }];
      },
    });
    const { dqScoreTrend } = await import("../src/tools/quality-scores.js");
    const r = (await dqScoreTrend({ days: 7, scope: "team_a" })) as {
      caveats: string[]; rows: unknown[];
    };
    const sql = (globalThis as unknown as { __lastSql?: string }).__lastSql ?? "";
    expect(sql).not.toMatch(/team_a/);
    // ORDER BY date only — no comma-separated scope
    expect(sql).toMatch(/ORDER BY score_date DESC\s*$/);
    expect(r.caveats.some((c) => c.toLowerCase().includes("scope"))).toBe(true);
    expect(r.rows.length).toBe(1);
  });

  it("dq-tier-status falls back to overall_score path when DQ_COL_TIER=null", async () => {
    const { _setDriverForTest } = await import("../src/clients/dq-store.js");
    _setDriverForTest({
      query: async () => [
        { score_date: "2026-05-08", overall_score: 99.7, total_checks: 200, failed_checks: 0 },
      ],
    });
    const { dqTierStatus } = await import("../src/tools/quality-scores.js");
    const r = (await dqTierStatus({})) as {
      target: number; score: number; meeting: boolean;
    };
    expect(r.target).toBe(99.5);
    expect(r.score).toBe(99.7);
    expect(r.meeting).toBe(true);
  });
});
