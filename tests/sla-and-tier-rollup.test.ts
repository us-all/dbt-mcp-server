import { describe, it, expect, beforeAll, afterAll, beforeEach, vi } from "vitest";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

let tmpDir: string;
let slaPath: string;
let manifestPath: string;

const SLA_YAML = `
dbt_sla:
  test_pass_pct: 99.0
  freshness_pass_pct: 99.5

tier_sla:
  1: 99.5
  2: 99.0
  3: 95.0
`;

const MANIFEST = {
  metadata: {
    dbt_schema_version: "https://schemas.getdbt.com/dbt/manifest/v12.json",
    dbt_version: "1.7.0",
    generated_at: "2026-05-09T00:00:00Z",
    project_name: "test_proj",
  },
  nodes: {},
  macros: {},
  sources: {
    "source.test_proj.us_plus_next.users": {
      unique_id: "source.test_proj.us_plus_next.users",
      name: "users",
      source_name: "us_plus_next",
      resource_type: "source",
      package_name: "test_proj",
      meta: { tier: 1 },
    },
    "source.test_proj.us_plus_next.orders": {
      unique_id: "source.test_proj.us_plus_next.orders",
      name: "orders",
      source_name: "us_plus_next",
      resource_type: "source",
      package_name: "test_proj",
      meta: { tier: 1 },
    },
    "source.test_proj.us_campus_next.courses": {
      unique_id: "source.test_proj.us_campus_next.courses",
      name: "courses",
      source_name: "us_campus_next",
      resource_type: "source",
      package_name: "test_proj",
      meta: { tier: 2 },
    },
    "source.test_proj.legacy_logs.events": {
      unique_id: "source.test_proj.legacy_logs.events",
      name: "events",
      source_name: "legacy_logs",
      resource_type: "source",
      package_name: "test_proj",
      // no meta.tier — should land in caveats
    },
  },
};

beforeAll(() => {
  tmpDir = mkdtempSync(join(tmpdir(), "dbt-mcp-sla-"));
  slaPath = join(tmpDir, "sla_config.yml");
  manifestPath = join(tmpDir, "manifest.json");
  writeFileSync(slaPath, SLA_YAML, "utf8");
  writeFileSync(manifestPath, JSON.stringify(MANIFEST), "utf8");
});

afterAll(() => {
  rmSync(tmpDir, { recursive: true, force: true });
});

describe("SLA config reader", () => {
  beforeEach(() => {
    delete process.env.DBT_SLA_CONFIG_PATH;
    delete process.env.DQ_TIER1_TARGET_PCT;
    vi.resetModules();
  });

  it("returns hardcoded defaults when DBT_SLA_CONFIG_PATH is unset", async () => {
    const { getTierTargets, getTier1Target } = await import("../src/clients/sla-config.js");
    const targets = getTierTargets();
    expect(targets).toEqual({ "1": 99.5, "2": 99.0, "3": 95.0 });
    expect(getTier1Target()).toBe(99.5);
  });

  it("DQ_TIER1_TARGET_PCT env var overrides default tier-1 target when no SLA file is set", async () => {
    process.env.DQ_TIER1_TARGET_PCT = "97.5";
    const { getTier1Target } = await import("../src/clients/sla-config.js");
    expect(getTier1Target()).toBe(97.5);
  });

  it("loads tier_sla from YAML when DBT_SLA_CONFIG_PATH is set", async () => {
    process.env.DBT_SLA_CONFIG_PATH = slaPath;
    const { getTierTargets, getTier1Target } = await import("../src/clients/sla-config.js");
    expect(getTierTargets()).toEqual({ "1": 99.5, "2": 99.0, "3": 95.0 });
    expect(getTier1Target()).toBe(99.5);
  });

  it("SLA config tier_sla.1 takes precedence over DQ_TIER1_TARGET_PCT", async () => {
    process.env.DBT_SLA_CONFIG_PATH = slaPath;
    process.env.DQ_TIER1_TARGET_PCT = "98.0";
    const { getTier1Target } = await import("../src/clients/sla-config.js");
    expect(getTier1Target()).toBe(99.5);
  });

  it("throws when DBT_SLA_CONFIG_PATH points at a missing file", async () => {
    process.env.DBT_SLA_CONFIG_PATH = "/no/such/file.yml";
    const { loadSlaConfig } = await import("../src/clients/sla-config.js");
    expect(() => loadSlaConfig()).toThrowError(/does not exist/);
  });
});

describe("dq-tier-by-source rollup", () => {
  beforeEach(() => {
    process.env.DBT_PROJECT_DIR = tmpDir;
    process.env.DBT_TARGET_DIR = tmpDir;
    process.env.DBT_SLA_CONFIG_PATH = slaPath;
    process.env.DQ_BACKEND = "postgres";
    process.env.DQ_SCHEMA = "us-all";
    process.env.PG_CONNECTION_STRING = "postgres://u:p@localhost/data_ops";
    process.env.DQ_RESULTS_TABLE = "quality_checks";
    process.env.DQ_SCORE_TABLE = "quality_score_daily";
    delete process.env.DQ_COL_RUN_AT;
    delete process.env.DQ_COL_DATASET;
    delete process.env.DQ_COL_STATUS;
    delete process.env.DQ_COL_CHECK_NAME;
    delete process.env.DQ_COL_SCOPE;
    delete process.env.DQ_COL_TIER;
    vi.resetModules();
  });

  it("rolls per-source pass rates up by tier and flags untiered sources", async () => {
    const { _setDriverForTest } = await import("../src/clients/dq-store.js");
    _setDriverForTest({
      query: async () => [
        { rollup_key: "us_plus_next",   total_checks: 200, passed_checks: 199 }, // 99.5% — tier 1 meets 99.5
        { rollup_key: "us_campus_next", total_checks: 100, passed_checks: 98 },  // 98.0% — tier 2 misses 99.0
        { rollup_key: "legacy_logs",    total_checks: 50,  passed_checks: 50 },  // untiered
      ],
    });
    const { dqTierBySource } = await import("../src/tools/quality-scores.js");
    const r = (await dqTierBySource({})) as {
      schema: string;
      targets: Record<string, number>;
      sources: Array<{ source: string; tier: number | null; passPct: number; meeting: boolean | null }>;
      tierRollup: Record<string, { target: number; sources: number; meeting: number; missing: number; sourcesEvaluated: string[] }>;
      caveats: string[];
      sourcesWithTier: number;
    };
    expect(r.schema).toBe("us-all");
    expect(r.targets).toEqual({ "1": 99.5, "2": 99.0, "3": 95.0 });
    expect(r.sourcesWithTier).toBe(2); // 2 unique source_names with tier (us_plus_next, us_campus_next)

    const byName: Record<string, typeof r.sources[number]> = {};
    for (const s of r.sources) byName[s.source] = s;
    expect(byName.us_plus_next?.tier).toBe(1);
    expect(byName.us_plus_next?.meeting).toBe(true);
    expect(byName.us_campus_next?.tier).toBe(2);
    expect(byName.us_campus_next?.meeting).toBe(false); // 98.0 < 99.0
    expect(byName.legacy_logs?.tier).toBeNull();

    expect(r.tierRollup["1"]?.sources).toBe(1);
    expect(r.tierRollup["1"]?.meeting).toBe(1);
    expect(r.tierRollup["2"]?.sources).toBe(1);
    expect(r.tierRollup["2"]?.missing).toBe(1);

    expect(r.caveats.some((c) => c.includes("legacy_logs"))).toBe(true);
  });

  it("uses DQ_COL_DATASET override when collecting per-source pass rate", async () => {
    process.env.DQ_SCHEMA = "generic";
    process.env.DQ_COL_DATASET = "src_group";
    process.env.DQ_COL_RUN_AT = "ts";

    const calls: Array<{ sql: string }> = [];
    const { _setDriverForTest } = await import("../src/clients/dq-store.js");
    _setDriverForTest({
      query: async (sql) => {
        calls.push({ sql });
        return [{ rollup_key: "us_plus_next", total_checks: 100, passed_checks: 100 }];
      },
    });
    const { dqTierBySource } = await import("../src/tools/quality-scores.js");
    await dqTierBySource({ sinceHours: 24, mode: "source" });
    const sql = calls[0]!.sql;
    expect(sql).toContain("src_group AS rollup_key");
    expect(sql).toContain("GROUP BY src_group");
    expect(sql).toContain("ts >= NOW() -");
  });

  it("mode='table' + sourceFilter handles us-all-shaped data: groups by target_name, parses prefix.table, looks up table-level meta.tier", async () => {
    const calls: Array<{ sql: string; params: unknown[] }> = [];
    const { _setDriverForTest } = await import("../src/clients/dq-store.js");
    _setDriverForTest({
      query: async (sql, params) => {
        calls.push({ sql, params });
        // Emulate us-all bq rows: target_name in '<source_group>.<table>' shape.
        // us_plus_next.users → tier 1 (per fixture)
        // us_plus_next.untracked → no fixture entry (untiered)
        // us_campus_next.courses → tier 2
        // free_text_no_dot → unparseable
        return [
          { rollup_key: "us_plus_next.users",   total_checks: 50, passed_checks: 50 },  // 100% / tier 1 / target 99.5 / meeting
          { rollup_key: "us_plus_next.untracked", total_checks: 30, passed_checks: 28 }, // 93.3% / untiered (not in fixture)
          { rollup_key: "us_campus_next.courses", total_checks: 40, passed_checks: 38 }, // 95% / tier 2 / target 99.0 / missing
          { rollup_key: "free_text_no_dot",     total_checks: 10, passed_checks: 10 },  // unparseable / untiered
        ];
      },
    });
    const { dqTierBySource } = await import("../src/tools/quality-scores.js");
    const r = (await dqTierBySource({ mode: "table", sourceFilter: "bq" })) as {
      mode: string;
      sources: Array<{ source: string; tier: number | null; meeting: boolean | null; passPct: number }>;
      tierRollup: Record<string, { sources: number; meeting: number; missing: number }>;
      tablesWithTier: number;
      caveats: string[];
    };
    expect(r.mode).toBe("table");

    // SQL groups by tableName col + filters by sourceFilter.
    const sql = calls[0]!.sql;
    expect(sql).toContain("target_name AS rollup_key");
    expect(sql).toContain("GROUP BY target_name");
    expect(sql).toContain("source = ?");
    expect(calls[0]!.params).toContain("bq");

    // tablesWithTier reflects the manifest: 3 tier-bearing source entries.
    expect(r.tablesWithTier).toBe(3);

    const byKey: Record<string, typeof r.sources[number]> = {};
    for (const s of r.sources) byKey[s.source] = s;
    expect(byKey["us_plus_next.users"]?.tier).toBe(1);
    expect(byKey["us_plus_next.users"]?.meeting).toBe(true);
    expect(byKey["us_plus_next.untracked"]?.tier).toBeNull();
    expect(byKey["us_campus_next.courses"]?.tier).toBe(2);
    expect(byKey["us_campus_next.courses"]?.meeting).toBe(false);
    expect(byKey["free_text_no_dot"]?.tier).toBeNull();

    expect(r.tierRollup["1"]?.meeting).toBe(1);
    expect(r.tierRollup["2"]?.missing).toBe(1);
    expect(r.caveats.some((c) => c.includes("table(s)"))).toBe(true);
  });

  it("flags 'no sources have meta.tier' when manifest carries none", async () => {
    // Override manifest with tier-less sources only.
    const tierless = {
      ...MANIFEST,
      sources: {
        "source.test_proj.foo.bar": {
          unique_id: "source.test_proj.foo.bar",
          name: "bar",
          source_name: "foo",
          resource_type: "source",
          package_name: "test_proj",
          // no meta
        },
      },
    };
    const tierlessDir = mkdtempSync(join(tmpdir(), "dbt-mcp-tierless-"));
    writeFileSync(join(tierlessDir, "manifest.json"), JSON.stringify(tierless), "utf8");
    process.env.DBT_PROJECT_DIR = tierlessDir;
    process.env.DBT_TARGET_DIR = tierlessDir;

    const { _setDriverForTest } = await import("../src/clients/dq-store.js");
    _setDriverForTest({
      query: async () => [{ rollup_key: "foo", total_checks: 10, passed_checks: 10 }],
    });
    const { dqTierBySource } = await import("../src/tools/quality-scores.js");
    const r = (await dqTierBySource({})) as {
      sourcesWithTier: number; caveats: string[]; tierRollup: Record<string, unknown>;
    };
    expect(r.sourcesWithTier).toBe(0);
    expect(r.caveats.some((c) => c.toLowerCase().includes("no source group"))).toBe(true);
    expect(Object.keys(r.tierRollup)).toHaveLength(0);

    rmSync(tierlessDir, { recursive: true, force: true });
  });
});

describe("dq-tier-status uses SLA config when DBT_SLA_CONFIG_PATH is set", () => {
  beforeEach(() => {
    process.env.DBT_SLA_CONFIG_PATH = slaPath;
    process.env.DBT_PROJECT_DIR = tmpDir;
    process.env.DBT_TARGET_DIR = tmpDir;
    process.env.DQ_BACKEND = "postgres";
    process.env.DQ_SCHEMA = "us-all";
    process.env.PG_CONNECTION_STRING = "postgres://u:p@localhost/data_ops";
    process.env.DQ_RESULTS_TABLE = "quality_checks";
    process.env.DQ_SCORE_TABLE = "quality_score_daily";
    delete process.env.DQ_TIER1_TARGET_PCT;
    delete process.env.DQ_COL_TIER;
    delete process.env.DQ_COL_SCOPE;
    vi.resetModules();
  });

  it("us-all path: target comes from sla_config.tier_sla.1 (99.5)", async () => {
    const { _setDriverForTest } = await import("../src/clients/dq-store.js");
    _setDriverForTest({
      query: async () => [
        { score_date: "2026-05-08", overall_score: 99.6, total_checks: 200, failed_checks: 0 },
      ],
    });
    const { dqTierStatus } = await import("../src/tools/quality-scores.js");
    const r = (await dqTierStatus({})) as { target: number; meeting: boolean };
    expect(r.target).toBe(99.5);
    expect(r.meeting).toBe(true);
  });
});
