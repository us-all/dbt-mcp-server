import { describe, it, expect, beforeAll } from "vitest";
import { resolve } from "node:path";

beforeAll(() => {
  process.env.DBT_PROJECT_DIR = resolve(__dirname, "fixtures");
  process.env.DBT_TARGET_DIR = resolve(__dirname, "fixtures");
});

describe("dbt-models", () => {
  it("dbt-list-models returns models with filters", async () => {
    const { dbtListModels } = await import("../src/tools/dbt-models.js");
    const r = (await dbtListModels({ limit: 200 })) as { count: number; models: { name: string }[] };
    expect(r.count).toBe(2);
    expect(r.models.map((m) => m.name).sort()).toEqual(["stg_users", "users_dim"]);
  });

  it("dbt-list-models filters by tag", async () => {
    const { dbtListModels } = await import("../src/tools/dbt-models.js");
    const r = (await dbtListModels({ tag: "tier1", limit: 200 })) as { count: number; models: { name: string }[] };
    expect(r.count).toBe(1);
    expect(r.models[0]?.name).toBe("users_dim");
  });

  it("dbt-list-models filters by materialized", async () => {
    const { dbtListModels } = await import("../src/tools/dbt-models.js");
    const r = (await dbtListModels({ materialized: "view", limit: 200 })) as { count: number };
    expect(r.count).toBe(1);
  });

  it("dbt-get-model returns refs, sources, and attached tests", async () => {
    const { dbtGetModel } = await import("../src/tools/dbt-models.js");
    const r = (await dbtGetModel({ name: "users_dim", includeCompiledSql: false })) as {
      refs: string[];
      sources: string[];
      tests: { name: string }[];
      columns: { name: string }[];
    };
    expect(r.refs).toContain("stg_users");
    expect(r.sources).toContain("raw.users");
    expect(r.tests.map((t) => t.name).sort()).toEqual([
      "not_null_users_dim_email",
      "row_count_check_users_dim",
      "unique_users_dim_user_id",
    ]);
    expect(r.columns.map((c) => c.name).sort()).toEqual(["created_at", "email", "user_id"]);
  });

  it("dbt-graph walks parent_map and child_map", async () => {
    const { dbtGraph } = await import("../src/tools/dbt-models.js");
    const r = (await dbtGraph({ name: "stg_users", upstreamDepth: 2, downstreamDepth: 2 })) as {
      upstream: { name: string; depth: number }[];
      downstream: { name: string; depth: number }[];
    };
    expect(r.upstream.map((n) => n.name)).toContain("users");
    expect(r.downstream.map((n) => n.name)).toContain("users_dim");
  });

  it("dbt-coverage reports column coverage and table-level tests", async () => {
    const { dbtCoverage } = await import("../src/tools/dbt-models.js");
    const r = (await dbtCoverage({ name: "users_dim" })) as {
      totalColumns: number;
      columnsWithTests: number;
      coveragePct: number;
      tableLevelTests: string[];
    };
    expect(r.totalColumns).toBe(3);
    expect(r.columnsWithTests).toBe(2);
    expect(r.tableLevelTests).toContain("row_count_check_users_dim");
  });
});
