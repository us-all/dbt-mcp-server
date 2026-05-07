import { describe, it, expect, beforeAll } from "vitest";
import { resolve } from "node:path";

beforeAll(() => {
  process.env.DBT_PROJECT_DIR = resolve(__dirname, "fixtures");
  process.env.DBT_TARGET_DIR = resolve(__dirname, "fixtures");
});

describe("dbt-tests + dbt-macros", () => {
  it("dbt-list-tests filters by attached model", async () => {
    const { dbtListTests } = await import("../src/tools/dbt-tests.js");
    const r = (await dbtListTests({ attachedTo: "users_dim", limit: 100 })) as {
      count: number;
      tests: { name: string; kind: string }[];
    };
    expect(r.count).toBe(3);
    expect(r.tests.some((t) => t.kind === "singular")).toBe(true);
    expect(r.tests.some((t) => t.kind === "generic")).toBe(true);
  });

  it("dbt-list-tests filters by testKind", async () => {
    const { dbtListTests } = await import("../src/tools/dbt-tests.js");
    const r = (await dbtListTests({ testKind: "generic", limit: 100 })) as {
      count: number;
    };
    expect(r.count).toBe(2);
  });

  it("dbt-get-test returns latest run result when present", async () => {
    const { dbtGetTest } = await import("../src/tools/dbt-tests.js");
    const r = (await dbtGetTest({ uniqueId: "test.us_dbt.not_null_users_dim_email.def456" })) as {
      latestResult: { status: string; failures: number } | null;
    };
    expect(r.latestResult?.status).toBe("fail");
    expect(r.latestResult?.failures).toBe(17);
  });

  it("dbt-list-macros lists macros from manifest", async () => {
    const { dbtListMacros } = await import("../src/tools/dbt-macros.js");
    const r = (await dbtListMacros({ limit: 50 })) as {
      count: number;
      macros: { name: string }[];
    };
    expect(r.count).toBe(1);
    expect(r.macros[0]?.name).toBe("audit_columns");
  });

  it("dbt-get-macro returns macro signature + raw code", async () => {
    const { dbtGetMacro } = await import("../src/tools/dbt-macros.js");
    const r = (await dbtGetMacro({ name: "audit_columns" })) as {
      arguments: { name: string }[];
      rawCode: string;
    };
    expect(r.arguments[0]?.name).toBe("model_name");
    expect(r.rawCode).toContain("audit_columns");
  });
});
