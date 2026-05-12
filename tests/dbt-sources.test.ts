import { describe, it, expect, beforeAll } from "vitest";
import { resolve } from "node:path";

beforeAll(() => {
  process.env.DBT_PROJECT_DIR = resolve(__dirname, "fixtures");
  process.env.DBT_TARGET_DIR = resolve(__dirname, "fixtures");
});

describe("dbt-sources", () => {
  it("dbt-list-sources returns sources with hasFreshness flag and meta", async () => {
    const { dbtListSources } = await import("../src/tools/dbt-sources.js");
    const r = (await dbtListSources({ limit: 100 })) as {
      count: number;
      sources: { sourceName: string; tableName: string; hasFreshness: boolean; meta: Record<string, unknown> }[];
    };
    expect(r.count).toBe(1);
    expect(r.sources[0]?.hasFreshness).toBe(true);
    expect(r.sources[0]?.meta?.tier).toBe(1);
  });

  it("dbt-get-source surfaces freshness criteria + sources.json result + meta", async () => {
    const { dbtGetSource } = await import("../src/tools/dbt-sources.js");
    const r = (await dbtGetSource({ sourceName: "raw", tableName: "users" })) as {
      freshness: { error_after?: { count: number } };
      freshnessResult: { status: string; ageInSeconds: number };
      meta: Record<string, unknown>;
    };
    expect(r.freshness.error_after?.count).toBe(24);
    expect(r.freshnessResult.status).toBe("warn");
    expect(r.freshnessResult.ageInSeconds).toBeGreaterThan(43200);
    expect(r.meta?.tier).toBe(1);
  });

  it("dbt-list-exposures returns declared exposures", async () => {
    const { dbtListExposures } = await import("../src/tools/dbt-sources.js");
    const r = (await dbtListExposures({ limit: 100 })) as {
      count: number;
      exposures: { name: string; type: string }[];
    };
    expect(r.count).toBe(1);
    expect(r.exposures[0]?.type).toBe("dashboard");
  });
});
