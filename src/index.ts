#!/usr/bin/env node

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { startMcpServer } from "@us-all/mcp-toolkit/runtime";
import { validateConfig } from "./config.js";
import { wrapToolHandler } from "./tools/utils.js";

import {
  dbtListModelsSchema, dbtListModels,
  dbtGetModelSchema, dbtGetModel,
  dbtGraphSchema, dbtGraph,
  dbtCoverageSchema, dbtCoverage,
} from "./tools/dbt-models.js";
import {
  dbtListTestsSchema, dbtListTests,
  dbtGetTestSchema, dbtGetTest,
} from "./tools/dbt-tests.js";
import {
  dbtListSourcesSchema, dbtListSources,
  dbtGetSourceSchema, dbtGetSource,
  dbtListExposuresSchema, dbtListExposures,
} from "./tools/dbt-sources.js";
import {
  dbtListMacrosSchema, dbtListMacros,
  dbtGetMacroSchema, dbtGetMacro,
} from "./tools/dbt-macros.js";
import {
  dbtListRunsSchema, dbtListRuns,
  dbtGetRunResultsSchema, dbtGetRunResults,
  dbtFailedTestsSchema, dbtFailedTests,
  dbtSlowModelsSchema, dbtSlowModels,
} from "./tools/dbt-runs.js";
import {
  dqListChecksSchema, dqListChecks,
  dqGetCheckHistorySchema, dqGetCheckHistory,
  dqFailedChecksByDatasetSchema, dqFailedChecksByDataset,
} from "./tools/quality-results.js";
import {
  dqScoreTrendSchema, dqScoreTrend,
  dqTierStatusSchema, dqTierStatus,
  dqTierBySourceSchema, dqTierBySource,
} from "./tools/quality-scores.js";
import {
  failedTestsSummarySchema, failedTestsSummary,
  freshnessStatusSchema, freshnessStatus,
  dqScoreSnapshotSchema, dqScoreSnapshot,
  incidentContextSchema, incidentContext,
} from "./tools/aggregations.js";
import { registry, searchToolsSchema, searchTools, type Category } from "./tool-registry.js";
import { registerPrompts } from "./prompts/index.js";

const pkgPath = join(dirname(fileURLToPath(import.meta.url)), "..", "package.json");
const { version: pkgVersion } = JSON.parse(readFileSync(pkgPath, "utf-8")) as { version: string };

validateConfig();

const server = new McpServer({
  name: "dbt",
  version: pkgVersion,
});

let currentCategory: Category = "dbt";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function tool(name: string, description: string, schema: any, handler: any): void {
  registry.register(name, description, currentCategory);
  if (registry.isEnabled(currentCategory)) {
    server.tool(name, description, schema, handler);
  }
}

// --- dbt category ---
currentCategory = "dbt";

tool("dbt-list-models", "List dbt models from manifest.json with filters (package, tag, materialized, schema, name search)", dbtListModelsSchema.shape, wrapToolHandler(dbtListModels));
tool("dbt-get-model", "Get a single dbt model: refs, sources, columns (with catalog types if available), attached tests, raw/compiled SQL", dbtGetModelSchema.shape, wrapToolHandler(dbtGetModel));
tool("dbt-list-tests", "List dbt tests (generic or singular) optionally filtered to a specific model", dbtListTestsSchema.shape, wrapToolHandler(dbtListTests));
tool("dbt-get-test", "Get a single dbt test: definition, parameters, attached models, latest run result", dbtGetTestSchema.shape, wrapToolHandler(dbtGetTest));
tool("dbt-list-sources", "List dbt sources from manifest.json with optional source-group / name filters", dbtListSourcesSchema.shape, wrapToolHandler(dbtListSources));
tool("dbt-get-source", "Get a single dbt source: freshness criteria, columns, latest freshness result from sources.json", dbtGetSourceSchema.shape, wrapToolHandler(dbtGetSource));
tool("dbt-list-exposures", "List dbt exposures (downstream BI/ML/application consumers declared in YAML)", dbtListExposuresSchema.shape, wrapToolHandler(dbtListExposures));
tool("dbt-list-macros", "List dbt macros from manifest.json with package / name filters", dbtListMacrosSchema.shape, wrapToolHandler(dbtListMacros));
tool("dbt-get-macro", "Get a dbt macro: signature, raw SQL, and reverse-lookup of nodes that call it", dbtGetMacroSchema.shape, wrapToolHandler(dbtGetMacro));
tool("dbt-list-runs", "List recent dbt invocations from run_results.json files in target/ and DBT_RUN_HISTORY_DIR", dbtListRunsSchema.shape, wrapToolHandler(dbtListRuns));
tool("dbt-get-run-results", "Get per-node results from a specific dbt invocation (or the latest run if invocationId omitted)", dbtGetRunResultsSchema.shape, wrapToolHandler(dbtGetRunResults));
tool("dbt-failed-tests", "Find tests that failed across the last N runs, grouped and ordered by chronic failure count", dbtFailedTestsSchema.shape, wrapToolHandler(dbtFailedTests));
tool("dbt-slow-models", "Top N slowest models in a dbt run by execution_time, with bytes_processed when available", dbtSlowModelsSchema.shape, wrapToolHandler(dbtSlowModels));
tool("dbt-coverage", "Per-column test coverage for a dbt model (which columns have tests, table-level tests, coverage %)", dbtCoverageSchema.shape, wrapToolHandler(dbtCoverage));
tool("dbt-graph", "Walk dbt parent_map / child_map to return upstream and downstream nodes (model/source/test) up to a given depth", dbtGraphSchema.shape, wrapToolHandler(dbtGraph));

// dbt-centric aggregations live in the dbt category.
tool("freshness-status", "Cross-reference dbt source freshness criteria with sources.json results in a single 'is anything stale right now?' answer.", freshnessStatusSchema.shape, wrapToolHandler(freshnessStatus));
tool("incident-context", "Single asset deep-dive: dbt definition + recent test failures + DQ checks for the dataset. Designed to anchor an LLM-driven incident triage.", incidentContextSchema.shape, wrapToolHandler(incidentContext));

// --- quality category ---
currentCategory = "quality";

tool("dq-list-checks", "List recent rows from DQ_RESULTS_TABLE filtered by dataset / status / type / time window", dqListChecksSchema.shape, wrapToolHandler(dqListChecks));
tool("dq-get-check-history", "Time-series of one check_name's status across the last N days", dqGetCheckHistorySchema.shape, wrapToolHandler(dqGetCheckHistory));
tool("dq-failed-checks-by-dataset", "Group failing checks by dataset across a recent window with the latest 5 failures per dataset", dqFailedChecksByDatasetSchema.shape, wrapToolHandler(dqFailedChecksByDataset));
tool("dq-score-trend", "Time-series of the 4-axis DQ score (completeness / freshness / validity / anomaly_free) plus overall_score from DQ_SCORE_TABLE", dqScoreTrendSchema.shape, wrapToolHandler(dqScoreTrend));
tool("dq-tier-status", "Compare today's overall_score per scope against Tier SLA targets (defaults Tier 1 99.5 / 2 99.0 / 3 95.0; override via DBT_SLA_CONFIG_PATH yaml or DQ_TIER1_TARGET_PCT) and report meeting vs missing counts", dqTierStatusSchema.shape, wrapToolHandler(dqTierStatus));
tool("dq-tier-by-source", "Per-tier rollup computed from quality_checks grouped by source. Reads source-to-tier mapping from dbt sources.yml meta.tier and tier targets from DBT_SLA_CONFIG_PATH (falls back to defaults). Reports per-source pass rate, meeting/missing per tier, and untiered sources as caveats.", dqTierBySourceSchema.shape, wrapToolHandler(dqTierBySource));

tool("failed-tests-summary", "Aggregated 24h-ish view: dbt failed tests + DQ checks failures grouped by dataset + most recent failing rows. Replaces 3+ tool calls (dbt-failed-tests + dq-failed-checks-by-dataset + dq-list-checks).", failedTestsSummarySchema.shape, wrapToolHandler(failedTestsSummary));
tool("dq-score-snapshot", "Aggregated 4-axis score trend + today's Tier compliance + most recent failing checks. Combines dq-score-trend + dq-tier-status + dq-list-checks(fail).", dqScoreSnapshotSchema.shape, wrapToolHandler(dqScoreSnapshot));

// --- Meta ---
currentCategory = "meta";

tool("search-tools",
  "Discover available dbt MCP tools by natural language query across dbt / quality / meta categories.",
  searchToolsSchema.shape, wrapToolHandler(searchTools));

registerPrompts(server);

startMcpServer(server).catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
