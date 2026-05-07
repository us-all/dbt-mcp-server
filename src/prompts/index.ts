import { z } from "zod";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";

export function registerPrompts(server: McpServer): void {
  server.registerPrompt(
    "investigate-failed-tests",
    {
      title: "Investigate failed dbt tests + DQ checks",
      description:
        "Triage the latest dbt test failures + DQ check failures, group by dataset, infer likely root cause (upstream model error, source freshness, schema drift), and produce a prioritized action list.",
      argsSchema: {
        sinceHours: z.string().optional().describe("Lookback window in hours (default '24')"),
        recentRuns: z.string().optional().describe("How many dbt runs to scan (default '3')"),
      },
    },
    ({ sinceHours, recentRuns }) => {
      const hours = sinceHours ?? "24";
      const runs = recentRuns ?? "3";
      return {
        messages: [
          {
            role: "user",
            content: {
              type: "text",
              text: [
                `Investigate failed dbt tests + DQ checks across the last ${hours}h (dbt: last ${runs} runs).`,
                "",
                "Steps:",
                `1. Call \`failed-tests-summary\` with recentRuns=${runs}, sinceHours=${hours}. Capture dbt failures and dq failures by dataset.`,
                "2. For the top 5 failing datasets, call `dbt-list-models` filtered by schema=<dataset> and shortlist models with the highest failure count.",
                "3. For each top failing test, call `dbt-get-test` to read the test definition and `dbt-graph` (upstream depth=2) on the attached model — note any failed upstream models or stale sources.",
                "4. Cross-check: call `freshness-status` (failingOnly=true) to see if any of these tests sit downstream of a freshness violation.",
                "5. If @us-all/airflow-mcp is also installed, suggest the user call `airflow-list-runs` for the loading DAGs of the affected datasets to confirm scheduling/run-time issues.",
                "6. Produce a remediation report:",
                "   - Top failing datasets (with failure counts and severity).",
                "   - Failures classified as: 'upstream broken' / 'source stale' / 'schema drift' / 'data anomaly' / 'unknown'.",
                "   - Per-failure: test name, attached model, severity, last failure timestamp, message, suggested action.",
                "   - Owners-to-notify based on dbt model meta (if present).",
              ].join("\n"),
            },
          },
        ],
      };
    },
  );

  server.registerPrompt(
    "freshness-degradation-triage",
    {
      title: "Freshness degradation triage",
      description:
        "Identify sources/models violating freshness SLAs and rank by Tier impact. If @us-all/airflow-mcp is installed, the prompt also recommends cross-referencing with loading DAG runs.",
      argsSchema: {
        tierFocus: z.string().optional().describe("'1' to focus only on Tier 1 (default 'all')"),
      },
    },
    ({ tierFocus }) => {
      const tier = tierFocus ?? "all";
      return {
        messages: [
          {
            role: "user",
            content: {
              type: "text",
              text: [
                `Triage freshness degradation across sources (tier focus: ${tier}).`,
                "",
                "Steps:",
                "1. Call `freshness-status` with failingOnly=true to enumerate sources currently in warn/error.",
                "2. For each failing source: call `dbt-get-source` (with sourceName + tableName) for full freshness criteria, loader, and tags.",
                "3. Call `dq-tier-status` to confirm current Tier compliance per scope.",
                tier === "1"
                  ? "4. Filter the resulting list down to scopes tagged Tier 1 (in dbt source meta or DQ tier column). Drop the rest."
                  : "4. Group results by inferred Tier (from dbt source meta `tier` field or DQ score table).",
                "5. If @us-all/airflow-mcp is installed, recommend the user call `airflow-list-runs` for the loading DAG (e.g. 'load_<source_name>') and look at the last 5 runs.",
                "6. Produce a triage table:",
                "   - source / model name | tier | criteria | actual age | suggested action.",
                "   - Suggested actions: 'Re-run loading DAG', 'Investigate connector config', 'Escalate to data engineering oncall', 'Update freshness criteria (if intentional)'.",
                "7. Final summary: count of failures by tier and the SLA cushion remaining.",
              ].join("\n"),
            },
          },
        ],
      };
    },
  );

  server.registerPrompt(
    "dq-trend-report",
    {
      title: "Data quality trend report",
      description:
        "Compile a trend report of the 4-axis DQ score over N days, flag regressions vs the prior week, and produce an executive summary.",
      argsSchema: {
        days: z.string().optional().describe("Lookback window in days (default '14')"),
      },
    },
    ({ days }) => {
      const d = days ?? "14";
      return {
        messages: [
          {
            role: "user",
            content: {
              type: "text",
              text: [
                `Produce a DQ trend report covering the last ${d} days.`,
                "",
                "Steps:",
                `1. Call \`dq-score-snapshot\` with days=${d}.`,
                "2. Compute prior-period vs current-period averages for each axis (completeness, freshness, validity, anomaly_free, overall_score).",
                `3. Flag any axis with a regression of ≥5pp vs the prior period of equal length.`,
                "4. Call `dq-tier-status` to capture today's tier compliance, then compare to the same date last week using `dq-score-trend` filtered rows.",
                "5. Produce an executive summary suitable for stakeholders:",
                "   - One-line headline (e.g. 'DQ stable at 99.2%, but Validity dropped 6pp').",
                "   - Per-axis sparkline-ready values (an array of {date, score}).",
                "   - Tier 1/2/3 SLA compliance for today vs target.",
                "   - Top 3 regressions with rationale (link back to failing checks if known).",
                "   - Actions taken / required (use plain English, not jargon).",
              ].join("\n"),
            },
          },
        ],
      };
    },
  );

  server.registerPrompt(
    "incident-triage",
    {
      title: "Incident triage for a model or source",
      description:
        "Bundle dbt definition, recent test results, and DQ check results for a single asset into a triage page. If @us-all/airflow-mcp is installed, the prompt instructs the user to also pull DAG run history for full context.",
      argsSchema: {
        modelName: z.string().optional().describe("dbt model name (provide modelName OR sourceFqn)"),
        sourceFqn: z.string().optional().describe("Source 'source_name.table_name'"),
      },
    },
    ({ modelName, sourceFqn }) => ({
      messages: [
        {
          role: "user",
          content: {
            type: "text",
            text: [
              `Triage incident for ${modelName ? `model '${modelName}'` : `source '${sourceFqn}'`}.`,
              "",
              "Steps:",
              `1. Call \`incident-context\` with ${
                modelName ? `modelName=${JSON.stringify(modelName)}` : `sourceFqn=${JSON.stringify(sourceFqn)}`
              }, sinceHours=48.`,
              "2. Inspect the bundle:",
              "   - dbt definition (path, materialized, tags, refs, tests).",
              "   - Recent test failures attached to this asset.",
              "   - DQ check results for the dataset (filter to fails first).",
              "3. If @us-all/airflow-mcp is installed, call its `airflow-list-runs` for the loading DAG to add run-time context (last success vs last attempt).",
              "4. If lineage might extend beyond dbt (consumed by dashboards/ML models), recommend the user run the OpenMetadata MCP `lineage-impact` tool to find downstream consumers.",
              "5. Produce a one-page triage summary:",
              "   - Severity assessment (P0/P1/P2/P3 based on Tier + downstream impact).",
              "   - Likely root cause (1 sentence + supporting evidence).",
              "   - Immediate actions (3-5 bullets, ranked).",
              "   - Owners to notify (from dbt meta).",
            ].join("\n"),
          },
        },
      ],
    }),
  );
}
