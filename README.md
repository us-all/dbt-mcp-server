# @us-all/dbt-mcp

> dbt MCP server — `manifest.json`, `run_results.json`, `sources.json`, `catalog.json`, plus DQ result tables (BigQuery / Postgres) behind one stdio MCP. Built on `@us-all/mcp-toolkit`.

A read-only window into your dbt project for LLM clients. No `dbt run` triggering — just deep introspection, run-history analysis, source freshness, per-column test coverage, lineage walks, and (if you have a custom DQ result table) historical check trends and Tier SLA status.

For DAG triggering / run history / log tails, install the companion **[@us-all/airflow-mcp](https://www.npmjs.com/package/@us-all/airflow-mcp)** alongside.

- 22 tools across 3 categories (`dbt`, `quality`, `meta`)
- 4 MCP Prompts for triage workflows
- 4 aggregation tools that replace 3-5 round-trips of "list / get / list"
- Read-only by default
- Hybrid backend: BigQuery (default) or Postgres for DQ result tables — both peer-imported lazily

## Install

```bash
# 1. add the MCP server
pnpm add -D @us-all/dbt-mcp
# 2. add the DQ backend you actually use (only if you query custom DQ tables):
pnpm add -D @google-cloud/bigquery   # OR
pnpm add -D pg
```

## Run

```bash
DBT_PROJECT_DIR=/path/to/dbt-project \
DQ_RESULTS_TABLE=my-project.data_ops.quality_checks \
npx @us-all/dbt-mcp
```

The server speaks MCP stdio; wire it into Claude Desktop / Cursor / any MCP client. Set `MCP_TRANSPORT=http` to opt in to Streamable HTTP transport (Bearer auth, `/health` endpoint).

## Categories

| Category | Tools | Purpose |
|----------|-------|---------|
| `dbt`    | 15 + 2 aggregations | Parse `manifest.json` / `run_results.json` / `sources.json` / `catalog.json` |
| `quality`| 5 + 2 aggregations | Query `quality_checks` and `quality_score_daily` (BQ or PG) |
| `meta`   | 1 (always on) | `search-tools` for natural-language tool discovery |

Toggle with `DBT_TOOLS=dbt` (allowlist) or `DBT_DISABLE=quality` (denylist).

## Tools at a glance

### `dbt` (15 + 2)

`dbt-list-models`, `dbt-get-model`, `dbt-list-tests`, `dbt-get-test`, `dbt-list-sources`, `dbt-get-source`, `dbt-list-exposures`, `dbt-list-macros`, `dbt-get-macro`, `dbt-list-runs`, `dbt-get-run-results`, `dbt-failed-tests`, `dbt-slow-models`, `dbt-coverage`, `dbt-graph`, `freshness-status`, `incident-context`

### `quality` (5 + 2)

`dq-list-checks`, `dq-get-check-history`, `dq-failed-checks-by-dataset`, `dq-score-trend`, `dq-tier-status`, `failed-tests-summary`, `dq-score-snapshot`

### Prompts

| Prompt | Use when |
|--------|----------|
| `investigate-failed-tests` | "What's broken in the last 24h?" |
| `freshness-degradation-triage` | "Are any sources stale?" (Tier 1 focus optional) |
| `dq-trend-report` | "Give me a stakeholder-friendly DQ trend report" |
| `incident-triage` | "Triage <model \| source>" — bundles all signals |

## Environment variables

| Env | Required | Notes |
|-----|----------|-------|
| `DBT_PROJECT_DIR` | yes | dbt project root (where `dbt_project.yml` lives) |
| `DBT_TARGET_DIR`  | no  | Defaults to `$DBT_PROJECT_DIR/target` |
| `DBT_RUN_HISTORY_DIR` | no | Optional dir for archived `run_results.json` history |
| `DQ_BACKEND` | no | `bigquery` (default) or `postgres` |
| `DQ_RESULTS_TABLE` | no | FQN of the checks table (without it, `quality` category errors at call time) |
| `DQ_SCORE_TABLE` | no | FQN of the score-daily table |
| `GOOGLE_APPLICATION_CREDENTIALS` | no | For BigQuery backend (ADC fallback supported) |
| `BQ_PROJECT_ID` | no | Explicit BQ project (otherwise inferred from ADC) |
| `PG_CONNECTION_STRING` | no | When `DQ_BACKEND=postgres` (secret) |
| `DQ_SCHEMA` | no | `generic` (default) or `us-all` — selects schema flavor for the `quality` category |
| `DQ_TIER1_TARGET_PCT` | no | Tier 1 SLA threshold for `dq-tier-status` when `DQ_SCHEMA=us-all` (default 99.5) |
| `DBT_ALLOW_WRITE` | no | Reserved for future write tools (none in v0.1) |
| `DBT_TOOLS` / `DBT_DISABLE` | no | Category toggles |

## DQ result-table schema flavors

The `quality` category supports two schema presets via `DQ_SCHEMA`:

### `DQ_SCHEMA=generic` (default)

Columns assumed on `DQ_RESULTS_TABLE`: `run_at`, `check_name`, `check_type`, `dataset`, `table_name`, `status`, `severity`, `failure_count`, `message`.

Columns assumed on `DQ_SCORE_TABLE`: `score_date`, `scope`, `tier`, `completeness_pct`, `freshness_pct`, `validity_pct`, `anomaly_free_pct`, `overall_score`.

`dq-tier-status` rolls up by Tier 1/2/3 against the per-`scope` rows.

### `DQ_SCHEMA=us-all`

Real schema used at us-all (Postgres `data_ops` database):

`quality_checks`: `run_date`, `check_type`, `dimension`, `source`, `target_name`, `status`, `metric_value`, `threshold`, `details (JSONB)`.

`quality_score_daily`: `run_date`, `completeness_pct`, `freshness_pct`, `validity_pct`, `anomaly_free_pct`, `overall_score`, `total_checks`, `failed_checks`.

In this flavor `quality_score_daily` is one row per day (no per-scope rollup, no `tier` column). `dq-tier-status` falls back to comparing the day's `overall_score` against `DQ_TIER1_TARGET_PCT` (default 99.5).

`dq-get-check-history` requires `checkName` formatted as `'<check_type>:<target_name>'` since us-all has no native `check_name` column.

v0.2 will add per-column env vars (`DQ_COL_*`) for arbitrary schemas.

## Tested-against schemas

- dbt manifest schema v11 / v12 / v13 (others usually parse but a `caveats` line will flag them)

## Companion server

For Airflow DAG operations (list, runs, task instances, log tail, trigger, clear), install [`@us-all/airflow-mcp`](https://www.npmjs.com/package/@us-all/airflow-mcp) alongside this server.

## Build

```bash
pnpm install
pnpm run build      # tsc → dist/
pnpm test           # vitest
pnpm run smoke      # spawns dist/index.js, calls initialize + tools/list (set env first)
```

## License

MIT — see [LICENSE](./LICENSE).
