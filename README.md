# @us-all/dbt-mcp

> dbt MCP server — `manifest.json`, `run_results.json`, `sources.json`, `catalog.json`, plus DQ result tables (BigQuery / Postgres) behind one stdio MCP. Built on `@us-all/mcp-toolkit`.

A read-only window into your dbt project for LLM clients. No `dbt run` triggering — just deep introspection, run-history analysis, source freshness, per-column test coverage, lineage walks, and (if you have a custom DQ result table) historical check trends and Tier SLA status.

For DAG triggering / run history / log tails, install the companion **[@us-all/airflow-mcp](https://www.npmjs.com/package/@us-all/airflow-mcp)** alongside.

- 23 tools across 3 categories (`dbt`, `quality`, `meta`)
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
| `quality`| 6 + 2 aggregations | Query `quality_checks` and `quality_score_daily` (BQ or PG); per-tier rollup via `dq-tier-by-source` |
| `meta`   | 1 (always on) | `search-tools` for natural-language tool discovery |

Toggle with `DBT_TOOLS=dbt` (allowlist) or `DBT_DISABLE=quality` (denylist).

## Tools at a glance

### `dbt` (15 + 2)

`dbt-list-models`, `dbt-get-model`, `dbt-list-tests`, `dbt-get-test`, `dbt-list-sources`, `dbt-get-source`, `dbt-list-exposures`, `dbt-list-macros`, `dbt-get-macro`, `dbt-list-runs`, `dbt-get-run-results`, `dbt-failed-tests`, `dbt-slow-models`, `dbt-coverage`, `dbt-graph`, `freshness-status`, `incident-context`

### `quality` (6 + 2)

`dq-list-checks`, `dq-get-check-history`, `dq-failed-checks-by-dataset`, `dq-score-trend`, `dq-tier-status`, `dq-tier-by-source`, `failed-tests-summary`, `dq-score-snapshot`

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
| `DQ_SCHEMA` | no | `generic` (default) or `us-all` — base schema preset for the `quality` category |
| `DQ_COL_*` | no | Per-column overrides on top of `DQ_SCHEMA` (see below). Lets you point at any DQ schema without writing a SQL view. |
| `DQ_TIER1_TARGET_PCT` | no | Tier 1 SLA threshold for `dq-tier-status` when no `tier` column is configured (default 99.5). Superseded by `DBT_SLA_CONFIG_PATH` `tier_sla.1` if both are set. |
| `DBT_SLA_CONFIG_PATH` | no | Optional YAML path with `tier_sla` and `dbt_sla` blocks. Drives `dq-tier-status` thresholds and `dq-tier-by-source` per-tier targets. Mtime cached. |
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

### Per-column overrides — `DQ_COL_*`

If your DQ tables don't match either preset, layer per-column overrides on top of `DQ_SCHEMA`. Any `DQ_COL_*` env var, when set, replaces the preset value for that single column. Unset vars keep the preset default.

| Env var | Logical concept | Generic preset | us-all preset |
|---------|-----------------|----------------|---------------|
| `DQ_COL_RUN_AT`        | timestamp/date on the checks table | `run_at` | `run_date` |
| `DQ_COL_CHECK_TYPE`    | check type / dimension family       | `check_type` | `check_type` |
| `DQ_COL_STATUS`        | pass/fail/warn/error               | `status` | `status` |
| `DQ_COL_DATASET`       | dataset / source / schema           | `dataset` | `source` |
| `DQ_COL_TABLE_NAME`    | table or target name                | `table_name` | `target_name` |
| `DQ_COL_SEVERITY`      | severity / dimension                | `severity` | `dimension` |
| `DQ_COL_FAILURE_COUNT` | numeric failure count / metric      | `failure_count` | `metric_value` |
| `DQ_COL_MESSAGE`       | free-text or JSON message           | `message` | `details::text` |
| `DQ_COL_CHECK_NAME`    | natural identifier of the check     | `check_name` | _(none)_ |
| `DQ_COL_SCORE_DATE`    | date column on the score table      | `score_date` | `run_date` |
| `DQ_COL_SCOPE`         | scope/tenant column on score table  | `scope` | _(none)_ |
| `DQ_COL_TIER`          | tier column on score table          | `tier` | _(none)_ |

For the three nullable columns (`DQ_COL_CHECK_NAME`, `DQ_COL_SCOPE`, `DQ_COL_TIER`), set the value to `none` / `null` / `-` to declare "no native column":
- Without `check_name` → the tools synthesize one from `check_type || ':' || table_name`. `dq-get-check-history` then expects `checkName` formatted as `'<check_type>:<table_name>'`.
- Without `scope` → `dq-score-trend`'s `scope` filter is ignored (with a caveat) and `dq-tier-status` switches to the single-`overall_score` path that compares against `DQ_TIER1_TARGET_PCT`.
- Without `tier` → same single-`overall_score` fallback.

Example — generic preset against a Postgres schema where columns happen to be named differently:

```
DQ_SCHEMA=generic
DQ_COL_RUN_AT=checked_at
DQ_COL_DATASET=schema_name
DQ_COL_TABLE_NAME=tbl
DQ_COL_FAILURE_COUNT=fail_n
DQ_COL_CHECK_NAME=none      # synthesize from check_type+tbl
DQ_COL_SCOPE=none           # no per-team rollup
DQ_COL_TIER=none            # use DQ_TIER1_TARGET_PCT instead
```

## SLA config (optional) — `DBT_SLA_CONFIG_PATH`

Set `DBT_SLA_CONFIG_PATH` to a YAML file to surface project-defined tier targets and DBT SLAs to the quality tools. Schema (extra keys ignored):

```yaml
dbt_sla:
  test_pass_pct: 99.0          # used by future dbt-test SLA tools
  freshness_pass_pct: 99.5     # ditto, freshness

tier_sla:
  1: 99.5                      # tier-1 overall_score / per-source pass-rate target
  2: 99.0
  3: 95.0
```

When set, the `tier_sla` map drives:

- `dq-tier-status` — per-tier rollup compares each row's `overall_score` against the matching target. Without this file, hardcoded `{1: 99.5, 2: 99.0, 3: 95.0}` is used.
- `dq-tier-by-source` — per-source pass-rate is compared to the target for that source's tier (resolved from dbt sources.yml `meta.tier`).
- `dq-tier-status` no-tier-column path (us-all preset / `DQ_COL_TIER=none`) — uses `tier_sla.1` as the single target. `DQ_TIER1_TARGET_PCT` env still works as a fallback when no SLA file is set.

The file is mtime-cached; edits between tool calls are picked up automatically.

## Per-tier rollup from `quality_checks` — `dq-tier-by-source`

For schemas where `quality_score_daily` has only one row per day (no per-scope/tier breakdown), `dq-tier-by-source` reconstructs a per-tier picture from the raw `quality_checks` rows. Two modes:

### `mode: "source"` (default) — group by source/dataset column

Use when each row of `quality_checks` represents a check on a *source group* and the dataset/source column carries the dbt source-group name directly.

1. Builds a `source_name -> tier` map from the dbt manifest's `sources.<source>.<table>.meta.tier` (first table's tier per source group).
2. Groups `quality_checks` rows by the dataset/source column and computes pass rate per source over a date or `sinceHours` window.
3. Looks up each source's tier and target (from SLA config or defaults), reports meeting / missing per tier.

### `mode: "table"` — group by table_name column

Use when the dataset/source column is a category (`bq` / `dbt` / `airflow`) and the actual dbt source-table identifier lives in the `table_name` / `target_name` column as `<source_group>.<table>`. Common in checks tables that consolidate signals from heterogeneous backends.

1. Builds a `<source_group>.<table> -> tier` map from the manifest using each source entry's `source_name + name + meta.tier` — picks up *table-level* tier overrides naturally.
2. Groups `quality_checks` rows by the `table_name` column. Pre-filter via `sourceFilter` (e.g. `sourceFilter: "bq"`) when only some categories produce parseable target names.
3. Each rollup key is parsed as `<source_group>.<table>`; rows without a `.` or whose key is not in the manifest land in `caveats[]`.

Untiered rows (no manifest `meta.tier`) and unparseable rows always appear in `caveats[]` so you can tier them or accept the gap.

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
