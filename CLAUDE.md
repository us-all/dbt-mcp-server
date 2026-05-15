# CLAUDE.md

이 파일은 Claude Code가 이 저장소에서 작업할 때 참고하는 컨텍스트입니다.

## 프로젝트 개요

`@us-all/dbt-mcp` — dbt artifacts(`manifest.json`/`run_results.json`/`sources.json`/`catalog.json`) + 사용자 DQ 결과 테이블(BQ/PG)을 노출하는 stdio MCP. **27 도구 (21 primitive + 5 aggregations + 1 meta) + 4 Prompts**. 전부 read-only.

- **타겟 dbt**: 1.7+ (manifest schema v11~v14 검증)
- **DQ 백엔드**: BigQuery 기본 + Postgres 옵션, 둘 다 lazy peer import
- **Airflow는 별도 서버**: [`@us-all/airflow-mcp`](https://github.com/us-all/airflow-mcp-server)
- **표준**: [@us-all MCP Standard](https://github.com/us-all/mcp-toolkit/blob/main/STANDARD.md) 준수

## 디렉토리

```
src/
├── index.ts                   # 카테고리별 tool() 등록 + Prompts
├── config.ts                  # ENV 로딩 + 검증 (DBT/DQ 2 블록)
├── tool-registry.ts           # CATEGORIES = dbt / quality / meta
├── clients/
│   ├── dbt-artifacts.ts       # manifest/run_results/sources/catalog 로더 + mtime cache + run history walk
│   └── dq-store.ts            # BQ/PG 듀얼 백엔드 lazy import + ?-style placeholder rewrite
├── tools/
│   ├── utils.ts               # wrapToolHandler + WriteBlocked / ConfigMissing / DqStore 에러 클래스
│   ├── dbt-models.ts          # dbt-list-models, dbt-get-model, dbt-graph, dbt-coverage (4)
│   ├── dbt-tests.ts           # dbt-list-tests, dbt-get-test (2)
│   ├── dbt-sources.ts         # dbt-list-sources, dbt-get-source, dbt-list-exposures (3)
│   ├── dbt-macros.ts          # dbt-list-macros, dbt-get-macro (2)
│   ├── dbt-runs.ts            # dbt-list-runs, dbt-get-run-results, dbt-failed-tests, dbt-slow-models (4)
│   ├── quality-results.ts     # dq-list-checks, dq-get-check-history, dq-failed-checks-by-dataset (3)
│   ├── quality-scores.ts      # dq-score-trend, dq-tier-status, dq-tier-by-source (3) — Tier SLA 비교 + source.meta.tier 롤업
│   └── aggregations.ts        # failed-tests-summary, freshness-status, dq-score-snapshot, incident-context, dbt-sla-status (5)
└── prompts/
    └── index.ts               # 4 Prompts

tests/
├── fixtures/                  # 작은 manifest.json / run_results.json / sources.json
├── dbt-models.test.ts         # 6
├── dbt-tests-and-macros.test.ts # 5
├── dbt-sources.test.ts        # 3
├── dbt-runs.test.ts           # 4
└── dq-store.test.ts           # 3 (driver 주입)
```

## Build & Run

```bash
pnpm install
pnpm build              # tsc → dist/
pnpm test               # vitest (21 케이스)
pnpm smoke              # dist/index.js 스폰 + initialize + tools/list
```

## 카테고리 (3)

| 카테고리 | 도구 수 | 토글 키 |
|---------|--------|---------|
| `dbt`     | 15 + 3 aggregations | `DBT_TOOLS=dbt` |
| `quality` | 6 + 2 aggregations | `DBT_TOOLS=quality` |
| `meta`    | 1 (always) | — |

Aggregation 분배:
- `freshness-status` / `incident-context` / `dbt-sla-status` → dbt category (anchor가 dbt artifact)
- `failed-tests-summary` / `dq-score-snapshot` → quality category (DQ 결과 테이블 의존도 큼)

## 설계 원칙

- **Read-only**: v0.1은 write tool 없음. `DBT_ALLOW_WRITE`는 미래 예약.
- **Lazy peer imports**: `@google-cloud/bigquery`, `pg` 둘 다 `peerDependencies` + `optional: true`. 사용자가 백엔드 하나만 깔아도 됨.
- **Schema-first**: 모든 도구 `<name>Schema` (zod) + `<name>` handler 페어. 모든 필드 `.describe()`.
- **mtime cache**: dbt artifact는 파일 mtime 기준 in-process 캐시. 새 dbt run이 끝나면 자동 무효화.
- **`?` placeholder convention**: dq-store는 `?`-style 플레이스홀더만 받고 BQ는 `@p0`/`@p1`로, PG는 `$1`/`$2`로 자동 변환.
- **Aggregation caveats**: `aggregate()` 헬퍼로 fan-out 호출 → 부분 실패는 `caveats` 배열로 노출.
- **민감정보 redaction**: `wrapToolHandler` redactionPatterns에 `PG_CONNECTION_STRING`, PEM private key 마스킹.

## DQ 결과 테이블 스키마

`quality` 카테고리 도구는 두 단계로 컬럼을 해석:

1. **Preset (`DQ_SCHEMA`)**: `generic` (기본) — `check_name`, `check_type`, `dataset`, `table_name`, `status`, `severity`, `failure_count`, `run_at`, `message` / `score_date`, `scope`, `tier`, ...; `us-all` — `run_date`, `check_type`, `dimension`, `source`, `target_name`, `metric_value`, ... (no scope/tier).
2. **Per-column 오버라이드 (`DQ_COL_*`, v0.2+)**: 각 컬럼별 env로 preset 값 위에 덮어쓰기. nullable 컬럼(`DQ_COL_CHECK_NAME`/`DQ_COL_SCOPE`/`DQ_COL_TIER`)은 `none`/`null`/`-` 센티넬로 "이 컬럼 없음"을 선언 가능 — `check_name`은 synthesized, scope/tier는 single-`overall_score` 경로로 폴백.

따라서 일반화된 외부 스키마도 view 없이 env 매핑만으로 사용 가능. 자동 감지(INFORMATION_SCHEMA probe)는 v0.4+ 후보.

## Tier SLA 통합 (v0.3+)

`DBT_SLA_CONFIG_PATH` (선택) — YAML 파일로 `tier_sla.{1,2,3}` 임계값과 `dbt_sla.{test,freshness}_pass_pct`를 정의. `dq-tier-status`/`dq-tier-by-source`가 tier_sla를 우선 사용; 없으면 hardcoded `{1: 99.5, 2: 99.0, 3: 95.0}`. `DQ_TIER1_TARGET_PCT` env는 SLA 파일 없을 때 단일 fallback. mtime cached.

`dbt-sla-status` (v0.4 신규 도구) — 최신 `run_results.json`에서 test pass rate, `sources.json`에서 freshness pass rate를 계산해 `dbt_sla.test_pass_pct` / `freshness_pass_pct` 임계값과 비교. 각 축에 `passPct`, `target`, `meeting` 반환. SLA 파일 미설정 시 passPct만 보고하고 target/meeting은 null + caveat. 둘 중 하나만 설정된 경우도 부분 비교 + caveat. skipped test는 분모에서 제외. v0.3 `dbt_sla` 블록이 처음으로 실 사용처를 갖는 도구.

`dq-tier-by-source` (신규 도구) — `quality_score_daily`가 하루 한 줄(scope/tier 컬럼 없음)인 환경에서도 per-tier 롤업 제공. 두 모드:

- `mode: "source"` (기본) — dataset/source 컬럼이 dbt source group 이름과 동일할 때. source-level meta.tier (각 source group의 첫 테이블 tier 사용).
- `mode: "table"` — dataset/source 컬럼이 카테고리(bq/dbt/airflow)이고 실제 dbt source-table은 `target_name`이 `<source_group>.<table>` 형식일 때 (us-all 데이터 모양). table-level meta.tier로 tier 정의가 테이블별로 다른 케이스도 처리. `sourceFilter` 옵션으로 사전 필터(e.g. `sourceFilter: "bq"`).

tier 미설정 row + parsing 실패 row는 항상 `caveats[]`로 노출. v0.3.1에서 라이브 us-all 데이터로 검증한 결과 `mode: "source"`만으로는 us-all에 부합하지 않아 `mode: "table"` 추가.

## 알려진 제약

- v0.1.0은 **dbt run/test 트리거 없음**. Airflow에서 dbt를 실행하는 환경이라면 `@us-all/airflow-mcp`로 우회.
- DQ 결과 테이블 컬럼 이름은 v0.1 가정 스키마. v0.2 ColumnMapping config로 일반화 예정.

## 표준 가이드

`@us-all` MCP 작성 표준은 [mcp-toolkit/STANDARD.md](https://github.com/us-all/mcp-toolkit/blob/main/STANDARD.md)에 있음.
