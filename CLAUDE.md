# CLAUDE.md

이 파일은 Claude Code가 이 저장소에서 작업할 때 참고하는 컨텍스트입니다.

## 프로젝트 개요

`@us-all/dbt-mcp` — dbt artifacts(`manifest.json`/`run_results.json`/`sources.json`/`catalog.json`) + 사용자 DQ 결과 테이블(BQ/PG)을 노출하는 stdio MCP. **23 도구 + 4 Prompts**. 전부 read-only.

- **타겟 dbt**: 1.7+ (manifest schema v11~v13 검증)
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
│   └── aggregations.ts        # failed-tests-summary, freshness-status, dq-score-snapshot, incident-context (4)
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
| `dbt`     | 15 + 2 aggregations | `DBT_TOOLS=dbt` |
| `quality` | 6 + 2 aggregations | `DBT_TOOLS=quality` |
| `meta`    | 1 (always) | — |

Aggregation 분배:
- `freshness-status` / `incident-context` → dbt category (anchor가 dbt 자산)
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

`DBT_SLA_CONFIG_PATH` (선택) — YAML 파일로 `tier_sla.{1,2,3}` 임계값과 `dbt_sla.{test,freshness}_pass_pct`를 정의. `dq-tier-status`/`dq-tier-by-source`가 이 값을 우선 사용; 없으면 hardcoded `{1: 99.5, 2: 99.0, 3: 95.0}`. `DQ_TIER1_TARGET_PCT` env는 SLA 파일 없을 때 단일 fallback. mtime cached.

`dq-tier-by-source` (신규 도구) — `quality_score_daily`가 하루 한 줄(scope/tier 컬럼 없음)인 환경에서도 per-tier 롤업 제공. 흐름: dbt manifest의 `sources.<source>.<table>.meta.tier` → `source_name -> tier` 맵 구축 → `quality_checks`를 dataset/source 컬럼으로 그룹핑 → 통과율 계산 → tier별 meeting/missing 집계. tier 미설정 source는 `caveats[]`로 노출.

## 알려진 제약

- v0.1.0은 **dbt run/test 트리거 없음**. Airflow에서 dbt를 실행하는 환경이라면 `@us-all/airflow-mcp`로 우회.
- DQ 결과 테이블 컬럼 이름은 v0.1 가정 스키마. v0.2 ColumnMapping config로 일반화 예정.

## 표준 가이드

`@us-all` MCP 작성 표준은 [mcp-toolkit/STANDARD.md](https://github.com/us-all/mcp-toolkit/blob/main/STANDARD.md)에 있음.
