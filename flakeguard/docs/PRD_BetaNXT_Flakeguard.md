# Product Requirements Document: flakeguard at BetaNXT

**Status:** Draft MVP  
**Author:** Data Engineering  
**Audience:** Architecture Review Board, Data Platform Leadership  
**Date:** February 2026

---

## 1. Problem Statement

BetaNXT delivers data files (Beta Access, HMRO, RAW) to subscribing wealth management clients through the DataXChange platform, powered by Snowflake. The batch pipeline runs on a fixed schedule via Control-M: source files land in S3, a Step Function ingests and transforms them into a client-scoped staging S3 bucket, Snowpipe loads them into the RAW layer in Snowflake, and dbt pushes data through TYPED, SNAPSHOT, and VIEW layers before client delivery.

**Today, there is no automated SQL sanity check between code change and deployment.** A single bad SQL pattern -- a `SELECT *` in TYPED, a missing `WHERE` on a RAW table, an accidental `CROSS JOIN` -- can silently:

- **Increase Snowflake credit burn** by 10-50x on a single batch run, repeated every Control-M cycle.
- **Delay file delivery SLAs** when a SNAPSHOT rebuild takes 40 minutes instead of 4, pushing the entire batch window.
- **Cascade across layers** -- one slow TYPED model delays every SNAPSHOT, VIEW, and client delivery downstream.
- **Break Cortex readiness** -- views that work fine for known queries may cause full scans when exposed to unpredictable LLM-generated prompts.
- **Reach production undetected** because the query "works" but is just inefficient.

In regulated financial services (broker-dealers, wealth managers), missed delivery SLAs and unexplained cost spikes create audit risk and client trust issues.

---

## 2. Proposed Solution

**flakeguard** is a lightweight, zero-connection cost-intelligence layer that plugs into the existing dbt/Snowflake pipeline as a pre-deployment quality gate.

It reads `target/manifest.json` (already produced by `dbt compile`), parses every model's SQL through a 15-rule AST engine, and returns:

- **Pass / Fail gate** with a configurable weighted score threshold
- **Granular findings** per model: offending SQL snippet, severity, concrete Snowflake-specific fix, estimated impact
- **Cost estimation** across the DAG with downstream propagation
- **Warehouse scaling simulation** (XS vs S vs M tradeoff)

No Snowflake credentials, no query history access, no additional infrastructure. It runs in CI (GitHub Actions) or locally in under 2 seconds.

---

## 3. BetaNXT Batch Pipeline Architecture

```
  ┌─────────────────────────────────────────────────────────────────────────┐
  │                     BetaNXT Batch Data Flow                            │
  │                                                                         │
  │  Control-M          S3              Step Function       Staging S3      │
  │  (scheduler)  →  (landing)  →  (ingest / transform)  →  (client-scoped │
  │                                                           bucket)       │
  │       │                                                     │           │
  │       ▼                                                     ▼           │
  │                            Snowpipe                                     │
  │                     ┌──────────────────┐                                │
  │  Staging S3  ──────→│  RAW layer       │  (Snowflake)                   │
  │                     │  (as-is landing) │                                │
  │                     └────────┬─────────┘                                │
  │                              │  dbt                                     │
  │                     ┌────────▼─────────┐                                │
  │                     │  TYPED layer     │  (cast, rename, validate)      │
  │                     └────────┬─────────┘                                │
  │                              │  dbt                                     │
  │                     ┌────────▼─────────┐                                │
  │                     │  SNAPSHOT layer  │  (SCD Type 2, history)         │
  │                     └────────┬─────────┘                                │
  │                              │  dbt                                     │
  │                     ┌────────▼─────────┐                                │
  │                     │  VIEWS           │  (client-facing, reports)      │
  │                     └────────┬─────────┘                                │
  │                              │  (future)                                │
  │                     ┌────────▼─────────┐                                │
  │                     │  CORTEX / LLM    │  (prompt → insight)            │
  │                     └──────────────────┘                                │
  └─────────────────────────────────────────────────────────────────────────┘
```

### Where flakeguard plugs in (3 insertion points)

```
  ┌─────────────────────────────────────────────────────────────────────────┐
  │                                                                         │
  │  Control-M → S3 → Step Function → Staging S3 → Snowpipe → RAW         │
  │                                                               │         │
  │                                          ┌──────────────┐     │         │
  │  GATE 1: PR / CI                         │  flakeguard  │◄────┘         │
  │  (before dbt run)                        │  lint + cost  │              │
  │  Catches SQL anti-patterns               │  PASS / FAIL  │              │
  │  in TYPED, SNAPSHOT, VIEW models         └───────┬──────┘              │
  │                                                  │ PASS                 │
  │                                          ┌───────▼──────┐              │
  │                                          │  dbt run      │              │
  │                                          │  RAW → TYPED  │              │
  │                                          │  → SNAPSHOT   │              │
  │                                          │  → VIEWS      │              │
  │                                          └───────┬──────┘              │
  │                                                  │                      │
  │  GATE 2: Post-deploy audit               ┌──────▼───────┐             │
  │  (dashboard, scheduled)                  │  flakeguard   │             │
  │  Ongoing cost + anti-pattern             │  dashboard    │             │
  │  monitoring across all client vaults     └──────┬───────┘             │
  │                                                  │                      │
  │  GATE 3: Cortex readiness               ┌───────▼──────┐              │
  │  (future)                                │  flakeguard   │              │
  │  Validate views Cortex will query        │  Cortex rules │              │
  │  are optimized for ad-hoc prompts        └──────────────┘              │
  │                                                                         │
  └─────────────────────────────────────────────────────────────────────────┘
```

### Gate 1: Pre-deployment CI gate (primary)

**Where:** After `dbt compile`, before `dbt run`.  
**What it checks:** Every SQL model in the TYPED, SNAPSHOT, and VIEW layers.  
**Why:** A bad pattern in a TYPED model (e.g. `SELECT *` from RAW) cascades to SNAPSHOT, every VIEW, and ultimately to client file delivery. Catching it here prevents:
- Snowpipe → RAW is fine (no SQL to lint), but RAW → TYPED is dbt SQL and needs checking.
- SNAPSHOT models that do full refresh instead of incremental (E301) waste credits on every Control-M batch run.
- VIEWs with CROSS JOINs (E103) or missing WHERE (E102) cause timeouts when clients query them.

**Integration:** Add to GitHub Actions or the Step Function that runs `dbt run`. Exit code 2 = fail = pipeline stops.

### Gate 2: Ongoing dashboard monitoring

**Where:** Scheduled or on-demand against production manifests.  
**What it checks:** Anti-pattern density, cost trends, DAG health across all Client Data Vaults.  
**Why:** Even if PRs pass the gate, accumulated patterns across hundreds of models create drift. The dashboard gives architects a single view of "data quality posture" per client vault.

### Gate 3: Cortex readiness (future)

**Where:** Before views are exposed to Cortex for LLM-driven queries.  
**What it checks:** Views that Cortex will query must be optimized for ad-hoc access patterns -- no `SELECT *`, proper partitioning, no fan-out joins. When a user prompts Cortex with "show me top accounts by revenue", the underlying view must not do a full scan.  
**Why:** Cortex queries are unpredictable (user-driven prompts). The views must be robust enough to handle arbitrary filters efficiently. flakeguard can validate this with a "Cortex-ready" rule set.

---

## 4. Use Cases (mapped to BetaNXT pipeline)

### UC-1: TYPED + SNAPSHOT Layer Quality Gate

**Actor:** Data engineer opening a PR that modifies RAW → TYPED or TYPED → SNAPSHOT SQL  
**Trigger:** PR changes any `.sql` model in `models/typed/` or `models/snapshot/`  
**Flow:**
1. CI runs `dbt compile` (already triggered by Control-M or GitHub Actions).
2. CI runs `flakeguard --manifest target/manifest.json --fail-threshold 20`.
3. PASS: PR is mergeable; `dbt run` proceeds.
4. FAIL: PR is blocked. Engineer sees exact findings: "TYPED model `stg_accounts` uses SELECT * from RAW -- specify columns to prevent schema drift when source files change."

**Value:** Prevents bad SQL from reaching the TYPED layer, where it would cascade through SNAPSHOT, all client-facing VIEWs, and ultimately delay file delivery to subscribing firms.

### UC-2: Batch Run Cost Visibility

**Actor:** Data engineer, tech lead  
**Trigger:** New model added, materialization changed, or Control-M batch schedule adjusted  
**Flow:**
1. Run `flakeguard` against the manifest.
2. See that the SNAPSHOT model `snap_positions` costs 0.47 credits/run as a full-refresh TABLE.
3. Convert to incremental; cost drops to 0.02 credits/run.
4. Over 24 daily batch runs, this saves ~10.8 credits/day.

**Value:** Quantifies credit impact before the change hits Snowflake billing. Especially important because Control-M triggers batch runs at fixed intervals -- inefficient models burn credits every single run.

### UC-3: Client Data Vault Architecture Review

**Actor:** Data architect, platform lead  
**Trigger:** Quarterly review, new client onboarding, or audit preparation  
**Flow:**
1. Upload the client's Data Vault manifest to the Streamlit dashboard.
2. Review: DAG depth (RAW → TYPED → SNAPSHOT → VIEW), fan-out hotspots, anti-pattern density per layer.
3. Compare anti-pattern posture across client vaults: "Client A has 3 CROSS JOINs in their TYPED layer; Client B has zero."
4. Produce a standardized report for the architecture review board.

**Value:** Consistent, data-driven assessment across all Client Data Vaults on DataXChange.

### UC-4: Warehouse Right-Sizing for Batch Runs

**Actor:** Platform ops, FinOps  
**Trigger:** Monthly Snowflake cost review  
**Flow:**
1. Run the warehouse scaling simulator against the production manifest.
2. See that the current M warehouse costs 0.4 credits for the full DAG, but an S warehouse at 0.2 credits adds only 30 seconds to the batch window.
3. Downsize the batch warehouse from M to S; save 50% on batch credits.

**Value:** Evidence-based warehouse sizing for the Control-M batch runs instead of "we've always used M."

### UC-5: Cortex-Ready View Validation (future)

**Actor:** Data engineer, AI/ML lead  
**Trigger:** Before exposing views to Cortex for prompt-driven analytics  
**Flow:**
1. Run flakeguard with a "Cortex readiness" rule set against the VIEW layer.
2. Findings: "View `v_client_positions` uses SELECT * and has no WHERE -- a Cortex prompt like 'show me positions for client X' will trigger a full scan."
3. Fix: add explicit columns and ensure partition-key filters are present.

**Value:** Cortex queries are unpredictable (LLM-generated). Views must handle arbitrary filters efficiently. flakeguard validates this before views go live for Cortex.

---

## 5. MVP Scope (v1 -- what's built)

| Component | Status | Description |
|-----------|--------|-------------|
| DAG Parser | Done | Reads manifest.json, builds networkx DAG, computes topological order, fan-in/out, critical path |
| Cost Estimator | Done | Per-model and total cost, downstream propagation, top-N expensive models |
| SQL Lint Engine (15 rules) | Done | Pluggable AST rules: 9 performance, 4 correctness, 2 dbt-practice |
| Quality Gate | Done | Weighted pass/fail with configurable threshold |
| CLI | Done | `flakeguard analyze . --fail-threshold 20` with exit code 2 on fail |
| Streamlit Dashboard | Done | DAG, cost, lint report (filters, charts, expandable details), scaling sim |
| GitHub Action | Done | cost-check.yml -- runs on PR, posts markdown summary |
| Example Manifests | Done | E-commerce (14 models), analytics (10 models) with realistic SQL |

---

## 6. Rules Implemented (19) -- mapped to pipeline layers

| ID | Category | What It Catches | Pipeline Layer Impact |
|----|----------|----------------|----------------------|
| E101 | Performance | SELECT * | **TYPED:** pulling all RAW columns causes schema drift when source files change. **VIEWS:** excess bytes scanned on every client query. |
| E102 | Performance | No WHERE clause | **TYPED/SNAPSHOT:** full micro-partition scan on RAW tables that Snowpipe loads into. Every batch run scans everything. |
| E103 | Performance | CROSS JOIN | **TYPED:** Cartesian product between RAW tables can OOM the batch warehouse and delay Control-M schedule. |
| E104 | Performance | Implicit Cartesian | Same as E103 but harder to spot in code review. |
| E105 | Performance | OR in JOIN ON | **TYPED:** forces nested-loop join when joining RAW tables; batch runtime scales quadratically with data growth. |
| E106 | Performance | Leading wildcard LIKE | **VIEWS:** client-facing queries with `LIKE '%...'` cannot use search optimization; slow for Cortex prompts. |
| E107 | Performance | Non-sargable predicate | **TYPED:** `UPPER(col)` in WHERE disables partition pruning on RAW layer tables. |
| E108 | Performance | ORDER BY without LIMIT | **SNAPSHOT:** needless sort in subquery wastes compute on every batch run. |
| E109 | Performance | Fan-out JOIN | **VIEWS:** multiple joins without GROUP BY cause row explosion in client-facing views. |
| E201 | Correctness | Deep nested subqueries | **TYPED:** optimizer can't inline deeply nested logic; refactor to CTEs or intermediate models. |
| E202 | Correctness | DISTINCT + GROUP BY | **SNAPSHOT:** redundant dedup adds a sort step on every batch run. |
| E203 | Correctness | UNION vs UNION ALL | **TYPED:** UNION does an unnecessary dedup when combining RAW sources. |
| E204 | Correctness | Unused CTE | **Any layer:** dead code; Snowflake may still execute it, wasting credits. |
| E301 | dbt Practice | Full-refresh table > 120s | **SNAPSHOT:** full rebuild on every Control-M batch; convert to incremental to cut 10-50x. |
| E302 | dbt Practice | Incremental without is_incremental() | **SNAPSHOT:** model is declared incremental but does full scan every run; negates the benefit. |
| E303 | dbt Practice | No schema tests defined | **TYPED/SNAPSHOT:** model has zero tests in schema.yml. Data quality issues (NULLs, invalid enums, dupes) reach client files undetected. Replaces tests removed from `dbt run` to save runtime. |
| E304 | dbt Practice | Missing `not_null` test | **TYPED/SNAPSHOT:** key columns lack not_null tests. NULL keys cause silent join failures and missing rows in SNAPSHOT/VIEW layers. |
| E305 | dbt Practice | Missing `accepted_values` test | **TYPED/SNAPSHOT:** status/type/category columns lack accepted_values tests. Unexpected enum values propagate to HMRO/RAW/Beta Access client deliverables. |
| E306 | dbt Practice | Missing `unique` test | **TYPED/SNAPSHOT:** primary key lacks unique test. Duplicate rows cascade to reports, SNAPSHOT SCD2 logic, and client file deliveries. |

> **Key design decision (E303-E306):** These rules replace the `accepted_values`, `not_null`, and `unique` tests that were removed from `dbt compile`/`dbt run` to cut batch runtimes. flakeguard enforces the same coverage **statically from manifest.json in <1 second** -- zero Snowflake queries, zero impact on dbt job runtime. Think of it as a schema test coverage report that runs before deployment instead of during it.

---

## 7. Architecture

```
flakeguard/
├── flakeguard/
│   ├── config.py              # Pydantic config, warehouse sizes, lint config
│   ├── dag_parser.py          # manifest.json → networkx DAG
│   ├── cost_estimator.py      # credit cost per model + downstream
│   ├── sql_linter.py          # orchestration: parse SQL → run rules → gate
│   ├── simulator.py           # warehouse scaling comparison
│   ├── cli.py                 # typer CLI with --fail-threshold
│   └── rules/
│       ├── base.py            # BaseRule ABC
│       ├── registry.py        # auto-discover + run_all()
│       ├── performance_rules.py   # E101–E109
│       ├── correctness_rules.py   # E201–E204
│       └── dbt_rules.py          # E301–E306 (incl. test coverage rules)
├── dashboard/
│   └── app.py                 # Streamlit: upload, gate, filters, charts
├── .github/workflows/
│   └── cost-check.yml         # GitHub Action for PR gate
└── tests/                     # 54 tests, all passing
```

**Key design decisions:**
- **Zero connection:** No Snowflake credentials needed. Works offline from manifest.json.
- **Pluggable rules:** Add a new rule by subclassing `BaseRule` and placing it in `rules/`. Auto-discovered.
- **Sub-second runtime:** Full 14-model manifest analyzed in <300ms.

---

## 8. Integration Plan for BetaNXT

### Phase 1: Baseline + Local Adoption (Week 1-2)
- Run flakeguard against the production manifest for each Client Data Vault.
- Baseline: how many findings per layer (TYPED, SNAPSHOT, VIEW)?
- Share dashboard screenshots with architecture team.
- Identify quick wins: models where switching TABLE → INCREMENTAL saves the most credits per batch run.

### Phase 2: CI Gate in GitHub Actions (Week 3-4)
- Add `cost-check.yml` to the dbt project repo.
- Set `--fail-threshold 50` (conservative) so existing PRs aren't blocked immediately.
- Gate runs after `dbt compile` in the PR workflow; findings posted as PR comment.
- Engineers fix flagged issues in new code; gradually tighten threshold to 20.

### Phase 3: Step Function Integration (Month 2)
- Add flakeguard as a step in the AWS Step Function that orchestrates batch runs.
- After `dbt compile`, before `dbt run`: run flakeguard.
- If FAIL: Step Function short-circuits; Control-M gets a failure status; alert fires.
- If PASS: proceed to `dbt run` → Snowpipe → client delivery.

### Phase 4: Dashboard + Client Vault Audits (Month 2-3)
- Deploy Streamlit dashboard on an internal VM or Streamlit Cloud.
- Each Client Data Vault manifest is uploaded for quarterly architecture review.
- Standardized "data quality posture" report per vault: findings, cost, DAG health.

### Phase 5: BetaNXT Custom Rules + Cortex Readiness (Month 3+)
- Add `rules/betanxt_rules.py` with:
  - E401: TYPED models must not SELECT * from RAW (enforce explicit column mapping).
  - E402: SNAPSHOT models must use `updated_at` or `dbt_valid_from` for SCD Type 2.
  - E403: Client-facing VIEWs must have a WHERE clause (Cortex readiness).
  - E404: Models must have a `delivery_type` tag (HMRO / RAW / BETA_ACCESS).
- Integrate `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` for real execution times.
- Cost diff on PR: compare branch manifest vs main manifest, show delta.

---

## 9. Success Metrics

| Metric | Target | How Measured |
|--------|--------|-------------|
| Anti-patterns caught pre-deploy | > 80% of production-impacting SQL issues caught in PR | Gate findings vs production incidents |
| Snowflake credit reduction | 15-25% reduction in dev/staging warehouse spend | Snowflake billing before/after |
| File delivery SLA compliance | Zero SLA breaches from SQL inefficiency | SLA tracking dashboard |
| Developer adoption | 100% of dbt PRs run flakeguard in CI within 60 days | GitHub Action logs |
| Mean time to fix | < 30 min from finding to fix (findings include exact SQL + suggestion) | PR cycle time |

---

## 10. Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| False positives block PRs unnecessarily | Start with high threshold (50); tune down as team calibrates. Engineers can use `--fail-threshold 999` locally to bypass. |
| Manifest doesn't reflect production SQL (Jinja not compiled) | Require `dbt compile` before `flakeguard`. Compiled SQL is what gets analyzed. |
| Rules don't cover BetaNXT-specific patterns | Rule engine is pluggable; add custom rules in `rules/betanxt_rules.py`. |
| dbt tests removed from `dbt run` leave quality gaps | E303-E306 enforce the same test coverage statically from manifest.json -- zero runtime, same contract enforcement. Runs in CI and dashboard. |
| Dashboard adoption is low | Embed dashboard link in PR comment (GitHub Action). Make it the default view for architecture reviews. |

---

## 11. Future Roadmap

- **Snowflake query history integration** -- replace mock execution times with real values from `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`. Enables accurate cost estimation tied to the actual batch warehouse.
- **Cost diff on PR** -- compare manifest on branch vs main, show delta ("+0.3 credits/run" or "-2 findings"). Post as a PR comment.
- **BetaNXT custom rules** -- naming conventions, required model tags (`delivery_type: HMRO`), SCD2 pattern enforcement in SNAPSHOT layer, explicit column mapping in TYPED layer.
- **Step Function native integration** -- flakeguard as a Lambda or ECS task in the AWS Step Function, so Control-M batches automatically gate on SQL quality.
- **Cortex readiness scoring** -- separate "Cortex-ready" score for VIEW layer: no SELECT *, partition-key filters present, no fan-out joins. Score must pass before views are exposed to Cortex prompt engine.
- **Multi-vault comparison** -- compare Client Data Vault manifests side-by-side for standardization across clients.
- **Slack/Teams alerts** -- post gate results to a channel on merge or batch failure.

---

## 12. Demo

To see flakeguard in action right now:

```bash
cd flakeguard
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
flakeguard --manifest examples/ecommerce_manifest.json --warehouse M --fail-threshold 200 .
streamlit run dashboard/app.py
```

The ecommerce manifest (14 models) triggers all 15 rules and produces 37 findings across 4 severity levels with Snowflake-specific suggestions.
