# Seed-and-Seek

Dataset Discovery Pipeline — Stages 1 & 2.

## Purpose

Given any structured CSV (country indicators, company records, product catalogs, etc.),
the pipeline automatically discovers additional joinable datasets on the web
and generates ranked search queries to drive that discovery.

## Project Structure

```
Seed-and-Seek/
├── data/
│   └── seed.csv
├── notebooks/
│   ├── seed_profiling.ipynb
│   └── query_generation.ipynb
├── src/
│   ├── profiler.py                ← Stage 1: profiling engine
│   └── query_generator.py         ← Stage 2: query generation engine
├── output/
│   ├── seed_signature.json        ← Stage 1 output
│   ├── queries.json               ← Stage 2 output (normal mode)
│   ├── queries_fallback.json      ← Stage 2 output (fallback mode, if triggered)
│   └── discovery_feedback.json    ← Stage 3 input → Stage 2 fallback trigger
├── requirements.txt
└── README.md
```

## Quick Start

### Jupyter (local)
```bash
pip install -r requirements.txt
jupyter notebook notebooks/seed_profiling.ipynb
jupyter notebook notebooks/query_generation.ipynb
```

### Google Colab
1. Upload this folder (or clone from your repo)
2. Open the notebooks in order: `seed_profiling.ipynb` → `query_generation.ipynb`
3. Run all cells — the first cell installs dependencies automatically

### CLI (no notebook)
```bash
# Stage 1 — profile the seed dataset
python src/profiler.py --input data/seed.csv --output output/seed_signature.json

# Stage 2 — generate queries (normal mode)
python src/query_generator.py --input output/seed_signature.json --output output/queries.json

# Stage 2 — generate fallback queries (after Stage 3 reports insufficient results)
python src/query_generator.py \
    --input    output/seed_signature.json \
    --output   output/queries_fallback.json \
    --mode     fallback \
    --feedback output/discovery_feedback.json
```

---

## Stage 1 — Seed Profiling

### Purpose
Analyzes the input CSV and writes a machine-readable **seed signature** —
the input to Stage 2.

### What the Profiler Does

For each column it computes:

| Field | Description |
|---|---|
| `semantic_type` | `numeric`, `datetime`, `categorical`, `identifier`, `entity`, `free_text` |
| `null_ratio` | fraction of missing values |
| `unique_ratio` | cardinality relative to row count |
| `sample_values` | up to 5 representative values |
| `min / max / mean / std` | numeric and datetime ranges |
| `top_values` | most frequent values for categorical/entity columns |
| `is_candidate_identifier` | likely a join key (ID, code, etc.) |
| `is_entity_like` | proper-noun strings useful for querying |
| `is_useful_for_querying` | recommended for WHERE / filter clauses |
| `is_useful_for_joining` | recommended for JOIN / merge keys |

At dataset level it also extracts:
- `candidate_identifiers` — columns suitable as primary keys
- `candidate_composite_keys` — column pairs whose combination is unique
- `entity_like_columns` — columns containing named entities
- `important_attributes` — high-variance numeric columns
- `quality_profile` — duplicates, missingness, outliers
- `salient_column_names` — columns most relevant for discovery queries
- `entity_value_samples` — example values for entity columns
- `inferred_concepts` — dataset-level tags (time_series, geospatial, economic, …)

### Output Format

```json
{
  "dataset_name": "seed.csv",
  "n_rows": 32,
  "n_columns": 14,
  "columns": [...],
  "candidate_identifiers": ["country_code", "currency_code"],
  "candidate_composite_keys": [],
  "entity_like_columns": ["country_name", "region"],
  "important_attributes": ["gdp_usd", "population", ...],
  "quality_profile": { "duplicate_row_count": 0, ... },
  "salient_column_names": [...],
  "entity_value_samples": { "country_name": ["United States", ...] },
  "inferred_concepts": ["economic", "geospatial", "time_series", ...]
}
```

### Replacing the Sample Data
Drop your own CSV into `data/` and update the path in the notebook cell
that sets `SEED_PATH`, or pass `--input your_file.csv` on the CLI.
No other changes needed.

---

## Stage 2 — Query Generation

### Purpose
Reads `output/seed_signature.json` and generates a ranked list of search queries
targeting structured dataset ecosystems. Used by Stage 3 (Crawler) to discover
joinable datasets.

### Target Ecosystems

**Primary (normal mode)**
- **CKAN** — open data portal software (data.gov, open.canada.ca, etc.)
- **Schema.org** — structured metadata embedded in web pages (Google Dataset Search)
- **DCAT** — standard format for describing datasets in catalogs

**Extended (fallback mode only)**
- Hugging Face Datasets, Kaggle, World Bank Open Data, UN Data, Eurostat, Our World in Data

### Query Strategies

**Normal mode — 5 strategies:**

| Strategy | Description | Priority |
|---|---|---|
| `direct_platform` | Targets CKAN portals, Schema.org datasets, DCAT catalogs | 1 |
| `concept_x_source` | Combines inferred concepts with known data sources | 1 |
| `joinability` | Anchored on candidate identifiers (e.g. `country_code`) | 2 |
| `synonym` | Alternative terminology for existing seed columns | 2 |
| `augmentation` | Targets columns *not* already in the seed dataset | 3 |

**Fallback mode — tiered relaxation:**

| Strategy | Description | Tier | Priority |
|---|---|---|---|
| `fallback_tier1_no_year` | Year range dropped, all topics expanded | 1 | 4 |
| `fallback_tier1_join_no_year` | Joinability queries without year constraint | 1 | 4 |
| `fallback_tier2_extended_sources` | Bare topic queries on extended sources | 2 | 5 |
| `fallback_tier2_synonym_extended` | Synonym queries on extended sources | 2 | 5 |
| `fallback_tier2_join_extended` | Joinability queries on extended sources | 2 | 5 |

### Sufficiency Thresholds

Before triggering fallback, the query generator checks whether Stage 3's
results meet these minimum thresholds:

| Threshold | Default | Description |
|---|---|---|
| `min_candidates` | 5 | Minimum number of candidate datasets returned |
| `min_joinable_ratio` | 0.4 | Minimum fraction of candidates with a usable join key |
| `min_avg_relevance` | 0.3 | Minimum average relevance score (0–1) |

If one threshold fails → **relaxation level 1** (tier 1 only).  
If two or more fail → **relaxation level 2** (tier 1 + tier 2).

### Discovery Feedback Schema

Stage 3 must write `output/discovery_feedback.json` with this structure
for the fallback to work:

```json
{
  "total_candidates":  10,
  "joinable_count":    4,
  "avg_relevance":     0.35,
  "connectors_tried":  ["CKAN", "Schema.org", "DCAT"],
  "failed_connectors": ["DCAT"]
}
```

### Output Format

```json
{
  "generated_at": "2026-04-11T08:48:14Z",
  "mode": "normal",
  "seed_dataset": "seed.csv",
  "year_range": [2020, 2022],
  "concepts": ["economic", "geospatial", "time_series", "..."],
  "identifiers": ["country_code", "currency_code"],
  "target_connectors": ["CKAN", "Schema.org", "DCAT"],
  "total_queries": 346,
  "queries": [
    {
      "id": 1,
      "query": "CKAN economic country year dataset",
      "concept": "economic",
      "target_connector": "CKAN",
      "strategy": "direct_platform",
      "priority": 1
    }
  ]
}
```

Fallback output adds these fields at the top level:

```json
{
  "mode": "fallback",
  "relaxation_level": 1,
  "failed_checks": ["too_few_candidates: got 2, need 5"]
}
```

And each fallback query also includes:

```json
{
  "fallback_tier": 1
}
```

---

## Pipeline Flow

```
seed.csv
   │
   ▼
[Stage 1] profiler.py
   │
   ▼
seed_signature.json
   │
   ▼
[Stage 2] query_generator.py  (normal mode)
   │
   ▼
queries.json
   │
   ▼
[Stage 3] Crawler  ──── discovery_feedback.json ────►  [Stage 2] query_generator.py (fallback mode)
   │                                                         │
   ▼                                                         ▼
candidates.json                                     queries_fallback.json
   │
   ▼
[Stage 4] Ranking → ranked_candidates.json
   │
   ▼
[Stage 5] Integration → augmented_dataset.csv
   │
   ▼
[Stage 6] Evaluation → eval_report.json
```
