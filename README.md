# Seed-and-Seek

**Universal Web Dataset Discovery and Augmentation from a Seed Dataset**

Seed-and-Seek is a domain-agnostic pipeline that takes any structured dataset as input, automatically discovers relevant datasets on the web, integrates them, and produces an augmented dataset with a full audit trail.

It works across domains — public health indicators, company records, product catalogs, regional statistics, education data, and more — without any domain-specific rules.

---

## Team

Eddy El Kallaye · Ahmad Hijazi · Yesmine Kallel · Amartya Chandragupta Dudhe

---

## Pipeline Overview

```
Seed Dataset
     ↓
1. Seed Profiling       → output/seed_signature.json
     ↓
2. Query Generation     → output/queries.json
     ↓
3. Web Discovery        → output/discovery_results.json
     ↓
4. Ranking              → output/ranked_results.json
     ↓
5. Integration          → output/augmented_dataset.json
                          output/integration_report.json
     ↓
6. Evaluation           → output/evaluation_report.json
```

---

## Project Structure

```
SEED-AND-SEEK/
├── data/
│   └── seed.json               ← your input dataset (any supported format)
├── notebooks/
│   ├── seed_profiling.ipynb    ← interactive profiling notebook
│   └── query_generation.ipynb ← interactive query generation notebook
├── src/
│   ├── profiler.py             ← Stage 1: seed dataset profiling
│   ├── query_generator.py      ← Stage 2: discovery query generation
│   ├── web_discovery.py        ← Stage 3: web crawling across 6 connectors
│   ├── ranking.py              ← Stage 4: candidate ranking and filtering
│   ├── integration.py          ← Stage 5: schema matching, join, augmentation
│   ├── evaluate.py             ← Stage 6: IR and augmentation metrics
│   └── main.py                 ← FastAPI server (runs full pipeline via API)
├── output/                     ← auto-created on first run
├── requirements.txt
└── README.md
```

---

## Supported Input Formats

The pipeline accepts any of the following seed dataset formats:

| Format | Extension |
|---|---|
| CSV | `.csv` |
| TSV | `.tab-separated` |
| JSON | `.json` |
| Excel | `.xlsx`, `.xls` |
| Parquet | `.parquet` |
| SQLite | `.db`, `.sqlite`, `.sqlite3` |

Drop your file into `data/` — the pipeline auto-detects the format.

---

## Discovery Connectors

Seed-and-Seek crawls the following dataset ecosystems:

| Connector | Source | Type |
|---|---|---|
| CKAN | Humanitarian Data Exchange, Open Canada, London Datastore, HealthData.gov, Africa Open Data | Open data portals |
| DCAT | DOT Open Data, NASA, data.gov.au, data.gov.uk | W3C catalog standard |
| Schema.org | Web-wide dataset pages | JSON-LD metadata |
| Socrata | All Socrata portals globally | Discovery API |
| ArcGIS Hub | ArcGIS Hub open datasets | Hub search API |
| DataCite | DOI-registered research datasets | REST API |

---

## Installation

**Requirements:** Python 3.10+

```bash
pip install -r requirements.txt
```

---

## Running the Project

### Option A — FastAPI (recommended)

Start the server:

```bash
python src/main.py
```

Open the interactive API docs at:

```
http://localhost:8000/docs
```

Use the `POST /pipeline/run` endpoint to upload your seed file. The full pipeline runs automatically and returns a `job_id`. Use that ID to:

- `GET /pipeline/{job_id}` — check status
- `GET /pipeline/{job_id}/metrics` — view evaluation metrics
- `GET /pipeline/{job_id}/download` — download augmented dataset as CSV
- `GET /pipeline/{job_id}/logs` — view step-by-step logs

Each job is stored independently under `output/jobs/<job_id>/`.

---

### Option B — Manual step by step

```bash
python src/profiler.py
python src/query_generator.py
python src/web_discovery.py
python src/ranking.py
python src/integration.py
python src/evaluate.py
```

All scripts auto-detect the seed file from `data/` and use default output paths. No arguments required for a standard run.

#### Optional CLI arguments

```bash
# Use a specific input file
python src/profiler.py --input data/myfile.xlsx

# Adjust integration behaviour
python src/integration.py --top-k 50 --score-threshold 0.05 --join-threshold 60

# Adjust evaluation K values
python src/evaluate.py --k 5 10 20
```

---

## Output Files

| File | Description |
|---|---|
| `seed_signature.json` | Column profiles, inferred types, join keys, quality metrics, inferred concepts |
| `queries.json` | Generated discovery queries with connector routing |
| `discovery_results.json` | Raw candidates from all connectors |
| `ranked_results.json` | Filtered and ranked candidates by relevance + joinability |
| `augmented_dataset.json` | Seed dataset enriched with new columns from integrated sources |
| `integration_report.json` | Audit trail: every candidate attempted, join decisions, provenance |
| `evaluation_report.json` | Precision@K, MAP, nDCG, join rate, coverage gain, completeness gain |

---

## Evaluation Metrics

**Retrieval quality** (from `ranked_results.json`):
- Precision@K, nDCG@K (K = 5, 10, 20 by default)
- MAP (Mean Average Precision)

**Integration quality** (from `integration_report.json`):
- Download success rate
- Join success rate
- Skip reason breakdown by category

**Augmentation quality** (from `integration_report.json`):
- Coverage gain — new columns added relative to original column count
- Completeness gain — change in average column completeness after augmentation
- Conflict rate — frequency of contradictory values across sources

---

## Notes

- The `output/` folder is created automatically on first run — no manual setup needed.
- All pipeline steps respect `robots.txt` conventions and apply rate limiting.
- Downloaded files over 10 MB are skipped to avoid excessive load on data sources.
- Every new column added to the augmented dataset is tagged with a provenance field (`column__source`) recording the originating URL.
- The pipeline is reproducible — every run produces a dated snapshot with source list and configuration.
