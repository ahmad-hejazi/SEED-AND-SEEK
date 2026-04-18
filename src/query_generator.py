"""
query_generator.py
------------------
Query Generator: reads the seed_signature.json produced by profiler.py
and generates a ranked list of search queries to discover joinable,
augmenting datasets on the web.

Targets structured dataset ecosystems:
  - CKAN portals (e.g. data.gov, open.canada.ca)
  - Schema.org Dataset pages (Google Dataset Search)
  - DCAT catalogs

Output: output/queries.json

Usage (normal mode):
    python src/query_generator.py --input output/seed_signature.json --output output/queries.json

Usage (fallback mode — triggered when discovery results are insufficient):
    python src/query_generator.py \\
        --input    output/seed_signature.json \\
        --output   output/queries_fallback.json \\
        --mode     fallback \\
        --feedback output/discovery_feedback.json
"""

import json
import argparse
from pathlib import Path
from itertools import product
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Sufficiency thresholds
# Defines what "insufficient" means for the discovery module.
# The discovery module writes a feedback JSON using these keys so the
# query generator knows which relaxation strategies to apply.
# ---------------------------------------------------------------------------

SUFFICIENCY_THRESHOLDS = {
    # Minimum number of candidate datasets that must be returned
    "min_candidates": 5,

    # Minimum fraction of candidates that must have a joinable key
    "min_joinable_ratio": 0.4,

    # Minimum average relevance score across all returned candidates
    # Relevance is scored 0–1 by the ranking module
    "min_avg_relevance": 0.3,
}

# ---------------------------------------------------------------------------
# Concept → topic keywords mapping
# ---------------------------------------------------------------------------

CONCEPT_TOPICS = {
    "economic": [
        "GDP per capita",
        "inflation rate",
        "unemployment rate",
        "trade balance",
        "foreign direct investment",
        "government debt",
        "interest rate",
        "exchange rate",
    ],
    "demographics": [
        "population growth rate",
        "age distribution",
        "fertility rate",
        "migration rate",
        "gender ratio",
        "median age",
    ],
    "public_health": [
        "life expectancy",
        "mortality rate",
        "disease prevalence",
        "healthcare expenditure",
        "hospital beds per capita",
        "vaccination rate",
        "malnutrition rate",
    ],
    "education": [
        "literacy rate",
        "school enrollment rate",
        "education expenditure",
        "human development index",
        "PISA scores",
    ],
    "environmental": [
        "CO2 emissions",
        "renewable energy share",
        "deforestation rate",
        "access to clean water",
        "air quality index",
        "energy consumption",
    ],
    "geospatial": [
        "urbanization rate",
        "land area",
        "population density",
        "geographic coordinates",
    ],
    "time_series": [
        "annual statistics",
        "yearly indicators",
        "longitudinal data",
    ],
}

# ---------------------------------------------------------------------------
# Synonyms — alternative terms for seed column names
# Used to generate additional query variants beyond the column name itself
# ---------------------------------------------------------------------------

SYNONYMS = {
    "gdp_usd":               ["gross domestic product", "national income", "economic output"],
    "gdp_growth_pct":        ["economic growth rate", "GDP growth", "output growth"],
    "co2_emissions_kt":      ["carbon emissions", "greenhouse gas emissions", "pollution"],
    "infant_mortality_rate": ["child mortality", "under-5 mortality", "neonatal mortality"],
    "literacy_rate_pct":     ["literacy", "education rate", "reading ability"],
    "life_expectancy":       ["longevity", "health outcomes", "mortality rate"],
    "urban_pop_pct":         ["urbanization rate", "urban population share", "city population"],
    "population":            ["population count", "demographic size", "inhabitants"],
    "income_group":          ["income classification", "development status", "wealth group"],
    "region":                ["geographic region", "world region", "continental area"],
}

# ---------------------------------------------------------------------------
# Dataset ecosystems — primary and extended
# ---------------------------------------------------------------------------

DATASET_SOURCES = [
    "CKAN",
    "Schema.org dataset",
    "DCAT catalog",
    "Socrata",
    "ArcGIS Hub",
    "DataCite",
]

# Extended sources used in fallback tier 2 and tier 3
EXTENDED_SOURCES = [
    "Hugging Face datasets",
    "Kaggle dataset",
    "World Bank Open Data",
    "UN Data",
    "Eurostat",
    "Our World in Data",
]

# Connector mapping — which source maps to which discovery connector
SOURCE_CONNECTOR = {
    "CKAN":                   "CKAN",
    "Schema.org dataset":     "Schema.org",
    "DCAT catalog":           "DCAT",
    "Socrata":                "Socrata",
    "ArcGIS Hub":             "ArcGIS",
    "DataCite":               "DataCite",
    "Hugging Face datasets":  "HuggingFace",
    "Kaggle dataset":         "Kaggle",
    "World Bank Open Data":   "WorldBank",
    "UN Data":                "UNData",
    "Eurostat":               "Eurostat",
    "Our World in Data":      "OWID",
}

# Joinability terms — phrases that signal a dataset is joinable on country+year
JOIN_TERMS = [
    "by country by year",
    "country year panel data",
    "ISO country code dataset",
    "cross-country dataset",
]


# ---------------------------------------------------------------------------
# Query builders (original strategies — unchanged)
# ---------------------------------------------------------------------------

def _build_concept_queries(concepts: list, year_min: int, year_max: int) -> list:
    """
    For each concept, generate topic x source queries targeting
    CKAN, Schema.org, and DCAT ecosystems.
    """
    queries = []
    for concept in concepts:
        topics = CONCEPT_TOPICS.get(concept, [])
        for topic, source in product(topics[:4], DATASET_SOURCES):
            connector = SOURCE_CONNECTOR[source]
            q = f"{topic} {source} {year_min} {year_max}"
            queries.append({
                "query":            q,
                "concept":          concept,
                "topic":            topic,
                "source":           source,
                "target_connector": connector,
                "strategy":         "concept_x_source",
                "priority":         1,
            })
    return queries


def _build_direct_platform_queries(concepts: list, year_min: int, year_max: int) -> list:
    """
    Direct queries for each structured dataset ecosystem.
    """
    platform_templates = [
        ("CKAN {concept} country year dataset",                           "CKAN"),
        ("Schema.org dataset {concept} by country {year_min} {year_max}", "Schema.org"),
        ("DCAT catalog {concept} country panel data",                     "DCAT"),
    ]
    queries = []
    for concept in concepts:
        for template, connector in platform_templates:
            q = template.format(
                concept=concept.replace("_", " "),
                year_min=year_min,
                year_max=year_max,
            )
            queries.append({
                "query":            q,
                "concept":          concept,
                "target_connector": connector,
                "strategy":         "direct_platform",
                "priority":         1,
            })
    return queries


def _build_joinability_queries(
    identifiers: list,
    entity_samples: dict,
    year_min: int,
    year_max: int,
) -> list:
    """
    Build queries targeting datasets joinable on country_code / year.
    """
    queries = []

    for id_col in identifiers:
        for join_term in JOIN_TERMS:
            for source in DATASET_SOURCES:
                connector = SOURCE_CONNECTOR[source]
                q = f"{id_col.replace('_', ' ')} {join_term} {source} {year_min} {year_max}"
                queries.append({
                    "query":            q,
                    "identifier":       id_col,
                    "source":           source,
                    "target_connector": connector,
                    "strategy":         "joinability",
                    "priority":         2,
                })

    # Queries anchored on actual entity values (country names)
    country_names = entity_samples.get("country_name", [])
    if country_names:
        sample = ", ".join(country_names[:3])
        for source in DATASET_SOURCES[:2]:
            connector = SOURCE_CONNECTOR[source]
            q = f"countries {sample} cross-country dataset {source} {year_min} {year_max}"
            queries.append({
                "query":            q,
                "target_connector": connector,
                "strategy":         "joinability_entity_sample",
                "priority":         2,
            })

    return queries


def _build_augmentation_queries(
    concepts: list,
    entity_samples: dict,
    existing_columns: list,
    year_min: int,
    year_max: int,
) -> list:
    """
    Build queries aimed at finding columns NOT already in the seed dataset.
    """
    covered = {
        "gdp", "population", "life expectancy", "literacy", "co2",
        "infant mortality", "urban", "income",
    }

    queries = []
    regions = entity_samples.get("region", [])

    for concept in concepts:
        topics = CONCEPT_TOPICS.get(concept, [])
        for topic in topics:
            if any(c in topic.lower() for c in covered):
                continue
            for region in regions[:2]:
                for source in DATASET_SOURCES[:2]:
                    connector = SOURCE_CONNECTOR[source]
                    q = f"{topic} {region} {source} {year_min} {year_max}"
                    queries.append({
                        "query":            q,
                        "concept":          concept,
                        "topic":            topic,
                        "region":           region,
                        "target_connector": connector,
                        "strategy":         "augmentation",
                        "priority":         3,
                    })

    return queries


def _build_synonym_queries(
    existing_columns: list,
    year_min: int,
    year_max: int,
) -> list:
    """
    Generate additional query variants using synonyms for seed column names.
    Helps discover datasets that use different terminology for the same concept.
    """
    queries = []

    for col in existing_columns:
        synonyms = SYNONYMS.get(col, [])
        for synonym in synonyms:
            for source in DATASET_SOURCES:
                connector = SOURCE_CONNECTOR[source]
                q = f"{synonym} by country year {source} {year_min} {year_max}"
                queries.append({
                    "query":            q,
                    "column":           col,
                    "synonym":          synonym,
                    "target_connector": connector,
                    "strategy":         "synonym",
                    "priority":         2,
                })

    return queries


# ---------------------------------------------------------------------------
# Sufficiency check
# ---------------------------------------------------------------------------

def check_sufficiency(feedback: dict) -> dict:
    """
    Reads a discovery_feedback.json produced by the discovery module and
    checks it against SUFFICIENCY_THRESHOLDS.

    Returns a dict with:
      - is_sufficient (bool)         : True if all thresholds are met
      - failed_checks (list[str])    : which thresholds were not met
      - relaxation_level (int)       : 0 = sufficient, 1 = mild, 2 = aggressive

    Expected feedback JSON schema:
    {
        "total_candidates":   int,
        "joinable_count":     int,
        "avg_relevance":      float,
        "connectors_tried":   list[str],
        "failed_connectors":  list[str]   // connectors that returned 0 results
    }
    """
    total      = feedback.get("total_candidates", 0)
    joinable   = feedback.get("joinable_count", 0)
    avg_rel    = feedback.get("avg_relevance", 0.0)

    joinable_ratio = (joinable / total) if total > 0 else 0.0

    failed_checks = []

    if total < SUFFICIENCY_THRESHOLDS["min_candidates"]:
        failed_checks.append(
            f"too_few_candidates: got {total}, need {SUFFICIENCY_THRESHOLDS['min_candidates']}"
        )
    if joinable_ratio < SUFFICIENCY_THRESHOLDS["min_joinable_ratio"]:
        failed_checks.append(
            f"low_joinability: {joinable_ratio:.0%} joinable, need {SUFFICIENCY_THRESHOLDS['min_joinable_ratio']:.0%}"
        )
    if avg_rel < SUFFICIENCY_THRESHOLDS["min_avg_relevance"]:
        failed_checks.append(
            f"low_relevance: avg score {avg_rel:.2f}, need {SUFFICIENCY_THRESHOLDS['min_avg_relevance']:.2f}"
        )

    is_sufficient = len(failed_checks) == 0

    # Relaxation level: 0 = ok, 1 = one failure, 2 = two or more failures
    relaxation_level = min(len(failed_checks), 2)

    return {
        "is_sufficient":    is_sufficient,
        "failed_checks":    failed_checks,
        "relaxation_level": relaxation_level,
    }


# ---------------------------------------------------------------------------
# Fallback query builders (tiered relaxation)
# ---------------------------------------------------------------------------

def _build_fallback_tier1_queries(
    concepts: list,
    identifiers: list,
    year_min: int,
    year_max: int,
) -> list:
    """
    Tier 1 fallback — mild relaxation.
    Drops the year range constraint from queries so we cast a wider net
    across datasets that may not be labelled with specific years.
    Also expands topics from 4 to all available topics per concept.
    """
    queries = []

    for concept in concepts:
        topics = CONCEPT_TOPICS.get(concept, [])
        # Use ALL topics (not just top 4) and drop year range
        for topic, source in product(topics, DATASET_SOURCES):
            connector = SOURCE_CONNECTOR[source]
            q = f"{topic} {source} country dataset"
            queries.append({
                "query":            q,
                "concept":          concept,
                "topic":            topic,
                "source":           source,
                "target_connector": connector,
                "strategy":         "fallback_tier1_no_year",
                "priority":         4,
                "fallback_tier":    1,
            })

    # Joinability queries without year constraint
    for id_col in identifiers:
        for join_term in JOIN_TERMS:
            for source in DATASET_SOURCES:
                connector = SOURCE_CONNECTOR[source]
                q = f"{id_col.replace('_', ' ')} {join_term} {source}"
                queries.append({
                    "query":            q,
                    "identifier":       id_col,
                    "target_connector": connector,
                    "strategy":         "fallback_tier1_join_no_year",
                    "priority":         4,
                    "fallback_tier":    1,
                })

    return queries


def _build_fallback_tier2_queries(
    concepts: list,
    identifiers: list,
    existing_columns: list,
) -> list:
    """
    Tier 2 fallback — aggressive relaxation.
    Expands to extended sources (Kaggle, HuggingFace, World Bank, etc.)
    and drops both year range and source name from query terms,
    using only broad topic + join key signals.
    """
    queries = []

    all_sources = EXTENDED_SOURCES  # Switch to extended sources only

    for concept in concepts:
        topics = CONCEPT_TOPICS.get(concept, [])
        for topic in topics:
            for source in all_sources:
                connector = SOURCE_CONNECTOR.get(source, source)
                # Bare topic query — no year, no platform name in the query string
                q = f"{topic} country dataset open data"
                queries.append({
                    "query":            q,
                    "concept":          concept,
                    "topic":            topic,
                    "source":           source,
                    "target_connector": connector,
                    "strategy":         "fallback_tier2_extended_sources",
                    "priority":         5,
                    "fallback_tier":    2,
                })

    # Synonym queries on extended sources — no year range
    for col in existing_columns:
        synonyms = SYNONYMS.get(col, [])
        for synonym in synonyms:
            for source in all_sources[:3]:  # top 3 extended sources
                connector = SOURCE_CONNECTOR.get(source, source)
                q = f"{synonym} country open dataset"
                queries.append({
                    "query":            q,
                    "column":           col,
                    "synonym":          synonym,
                    "target_connector": connector,
                    "strategy":         "fallback_tier2_synonym_extended",
                    "priority":         5,
                    "fallback_tier":    2,
                })

    # Generic broad joinability queries for extended sources
    for id_col in identifiers:
        for source in all_sources[:4]:
            connector = SOURCE_CONNECTOR.get(source, source)
            q = f"{id_col.replace('_', ' ')} cross-country open data"
            queries.append({
                "query":            q,
                "identifier":       id_col,
                "target_connector": connector,
                "strategy":         "fallback_tier2_join_extended",
                "priority":         5,
                "fallback_tier":    2,
            })

    return queries


# ---------------------------------------------------------------------------
# Fallback mode entry point
# ---------------------------------------------------------------------------

def generate_fallback(signature_path: str, feedback_path: str) -> dict:
    """
    Fallback mode: reads the seed signature AND a discovery_feedback.json,
    checks sufficiency, and generates relaxed queries based on what failed.

    relaxation_level 1 → tier 1 only (drop year range, expand topics)
    relaxation_level 2 → tier 1 + tier 2 (extended sources, bare queries)

    The output queries.json is structured identically to the normal output
    so the discovery module can consume it without any changes.
    """
    with open(signature_path, "r", encoding="utf-8") as f:
        sig = json.load(f)
    with open(feedback_path, "r", encoding="utf-8") as f:
        feedback = json.load(f)

    sufficiency = check_sufficiency(feedback)

    if sufficiency["is_sufficient"]:
        print("[query_generator] Discovery results are sufficient — fallback not needed.")
        return None

    print(f"[query_generator] Fallback triggered. Failed checks: {sufficiency['failed_checks']}")
    print(f"[query_generator] Relaxation level: {sufficiency['relaxation_level']}")

    concepts       = sig.get("inferred_concepts", [])
    identifiers    = sig.get("candidate_identifiers", [])
    existing_cols  = [c["name"] for c in sig.get("columns", [])]

    # Year range from original signature — used only in tier1 for reference
    year_min = 2020
    year_max = 2022

    fallback_queries = []

    # Always apply tier 1
    fallback_queries += _build_fallback_tier1_queries(concepts, identifiers, year_min, year_max)

    # Apply tier 2 only if relaxation level is 2 (two or more failed checks)
    if sufficiency["relaxation_level"] >= 2:
        fallback_queries += _build_fallback_tier2_queries(concepts, identifiers, existing_cols)

    fallback_queries = _deduplicate(fallback_queries)
    fallback_queries.sort(key=lambda q: q.get("priority", 9))

    for i, q in enumerate(fallback_queries):
        q["id"] = i + 1

    output = {
        "generated_at":      datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "mode":              "fallback",
        "relaxation_level":  sufficiency["relaxation_level"],
        "failed_checks":     sufficiency["failed_checks"],
        "seed_dataset":      sig.get("dataset_name"),
        "concepts":          concepts,
        "identifiers":       identifiers,
        "target_connectors": list({SOURCE_CONNECTOR.get(s, s) for q in fallback_queries for s in [q.get("target_connector", "")]}),
        "total_queries":     len(fallback_queries),
        "queries":           fallback_queries,
    }

    print(f"[query_generator] Fallback queries generated: {len(fallback_queries)}")
    return output


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def _deduplicate(queries: list) -> list:
    seen = set()
    unique = []
    for q in queries:
        key = q["query"].lower().strip()
        if key not in seen:
            seen.add(key)
            unique.append(q)
    return unique


# ---------------------------------------------------------------------------
# Main generator (unchanged)
# ---------------------------------------------------------------------------

def generate(signature_path: str) -> dict:
    with open(signature_path, "r", encoding="utf-8") as f:
        sig = json.load(f)

    concepts        = sig.get("inferred_concepts", [])
    identifiers     = sig.get("candidate_identifiers", [])
    entity_samples  = sig.get("entity_value_samples", {})
    existing_cols   = [c["name"] for c in sig.get("columns", [])]
    year_min        = 2020
    year_max        = 2022

    print(f"[query_generator] Concepts     : {concepts}")
    print(f"[query_generator] Identifiers  : {identifiers}")
    print(f"[query_generator] Year range   : {year_min} – {year_max}")
    print(f"[query_generator] Columns      : {existing_cols}")

    all_queries = []
    all_queries += _build_direct_platform_queries(concepts, year_min, year_max)
    all_queries += _build_concept_queries(concepts, year_min, year_max)
    all_queries += _build_joinability_queries(identifiers, entity_samples, year_min, year_max)
    all_queries += _build_augmentation_queries(concepts, entity_samples, existing_cols, year_min, year_max)
    all_queries += _build_synonym_queries(existing_cols, year_min, year_max)

    all_queries = _deduplicate(all_queries)
    all_queries.sort(key=lambda q: q.get("priority", 9))

    for i, q in enumerate(all_queries):
        q["id"] = i + 1

    output = {
        "generated_at":      datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "mode":              "normal",
        "seed_dataset":      sig.get("dataset_name"),
        "year_range":        [year_min, year_max],
        "concepts":          concepts,
        "identifiers":       identifiers,
        "target_connectors": list(SOURCE_CONNECTOR.values()),
        "total_queries":     len(all_queries),
        "queries":           all_queries,
    }

    print(f"[query_generator] Total queries generated: {len(all_queries)}")
    return output


def save_queries(output: dict, output_path: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"[query_generator] Queries written to {output_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cli():
    parser = argparse.ArgumentParser(description="Query Generator")
    parser.add_argument("--input",    default="output/seed_signature.json",
                        help="Path to seed_signature.json")
    parser.add_argument("--output",   default="output/queries.json",
                        help="Output path for queries JSON")
    parser.add_argument("--mode",     default="normal", choices=["normal", "fallback"],
                        help="normal = first-pass generation; fallback = relaxed generation after insufficient discovery")
    parser.add_argument("--feedback", default=None,
                        help="Path to discovery_feedback.json (required when --mode fallback)")
    args = parser.parse_args()

    if args.mode == "fallback":
        if not args.feedback:
            parser.error("--feedback is required when --mode is fallback")
        print(f"[query_generator] Fallback mode. Reading feedback: {args.feedback}")
        output = generate_fallback(args.input, args.feedback)
        if output is None:
            print("[query_generator] No fallback needed — exiting without writing.")
            return
    else:
        print(f"[query_generator] Normal mode. Reading signature: {args.input}")
        output = generate(args.input)

    save_queries(output, args.output)
    print(f"[query_generator] Done. {output['total_queries']} queries ready for crawling.")


if __name__ == "__main__":
    _cli()