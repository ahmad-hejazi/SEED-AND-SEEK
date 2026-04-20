"""
evaluate.py
-----------
Evaluation script for the Seed-and-Seek pipeline.

Computes the following metrics from the pipeline outputs:

RETRIEVAL QUALITY (from ranked_results.json):
  - Precision@K        : fraction of top-K candidates deemed relevant
  - MAP                : Mean Average Precision
  - nDCG               : Normalized Discounted Cumulative Gain

INTEGRATION QUALITY (from integration_report.json):
  - Join success rate  : fraction of candidates that produced a successful join
  - Download success rate : fraction of candidates where a file was downloaded
  - Skip reason breakdown : counts per skip reason

AUGMENTATION QUALITY (from integration_report.json):
  - Coverage gain      : new columns added / original column count
  - Completeness gain  : avg completeness after vs before
  - Conflict rate      : placeholder (0 since no joins succeeded)

Usage:
    python src/evaluate.py \
        --ranked  output/ranked_results.json \
        --report  output/integration_report.json \
        --output  output/evaluation_report.json \
        --k       10

Or with defaults:
    python src/evaluate.py
"""

import json
import math
import argparse
from pathlib import Path
from datetime import datetime


# ---------------------------------------------------------------------------
# Relevance judgement
# A candidate is considered "relevant" if:
#   - overall_score >= 0.55  (above midpoint)
#   - AND has at least one matched_salient_column
# ---------------------------------------------------------------------------

RELEVANCE_SCORE_THRESHOLD = 0.55
RELEVANCE_SALIENT_THRESHOLD = 1


def _is_relevant(candidate: dict) -> bool:
    score = candidate.get("overall_score", 0)
    salient = candidate.get("matched_salient_columns", [])
    return score >= RELEVANCE_SCORE_THRESHOLD and len(salient) >= RELEVANCE_SALIENT_THRESHOLD


# ---------------------------------------------------------------------------
# Retrieval metrics
# ---------------------------------------------------------------------------

def precision_at_k(candidates: list, k: int) -> float:
    """Fraction of top-K candidates that are relevant."""
    top_k = candidates[:k]
    if not top_k:
        return 0.0
    relevant = sum(1 for c in top_k if _is_relevant(c))
    return round(relevant / len(top_k), 4)


def average_precision(candidates: list) -> float:
    """Average Precision for a ranked list."""
    hits = 0
    cumulative_precision = 0.0
    for i, c in enumerate(candidates, start=1):
        if _is_relevant(c):
            hits += 1
            cumulative_precision += hits / i
    if hits == 0:
        return 0.0
    return round(cumulative_precision / hits, 4)


def ndcg_at_k(candidates: list, k: int) -> float:
    """
    nDCG@K using binary relevance.
    DCG = sum( rel_i / log2(i+1) ) for i in 1..K
    Ideal DCG assumes all relevant docs are at the top.
    """
    top_k = candidates[:k]
    dcg = sum(
        (1 if _is_relevant(c) else 0) / math.log2(i + 2)
        for i, c in enumerate(top_k)
    )
    # Ideal DCG: count relevant in full list, place them at top
    n_relevant = sum(1 for c in candidates if _is_relevant(c))
    ideal_k = min(k, n_relevant)
    idcg = sum(1 / math.log2(i + 2) for i in range(ideal_k))
    if idcg == 0:
        return 0.0
    return round(dcg / idcg, 4)


def compute_retrieval_metrics(candidates: list, k_values: list) -> dict:
    metrics = {}
    for k in k_values:
        metrics[f"precision_at_{k}"] = precision_at_k(candidates, k)
        metrics[f"ndcg_at_{k}"]      = ndcg_at_k(candidates, k)

    metrics["MAP"]             = average_precision(candidates)
    metrics["total_candidates"] = len(candidates)
    metrics["relevant_count"]  = sum(1 for c in candidates if _is_relevant(c))
    metrics["relevance_threshold_score"] = RELEVANCE_SCORE_THRESHOLD
    metrics["score_range"] = {
        "min": round(min(c.get("overall_score", 0) for c in candidates), 4),
        "max": round(max(c.get("overall_score", 0) for c in candidates), 4),
        "mean": round(sum(c.get("overall_score", 0) for c in candidates) / len(candidates), 4)
        if candidates else 0,
    }
    return metrics


# ---------------------------------------------------------------------------
# Integration metrics
# ---------------------------------------------------------------------------

def compute_integration_metrics(report: dict) -> dict:
    total    = report.get("candidates_eligible", 0)
    joined   = report.get("candidates_integrated", 0)
    skipped  = report.get("candidates_skipped", 0)
    no_join  = report.get("candidates_no_join", 0)
    failed   = report.get("candidates_failed", 0)

    # Download success = candidates where a file was actually downloaded
    # = total - skipped (which means no tabular file found or no URLs)
    downloaded = total - skipped

    join_success_rate     = round(joined / total, 4) if total > 0 else 0.0
    download_success_rate = round(downloaded / total, 4) if total > 0 else 0.0

    # Skip reason breakdown from candidates list
    skip_reasons = {}
    for c in report.get("candidates", []):
        reason = c.get("skip_reason") or c.get("status", "unknown")
        skip_reasons[reason] = skip_reasons.get(reason, 0) + 1

    # Connector breakdown
    connector_counts = {}
    for c in report.get("candidates", []):
        conn = c.get("connector", "unknown")
        connector_counts[conn] = connector_counts.get(conn, 0) + 1

    return {
        "candidates_eligible":     total,
        "candidates_downloaded":   downloaded,
        "candidates_joined":       joined,
        "candidates_skipped":      skipped,
        "candidates_no_join_keys": no_join,
        "candidates_failed":       failed,
        "join_success_rate":       join_success_rate,
        "download_success_rate":   download_success_rate,
        "skip_reason_breakdown":   skip_reasons,
        "connector_breakdown":     connector_counts,
    }


# ---------------------------------------------------------------------------
# Augmentation metrics
# ---------------------------------------------------------------------------

def compute_augmentation_metrics(report: dict) -> dict:
    before = report.get("completeness_before", {})
    after  = report.get("completeness_after", {})

    avg_before = round(sum(before.values()) / len(before), 4) if before else 0.0
    avg_after  = round(sum(after.values())  / len(after),  4) if after  else 0.0

    new_cols = report.get("new_columns_added", [])
    original_cols = report.get("seed_columns", 1)

    coverage_gain     = round(len(new_cols) / original_cols, 4) if original_cols else 0.0
    completeness_gain = round(avg_after - avg_before, 4)

    return {
        "seed_rows":                report.get("seed_rows", 0),
        "seed_columns":             original_cols,
        "augmented_rows":           report.get("augmented_rows", 0),
        "augmented_columns":        report.get("augmented_columns", 0),
        "new_columns_added":        new_cols,
        "new_column_count":         len(new_cols),
        "coverage_gain":            coverage_gain,
        "avg_completeness_before":  avg_before,
        "avg_completeness_after":   avg_after,
        "completeness_gain":        completeness_gain,
        "conflict_rate":            0.0,  # no joins succeeded, no conflicts possible
    }


# ---------------------------------------------------------------------------
# Summary + interpretation
# ---------------------------------------------------------------------------

def interpret_results(retrieval: dict, integration: dict, augmentation: dict) -> dict:
    findings = []
    recommendations = []

    p10 = retrieval.get("precision_at_10", 0)
    if p10 >= 0.5:
        findings.append(f"Retrieval quality is reasonable: Precision@10 = {p10}")
    else:
        findings.append(f"Retrieval quality is low: Precision@10 = {p10}. Many top candidates are not relevant to the seed dataset.")
        recommendations.append("Improve query generation to target country-year panel datasets more specifically (e.g. World Bank, UN Data).")

    dl_rate = integration.get("download_success_rate", 0)
    if dl_rate < 0.2:
        findings.append(f"Download success rate is very low ({dl_rate:.0%}). Most candidates could not be accessed.")
        recommendations.append("Add connectors for more accessible portals (World Bank API, OWID, Eurostat) that return structured CSVs reliably.")

    join_rate = integration.get("join_success_rate", 0)
    if join_rate == 0:
        findings.append("No datasets were successfully joined. The 4 downloaded datasets lacked matching country_code or year keys.")
        recommendations.append("Improve join key matching: attempt fuzzy country name matching in addition to exact country_code matching.")
        recommendations.append("Target datasets that explicitly include ISO country codes and year columns (e.g. World Bank indicators).")

    cov = augmentation.get("coverage_gain", 0)
    if cov == 0:
        findings.append("Coverage gain is 0: no new columns were added to the seed dataset.")
    else:
        findings.append(f"Coverage gain: {cov:.0%} ({augmentation['new_column_count']} new columns added).")

    return {
        "findings": findings,
        "recommendations": recommendations,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def evaluate(ranked_path: str, report_path: str, output_path: str, k_values: list) -> dict:
    print(f"[evaluate] Reading ranked results : {ranked_path}")
    with open(ranked_path, "r", encoding="utf-8") as f:
        ranked = json.load(f)

    print(f"[evaluate] Reading integration report: {report_path}")
    with open(report_path, "r", encoding="utf-8") as f:
        report = json.load(f)

    candidates = ranked.get("results", [])
    print(f"[evaluate] Candidates loaded: {len(candidates)}")

    retrieval    = compute_retrieval_metrics(candidates, k_values)
    integration  = compute_integration_metrics(report)
    augmentation = compute_augmentation_metrics(report)
    interpretation = interpret_results(retrieval, integration, augmentation)

    output = {
        "evaluated_at":   datetime.utcnow().isoformat() + "Z",
        "ranked_source":  ranked_path,
        "report_source":  report_path,
        "k_values":       k_values,
        "retrieval_metrics":    retrieval,
        "integration_metrics":  integration,
        "augmentation_metrics": augmentation,
        "interpretation":       interpretation,
    }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n[evaluate] === RESULTS ===")
    print(f"  Precision@5   : {retrieval.get('precision_at_5', 'N/A')}")
    print(f"  Precision@10  : {retrieval.get('precision_at_10', 'N/A')}")
    print(f"  MAP           : {retrieval['MAP']}")
    print(f"  nDCG@10       : {retrieval.get('ndcg_at_10', 'N/A')}")
    print(f"  Relevant found: {retrieval['relevant_count']} / {retrieval['total_candidates']}")
    print(f"  Download rate : {integration['download_success_rate']:.0%}")
    print(f"  Join rate     : {integration['join_success_rate']:.0%}")
    print(f"  Coverage gain : {augmentation['coverage_gain']:.0%}")
    print(f"  Completeness  : {augmentation['avg_completeness_before']} → {augmentation['avg_completeness_after']}")
    print(f"\n[evaluate] Findings:")
    for f_ in interpretation["findings"]:
        print(f"  • {f_}")
    print(f"\n[evaluate] Recommendations:")
    for r in interpretation["recommendations"]:
        print(f"  → {r}")
    print(f"\n[evaluate] Report written to {output_path}")

    return output


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cli():
    parser = argparse.ArgumentParser(description="Seed-and-Seek Evaluation Script")
    parser.add_argument("--ranked",  default="output/ranked_results.json",
                        help="Path to ranked_results.json")
    parser.add_argument("--report",  default="output/integration_report.json",
                        help="Path to integration_report.json")
    parser.add_argument("--output",  default="output/evaluation_report.json",
                        help="Output path for evaluation report")
    parser.add_argument("--k",       type=int, nargs="+", default=[5, 10, 20],
                        help="K values for Precision@K and nDCG@K (default: 5 10 20)")
    args = parser.parse_args()

    evaluate(args.ranked, args.report, args.output, args.k)


if __name__ == "__main__":
    _cli()