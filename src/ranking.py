"""Rank and filter discovery results.

Reads discovery JSON output, removes candidates matching the filter:
  - priority in [1, 2]
  - relevance_score >= 0.8
  - metadata_completeness_score >= 0.8

Writes the filtered and re-ranked results to a new JSON file.
"""

import argparse
import json
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _save_json(data: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)


def _should_remove(candidate: dict[str, Any]) -> bool:
    return (
        candidate.get("priority") in {1, 2}
        and candidate.get("relevance_score", 0) >= 0.8
        and candidate.get("metadata_completeness_score", 0) >= 0.8
    )


def _rank_results(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        results,
        key=lambda item: (
            item.get("overall_score", 0),
            item.get("relevance_score", 0),
            item.get("joinability_score", 0),
            item.get("metadata_completeness_score", 0),
        ),
        reverse=True,
    )


def filter_and_rank_discovery(
    input_path: str,
    output_path: str,
) -> dict[str, Any]:
    input_file = Path(input_path)
    output_file = Path(output_path)

    doc = _load_json(input_file)
    results = doc.get("results", [])

    filtered = [candidate for candidate in results if not _should_remove(candidate)]
    ranked = _rank_results(filtered)

    output = {
        **{k: v for k, v in doc.items() if k not in {"results"}},
        "filtered_candidates": len(ranked),
        "removed_candidates": len(results) - len(ranked),
        "results": ranked,
    }

    _save_json(output, output_file)
    return output


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Filter and rank discovery output")
    parser.add_argument(
        "--input",
        default="output/discovery_results.json",
        help="Path to discovery results JSON",
    )
    parser.add_argument(
        "--output",
        default="output/rankeddiscovery.json",
        help="Output path for filtered and ranked results JSON",
    )
    args = parser.parse_args()

    output = filter_and_rank_discovery(args.input, args.output)
    print(
        f"[ranking] Wrote {output['filtered_candidates']} filtered candidates "
        f"to {args.output}. Removed {output['removed_candidates']} candidates."
    )


if __name__ == "__main__":
    _cli()
