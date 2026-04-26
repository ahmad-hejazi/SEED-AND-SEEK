"""
integration.py
--------------
Stage 5 of the Dataset Discovery Pipeline.

Reads rankeddiscovery.json, downloads candidate datasets, performs schema
matching and entity resolution, joins them onto the seed, and writes:
  - output/augmented_dataset.json   (enriched seed records)
  - output/integration_report.json  (audit trail of every decision)

Domain-agnostic: all join keys, entity columns, and attribute names are
inferred from seed_signature.json — no hardcoded domain assumptions.

Usage:
    python src/integration.py
    python src/integration.py --ranked output/rankeddiscovery.json \\
                               --signature output/seed_signature.json \\
                               --seed data/seed.csv \\
                               --output-data output/augmented_dataset.json \\
                               --output-report output/integration_report.json \\
                               --top-k 10 \\
                               --score-threshold 0.3 \\
                               --join-threshold 80
"""

from __future__ import annotations

import argparse
import io
import json
import re
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import chardet
import pandas as pd
import requests
from rapidfuzz import fuzz, process

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_HEADERS = {
    "User-Agent": (
        "Seed-and-Seek/1.0 (+structured dataset discovery; "
        "CKAN/DCAT/Schema.org only)"
    )
}

# File extensions we consider potentially tabular
TABULAR_EXTENSIONS = {".csv", ".tsv", ".json", ".xlsx", ".xls", ".parquet", ".txt"}

# Archive extensions we attempt to open and search for tabular files inside
ARCHIVE_EXTENSIONS = {".zip", ".gz"}

# Extensions we immediately skip (binary, media, markup — never tabular)
SKIP_EXTENSIONS = {
    ".tif", ".tiff", ".tar", ".7z",
    ".pdf", ".doc", ".docx", ".ppt", ".pptx",
    ".png", ".jpg", ".jpeg", ".gif", ".svg",
    ".html", ".htm", ".xml", ".rdf", ".ttl",
    ".nc", ".hdf", ".h5", ".shp", ".kml", ".kmz",
}

# Max bytes to download per candidate file (10 MB)
MAX_DOWNLOAD_BYTES = 10 * 1024 * 1024

# Fuzzy match threshold for column name matching (0–100)
DEFAULT_JOIN_THRESHOLD = 60

# Minimum overall_score to attempt integration
DEFAULT_SCORE_THRESHOLD = 0.05

# Default number of top candidates to attempt
DEFAULT_TOP_K = 50


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(data: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)


def _is_api_url(url: str) -> bool:
    """Heuristic: URLs with query strings or known API patterns."""
    parsed = urlparse(url)
    return bool(parsed.query) or any(
        kw in url.lower() for kw in ["api", "cone?", "vo/", "arcgis/rest", "wfs", "wms"]
    )


def _url_extension(url: str) -> str:
    parsed = urlparse(url)
    return Path(parsed.path).suffix.lower()


def _detect_encoding(raw: bytes) -> str:
    result = chardet.detect(raw[:10000])
    return result.get("encoding") or "utf-8"


# ---------------------------------------------------------------------------
# Seed loader (domain-agnostic, reuses profiler logic)
# ---------------------------------------------------------------------------

def _load_seed(seed_path: Path) -> pd.DataFrame:
    ext = seed_path.suffix.lower()
    if ext == ".csv":
        return pd.read_csv(seed_path)
    elif ext == ".tsv":
        return pd.read_csv(seed_path, sep="\t")
    elif ext == ".json":
        return pd.read_json(seed_path)
    elif ext in (".xlsx", ".xls"):
        return pd.read_excel(seed_path)
    elif ext == ".parquet":
        return pd.read_parquet(seed_path)
    elif ext in (".db", ".sqlite", ".sqlite3"):
        import sqlite3
        con = sqlite3.connect(seed_path)
        tables = pd.read_sql(
            "SELECT name FROM sqlite_master WHERE type='table'", con
        )
        df = pd.read_sql(f"SELECT * FROM [{tables.iloc[0, 0]}]", con)
        con.close()
        return df
    else:
        raise ValueError(f"Unsupported seed format: {ext}")


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def _download_candidate(url: str, timeout: int = 30) -> tuple[bytes | None, str]:
    """
    Download a URL. Returns (raw_bytes, error_message).
    Returns (None, reason) if skipped or failed.

    Handles:
    - Direct tabular files (csv, tsv, json, xlsx, parquet, txt)
    - ZIP archives (extracts first tabular file found inside)
    - GZ compressed files (decompresses and attempts parse)
    - Skips binary/media/markup extensions immediately
    """
    ext = _url_extension(url)

    if ext in SKIP_EXTENSIONS:
        return None, f"skipped non-tabular extension: {ext}"

    if _is_api_url(url) and ext not in TABULAR_EXTENSIONS and ext not in ARCHIVE_EXTENSIONS:
        return None, "skipped API/service URL"

    try:
        resp = requests.get(
            url,
            headers=DEFAULT_HEADERS,
            timeout=timeout,
            stream=True,
        )
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "").lower()
        # Skip obvious non-tabular content types
        if any(ct in content_type for ct in ["html", "xml", "image", "video", "audio"]):
            return None, f"skipped content-type: {content_type}"

        raw = b""
        for chunk in resp.iter_content(chunk_size=65536):
            raw += chunk
            if len(raw) > MAX_DOWNLOAD_BYTES:
                return None, f"skipped: file exceeds {MAX_DOWNLOAD_BYTES // (1024*1024)} MB"

        return raw, ""

    except requests.exceptions.Timeout:
        return None, "timeout"
    except requests.exceptions.HTTPError as e:
        return None, f"HTTP error: {e}"
    except Exception as e:
        return None, f"download error: {e}"


def _extract_from_zip(raw: bytes) -> tuple[bytes | None, str, str]:
    """
    Extract the first tabular file from a ZIP archive.
    Returns (file_bytes, filename, error_message).
    """
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            members = zf.namelist()
            # Prefer CSV, then TSV, then JSON, then others
            priority_order = [".csv", ".tsv", ".json", ".xlsx", ".xls", ".parquet", ".txt"]
            candidates = []
            for name in members:
                ext = Path(name).suffix.lower()
                if ext in priority_order:
                    # Skip hidden/system files (macOS __MACOSX, etc.)
                    if not any(part.startswith("__") or part.startswith(".") 
                               for part in Path(name).parts):
                        candidates.append(name)

            if not candidates:
                return None, "", f"zip contains no tabular files (found: {members[:5]})"

            # Sort by priority order
            candidates.sort(key=lambda n: priority_order.index(Path(n).suffix.lower())
                            if Path(n).suffix.lower() in priority_order else 99)

            chosen = candidates[0]
            file_bytes = zf.read(chosen)
            print(f"  unzipped: {chosen} ({len(file_bytes)} bytes, "
                  f"{len(members)} total files in archive)")
            return file_bytes, chosen, ""

    except zipfile.BadZipFile:
        return None, "", "not a valid ZIP file"
    except Exception as e:
        return None, "", f"zip extraction error: {e}"


def _decompress_gz(raw: bytes) -> tuple[bytes | None, str]:
    """Decompress a gzip file. Returns (decompressed_bytes, error_message)."""
    import gzip
    try:
        return gzip.decompress(raw), ""
    except Exception as e:
        return None, f"gzip decompression error: {e}"


def _parse_to_dataframe(raw: bytes, url: str) -> tuple[pd.DataFrame | None, str]:
    """
    Parse raw bytes into a DataFrame.
    Handles: CSV, TSV, JSON, Excel, Parquet, TXT, ZIP (extracts first tabular
    file), GZ (decompresses then parses).
    Returns (df, error_message).
    """
    ext = _url_extension(url)
    encoding = _detect_encoding(raw)

    # ── ZIP: extract first tabular file and recurse ───────────────────────────
    if ext == ".zip" or raw[:4] == b"PK\x03\x04":
        file_bytes, filename, err = _extract_from_zip(raw)
        if file_bytes is None:
            return None, err
        # Recurse with the extracted file, using its extension for parsing
        inner_url = filename  # just used for extension detection
        return _parse_to_dataframe(file_bytes, inner_url)

    # ── GZ: decompress then recurse ───────────────────────────────────────────
    if ext == ".gz" or raw[:2] == b"\x1f\x8b":
        decompressed, err = _decompress_gz(raw)
        if decompressed is None:
            return None, err
        inner_url = url[:-3] if url.endswith(".gz") else url
        return _parse_to_dataframe(decompressed, inner_url)

    try:
        # ── TXT: treat as delimited text ─────────────────────────────────────
        if ext == ".txt":
            for sep in [",", "\t", ";", "|"]:
                try:
                    df = pd.read_csv(io.BytesIO(raw), sep=sep, encoding=encoding,
                                     low_memory=False)
                    if df.shape[1] > 1:
                        return df, ""
                except Exception:
                    continue
            return None, "txt file could not be parsed as delimited text"

        # ── CSV / unknown extension: try multiple separators ─────────────────
        if ext in (".csv", "") or (ext not in TABULAR_EXTENSIONS):
            try:
                return pd.read_csv(io.BytesIO(raw), encoding=encoding,
                                   low_memory=False), ""
            except Exception:
                pass
            for sep in ["\t", ";", "|"]:
                try:
                    df = pd.read_csv(io.BytesIO(raw), sep=sep, encoding=encoding,
                                     low_memory=False)
                    if df.shape[1] > 1:
                        return df, ""
                except Exception:
                    continue

        # ── TSV ───────────────────────────────────────────────────────────────
        if ext == ".tsv":
            return pd.read_csv(io.BytesIO(raw), sep="\t", encoding=encoding,
                               low_memory=False), ""

        # ── JSON ──────────────────────────────────────────────────────────────
        if ext == ".json":
            try:
                return pd.read_json(io.BytesIO(raw)), ""
            except Exception:
                data = json.loads(raw.decode(encoding, errors="replace"))
                if isinstance(data, list):
                    return pd.DataFrame(data), ""
                elif isinstance(data, dict):
                    for v in data.values():
                        if isinstance(v, list) and v:
                            return pd.DataFrame(v), ""

        # ── Excel ─────────────────────────────────────────────────────────────
        if ext in (".xlsx", ".xls"):
            return pd.read_excel(io.BytesIO(raw)), ""

        # ── Parquet ───────────────────────────────────────────────────────────
        if ext == ".parquet":
            return pd.read_parquet(io.BytesIO(raw)), ""

        return None, "could not parse as tabular data"

    except Exception as e:
        return None, f"parse error: {e}"


# ---------------------------------------------------------------------------
# Schema matching  (domain-agnostic fuzzy column alignment)
# ---------------------------------------------------------------------------

def _normalise_col(name: str) -> str:
    """Lowercase, replace separators with space, strip."""
    return re.sub(r"[\s_\-\.]+", " ", str(name).lower()).strip()


def _match_columns(
    seed_cols: list[str],
    candidate_cols: list[str],
    threshold: int = DEFAULT_JOIN_THRESHOLD,
) -> dict[str, str]:
    """
    Returns {candidate_col: seed_col} for columns that fuzzy-match
    above the threshold. Uses token_set_ratio for robustness.
    """
    mapping: dict[str, str] = {}
    norm_seed = {col: _normalise_col(col) for col in seed_cols}

    for cand_col in candidate_cols:
        norm_cand = _normalise_col(cand_col)
        best_match = process.extractOne(
            norm_cand,
            norm_seed,
            scorer=fuzz.token_set_ratio,
            score_cutoff=threshold,
        )
        if best_match:
            # best_match = (matched_value, score, matched_key)
            seed_col = best_match[2]
            # Avoid duplicate mappings — keep highest score
            if seed_col not in mapping.values():
                mapping[cand_col] = seed_col

    return mapping


def _value_overlap(
    seed_series: pd.Series,
    candidate_series: pd.Series,
    min_overlap: float = 0.05,
) -> float:
    """
    Compute the fraction of candidate values that appear in the seed values.
    Returns a float between 0.0 and 1.0.
    Only applied to non-numeric columns — numeric columns (e.g. year) are
    checked by range overlap instead.
    """
    # For numeric columns, check range overlap instead of value overlap
    if pd.api.types.is_numeric_dtype(seed_series) and pd.api.types.is_numeric_dtype(candidate_series):
        seed_min, seed_max = seed_series.min(), seed_series.max()
        cand_min, cand_max = candidate_series.min(), candidate_series.max()
        # Overlap exists if ranges intersect
        if cand_max < seed_min or cand_min > seed_max:
            return 0.0
        return 1.0  # ranges overlap

    # For string/categorical columns, check value overlap
    seed_vals = set(_normalise_value(v) for v in seed_series.dropna().unique())
    cand_vals  = [_normalise_value(v) for v in candidate_series.dropna().unique()]

    if not seed_vals or not cand_vals:
        return 0.0

    matched = sum(1 for v in cand_vals if v in seed_vals)
    return round(matched / len(cand_vals), 4)


def _find_join_keys(
    seed_cols: list[str],
    candidate_cols: list[str],
    signature: dict[str, Any],
    threshold: int = DEFAULT_JOIN_THRESHOLD,
    seed_df: pd.DataFrame | None = None,
    cand_df: pd.DataFrame | None = None,
    min_value_overlap: float = 0.05,
) -> list[tuple[str, str]]:
    """
    Returns [(candidate_col, seed_col)] pairs that are likely join keys.
    Prioritises candidate_identifiers and salient columns from the signature.
    Also checks value overlap to reject false schema matches.
    """
    priority_seed_cols = (
        signature.get("candidate_identifiers", [])
        + [c["name"] for c in signature.get("columns", []) if c.get("semantic_type") == "datetime"]
        + signature.get("entity_like_columns", [])
    )
    # Deduplicate while preserving order
    seen = set()
    priority_seed_cols = [c for c in priority_seed_cols if not (c in seen or seen.add(c))]

    mapping = _match_columns(priority_seed_cols, candidate_cols, threshold)
    pairs = [(cand, seed) for cand, seed in mapping.items()]

    # Value overlap check — reject pairs where values don't actually match
    if seed_df is not None and cand_df is not None:
        validated = []
        for cand_col, seed_col in pairs:
            if cand_col not in cand_df.columns or seed_col not in seed_df.columns:
                validated.append((cand_col, seed_col))
                continue
            overlap = _value_overlap(seed_df[seed_col], cand_df[cand_col], min_value_overlap)
            if overlap >= min_value_overlap:
                validated.append((cand_col, seed_col))
            else:
                print(f"  rejected join key: {cand_col} -> {seed_col} "
                      f"(value overlap: {overlap:.0%} < {min_value_overlap:.0%})")
        return validated

    return pairs


# ---------------------------------------------------------------------------
# Entity resolution  (domain-agnostic value normalisation)
# ---------------------------------------------------------------------------

def _normalise_value(val: Any) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return re.sub(r"\s+", " ", str(val).lower().strip())


def _resolve_entities(
    seed_series: pd.Series,
    candidate_series: pd.Series,
    threshold: int = DEFAULT_JOIN_THRESHOLD,
) -> dict[str, str]:
    """
    Build a mapping {normalised_candidate_value: seed_value} using
    fuzzy matching. Used for non-numeric join keys (e.g. country names,
    region names, category labels).
    """
    seed_values = seed_series.dropna().unique().tolist()
    norm_seed = {_normalise_value(v): v for v in seed_values}

    mapping: dict[str, str] = {}
    for raw_val in candidate_series.dropna().unique():
        norm_cand = _normalise_value(raw_val)
        if norm_cand in norm_seed:
            mapping[norm_cand] = norm_seed[norm_cand]
            continue
        match = process.extractOne(
            norm_cand,
            list(norm_seed.keys()),
            scorer=fuzz.token_set_ratio,
            score_cutoff=threshold,
        )
        if match:
            mapping[norm_cand] = norm_seed[match[0]]

    return mapping


# ---------------------------------------------------------------------------
# Join & fuse
# ---------------------------------------------------------------------------

def _join_candidate(
    seed_df: pd.DataFrame,
    cand_df: pd.DataFrame,
    join_pairs: list[tuple[str, str]],  # [(cand_col, seed_col)]
    col_mapping: dict[str, str],         # {cand_col: seed_col} for attributes
    source_meta: dict[str, Any],
    entity_threshold: int = DEFAULT_JOIN_THRESHOLD,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Left-join seed_df with cand_df on join_pairs.
    Returns (augmented_df, join_report).
    """
    report: dict[str, Any] = {
        "join_pairs": join_pairs,
        "col_mapping": col_mapping,
        "rows_matched": 0,
        "new_columns_added": [],
        "conflicts": [],
        "entity_resolutions": {},
    }

    if not join_pairs:
        report["skip_reason"] = "no join keys found"
        return seed_df, report

    # Work on copies
    s = seed_df.copy()
    c = cand_df.copy()

    # For each join pair, normalise both sides
    for cand_col, seed_col in join_pairs:
        if cand_col not in c.columns or seed_col not in s.columns:
            continue

        seed_dtype = s[seed_col].dtype
        cand_dtype = c[cand_col].dtype

        # Numeric join keys (e.g. year) — cast to same type
        if pd.api.types.is_numeric_dtype(seed_dtype):
            try:
                c[cand_col] = pd.to_numeric(c[cand_col], errors="coerce")
                s[seed_col] = pd.to_numeric(s[seed_col], errors="coerce")
            except Exception:
                pass
        else:
            # String join key — attempt entity resolution
            entity_map = _resolve_entities(s[seed_col], c[cand_col], entity_threshold)
            if entity_map:
                report["entity_resolutions"][cand_col] = entity_map
                c[cand_col] = c[cand_col].apply(
                    lambda v: entity_map.get(_normalise_value(v), v)
                )
            # Normalise both to lowercase string for matching
            c[cand_col] = c[cand_col].astype(str).str.lower().str.strip()
            s[f"__join_{seed_col}"] = s[seed_col].astype(str).str.lower().str.strip()

    # Build the actual join key column names
    left_keys = []
    right_keys = []
    for cand_col, seed_col in join_pairs:
        right_keys.append(cand_col)
        tmp = f"__join_{seed_col}"
        left_keys.append(tmp if tmp in s.columns else seed_col)

    # Identify new attribute columns to bring over
    existing_seed_cols = set(s.columns)
    new_attr_cols = [
        cand_col for cand_col, seed_col in col_mapping.items()
        if cand_col not in [jc for jc, _ in join_pairs]
        and seed_col not in existing_seed_cols
    ]
    # Also bring over unmapped columns with unique names
    for cand_col in c.columns:
        if cand_col not in [jc for jc, _ in join_pairs] and cand_col not in col_mapping:
            candidate_norm = _normalise_col(cand_col)
            seed_norms = [_normalise_col(sc) for sc in existing_seed_cols]
            if candidate_norm not in seed_norms:
                new_attr_cols.append(cand_col)

    new_attr_cols = list(dict.fromkeys(new_attr_cols))  # deduplicate

    if not new_attr_cols:
        report["skip_reason"] = "no new attributes to add"
        # Clean up temp columns
        s = s.drop(columns=[c for c in s.columns if c.startswith("__join_")])
        return s, report

    # Perform merge
    try:
        merge_right = c[right_keys + new_attr_cols].copy()
        # Rename right join keys to match left
        rename_map = {rk: lk for lk, rk in zip(left_keys, right_keys) if lk != rk}
        merge_right = merge_right.rename(columns=rename_map)

        merged = s.merge(
            merge_right,
            on=left_keys,
            how="left",
            suffixes=("", f"__{source_meta.get('source_name', 'candidate')}"),
        )

        rows_matched = int(merged[new_attr_cols[0]].notna().sum()) if new_attr_cols else 0
        report["rows_matched"] = rows_matched
        report["new_columns_added"] = new_attr_cols

        # Tag new columns with provenance
        for col in new_attr_cols:
            if col in merged.columns:
                prov_col = f"{col}__source"
                merged[prov_col] = merged[col].apply(
                    lambda v: source_meta.get("landing_page", source_meta.get("source_url", ""))
                    if pd.notna(v) else None
                )

        # Detect conflicts (columns that already existed)
        conflict_cols = [
            c for c in merged.columns
            if c.endswith(f"__{source_meta.get('source_name', 'candidate')}")
        ]
        if conflict_cols:
            report["conflicts"] = conflict_cols

        # Clean up temp join columns
        merged = merged.drop(
            columns=[c for c in merged.columns if c.startswith("__join_")],
            errors="ignore",
        )

        return merged, report

    except Exception as e:
        report["skip_reason"] = f"merge error: {e}"
        s = s.drop(columns=[c for c in s.columns if c.startswith("__join_")], errors="ignore")
        return s, report


# ---------------------------------------------------------------------------
# Main integration logic
# ---------------------------------------------------------------------------

def integrate(
    ranked_path: str,
    signature_path: str,
    seed_path: str,
    output_data_path: str,
    output_report_path: str,
    top_k: int = DEFAULT_TOP_K,
    score_threshold: float = DEFAULT_SCORE_THRESHOLD,
    join_threshold: int = DEFAULT_JOIN_THRESHOLD,
    timeout: int = 30,
) -> dict[str, Any]:

    ranked_doc   = _load_json(Path(ranked_path))
    signature    = _load_json(Path(signature_path))
    seed_df      = _load_seed(Path(seed_path))

    candidates   = ranked_doc.get("results", [])
    seed_cols    = list(seed_df.columns)

    print(f"[integration] Seed: {seed_df.shape[0]} rows x {seed_df.shape[1]} columns")
    print(f"[integration] Candidates available: {len(candidates)}")

    # Filter by score threshold
    eligible = [c for c in candidates if c.get("overall_score", 0) >= score_threshold]
    eligible = eligible[:top_k]
    print(f"[integration] Eligible (score >= {score_threshold}, top {top_k}): {len(eligible)}")

    augmented_df = seed_df.copy()
    candidate_reports: list[dict[str, Any]] = []
    total_new_cols = 0
    total_rows_matched = 0

    for idx, candidate in enumerate(eligible):
        title        = candidate.get("title", f"candidate_{idx}")
        download_urls = candidate.get("download_urls") or []
        overall_score = candidate.get("overall_score", 0)

        print(f"\n[integration] [{idx+1}/{len(eligible)}] {title[:60]}")
        print(f"  score={overall_score:.4f}  urls={len(download_urls)}")

        cand_report: dict[str, Any] = {
            "index":         idx + 1,
            "title":         title,
            "connector":     candidate.get("connector"),
            "source_name":   candidate.get("source_name"),
            "landing_page":  candidate.get("landing_page"),
            "overall_score": overall_score,
            "download_urls": download_urls,
            "attempted_urls": [],
            "status":        "skipped",
            "skip_reason":   "",
            "join_report":   {},
            "retrieved_at":  _utc_now(),
        }

        if not download_urls:
            cand_report["skip_reason"] = "no download URLs"
            candidate_reports.append(cand_report)
            continue

        # Try each download URL until one works
        cand_df = None
        used_url = None

        for url in download_urls:
            url = str(url).strip()
            if not url:
                continue

            cand_report["attempted_urls"].append(url)
            raw, err = _download_candidate(url, timeout=timeout)

            if raw is None:
                print(f"  skip url: {err}")
                continue

            df, parse_err = _parse_to_dataframe(raw, url)
            if df is None or df.empty:
                print(f"  parse failed: {parse_err}")
                continue

            if df.shape[1] < 2:
                print(f"  skip: only {df.shape[1]} column(s)")
                continue

            cand_df = df
            used_url = url
            print(f"  downloaded: {df.shape[0]} rows x {df.shape[1]} cols from {url[:60]}")
            break

        if cand_df is None:
            cand_report["skip_reason"] = "no downloadable tabular file found"
            candidate_reports.append(cand_report)
            continue

        # Schema matching
        cand_cols = list(cand_df.columns)
        join_pairs = _find_join_keys(seed_cols, cand_cols, signature, join_threshold,
                                    seed_df=augmented_df, cand_df=cand_df)
        col_mapping = _match_columns(seed_cols, cand_cols, join_threshold)

        print(f"  join pairs   : {join_pairs}")
        print(f"  col mapping  : {dict(list(col_mapping.items())[:5])}{'...' if len(col_mapping) > 5 else ''}")

        if not join_pairs:
            cand_report["skip_reason"] = "no join keys matched"
            cand_report["status"] = "no_join_keys"
            candidate_reports.append(cand_report)
            continue

        # Join
        source_meta = {
            "source_name":  candidate.get("source_name", "unknown"),
            "landing_page": candidate.get("landing_page", ""),
            "source_url":   used_url,
            "retrieved_at": _utc_now(),
        }

        augmented_df, join_report = _join_candidate(
            augmented_df, cand_df, join_pairs, col_mapping,
            source_meta, entity_threshold=join_threshold,
        )

        skip_reason = join_report.get("skip_reason", "")
        if skip_reason:
            cand_report["skip_reason"] = skip_reason
            cand_report["status"] = "join_failed"
        else:
            rows_matched = join_report.get("rows_matched", 0)
            new_cols     = join_report.get("new_columns_added", [])
            cand_report["status"] = "integrated"
            total_new_cols    += len(new_cols)
            total_rows_matched = max(total_rows_matched, rows_matched)
            print(f"  integrated: +{len(new_cols)} columns, {rows_matched} rows matched")

        cand_report["join_report"] = join_report
        candidate_reports.append(cand_report)

        # Small delay to be polite to servers
        time.sleep(0.5)

    # ---------------------------------------------------------------------------
    # Build outputs
    # ---------------------------------------------------------------------------

    # Compute augmentation metrics
    original_cols   = list(seed_df.columns)
    augmented_cols  = list(augmented_df.columns)
    added_cols      = [c for c in augmented_cols if c not in original_cols and not c.endswith("__source")]
    provenance_cols = [c for c in augmented_cols if c.endswith("__source")]

    coverage_gain   = len(added_cols)
    completeness_before = {col: float(seed_df[col].notna().mean()) for col in original_cols}
    completeness_after  = {
        col: float(augmented_df[col].notna().mean())
        for col in original_cols if col in augmented_df.columns
    }

    integrated_count = sum(1 for r in candidate_reports if r["status"] == "integrated")
    skipped_count    = sum(1 for r in candidate_reports if r["status"] == "skipped")
    no_join_count    = sum(1 for r in candidate_reports if r["status"] == "no_join_keys")
    failed_count     = sum(1 for r in candidate_reports if r["status"] == "join_failed")

    report: dict[str, Any] = {
        "generated_at":        _utc_now(),
        "seed_dataset":        ranked_doc.get("seed_dataset"),
        "seed_rows":           int(seed_df.shape[0]),
        "seed_columns":        int(seed_df.shape[1]),
        "candidates_eligible": len(eligible),
        "candidates_integrated": integrated_count,
        "candidates_skipped":    skipped_count,
        "candidates_no_join":    no_join_count,
        "candidates_failed":     failed_count,
        "augmented_rows":        int(augmented_df.shape[0]),
        "augmented_columns":     int(augmented_df.shape[1]),
        "new_columns_added":     added_cols,
        "coverage_gain":         coverage_gain,
        "completeness_before":   completeness_before,
        "completeness_after":    completeness_after,
        "score_threshold_used":  score_threshold,
        "join_threshold_used":   join_threshold,
        "top_k_used":            top_k,
        "candidates":            candidate_reports,
    }

    # Save augmented dataset
    output_data_path = Path(output_data_path)
    if output_data_path.suffix.lower() == ".csv":
        augmented_df.to_csv(output_data_path, index=False, encoding="utf-8")
    else:
        _save_json(augmented_df.to_dict(orient="records"), output_data_path)
    print(f"\n[integration] Augmented dataset -> {output_data_path}")
    print(f"[integration]   {augmented_df.shape[0]} rows x {augmented_df.shape[1]} columns")
    print(f"[integration]   {coverage_gain} new attribute columns added")

    # Save report
    _save_json(report, Path(output_report_path))
    print(f"[integration] Integration report -> {output_report_path}")
    print(f"[integration]   integrated={integrated_count}  skipped={skipped_count}  no_join={no_join_count}  failed={failed_count}")

    return report


# ---------------------------------------------------------------------------
# Auto-detect seed file (mirrors profiler.py logic)
# ---------------------------------------------------------------------------

SUPPORTED_SEED_EXTENSIONS = {".csv", ".tsv", ".json", ".xlsx", ".xls", ".parquet", ".db", ".sqlite", ".sqlite3"}


def _find_seed_file(data_dir: Path) -> Path:
    found = [
        f for f in sorted(data_dir.iterdir())
        if f.is_file() and f.suffix.lower() in SUPPORTED_SEED_EXTENSIONS
    ]
    if not found:
        raise FileNotFoundError(f"No supported seed file found in '{data_dir}'.")
    if len(found) > 1:
        print(f"[integration] Multiple seed files found — using: {found[0].name}")
    return found[0]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cli() -> None:
    parser = argparse.ArgumentParser(description="Stage 5: Integration & Augmentation")
    parser.add_argument("--ranked",        default="output/rankeddiscovery.json",
                        help="Path to rankeddiscovery.json")
    parser.add_argument("--signature",     default="output/seed_signature.json",
                        help="Path to seed_signature.json")
    parser.add_argument("--seed",          default=None,
                        help="Path to seed dataset. Auto-detected from data/ if not provided.")
    parser.add_argument("--output-data",   default="output/augmented_dataset.csv",
                        help="Output path for augmented dataset JSON")
    parser.add_argument("--output-report", default="output/integration_report.json",
                        help="Output path for integration report JSON")
    parser.add_argument("--top-k",         type=int,   default=DEFAULT_TOP_K,
                        help=f"Max candidates to attempt (default: {DEFAULT_TOP_K})")
    parser.add_argument("--score-threshold", type=float, default=DEFAULT_SCORE_THRESHOLD,
                        help=f"Min overall_score to attempt (default: {DEFAULT_SCORE_THRESHOLD})")
    parser.add_argument("--join-threshold",  type=int,   default=DEFAULT_JOIN_THRESHOLD,
                        help=f"Fuzzy match threshold 0-100 (default: {DEFAULT_JOIN_THRESHOLD})")
    parser.add_argument("--timeout",       type=int,   default=30,
                        help="HTTP timeout in seconds (default: 30)")
    args = parser.parse_args()

    seed_path = Path(args.seed) if args.seed else _find_seed_file(Path("data"))
    print(f"[integration] Seed file      : {seed_path}")
    print(f"[integration] Ranked results : {args.ranked}")
    print(f"[integration] Signature      : {args.signature}")
    print(f"[integration] Top-K          : {args.top_k}")
    print(f"[integration] Score threshold: {args.score_threshold}")
    print(f"[integration] Join threshold : {args.join_threshold}")

    integrate(
        ranked_path       = args.ranked,
        signature_path    = args.signature,
        seed_path         = str(seed_path),
        output_data_path  = args.output_data,
        output_report_path= args.output_report,
        top_k             = args.top_k,
        score_threshold   = args.score_threshold,
        join_threshold    = args.join_threshold,
        timeout           = args.timeout,
    )


if __name__ == "__main__":
    _cli()
