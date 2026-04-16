"""
profiler.py
-----------
Seed Profiler: ingests any structured dataset and produces a machine-readable
seed_signature.json for downstream query generation and dataset discovery.

Supported input formats:
    .csv          — comma-separated values
    .tsv          — tab-separated values
    .json         — array-of-objects or records-oriented JSON
    .xlsx / .xls  — Excel workbooks (first sheet by default)
    .parquet      — columnar format (requires pyarrow)
    .db / .sqlite / .sqlite3 — SQLite databases (first table by default)

Usage (standalone):
    python src/profiler.py --input data/seed.csv   --output output/seed_signature.json
    python src/profiler.py --input data/seed.json  --output output/seed_signature.json
    python src/profiler.py --input data/seed.xlsx  --output output/seed_signature.json
    python src/profiler.py --input data/seed.db    --output output/seed_signature.json
"""

import json
import os
import re
import argparse
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Multi-format loader
# ---------------------------------------------------------------------------

SUPPORTED_FORMATS = {
    ".csv":     "CSV",
    ".tsv":     "TSV",
    ".json":    "JSON",
    ".xlsx":    "Excel",
    ".xls":     "Excel",
    ".parquet": "Parquet",
    ".db":      "SQLite",
    ".sqlite":  "SQLite",
    ".sqlite3": "SQLite",
}


def _load(path: Path) -> tuple[pd.DataFrame, str]:
    """
    Load a structured dataset into a DataFrame.

    Returns
    -------
    (df, format_label) — the DataFrame and a human-readable format string.
    """
    ext = path.suffix.lower()

    if ext not in SUPPORTED_FORMATS:
        supported = ", ".join(SUPPORTED_FORMATS.keys())
        raise ValueError(
            f"Unsupported file type '{ext}'. Supported: {supported}"
        )

    fmt = SUPPORTED_FORMATS[ext]

    if ext == ".csv":
        return pd.read_csv(path), fmt

    elif ext == ".tsv":
        return pd.read_csv(path, sep="\t"), fmt

    elif ext == ".json":
        try:
            df = pd.read_json(path)
        except ValueError:
            # Fallback: load as list of dicts
            with open(path, encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, list):
                df = pd.DataFrame(raw)
            elif isinstance(raw, dict):
                # Try to find the first list value (common API response shape)
                for v in raw.values():
                    if isinstance(v, list):
                        df = pd.DataFrame(v)
                        break
                else:
                    df = pd.DataFrame([raw])
            else:
                raise ValueError("JSON structure not recognised. Expected array-of-objects.")
        return df, fmt

    elif ext in (".xlsx", ".xls"):
        try:
            import openpyxl  # noqa: F401
        except ImportError:
            raise ImportError("openpyxl is required for Excel files. Run: pip install openpyxl")
        return pd.read_excel(path), fmt

    elif ext == ".parquet":
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            raise ImportError("pyarrow is required for Parquet files. Run: pip install pyarrow")
        return pd.read_parquet(path), fmt

    elif ext in (".db", ".sqlite", ".sqlite3"):
        import sqlite3
        con = sqlite3.connect(path)
        tables = pd.read_sql(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name", con
        )
        if tables.empty:
            raise ValueError(f"No tables found in SQLite database: {path}")
        first_table = tables.iloc[0, 0]
        df = pd.read_sql(f"SELECT * FROM [{first_table}]", con)
        con.close()
        print(f"[profiler] SQLite: loaded table '{first_table}' ({len(df)} rows)")
        return df, f"SQLite:{first_table}"


# ---------------------------------------------------------------------------
# Semantic type inference
# ---------------------------------------------------------------------------

_ID_PATTERNS = re.compile(
    r"(\b|_)(id|code|key|uuid|ref|num|no|number|iso|fips|isin|ticker|sku|ean|barcode)(\b|_)",
    re.IGNORECASE,
)
_GEO_PATTERNS  = re.compile(r"(country|nation|state|region|city|province|district|lat|lon|geo|zip|postal)", re.IGNORECASE)
_TIME_PATTERNS = re.compile(r"(year|date|month|quarter|timestamp|time|period|week|day)", re.IGNORECASE)
_DEMO_PATTERNS = re.compile(r"(population|pop|age|gender|sex|income|gdp|mortality|literacy|urban|rural)", re.IGNORECASE)
_ENV_PATTERNS  = re.compile(r"(co2|emission|temperature|rainfall|precipitation|pollution|energy|climate)", re.IGNORECASE)


def _infer_semantic_type(col: str, series: pd.Series) -> str:
    name = col.lower()

    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    if _TIME_PATTERNS.search(name) and pd.api.types.is_integer_dtype(series):
        if series.dropna().between(1800, 2100).all():
            return "datetime"

    if pd.api.types.is_numeric_dtype(series):
        return "numeric"

    if series.dtype == object:
        sample = series.dropna().head(20)
        try:
            parsed = pd.to_datetime(sample, infer_datetime_format=True, errors="coerce")
            if parsed.notna().mean() > 0.8:
                return "datetime"
        except Exception:
            pass

    n_unique     = series.nunique()
    n_total      = len(series.dropna())
    unique_ratio = n_unique / n_total if n_total else 0

    if _ID_PATTERNS.search(name):
        return "identifier"
    if unique_ratio > 0.95 and n_total > 5:
        avg_len = series.dropna().astype(str).str.len().mean()
        if avg_len <= 20:
            return "identifier"

    if series.dtype == object and 0.3 < unique_ratio < 0.98:
        avg_len = series.dropna().astype(str).str.len().mean()
        if avg_len > 3:
            return "entity"

    if series.dtype == object and unique_ratio <= 0.3:
        return "categorical"

    if series.dtype == object:
        avg_len = series.dropna().astype(str).str.len().mean()
        if avg_len > 60:
            return "free_text"

    return "categorical"


# ---------------------------------------------------------------------------
# Column profiler
# ---------------------------------------------------------------------------

def _profile_column(col: str, series: pd.Series, n_rows: int) -> dict:
    sem_type = _infer_semantic_type(col, series)
    n_null   = int(series.isna().sum())
    n_unique = int(series.nunique())

    profile = {
        "name":          col,
        "raw_dtype":     str(series.dtype),
        "semantic_type": sem_type,
        "null_count":    n_null,
        "null_ratio":    round(n_null / n_rows, 4) if n_rows else None,
        "unique_count":  n_unique,
        "unique_ratio":  round(n_unique / n_rows, 4) if n_rows else None,
        "sample_values": _safe_sample(series),
    }

    if sem_type == "numeric":
        desc = series.describe()
        profile.update({
            "min":    _safe_val(series.min()),
            "max":    _safe_val(series.max()),
            "mean":   round(float(desc["mean"]), 4) if "mean" in desc else None,
            "std":    round(float(desc["std"]),  4) if "std"  in desc else None,
            "median": _safe_val(series.median()),
        })

    elif sem_type == "datetime":
        non_null = series.dropna()
        if pd.api.types.is_integer_dtype(non_null):
            profile["min"] = int(non_null.min())
            profile["max"] = int(non_null.max())
        else:
            try:
                parsed = pd.to_datetime(non_null, errors="coerce").dropna()
                if len(parsed):
                    profile["min"] = str(parsed.min().date())
                    profile["max"] = str(parsed.max().date())
            except Exception:
                pass

    elif sem_type in ("categorical", "entity"):
        top = series.value_counts().head(10)
        profile["top_values"] = {str(k): int(v) for k, v in top.items()}

    name_lower = col.lower()
    profile["is_candidate_identifier"] = sem_type == "identifier" or bool(_ID_PATTERNS.search(name_lower))
    profile["is_entity_like"]          = sem_type in ("entity", "categorical") and n_unique > 3
    profile["is_useful_for_querying"]  = sem_type in ("categorical", "entity", "datetime", "identifier")
    profile["is_useful_for_joining"]   = (
        sem_type == "identifier"
        or bool(_GEO_PATTERNS.search(name_lower))
        or bool(_TIME_PATTERNS.search(name_lower))
        or (sem_type == "categorical" and n_unique <= 100)
    )

    return profile


def _safe_sample(series: pd.Series, n: int = 5) -> list:
    vals = series.dropna().unique()[:n]
    return [_safe_val(v) for v in vals]


def _safe_val(v):
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v) if not np.isnan(v) else None
    if isinstance(v, (pd.Timestamp, datetime)):
        return str(v)
    return v


# ---------------------------------------------------------------------------
# Composite key detection
# ---------------------------------------------------------------------------

def _find_composite_keys(df: pd.DataFrame, id_cols: list) -> list:
    candidates = []
    cols = [c for c in id_cols if c in df.columns] or list(df.columns[:10])

    for i, a in enumerate(cols):
        for b in cols[i + 1:]:
            combo = df[[a, b]].dropna()
            if len(combo) == combo.drop_duplicates().shape[0]:
                candidates.append([a, b])
            if len(candidates) >= 5:
                return candidates
    return candidates


# ---------------------------------------------------------------------------
# Inferred concepts
# ---------------------------------------------------------------------------

def _infer_concepts(df: pd.DataFrame) -> list:
    concepts = set()
    combined = " ".join(df.columns).lower()

    if _TIME_PATTERNS.search(combined):
        concepts.add("time_series")
    if _GEO_PATTERNS.search(combined):
        concepts.add("geospatial")
    if _DEMO_PATTERNS.search(combined):
        concepts.add("demographics")
    if _ENV_PATTERNS.search(combined):
        concepts.add("environmental")
    if re.search(r"(gdp|income|revenue|price|cost|wage|salary|trade|export|import)", combined):
        concepts.add("economic")
    if re.search(r"(health|mortality|disease|hospital|patient|clinical)", combined):
        concepts.add("public_health")
    if re.search(r"(school|education|enrollment|literacy|student|teacher)", combined):
        concepts.add("education")
    if re.search(r"(company|firm|employer|employee|sector|industry)", combined):
        concepts.add("corporate")
    if re.search(r"(product|sku|catalog|category|brand|price)", combined):
        concepts.add("product_catalog")

    return sorted(concepts)


# ---------------------------------------------------------------------------
# Quality profile
# ---------------------------------------------------------------------------

def _quality_profile(df: pd.DataFrame) -> dict:
    dup_count = int(df.duplicated().sum())
    high_miss = [c for c in df.columns if df[c].isna().mean() > 0.2]

    outlier_summary = {}
    for col in df.select_dtypes(include="number").columns:
        s = df[col].dropna()
        if len(s) < 4:
            continue
        q1, q3 = s.quantile(0.25), s.quantile(0.75)
        iqr = q3 - q1
        n_out = int(((s < q1 - 1.5 * iqr) | (s > q3 + 1.5 * iqr)).sum())
        if n_out:
            outlier_summary[col] = {"outlier_count": n_out, "method": "IQR_1.5x"}

    return {
        "duplicate_row_count":      dup_count,
        "high_missingness_columns": high_miss,
        "outlier_summary":          outlier_summary,
    }


# ---------------------------------------------------------------------------
# Main profiler
# ---------------------------------------------------------------------------

def profile(path) -> dict:
    """
    Profile any supported structured dataset and return a seed_signature dict.

    Supported formats: CSV, TSV, JSON, Excel (.xlsx/.xls), Parquet, SQLite.
    """
    path = Path(path)
    df, fmt = _load(path)
    n_rows, n_cols = df.shape

    col_profiles = [_profile_column(col, df[col], n_rows) for col in df.columns]

    cand_ids    = [p["name"] for p in col_profiles if p["is_candidate_identifier"]]
    entity_cols = [p["name"] for p in col_profiles if p["is_entity_like"]]
    imp_attrs   = [
        p["name"] for p in col_profiles
        if p["semantic_type"] == "numeric"
        and p["unique_ratio"] is not None
        and p["unique_ratio"] > 0.5
    ]
    salient_cols = [
        p["name"] for p in col_profiles
        if p["is_useful_for_querying"] or p["is_useful_for_joining"]
    ]

    entity_value_samples = {}
    for p in col_profiles:
        if p["is_entity_like"] and p["sample_values"]:
            entity_value_samples[p["name"]] = p["sample_values"]

    signature = {
        "dataset_name":             path.name,
        "file_path":                str(path),
        "file_size_bytes":          path.stat().st_size,
        "file_format":              fmt,
        "profiled_at":              datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "n_rows":                   n_rows,
        "n_columns":                n_cols,
        "columns":                  col_profiles,
        "candidate_identifiers":    cand_ids,
        "candidate_composite_keys": _find_composite_keys(df, cand_ids),
        "entity_like_columns":      entity_cols,
        "important_attributes":     imp_attrs,
        "quality_profile":          _quality_profile(df),
        "salient_column_names":     salient_cols,
        "entity_value_samples":     entity_value_samples,
        "inferred_concepts":        _infer_concepts(df),
    }

    return signature


def save_signature(signature: dict, output_path) -> None:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(signature, f, indent=2, ensure_ascii=False, default=str)
    print(f"[profiler] Signature written to {output_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cli():
    parser = argparse.ArgumentParser(description="Seed Profiler — supports CSV, TSV, JSON, Excel, Parquet, SQLite")
    parser.add_argument("--input",  default="data/seed.csv",              help="Path to seed dataset (csv/tsv/json/xlsx/parquet/db)")
    parser.add_argument("--output", default="output/seed_signature.json", help="Output JSON path")
    args = parser.parse_args()

    print(f"[profiler] Profiling: {args.input}")
    sig = profile(args.input)
    save_signature(sig, args.output)
    print(f"[profiler] Done. {sig['n_rows']} rows x {sig['n_columns']} columns")
    print(f"[profiler] Concepts inferred: {sig['inferred_concepts']}")


if __name__ == "__main__":
    _cli()
