"""
web_discovery.py
----------------
Stage 3 of the Dataset Discovery Pipeline.

Connectors
----------
Core (structured dataset catalogs):
  - CKAN          : CKAN Action API (package_search)
  - DCAT          : W3C DCAT data.json catalogs
  - Schema.org    : JSON-LD Dataset pages via DuckDuckGo

Extended (additional open-data ecosystems):
  - Socrata       : Socrata Discovery API (api.us.socrata.com/api/catalog/v1)
  - ArcGIS Hub    : ArcGIS Hub search API (hub.arcgis.com/api/search/v1)
  - DataCite      : DataCite REST API (doi.org metadata, research datasets)
"""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup


DEFAULT_HEADERS = {
    "User-Agent": (
        "Seed-and-Seek/1.0 (+structured dataset discovery; "
        "CKAN/DCAT/Schema.org/Socrata/ArcGIS/DataCite)"
    )
}

# ---------------------------------------------------------------------------
# Source registries
# ---------------------------------------------------------------------------

CKAN_PORTALS = [
    {
        "name": "Humanitarian Data Exchange",
        "base_url": "https://data.humdata.org",
        "api_url": "https://data.humdata.org/api/3/action/package_search",
    },
    {
        "name": "Open Canada",
        "base_url": "https://open.canada.ca",
        "api_url": "https://open.canada.ca/data/api/3/action/package_search",
    },
    {
        "name": "London Datastore",
        "base_url": "https://data.london.gov.uk",
        "api_url": "https://data.london.gov.uk/api/3/action/package_search",
    },
    {
        "name": "HealthData.gov",
        "base_url": "https://healthdata.gov",
        "api_url": "https://healthdata.gov/api/3/action/package_search",
    },
    {
        "name": "Africa Open Data",
        "base_url": "https://africaopendata.org",
        "api_url": "https://africaopendata.org/api/3/action/package_search",
    },
]

DCAT_CATALOGS = [
    {
        "name": "DOT Open Data Catalog",
        "url": "https://data.transportation.gov/data.json",
    },
    {
        "name": "NASA Open Data Catalog",
        "url": "https://data.nasa.gov/data.json",
    },
    {
        "name": "data.gov.au",
        "url": "https://data.gov.au/data.json",
    },
    {
        "name": "data.gov.uk",
        "url": "https://data.gov.uk/api/datasets?_format=json",
    },
]

SCHEMA_SEARCH_ENGINES = [
    {
        "name": "DuckDuckGo HTML",
        "url_template": "https://duckduckgo.com/html/?q={query}",
    }
]

# Socrata Discovery API — single endpoint, searches all Socrata portals globally
SOCRATA_DISCOVERY_URL = "https://api.us.socrata.com/api/catalog/v1"

# ArcGIS Hub search API
ARCGIS_HUB_SEARCH_URL = "https://hub.arcgis.com/api/search/v1/items"

# DataCite REST API
DATACITE_API_URL = "https://api.datacite.org/dois"


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _load_json(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _slug_tokens(text: str) -> list[str]:
    cleaned = re.sub(r"[^a-z0-9]+", " ", str(text).lower())
    return [token for token in cleaned.split() if len(token) > 2]


def _safe_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return " ".join(_safe_text(v) for v in value)
    if isinstance(value, dict):
        return " ".join(_safe_text(v) for v in value.values())
    return str(value)


def _extract_years(text: str) -> list[int]:
    years = []
    for match in re.findall(r"\b(19\d{2}|20\d{2}|21\d{2})\b", text):
        year = int(match)
        if 1900 <= year <= 2100:
            years.append(year)
    return sorted(set(years))


def _score_token_overlap(query_text: str, text_parts: list[str]) -> float:
    query_tokens = set(_slug_tokens(query_text))
    if not query_tokens:
        return 0.0
    haystack_tokens = set()
    for part in text_parts:
        haystack_tokens.update(_slug_tokens(part))
    return round(len(query_tokens & haystack_tokens) / len(query_tokens), 4)


def _derive_year_range(
    query_doc: dict[str, Any], signature: dict[str, Any]
) -> tuple[int | None, int | None]:
    q_year_range = query_doc.get("year_range") or []
    if len(q_year_range) == 2:
        return q_year_range[0], q_year_range[1]
    for col in signature.get("columns", []):
        if col.get("semantic_type") == "datetime":
            min_year = col.get("min")
            max_year = col.get("max")
            if isinstance(min_year, int) and isinstance(max_year, int):
                return min_year, max_year
    return None, None


def _derive_signature_path(query_path: Path, explicit_path: str | None) -> Path:
    if explicit_path:
        return Path(explicit_path)
    sibling = query_path.with_name("seed_signature.json")
    if sibling.exists():
        return sibling
    raise FileNotFoundError(
        "seed_signature.json not found next to queries.json; "
        "pass --signature explicitly"
    )


def _metadata_completeness(candidate: dict[str, Any]) -> float:
    filled = sum(
        1 for key in ("title", "description", "landing_page",
                      "source_name", "dataset_id", "download_urls")
        if candidate.get(key)
    )
    return round(filled / 6, 4)


def _score_joinability(
    candidate: dict[str, Any],
    signature: dict[str, Any],
    year_min: int | None,
    year_max: int | None,
) -> dict[str, Any]:
    join_fields = (
        signature.get("candidate_identifiers", [])
        + signature.get("entity_like_columns", [])
    )
    salient_columns = signature.get("salient_column_names", [])

    searchable_text = " ".join([
        candidate.get("title", ""),
        candidate.get("description", ""),
        " ".join(candidate.get("keywords", [])),
        " ".join(candidate.get("field_names", [])),
    ]).lower()

    matched_join_fields = [
        field for field in join_fields
        if all(t in searchable_text for t in _slug_tokens(field.replace("_", " ")))
    ]
    matched_salient_columns = [
        field for field in salient_columns
        if all(t in searchable_text for t in _slug_tokens(field.replace("_", " ")))
    ]

    years_found = _extract_years(searchable_text)
    has_year_overlap = False
    if year_min is not None and year_max is not None:
        has_year_overlap = (
            any(year_min <= y <= year_max for y in years_found)
            or "year" in searchable_text
        )

    joinability_score = 0.0
    if join_fields:
        joinability_score += min(len(matched_join_fields) / len(join_fields), 1.0) * 0.6
    if salient_columns:
        joinability_score += (
            min(len(matched_salient_columns) / max(len(salient_columns), 1), 1.0) * 0.25
        )
    if has_year_overlap:
        joinability_score += 0.15

    return {
        "joinability_score": round(joinability_score, 4),
        "matched_join_fields": matched_join_fields,
        "matched_salient_columns": matched_salient_columns,
        "years_found": years_found,
        "has_year_overlap": has_year_overlap,
    }


def _finalize_candidate(
    candidate: dict[str, Any],
    query_item: dict[str, Any],
    signature: dict[str, Any],
    year_min: int | None,
    year_max: int | None,
) -> dict[str, Any]:
    token_score = _score_token_overlap(
        query_item.get("query", ""),
        [
            candidate.get("title", ""),
            candidate.get("description", ""),
            " ".join(candidate.get("keywords", [])),
            " ".join(candidate.get("field_names", [])),
        ],
    )
    joinability = _score_joinability(candidate, signature, year_min, year_max)
    completeness_score = _metadata_completeness(candidate)
    overall_score = round(
        (0.55 * token_score)
        + (0.30 * joinability["joinability_score"])
        + (0.15 * completeness_score),
        4,
    )

    candidate["query"]                      = query_item.get("query")
    candidate["query_id"]                   = query_item.get("id")
    candidate["strategy"]                   = query_item.get("strategy")
    candidate["priority"]                   = query_item.get("priority")
    candidate["relevance_score"]            = token_score
    candidate["metadata_completeness_score"] = completeness_score
    candidate["overall_score"]              = overall_score
    candidate.update(joinability)
    return candidate


# ---------------------------------------------------------------------------
# Discovery client
# ---------------------------------------------------------------------------

class DiscoveryClient:
    def __init__(self, timeout: int = 20) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self._dcat_cache: dict[str, list[dict[str, Any]]] = {}
        self._dcat_error_cache: dict[str, str] = {}
        self._reported_dcat_errors: set[str] = set()

    def _get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def _get_text(self, url: str) -> str:
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        return response.text

    # ── CKAN ─────────────────────────────────────────────────────────────────

    def discover_ckan(
        self, query_item: dict[str, Any], limit: int
    ) -> list[dict[str, Any]]:
        candidates = []
        for portal in CKAN_PORTALS:
            try:
                payload = self._get_json(
                    portal["api_url"],
                    params={"q": query_item["query"], "rows": limit},
                )
                results = payload.get("result", {}).get("results", [])
            except Exception as exc:
                candidates.append({
                    "connector":       "CKAN",
                    "source_name":     portal["name"],
                    "source_url":      portal["base_url"],
                    "discovery_error": str(exc),
                    "query":           query_item.get("query"),
                    "query_id":        query_item.get("id"),
                    "is_error":        True,
                })
                continue

            for item in results[:limit]:
                resources   = item.get("resources") or []
                field_names = [
                    field.get("name")
                    for resource in resources
                    for field in resource.get("schema", {}).get("fields", [])
                    if field.get("name")
                ]
                tags = [
                    tag.get("display_name") if isinstance(tag, dict) else str(tag)
                    for tag in _safe_list(item.get("tags"))
                    if tag
                ]
                dataset_name = item.get("name", "")
                landing_page = (
                    urljoin(portal["base_url"], f"/dataset/{dataset_name}")
                    if dataset_name else portal["base_url"]
                )
                candidates.append({
                    "connector":    "CKAN",
                    "source_name":  portal["name"],
                    "source_url":   portal["base_url"],
                    "dataset_id":   item.get("id") or dataset_name,
                    "title":        item.get("title") or dataset_name,
                    "description":  item.get("notes") or "",
                    "landing_page": landing_page,
                    "keywords":     tags,
                    "download_urls": [
                        r.get("url") for r in resources if r.get("url")
                    ],
                    "field_names":  field_names,
                    "raw_metadata": {
                        "organization":      _safe_text((item.get("organization") or {}).get("title")),
                        "metadata_modified": item.get("metadata_modified"),
                        "license_title":     item.get("license_title"),
                    },
                })
        return candidates

    # ── DCAT ─────────────────────────────────────────────────────────────────

    def _load_dcat_catalog(self, catalog: dict[str, str]) -> list[dict[str, Any]]:
        if catalog["url"] in self._dcat_error_cache:
            raise RuntimeError(self._dcat_error_cache[catalog["url"]])
        if catalog["url"] in self._dcat_cache:
            return self._dcat_cache[catalog["url"]]
        try:
            payload = self._get_json(catalog["url"])
        except Exception as exc:
            self._dcat_error_cache[catalog["url"]] = str(exc)
            raise
        datasets = payload.get("dataset") or payload.get("datasets") or []
        if not isinstance(datasets, list):
            datasets = []
        self._dcat_cache[catalog["url"]] = datasets
        return datasets

    def discover_dcat(
        self, query_item: dict[str, Any], limit: int
    ) -> list[dict[str, Any]]:
        ranked = []
        for catalog in DCAT_CATALOGS:
            try:
                datasets = self._load_dcat_catalog(catalog)
            except Exception as exc:
                if catalog["url"] not in self._reported_dcat_errors:
                    ranked.append({
                        "connector":       "DCAT",
                        "source_name":     catalog["name"],
                        "source_url":      catalog["url"],
                        "discovery_error": self._dcat_error_cache.get(
                            catalog["url"], str(exc)
                        ),
                        "query":    query_item.get("query"),
                        "query_id": query_item.get("id"),
                        "is_error": True,
                    })
                    self._reported_dcat_errors.add(catalog["url"])
                continue

            for item in datasets:
                title       = _safe_text(item.get("title"))
                description = _safe_text(item.get("description"))
                keywords    = [str(k) for k in _safe_list(item.get("keyword")) if k]
                field_names = [
                    field.get("label") or field.get("name")
                    for field in _safe_list(item.get("dataDictionary"))
                    if isinstance(field, dict)
                    and (field.get("label") or field.get("name"))
                ]
                score = _score_token_overlap(
                    query_item["query"], [title, description, " ".join(keywords)]
                )
                if score <= 0:
                    continue
                ranked.append({
                    "connector":    "DCAT",
                    "source_name":  catalog["name"],
                    "source_url":   catalog["url"],
                    "dataset_id":   item.get("identifier") or item.get("@id") or title,
                    "title":        title,
                    "description":  description,
                    "landing_page": (
                        item.get("landingPage")
                        or item.get("accessURL")
                        or item.get("@id")
                    ),
                    "keywords":     keywords,
                    "download_urls": [
                        dist.get("downloadURL") or dist.get("accessURL")
                        for dist in _safe_list(item.get("distribution"))
                        if isinstance(dist, dict)
                        and (dist.get("downloadURL") or dist.get("accessURL"))
                    ],
                    "field_names":  field_names,
                    "raw_metadata": {
                        "publisher": _safe_text(item.get("publisher")),
                        "theme":     _safe_text(item.get("theme")),
                        "issued":    item.get("issued"),
                        "modified":  item.get("modified"),
                    },
                    "_catalog_score": score,
                })

        ranked.sort(key=lambda item: item.get("_catalog_score", 0), reverse=True)
        return ranked[:limit]

    # ── Schema.org ────────────────────────────────────────────────────────────

    def _extract_schema_dataset(
        self, page_url: str, html: str
    ) -> dict[str, Any] | None:
        soup    = BeautifulSoup(html, "html.parser")
        scripts = soup.find_all("script", attrs={"type": "application/ld+json"})
        for script in scripts:
            raw_json = script.string or script.get_text(strip=True)
            if not raw_json:
                continue
            try:
                payload = json.loads(raw_json)
            except json.JSONDecodeError:
                continue

            dataset = self._find_dataset_node(payload)
            if not dataset:
                continue

            distributions = _safe_list(dataset.get("distribution"))
            downloads = [
                dist.get("contentUrl") or dist.get("downloadUrl")
                for dist in distributions
                if isinstance(dist, dict)
                and (dist.get("contentUrl") or dist.get("downloadUrl"))
            ]

            keywords = dataset.get("keywords")
            if isinstance(keywords, str):
                keywords = [p.strip() for p in keywords.split(",") if p.strip()]

            title = dataset.get("name")
            if not title and soup.title:
                title = soup.title.string

            return {
                "connector":    "Schema.org",
                "source_name":  "Schema.org Dataset Page",
                "source_url":   page_url,
                "dataset_id":   dataset.get("@id") or dataset.get("identifier") or title,
                "title":        title or page_url,
                "description":  _safe_text(dataset.get("description")),
                "landing_page": dataset.get("url") or page_url,
                "keywords":     [str(k) for k in _safe_list(keywords) if k],
                "download_urls": downloads,
                "field_names":  [],
                "raw_metadata": {
                    "creator":           _safe_text(dataset.get("creator")),
                    "publisher":         _safe_text(dataset.get("publisher")),
                    "temporal_coverage": _safe_text(dataset.get("temporalCoverage")),
                    "spatial_coverage":  _safe_text(dataset.get("spatialCoverage")),
                },
            }
        return None

    def _find_dataset_node(self, payload: Any) -> dict[str, Any] | None:
        if isinstance(payload, dict):
            if "dataset" in _safe_text(payload.get("@type")).lower():
                return payload
            for key in ("@graph", "mainEntity", "itemListElement"):
                found = self._find_dataset_node(payload.get(key))
                if found:
                    return found
        elif isinstance(payload, list):
            for item in payload:
                found = self._find_dataset_node(item)
                if found:
                    return found
        return None

    def discover_schema_org(
        self, query_item: dict[str, Any], limit: int
    ) -> list[dict[str, Any]]:
        candidates  = []
        search_query = f'{query_item["query"]} "Dataset" "schema.org"'
        encoded      = quote_plus(search_query)

        for engine in SCHEMA_SEARCH_ENGINES:
            try:
                search_html = self._get_text(
                    engine["url_template"].format(query=encoded)
                )
            except Exception as exc:
                candidates.append({
                    "connector":       "Schema.org",
                    "source_name":     engine["name"],
                    "source_url":      engine["url_template"],
                    "discovery_error": str(exc),
                    "query":           query_item.get("query"),
                    "query_id":        query_item.get("id"),
                    "is_error":        True,
                })
                continue

            soup      = BeautifulSoup(search_html, "html.parser")
            seen_urls: set[str] = set()
            page_urls: list[str] = []

            for anchor in soup.select("a[href]"):
                href = anchor.get("href", "")
                if not isinstance(href, str) or not href.startswith("http"):
                    continue
                if href in seen_urls:
                    continue
                seen_urls.add(href)
                page_urls.append(href)
                if len(page_urls) >= limit * 2:
                    break

            for page_url in page_urls:
                try:
                    page_html = self._get_text(page_url)
                    candidate = self._extract_schema_dataset(page_url, page_html)
                except Exception:
                    candidate = None
                if candidate:
                    candidates.append(candidate)
                if len(candidates) >= limit:
                    break
            if len(candidates) >= limit:
                break

        return candidates[:limit]

    # ── Socrata ───────────────────────────────────────────────────────────────

    def discover_socrata(
        self, query_item: dict[str, Any], limit: int
    ) -> list[dict[str, Any]]:
        """
        Socrata Discovery API: searches all Socrata portals globally.
        Returns datasets with CSV download URLs built from the 4x4 asset ID.
        """
        candidates = []
        try:
            payload = self._get_json(
                SOCRATA_DISCOVERY_URL,
                params={
                    "q":         query_item["query"],
                    "limit":     limit,
                    "only":      "datasets",
                },
            )
            results = payload.get("results", [])
        except Exception as exc:
            return [{
                "connector":       "Socrata",
                "source_name":     "Socrata Discovery API",
                "source_url":      SOCRATA_DISCOVERY_URL,
                "discovery_error": str(exc),
                "query":           query_item.get("query"),
                "query_id":        query_item.get("id"),
                "is_error":        True,
            }]

        for item in results[:limit]:
            resource    = item.get("resource", {})
            metadata    = item.get("metadata", {})
            permalink   = item.get("permalink", "")
            domain      = item.get("metadata", {}).get("domain", "")
            asset_id    = resource.get("id", "")

            # Build a direct CSV export URL — Socrata exposes this for every dataset
            download_urls = []
            if domain and asset_id:
                download_urls.append(
                    f"https://{domain}/api/views/{asset_id}/rows.csv?accessType=DOWNLOAD"
                )

            columns = resource.get("columns_name") or []

            candidates.append({
                "connector":    "Socrata",
                "source_name":  f"Socrata ({domain})" if domain else "Socrata",
                "source_url":   f"https://{domain}" if domain else SOCRATA_DISCOVERY_URL,
                "dataset_id":   asset_id or permalink,
                "title":        resource.get("name", ""),
                "description":  resource.get("description", ""),
                "landing_page": permalink,
                "keywords":     _safe_list(metadata.get("domain_tags")),
                "download_urls": download_urls,
                "field_names":  columns,
                "raw_metadata": {
                    "domain":        domain,
                    "updatedAt":     resource.get("updatedAt"),
                    "createdAt":     resource.get("createdAt"),
                    "attribution":   resource.get("attribution"),
                    "license":       metadata.get("license"),
                },
            })

        return candidates

    # ── ArcGIS Hub ────────────────────────────────────────────────────────────

    def discover_arcgis_hub(
        self, query_item: dict[str, Any], limit: int
    ) -> list[dict[str, Any]]:
        """
        ArcGIS Hub search API: searches open datasets published on ArcGIS Hub.
        Focuses on CSV and GeoJSON distributions.
        """
        candidates = []
        try:
            payload = self._get_json(
                ARCGIS_HUB_SEARCH_URL,
                params={
                    "q":     query_item["query"],
                    "num":   limit,
                    "types": "Feature Service,CSV,File Geodatabase",
                    "categories": "/Categories/Datasets",
                },
            )
            results = payload.get("results", [])
        except Exception as exc:
            return [{
                "connector":       "ArcGIS",
                "source_name":     "ArcGIS Hub",
                "source_url":      ARCGIS_HUB_SEARCH_URL,
                "discovery_error": str(exc),
                "query":           query_item.get("query"),
                "query_id":        query_item.get("id"),
                "is_error":        True,
            }]

        for item in results[:limit]:
            item_id      = item.get("id", "")
            owner        = item.get("owner", "")
            landing_page = (
                f"https://hub.arcgis.com/datasets/{item_id}"
                if item_id else ""
            )

            # Build CSV download URL from ArcGIS Hub convention
            download_urls = []
            if item_id:
                download_urls.append(
                    f"https://hub.arcgis.com/api/download/v1/items/{item_id}"
                    f"/csv?redirect=true&layers=0"
                )

            tags = _safe_list(item.get("tags") or item.get("typeKeywords"))

            candidates.append({
                "connector":    "ArcGIS",
                "source_name":  "ArcGIS Hub",
                "source_url":   ARCGIS_HUB_SEARCH_URL,
                "dataset_id":   item_id,
                "title":        item.get("title", ""),
                "description":  item.get("snippet") or item.get("description") or "",
                "landing_page": landing_page,
                "keywords":     [str(t) for t in tags if t],
                "download_urls": download_urls,
                "field_names":  [],
                "raw_metadata": {
                    "owner":    owner,
                    "type":     item.get("type"),
                    "modified": item.get("modified"),
                    "access":   item.get("access"),
                    "licenseInfo": item.get("licenseInfo", ""),
                },
            })

        return candidates

    # ── DataCite ──────────────────────────────────────────────────────────────

    def discover_datacite(
        self, query_item: dict[str, Any], limit: int
    ) -> list[dict[str, Any]]:
        """
        DataCite REST API: searches DOI-registered research datasets.
        Filters to resourceTypeGeneral=Dataset.
        """
        candidates = []
        try:
            payload = self._get_json(
                DATACITE_API_URL,
                params={
                    "query":                   query_item["query"],
                    "resource-type-id":        "dataset",
                    "page[size]":              limit,
                    "sort":                    "relevance",
                },
            )
            items = payload.get("data", [])
        except Exception as exc:
            return [{
                "connector":       "DataCite",
                "source_name":     "DataCite REST API",
                "source_url":      DATACITE_API_URL,
                "discovery_error": str(exc),
                "query":           query_item.get("query"),
                "query_id":        query_item.get("id"),
                "is_error":        True,
            }]

        for item in items[:limit]:
            attrs       = item.get("attributes", {})
            doi         = attrs.get("doi", "")
            landing_url = f"https://doi.org/{doi}" if doi else ""

            # Extract download URLs from relatedIdentifiers of type HasPart/IsPartOf
            # and from contentUrl if present
            download_urls = []
            content_url = attrs.get("contentUrl")
            if content_url:
                if isinstance(content_url, list):
                    download_urls.extend(content_url)
                else:
                    download_urls.append(content_url)

            # Subjects as keywords
            subjects = [
                s.get("subject", "") for s in _safe_list(attrs.get("subjects"))
                if isinstance(s, dict) and s.get("subject")
            ]

            # Titles
            titles = attrs.get("titles") or []
            title  = titles[0].get("title", "") if titles else doi

            # Descriptions
            descriptions = attrs.get("descriptions") or []
            description  = (
                descriptions[0].get("description", "")
                if descriptions else ""
            )

            # Publisher
            publisher = _safe_text(attrs.get("publisher"))

            candidates.append({
                "connector":    "DataCite",
                "source_name":  "DataCite",
                "source_url":   DATACITE_API_URL,
                "dataset_id":   doi,
                "title":        title,
                "description":  description,
                "landing_page": landing_url,
                "keywords":     subjects,
                "download_urls": download_urls,
                "field_names":  [],
                "raw_metadata": {
                    "doi":          doi,
                    "publisher":    publisher,
                    "publicationYear": attrs.get("publicationYear"),
                    "version":      attrs.get("version"),
                    "license":      _safe_text(attrs.get("rightsList")),
                },
            })

        return candidates


# ---------------------------------------------------------------------------
# Main discover() function
# ---------------------------------------------------------------------------

def discover(
    query_path: str,
    output_path: str,
    signature_path: str | None = None,
    max_queries: int = 30,
    per_query_limit: int = 5,
    timeout: int = 20,
    dry_run: bool = False,
) -> dict[str, Any]:
    query_path_obj          = Path(query_path)
    derived_signature_path  = _derive_signature_path(query_path_obj, signature_path)
    signature               = _load_json(derived_signature_path)
    query_doc               = _load_json(query_path_obj)
    year_min, year_max      = _derive_year_range(query_doc, signature)

    query_items             = query_doc.get("queries", [])[:max_queries]
    connector_counts: dict[str, int] = defaultdict(int)
    results: list[dict[str, Any]]    = []
    run_plan: list[dict[str, Any]]   = []

    client = DiscoveryClient(timeout=timeout)

    for query_item in query_items:
        connector = query_item.get("target_connector")
        connector_counts[connector] += 1

        if dry_run:
            run_plan.append({
                "query_id": query_item.get("id"),
                "query":    query_item.get("query"),
                "connector": connector,
                "strategy": query_item.get("strategy"),
            })
            continue

        if connector == "CKAN":
            discovered = client.discover_ckan(query_item, per_query_limit)
        elif connector == "DCAT":
            discovered = client.discover_dcat(query_item, per_query_limit)
        elif connector == "Schema.org":
            discovered = client.discover_schema_org(query_item, per_query_limit)
        elif connector == "Socrata":
            discovered = client.discover_socrata(query_item, per_query_limit)
        elif connector == "ArcGIS":
            discovered = client.discover_arcgis_hub(query_item, per_query_limit)
        elif connector == "DataCite":
            discovered = client.discover_datacite(query_item, per_query_limit)
        else:
            discovered = []

        for candidate in discovered:
            if candidate.get("is_error"):
                results.append(candidate)
                continue
            results.append(
                _finalize_candidate(candidate, query_item, signature, year_min, year_max)
            )

    non_error_results = [r for r in results if not r.get("is_error")]
    non_error_results.sort(
        key=lambda r: r.get("overall_score", 0), reverse=True
    )

    output = {
        "generated_at":      _utc_now(),
        "seed_dataset":      query_doc.get("seed_dataset"),
        "query_source":      str(query_path_obj),
        "signature_source":  str(derived_signature_path),
        "dry_run":           dry_run,
        "processed_queries": len(query_items),
        "connector_counts":  dict(connector_counts),
        "year_range":        [year_min, year_max],
        "target_connectors": query_doc.get("target_connectors", []),
        "total_candidates":  len(non_error_results),
        "total_errors":      len([r for r in results if r.get("is_error")]),
        "run_plan":          run_plan,
        "results":           non_error_results,
        "errors":            [r for r in results if r.get("is_error")],
    }

    save_results(output, output_path)
    return output


def save_results(output: dict[str, Any], output_path: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"[web_discovery] Results written to {output_path}")


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Stage 3 Web Discovery")
    parser.add_argument("--input",           default="output/queries.json",
                        help="Path to queries.json")
    parser.add_argument("--output",          default="output/discovery_results.json",
                        help="Output JSON path")
    parser.add_argument("--signature",       default=None,
                        help="Optional explicit path to seed_signature.json")
    parser.add_argument("--max-queries",     type=int, default=30,
                        help="Maximum queries to process")
    parser.add_argument("--per-query-limit", type=int, default=5,
                        help="Max candidate datasets per query")
    parser.add_argument("--timeout",         type=int, default=20,
                        help="HTTP timeout in seconds")
    parser.add_argument("--dry-run",         action="store_true",
                        help="Write the execution plan without making HTTP requests")
    args = parser.parse_args()

    print(f"[web_discovery] Reading queries: {args.input}")
    output = discover(
        query_path       = args.input,
        output_path      = args.output,
        signature_path   = args.signature,
        max_queries      = args.max_queries,
        per_query_limit  = args.per_query_limit,
        timeout          = args.timeout,
        dry_run          = args.dry_run,
    )
    print(
        "[web_discovery] Done. "
        f"{output['processed_queries']} queries processed, "
        f"{output['total_candidates']} candidates discovered, "
        f"{output['total_errors']} errors."
    )


if __name__ == "__main__":
    _cli()
