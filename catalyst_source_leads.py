"""Extract frozen outbound source leads from secondary catalyst content."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import sys
from collections import Counter
from collections.abc import Mapping
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse

from historical_store import HistoricalDayStore
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-catalyst-source-leads-2026-07-19-expansion-v1"
SOURCE_DATASET_ID = "dataset-catalyst-news-enrichment-2026-07-19-expansion-v1"
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_news_enrichment"
    / "manifests"
    / (
        "dataset-catalyst-news-enrichment-2026-07-19-expansion-v1-"
        "154e5831aba1912ddaf3bd66af0ea2d5cc26f5a211c7a6292d6ffc88514fff2e.json"
    )
)
SOURCE_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-news-enrichment.json"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches" / "catalyst_source_leads" / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT / "historical_batches" / "catalyst_source_leads" / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-source-leads.json"
)

SECONDARY_HOSTS = frozenset(
    {
        "www.bloomberg.com",
        "www.cnbc.com",
        "www.wsj.com",
        "www.ft.com",
        "www.nytimes.com",
        "www.axios.com",
        "www.businessinsider.com",
        "www.reuters.com",
        "reuters.com",
    }
)
WIRE_HOST_SUFFIXES = (
    "prnewswire.com",
    "businesswire.com",
    "globenewswire.com",
    "accesswire.com",
)
EXCHANGE_HOST_SUFFIXES = (
    "nasdaqtrader.com",
    "nasdaq.com",
    "nyse.com",
    "cboe.com",
)
REJECTED_HOST_SUFFIXES = (
    "benzinga.com",
    "twitter.com",
    "x.com",
    "t.co",
    "youtube.com",
    "youtu.be",
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "myclickfunnels.com",
)
PRIMARY_LEAD_CATEGORIES = frozenset(
    {"AUTHORITY_CANDIDATE", "EXCHANGE_CANDIDATE", "ISSUER_HOST_CANDIDATE"}
)


class CatalystSourceLeadError(RuntimeError):
    """The frozen source-lead derivation contract is invalid."""


class _LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self.hrefs.append(href)


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CatalystSourceLeadError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CatalystSourceLeadError(f"{path} must contain an object")
    return value


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise CatalystSourceLeadError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CatalystSourceLeadError(f"{path} must contain an object")
    return value


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write_gzip(path: Path, value: Any) -> None:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as target:
        target.write(json.dumps(value, indent=2, sort_keys=True).encode())
        target.write(b"\n")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(buffer.getvalue())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise CatalystSourceLeadError(
            f"public path must be repository-relative: {path}"
        ) from exc


def _private_root(store_root: Path) -> Path:
    return store_root / "_derived" / "catalyst_source_leads" / DATASET_ID


def _source_root(store_root: Path) -> Path:
    return store_root / "_derived" / "catalyst_news_enrichment" / SOURCE_DATASET_ID


def _source_paths(store_root: Path) -> dict[str, Path]:
    source = _source_root(store_root)
    return {
        "discovery": source / "discovery-index.json.gz",
        "content": source / "content-index.json.gz",
    }


def _private_result_path(store_root: Path) -> Path:
    return _private_root(store_root) / "source-lead-index.json.gz"


def _host_matches(host: str, suffixes: tuple[str, ...]) -> bool:
    return any(host == suffix or host.endswith(f".{suffix}") for suffix in suffixes)


def normalize_url(raw: str) -> str | None:
    value = str(raw).strip()
    if not value:
        return None
    parsed = urlparse(value)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return None
    host = (parsed.hostname or "").lower().rstrip(".")
    if not host:
        return None
    port = parsed.port
    netloc = host if port is None else f"{host}:{port}"
    return urlunparse(
        (parsed.scheme.lower(), netloc, parsed.path or "/", "", parsed.query, "")
    )


def classify_url(url: str) -> str:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    path = parsed.path.lower()
    if _host_matches(host, REJECTED_HOST_SUFFIXES):
        return "REJECTED_PLATFORM_OR_SOURCE"
    if host.endswith(".gov") or host == "gov":
        return "AUTHORITY_CANDIDATE"
    if _host_matches(host, EXCHANGE_HOST_SUFFIXES):
        return "EXCHANGE_CANDIDATE"
    if _host_matches(host, WIRE_HOST_SUFFIXES):
        return "WIRE_CANDIDATE"
    if host in SECONDARY_HOSTS:
        return "SECONDARY_CORROBORATION_CANDIDATE"
    if (
        host.startswith(("ir.", "investor.", "investors."))
        or "/investor" in path
        or host.endswith(("q4inc.com", "q4cdn.com"))
    ):
        return "ISSUER_HOST_CANDIDATE"
    return "OTHER_EXTERNAL"


def _unique_content_articles(content: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    unique: dict[str, dict[str, Any]] = {}
    for date_record in content.get("date_records", {}).values():
        for key, record in date_record.get("articles", {}).items():
            if record.get("content_available") is not True:
                continue
            current = unique.get(key)
            if current is not None and current.get("content_sha256") != record.get(
                "content_sha256"
            ):
                raise CatalystSourceLeadError("source content drifts across dates")
            unique[key] = dict(record)
    return unique


def extract_source_leads(
    discovery: Mapping[str, Any], content: Mapping[str, Any]
) -> dict[str, Any]:
    unique = _unique_content_articles(content)
    records: dict[str, dict[str, Any]] = {}
    category_counts: Counter[str] = Counter()
    unique_urls: dict[str, str] = {}
    for key, record in sorted(unique.items()):
        article = record.get("article") or {}
        body = str(article.get("content") or "")
        parser = _LinkParser()
        parser.feed(body)
        leads: list[dict[str, str]] = []
        seen: set[str] = set()
        invalid = 0
        for raw in parser.hrefs:
            normalized = normalize_url(raw)
            if normalized is None:
                invalid += 1
                continue
            if normalized in seen:
                continue
            seen.add(normalized)
            category = classify_url(normalized)
            category_counts[category] += 1
            unique_urls.setdefault(normalized, category)
            leads.append(
                {
                    "url": normalized,
                    "host": (urlparse(normalized).hostname or "").lower(),
                    "category": category,
                }
            )
        records[key] = {
            "content_sha256": record.get("content_sha256"),
            "links_observed": len(parser.hrefs),
            "invalid_or_non_http_links": invalid,
            "leads": leads,
        }
    article_primary = {
        key
        for key, record in records.items()
        if any(lead["category"] in PRIMARY_LEAD_CATEGORIES for lead in record["leads"])
    }
    article_corroboration = {
        key
        for key, record in records.items()
        if any(
            lead["category"]
            in {"WIRE_CANDIDATE", "SECONDARY_CORROBORATION_CANDIDATE"}
            for lead in record["leads"]
        )
    }
    pairs_with_primary = 0
    pairs_with_corroboration = 0
    for pair in discovery["pair_records"]:
        keys = set(pair["article_keys"])
        pairs_with_primary += bool(keys & article_primary)
        pairs_with_corroboration += bool(keys & article_corroboration)
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "source_dataset_id": SOURCE_DATASET_ID,
        "status": "DERIVATION_COMPLETE",
        "article_records": records,
        "counts": {
            "content_articles": len(unique),
            "articles_with_primary_lead": len(article_primary),
            "articles_with_corroboration_lead": len(article_corroboration),
            "unique_normalized_urls": len(unique_urls),
            "pairs_with_primary_lead": pairs_with_primary,
            "pairs_with_corroboration_lead": pairs_with_corroboration,
            "category_link_counts": dict(sorted(category_counts.items())),
        },
        "classification_boundary": (
            "URL-routing leads only; host/path heuristics do not establish source "
            "ownership, issuer binding, causality, direction, or corroboration."
        ),
        "target_outcomes_observed_or_derived": False,
        "primary_catalyst_verified": False,
    }


def freeze_inputs(*, env_path: Path, output_root: Path) -> tuple[Path, dict[str, Any]]:
    store = HistoricalDayStore.from_env(env_path)
    source_manifest = load_frozen_dataset_contract(SOURCE_MANIFEST)
    source_result = _read_json(SOURCE_RESULT)
    if source_manifest.get("dataset_id") != SOURCE_DATASET_ID:
        raise CatalystSourceLeadError("unexpected content source")
    if source_result.get("status") != "READY" or source_result.get("inspected") is not True:
        raise CatalystSourceLeadError("content source is not inspected READY")
    paths = _source_paths(store.root)
    discovery = _read_gzip(paths["discovery"])
    content = _read_gzip(paths["content"])
    if discovery.get("unique_article_count") != 7_391:
        raise CatalystSourceLeadError("discovery source count changed")
    if len(_unique_content_articles(content)) != 4_205:
        raise CatalystSourceLeadError("content source count changed")
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": datetime_now(),
        "requested_dates": list(source_manifest["requested_dates"]),
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(SOURCE_MANIFEST),
                _repo_path(SOURCE_RESULT),
                "CATALYST_EVIDENCE_ACQUISITION.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            "source_dataset_id": SOURCE_DATASET_ID,
            "source_manifest_sha256": source_manifest["manifest_sha256"],
            "discovery_sha256": _sha256_file(paths["discovery"]),
            "content_sha256": _sha256_file(paths["content"]),
            "content_article_count": 4_205,
            "pair_count": 1_987,
            "symbols_articles_urls_and_rows_public": False,
        },
        "derivation_contract": {
            "extractor_sha256": _sha256_file(Path(__file__)),
            "allowed_schemes": ["http", "https"],
            "fragment_removed": True,
            "query_retained": True,
            "rejected_host_suffixes": list(REJECTED_HOST_SUFFIXES),
            "wire_host_suffixes": list(WIRE_HOST_SUFFIXES),
            "exchange_host_suffixes": list(EXCHANGE_HOST_SUFFIXES),
            "secondary_hosts": sorted(SECONDARY_HOSTS),
            "primary_lead_categories": sorted(PRIMARY_LEAD_CATEGORIES),
            "host_category_is_verification": False,
            "network_collection_allowed": False,
            "target_outcomes_observed_or_derived": False,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def datetime_now() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat()


def _verify_contract(
    manifest_path: Path, store: HistoricalDayStore
) -> tuple[dict[str, Any], dict[str, Path]]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise CatalystSourceLeadError("unexpected source-lead dataset")
    paths = _source_paths(store.root)
    contract = manifest["selection_contract"]
    if _sha256_file(paths["discovery"]) != contract.get("discovery_sha256"):
        raise CatalystSourceLeadError("discovery source changed")
    if _sha256_file(paths["content"]) != contract.get("content_sha256"):
        raise CatalystSourceLeadError("content source changed")
    if _sha256_file(Path(__file__)) != manifest["derivation_contract"].get(
        "extractor_sha256"
    ):
        raise CatalystSourceLeadError("extractor differs from frozen contract")
    return manifest, paths


def derive(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, paths = _verify_contract(manifest_path, store)
    private = extract_source_leads(
        _read_gzip(paths["discovery"]), _read_gzip(paths["content"])
    )
    private["manifest_sha256"] = manifest["manifest_sha256"]
    private_path = _private_result_path(store.root)
    _write_gzip(private_path, private)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "DERIVATION_COMPLETE",
        "counts": private["counts"],
        "private_result_sha256": _sha256_file(private_path),
        "symbols_articles_urls_and_rows_public": False,
        "primary_catalyst_verified": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(public_status_path, public)
    return public


def inspect(
    *, manifest_path: Path, env_path: Path, public_result_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, paths = _verify_contract(manifest_path, store)
    private_path = _private_result_path(store.root)
    private = _read_gzip(private_path)
    rebuilt = extract_source_leads(
        _read_gzip(paths["discovery"]), _read_gzip(paths["content"])
    )
    if not (
        private.get("manifest_sha256") == manifest["manifest_sha256"]
        and private.get("status") == "DERIVATION_COMPLETE"
        and private.get("counts") == rebuilt.get("counts")
        and private.get("target_outcomes_observed_or_derived") is False
        and private.get("primary_catalyst_verified") is False
    ):
        raise CatalystSourceLeadError("source-lead result failed inspection")
    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "counts": private["counts"],
        "private_result_sha256": _sha256_file(private_path),
        "findings": {
            "url_routing_leads_complete": True,
            "source_ownership_verified": False,
            "issuer_binding_verified": False,
            "causal_availability_verified": False,
            "primary_catalyst_verified": False,
            "independent_corroboration_complete": False,
            "production_rule_change_earned": False,
        },
        "next_required_stage": (
            "freeze network acquisition for candidate URLs, then verify ownership, "
            "issuer binding, causal timestamps, direction, conflicts, and corroboration"
        ),
        "claim_boundary": private["classification_boundary"],
        "symbols_articles_urls_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(public_result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "derive", "inspect"))
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--public-status", type=Path, default=DEFAULT_PUBLIC_STATUS)
    parser.add_argument("--public-result", type=Path, default=DEFAULT_PUBLIC_RESULT)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "freeze":
            path, manifest = freeze_inputs(
                env_path=args.env_file, output_root=args.output_root
            )
            output = {"manifest": _repo_path(path), **manifest}
        elif args.manifest is None:
            raise CatalystSourceLeadError("--manifest is required")
        elif args.command == "derive":
            output = derive(
                manifest_path=args.manifest,
                env_path=args.env_file,
                public_status_path=args.public_status,
            )
        else:
            output = inspect(
                manifest_path=args.manifest,
                env_path=args.env_file,
                public_result_path=args.public_result,
            )
    except (CatalystSourceLeadError, LearningDataError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
