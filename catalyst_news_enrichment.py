"""Enrich the frozen expansion's secondary news leads without reading outcomes."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import shutil
import sys
import time
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time as wall_time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from historical_providers import (
    AlpacaConfig,
    AlpacaHistoricalClient,
    HistoricalProviderError,
)
from historical_store import HistoricalDayStore
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)
from selected_candidate_join import NEWS_LOOKBACK_DAYS


PROJECT_ROOT = Path(__file__).resolve().parent
EASTERN = ZoneInfo("America/New_York")
DATASET_ID = "dataset-catalyst-news-enrichment-2026-07-19-expansion-v1"
SOURCE_DATASET_ID = "dataset-selected-candidate-join-2026-07-19-expansion-v1"
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "selected_candidate_join_expansion"
    / "manifests"
    / (
        "dataset-selected-candidate-join-2026-07-19-expansion-v1-"
        "15d8baefcd4bedb3cf473cad4b98638a0c101ae2a1bfc64552d80c72dede2f6b.json"
    )
)
SOURCE_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-selected-candidate-join-expansion.json"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches" / "catalyst_news_enrichment" / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT / "historical_batches" / "catalyst_news_enrichment" / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-news-enrichment.json"
)
EXPECTED_PAIRS = 1_987
EXPECTED_DATES = 100
MINIMUM_FREE_BYTES = 10 * 1024**3


class CatalystNewsEnrichmentError(RuntimeError):
    """The frozen secondary-news enrichment contract cannot be honored."""

    def __init__(self, message: str, *, category: str = "fidelity"):
        super().__init__(message)
        self.category = category


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
        raise CatalystNewsEnrichmentError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CatalystNewsEnrichmentError(f"{path} must contain an object")
    return value


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise CatalystNewsEnrichmentError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CatalystNewsEnrichmentError(f"{path} must contain an object")
    return value


def _gzip_bytes(value: Any) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as target:
        target.write(json.dumps(value, indent=2, sort_keys=True).encode())
        target.write(b"\n")
    return buffer.getvalue()


def _write_gzip(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(_gzip_bytes(value))
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


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


def _timestamp_now() -> str:
    return datetime.now(UTC).isoformat()


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise CatalystNewsEnrichmentError(
            f"public path must be repository-relative: {path}"
        ) from exc


def _private_root(store_root: Path) -> Path:
    return store_root / "_derived" / "catalyst_news_enrichment" / DATASET_ID


def _selection_path(store_root: Path) -> Path:
    return (
        store_root
        / "_derived"
        / "selected_candidate_join"
        / SOURCE_DATASET_ID
        / "selected-pairs.json.gz"
    )


def _discovery_path(store_root: Path) -> Path:
    return _private_root(store_root) / "discovery-index.json.gz"


def _content_path(store_root: Path) -> Path:
    return _private_root(store_root) / "content-index.json.gz"


def _article_key(article: Mapping[str, Any]) -> str:
    article_id = str(article.get("article_id") or "").strip()
    created_at = str(article.get("created_at") or "").strip()
    if not article_id or not created_at:
        raise CatalystNewsEnrichmentError("news article lacks stable ID or created_at")
    return f"{article_id}|{created_at}"


def _news_context(
    store: HistoricalDayStore, symbol: str, day: str
) -> Mapping[str, Any]:
    document = store.load(symbol, day)
    if document is None:
        raise CatalystNewsEnrichmentError("canonical pair document is missing")
    matches = [
        context
        for context in document.get("contexts", [])
        if context.get("kind") == "selected_candidate_catalyst_candidates"
        and context.get("payload", {}).get("dataset_id") == SOURCE_DATASET_ID
    ]
    if not matches:
        raise CatalystNewsEnrichmentError("pair lacks frozen discovery context")
    return matches[-1]


def build_discovery_index(
    store: HistoricalDayStore, selection: Mapping[str, Any]
) -> dict[str, Any]:
    pairs = selection.get("selected_pairs")
    if not isinstance(pairs, list) or len(pairs) != EXPECTED_PAIRS:
        raise CatalystNewsEnrichmentError("selection is not the exact 1,987 pairs")
    articles: dict[str, dict[str, Any]] = {}
    pair_records: list[dict[str, Any]] = []
    requested_dates: set[str] = set()
    for pair in pairs:
        day = str(pair.get("date") or "")
        symbol = str(pair.get("symbol") or "")
        context = _news_context(store, symbol, day)
        payload = context.get("payload", {})
        rows = payload.get("articles", [])
        if not isinstance(rows, list):
            raise CatalystNewsEnrichmentError("discovery articles are malformed")
        keys: list[str] = []
        for raw in rows:
            if not isinstance(raw, Mapping):
                raise CatalystNewsEnrichmentError("discovery article is malformed")
            article = dict(raw)
            key = _article_key(article)
            existing = articles.get(key)
            if existing is not None and _sha256_json(existing) != _sha256_json(article):
                raise CatalystNewsEnrichmentError("article metadata conflicts by stable ID")
            articles[key] = article
            keys.append(key)
        pair_records.append(
            {
                "date": day,
                "symbol": symbol,
                "article_keys": sorted(set(keys)),
            }
        )
        requested_dates.add(day)
    if len(requested_dates) != EXPECTED_DATES:
        raise CatalystNewsEnrichmentError("discovery corpus is not exactly 100 dates")
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "source_dataset_id": SOURCE_DATASET_ID,
        "created_at": _timestamp_now(),
        "pair_count": len(pair_records),
        "date_count": len(requested_dates),
        "unique_article_count": len(articles),
        "pair_records": pair_records,
        "articles": articles,
        "target_outcomes_observed_or_derived": False,
    }


def freeze_inputs(*, env_path: Path, output_root: Path) -> tuple[Path, dict[str, Any]]:
    store = HistoricalDayStore.from_env(env_path)
    source_manifest = load_frozen_dataset_contract(SOURCE_MANIFEST)
    source_result = _read_json(SOURCE_RESULT)
    if source_manifest.get("dataset_id") != SOURCE_DATASET_ID:
        raise CatalystNewsEnrichmentError("unexpected source join")
    if source_result.get("status") != "READY" or source_result.get("inspected") is not True:
        raise CatalystNewsEnrichmentError("source join is not inspected READY")
    selection_path = _selection_path(store.root)
    selection = _read_gzip(selection_path)
    discovery = build_discovery_index(store, selection)
    if discovery["unique_article_count"] != 7_391:
        raise CatalystNewsEnrichmentError("discovery article count drifted from audit")
    discovery_path = _discovery_path(store.root)
    _write_gzip(discovery_path, discovery)
    config = AlpacaConfig.optional_from_env(env_path)
    if config is None:
        raise CatalystNewsEnrichmentError("Alpaca credentials are not configured")
    free_bytes = shutil.disk_usage(store.root).free
    if free_bytes < MINIMUM_FREE_BYTES:
        raise CatalystNewsEnrichmentError("historical store is below its disk reserve")
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": _timestamp_now(),
        "requested_dates": sorted({row["date"] for row in discovery["pair_records"]}),
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
            "pair_count": EXPECTED_PAIRS,
            "date_count": EXPECTED_DATES,
            "unique_article_count": discovery["unique_article_count"],
            "selection_sha256": _sha256_file(selection_path),
            "discovery_index_sha256": _sha256_file(discovery_path),
            "symbols_articles_and_rows_public": False,
        },
        "collection_contract": {
            "collector_sha256": _sha256_file(Path(__file__)),
            "historical_provider_sha256": _sha256_file(
                PROJECT_ROOT / "historical_providers.py"
            ),
            "provider": "Alpaca Market Data API",
            "feed_source": "Benzinga",
            "include_content": True,
            "window_days": NEWS_LOOKBACK_DAYS,
            "information_cutoff": "09:35 America/New_York",
            "join_key": "article_id plus created_at",
            "same_provider_retries_only": True,
            "substitutions_allowed": False,
            "checkpoint_unit": "date",
            "missing_content_is_explicit": True,
            "secondary_discovery_only": True,
            "target_outcomes_observed_or_derived": False,
            "primary_catalyst_classification_allowed": False,
        },
        "capacity_contract": {
            "minimum_free_bytes": MINIMUM_FREE_BYTES,
            "observed_free_bytes_at_freeze": free_bytes,
        },
        "provider_config": config.public_dict(),
    }
    return freeze_dataset_contract(contract, output_root)


def _verify_contract(
    manifest_path: Path, store: HistoricalDayStore
) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise CatalystNewsEnrichmentError("unexpected enrichment dataset")
    contract = manifest["selection_contract"]
    discovery_path = _discovery_path(store.root)
    if _sha256_file(discovery_path) != contract.get("discovery_index_sha256"):
        raise CatalystNewsEnrichmentError("discovery index changed")
    if _sha256_file(_selection_path(store.root)) != contract.get("selection_sha256"):
        raise CatalystNewsEnrichmentError("source selection changed")
    collection = manifest["collection_contract"]
    if _sha256_file(Path(__file__)) != collection.get("collector_sha256"):
        raise CatalystNewsEnrichmentError("collector differs from frozen contract")
    if _sha256_file(PROJECT_ROOT / "historical_providers.py") != collection.get(
        "historical_provider_sha256"
    ):
        raise CatalystNewsEnrichmentError("provider adapter differs from contract")
    return manifest, _read_gzip(discovery_path)


def _retry_news(
    client: AlpacaHistoricalClient,
    symbols: Sequence[str],
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    for attempt in range(4):
        try:
            return client.fetch_news(
                symbols, start, end, include_content=True
            )
        except HistoricalProviderError as exc:
            if exc.category not in {
                "retryable_transport",
                "retryable_provider",
                "provider_rate_limit",
            } or attempt == 3:
                raise
            time.sleep(min(8.0, 2.0**attempt))
    raise AssertionError("retry loop must return or raise")


def _empty_content_index(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "COLLECTING",
        "updated_at": _timestamp_now(),
        "date_records": {},
        "errors": [],
        "target_outcomes_observed_or_derived": False,
        "primary_catalyst_classified": False,
    }


def _public_progress(
    manifest: Mapping[str, Any], content: Mapping[str, Any]
) -> dict[str, Any]:
    records = content.get("date_records", {})
    expected = sum(len(row.get("expected_article_keys", [])) for row in records.values())
    returned = sum(
        sum(item.get("returned") is True for item in row.get("articles", {}).values())
        for row in records.values()
    )
    available = sum(
        sum(
            item.get("content_available") is True
            for item in row.get("articles", {}).values()
        )
        for row in records.values()
    )
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": content["status"],
        "completed_dates": len(records),
        "requested_dates": EXPECTED_DATES,
        "date_attributed_articles_expected": expected,
        "date_attributed_articles_returned": returned,
        "date_attributed_articles_with_content": available,
        "errors": len(content.get("errors", [])),
        "private_content_sha256": None,
        "symbols_articles_and_content_public": False,
        "secondary_discovery_only": True,
        "target_outcomes_observed_or_derived": False,
    }


def collect(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, discovery = _verify_contract(manifest_path, store)
    content_path = _content_path(store.root)
    content = (
        _read_gzip(content_path)
        if content_path.exists()
        else _empty_content_index(manifest)
    )
    if content.get("manifest_sha256") != manifest["manifest_sha256"]:
        raise CatalystNewsEnrichmentError("checkpoint belongs to another manifest")
    config = AlpacaConfig.optional_from_env(env_path)
    if config is None:
        raise CatalystNewsEnrichmentError("Alpaca credentials are not configured")
    pairs_by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in discovery["pair_records"]:
        pairs_by_date[str(row["date"])].append(row)
    with AlpacaHistoricalClient(config) as client:
        for day in manifest["requested_dates"]:
            if day in content["date_records"]:
                continue
            if shutil.disk_usage(store.root).free < MINIMUM_FREE_BYTES:
                raise CatalystNewsEnrichmentError(
                    "historical store fell below its disk reserve", category="capacity"
                )
            pairs = pairs_by_date[day]
            symbols = sorted({str(row["symbol"]) for row in pairs})
            expected_keys = sorted(
                {key for row in pairs for key in row["article_keys"]}
            )
            parsed = date.fromisoformat(day)
            start = datetime.combine(
                parsed - timedelta(days=NEWS_LOOKBACK_DAYS),
                wall_time(0),
                tzinfo=EASTERN,
            )
            cutoff = datetime.combine(parsed, wall_time(9, 35), tzinfo=EASTERN)
            try:
                rows = _retry_news(client, symbols, start, cutoff)
            except HistoricalProviderError as exc:
                content["errors"].append(
                    {"date": day, "category": exc.category, "message": str(exc)}
                )
                content["updated_at"] = _timestamp_now()
                _write_gzip(content_path, content)
                status = _public_progress(manifest, content)
                status["private_content_sha256"] = _sha256_file(content_path)
                _write_json(public_status_path, status)
                raise CatalystNewsEnrichmentError(
                    f"Alpaca content enrichment failed for {day}: {exc}",
                    category=exc.category,
                ) from exc
            returned = {_article_key(row): row for row in rows}
            article_records: dict[str, dict[str, Any]] = {}
            for key in expected_keys:
                row = returned.get(key)
                raw_content = str(row.get("content") or "") if row else ""
                article_records[key] = {
                    "returned": row is not None,
                    "content_available": bool(raw_content.strip()),
                    "content_sha256": (
                        hashlib.sha256(raw_content.encode()).hexdigest()
                        if raw_content
                        else None
                    ),
                    "content_bytes": len(raw_content.encode()),
                    "article": row,
                }
            content["date_records"][day] = {
                "expected_article_keys": expected_keys,
                "query_symbol_count": len(symbols),
                "returned_query_articles": len(rows),
                "articles": article_records,
            }
            content["updated_at"] = _timestamp_now()
            _write_gzip(content_path, content)
            _write_json(public_status_path, _public_progress(manifest, content))
    content["status"] = "COLLECTION_COMPLETE"
    content["updated_at"] = _timestamp_now()
    _write_gzip(content_path, content)
    public = _public_progress(manifest, content)
    public["private_content_sha256"] = _sha256_file(content_path)
    _write_json(public_status_path, public)
    return public


def summarize_content(
    discovery: Mapping[str, Any], content: Mapping[str, Any]
) -> dict[str, int]:
    unique: dict[str, Mapping[str, Any]] = {}
    for date_record in content.get("date_records", {}).values():
        for key, record in date_record.get("articles", {}).items():
            previous = unique.get(key)
            if previous is not None:
                old_hash = previous.get("content_sha256")
                new_hash = record.get("content_sha256")
                if old_hash and new_hash and old_hash != new_hash:
                    raise CatalystNewsEnrichmentError("article content changed across dates")
                if previous.get("content_available") and not record.get(
                    "content_available"
                ):
                    continue
            unique[key] = record
    pairs_with_content = sum(
        any(
            unique.get(key, {}).get("content_available") is True
            for key in pair["article_keys"]
        )
        for pair in discovery["pair_records"]
    )
    return {
        "selected_pairs": int(discovery["pair_count"]),
        "requested_dates": int(discovery["date_count"]),
        "discovery_unique_articles": int(discovery["unique_article_count"]),
        "returned_unique_articles": sum(
            record.get("returned") is True for record in unique.values()
        ),
        "content_available_unique_articles": sum(
            record.get("content_available") is True for record in unique.values()
        ),
        "content_bytes": sum(int(record.get("content_bytes") or 0) for record in unique.values()),
        "pairs_with_content": pairs_with_content,
        "pairs_without_content": int(discovery["pair_count"]) - pairs_with_content,
    }


def inspect(
    *, manifest_path: Path, env_path: Path, public_result_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, discovery = _verify_contract(manifest_path, store)
    content_path = _content_path(store.root)
    content = _read_gzip(content_path)
    if not (
        content.get("manifest_sha256") == manifest["manifest_sha256"]
        and content.get("status") == "COLLECTION_COMPLETE"
        and len(content.get("date_records", {})) == EXPECTED_DATES
        and content.get("errors") == []
        and content.get("target_outcomes_observed_or_derived") is False
        and content.get("primary_catalyst_classified") is False
    ):
        raise CatalystNewsEnrichmentError("content enrichment is incomplete")
    counts = summarize_content(discovery, content)
    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "counts": counts,
        "private_content_sha256": _sha256_file(content_path),
        "findings": {
            "secondary_full_content_enrichment_complete": True,
            "primary_catalyst_verified": False,
            "independent_corroboration_complete": False,
            "target_outcomes_observed_or_derived": False,
            "production_rule_change_earned": False,
        },
        "next_required_stage": (
            "freeze direct-source extraction and independent corroboration; "
            "do not classify Benzinga content as primary evidence"
        ),
        "claim_boundary": (
            "Point-in-time secondary discovery content coverage only; not primary "
            "catalyst verification, alpha, promotion, or a strategy variant."
        ),
        "symbols_articles_and_content_public": False,
    }
    _write_json(public_result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "collect", "inspect"))
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
            raise CatalystNewsEnrichmentError("--manifest is required")
        elif args.command == "collect":
            output = collect(
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
    except (
        CatalystNewsEnrichmentError,
        HistoricalProviderError,
        LearningDataError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
