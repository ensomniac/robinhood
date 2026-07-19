"""Profile captured catalyst-source formats without accepting evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from collections import Counter
from collections.abc import Mapping
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import catalyst_primary_source_capture as capture
from historical_store import HistoricalDayStore
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-catalyst-source-profile-2026-07-19-expansion-v1"
SOURCE_DATASET_ID = capture.DATASET_ID
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_primary_source_capture"
    / "manifests"
    / (
        "dataset-catalyst-primary-source-capture-2026-07-19-expansion-v1-"
        "409a807f1cae100ced8b9018aeb99612a2efc9d5f496e5981c26654762b41ef4.json"
    )
)
SOURCE_RESULT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-19-catalyst-primary-source-capture.json"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches" / "catalyst_source_profile" / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_source_profile"
    / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-source-profile.json"
)
PUBLISHED_META_KEYS = frozenset(
    {
        "article:published_time",
        "datepublished",
        "date",
        "pubdate",
        "publishdate",
        "publish_date",
    }
)


class CatalystSourceProfileError(RuntimeError):
    """The frozen source-profile contract or inputs are invalid."""


class _StructureParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.canonical_urls: list[str] = []
        self.meta_timestamps: list[dict[str, str]] = []
        self.time_datetimes: list[str] = []
        self.title_depth = 0
        self.title_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized = {str(key).lower(): value for key, value in attrs}
        lowered = tag.lower()
        if lowered == "title":
            self.title_depth += 1
        elif lowered == "link":
            rel = str(normalized.get("rel") or "").lower()
            href = str(normalized.get("href") or "").strip()
            if rel == "canonical" and href:
                self.canonical_urls.append(href)
        elif lowered == "time":
            value = str(normalized.get("datetime") or "").strip()
            if value:
                self.time_datetimes.append(value)
        elif lowered == "meta":
            key = str(
                normalized.get("property") or normalized.get("name") or ""
            ).lower()
            value = str(normalized.get("content") or "").strip()
            if key in PUBLISHED_META_KEYS and value:
                self.meta_timestamps.append({"key": key, "value": value})

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "title" and self.title_depth:
            self.title_depth -= 1

    def handle_data(self, data: str) -> None:
        if self.title_depth:
            self.title_parts.append(data)


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


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise CatalystSourceProfileError(
            f"public path must be repository-relative: {path}"
        ) from exc


def _private_path(store_root: Path) -> Path:
    return (
        store_root
        / "_derived"
        / "catalyst_source_profile"
        / DATASET_ID
        / "profile-index.json.gz"
    )


def _source_index_path(store_root: Path) -> Path:
    return capture._index_path(store_root)


def _response_bundle_hash(index: Mapping[str, Any]) -> str:
    rows = sorted(
        (str(key), str(row.get("body_sha256") or ""))
        for key, row in index.get("records", {}).items()
    )
    return hashlib.sha256(capture._canonical_bytes(rows)).hexdigest()


def freeze_inputs(*, env_path: Path, output_root: Path) -> tuple[Path, dict[str, Any]]:
    store = HistoricalDayStore.from_env(env_path)
    source_manifest = load_frozen_dataset_contract(SOURCE_MANIFEST)
    source_result = capture._read_json(SOURCE_RESULT)
    if source_manifest.get("dataset_id") != SOURCE_DATASET_ID:
        raise CatalystSourceProfileError("unexpected source-capture dataset")
    if (
        source_result.get("status") != "READY"
        or source_result.get("inspected") is not True
    ):
        raise CatalystSourceProfileError(
            "source-capture dataset is not inspected READY"
        )
    index_path = _source_index_path(store.root)
    index = capture._read_json(index_path)
    if len(index.get("records", {})) != 134:
        raise CatalystSourceProfileError("capture index is not terminal for 134 URLs")
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": capture._timestamp_now(),
        "requested_dates": list(source_manifest["requested_dates"]),
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(SOURCE_MANIFEST),
                _repo_path(SOURCE_RESULT),
                "CATALYST_PRIMARY_SOURCE_CAPTURE.md",
                "CATALYST_EVIDENCE_ACQUISITION.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            "source_dataset_id": SOURCE_DATASET_ID,
            "source_manifest_sha256": source_manifest["manifest_sha256"],
            "capture_index_sha256": capture._sha256_file(index_path),
            "response_bundle_sha256": _response_bundle_hash(index),
            "terminal_urls": 134,
            "captured_responses": 121,
            "symbols_articles_urls_content_and_rows_public": False,
        },
        "derivation_contract": {
            "profiler_sha256": capture._sha256_file(Path(__file__)),
            "source_capture_implementation_sha256": capture._sha256_file(
                Path(capture.__file__)
            ),
            "published_meta_keys": sorted(PUBLISHED_META_KEYS),
            "extract_html_title_presence": True,
            "extract_canonical_links": True,
            "extract_time_datetime_candidates": True,
            "extract_json_ld_date_published_presence": True,
            "timestamp_candidates_are_causal_evidence": False,
            "canonical_links_establish_ownership": False,
            "network_access_allowed": False,
            "primary_catalyst_classification_allowed": False,
            "target_outcomes_observed_or_derived": False,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def _verify_contract(
    manifest_path: Path, store: HistoricalDayStore
) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise CatalystSourceProfileError("unexpected source-profile dataset")
    index_path = _source_index_path(store.root)
    index = capture._read_json(index_path)
    selection = manifest["selection_contract"]
    if capture._sha256_file(index_path) != selection.get("capture_index_sha256"):
        raise CatalystSourceProfileError("capture index changed")
    if _response_bundle_hash(index) != selection.get("response_bundle_sha256"):
        raise CatalystSourceProfileError("response bundle changed")
    if capture._sha256_file(Path(__file__)) != manifest["derivation_contract"].get(
        "profiler_sha256"
    ):
        raise CatalystSourceProfileError("profiler differs from frozen contract")
    return manifest, index


def derive_profile(
    store: HistoricalDayStore, index: Mapping[str, Any]
) -> dict[str, Any]:
    records: dict[str, dict[str, Any]] = {}
    counts: Counter[str] = Counter()
    http_statuses: Counter[str] = Counter()
    content_types: Counter[str] = Counter()
    categories: Counter[str] = Counter()
    for key, source in sorted(index["records"].items()):
        status = str(source.get("status"))
        counts[f"capture_{status.lower()}"] += 1
        category = str(source.get("category") or "<missing>")
        categories[category] += 1
        profile: dict[str, Any] = {
            "capture_status": status,
            "category": category,
            "http_status": source.get("http_status"),
            "content_type": source.get("headers", {}).get("content-type"),
            "has_last_modified_header": bool(
                source.get("headers", {}).get("last-modified")
            ),
            "redirect_count": max(0, len(source.get("redirect_history", [])) - 1),
            "final_host_changed": False,
            "html": None,
        }
        if source.get("http_status") is not None:
            http_statuses[str(source["http_status"])] += 1
        if status != "RESPONSE_CAPTURED":
            records[key] = profile
            continue
        content_type = str(source.get("headers", {}).get("content-type") or "")
        media_type = content_type.split(";", 1)[0] or "<missing>"
        content_types[media_type] += 1
        history = source.get("redirect_history", [])
        if history:
            initial_host = urlparse(str(history[0].get("url") or "")).hostname
            final_host = urlparse(str(source.get("final_url") or "")).hostname
            profile["final_host_changed"] = initial_host != final_host
            counts["final_host_changed"] += initial_host != final_host
        path = capture._response_path(store.root, key)
        if capture._sha256_file(path) != source.get("body_sha256"):
            raise CatalystSourceProfileError("response hash mismatch")
        if source.get("http_status") == 200 and "html" in content_type.lower():
            body = path.read_bytes().decode("utf-8", errors="replace")
            parser = _StructureParser()
            parser.feed(body)
            json_ld = bool(re.search(r'(?i)"datePublished"\s*:', body))
            profile["html"] = {
                "title_present": bool("".join(parser.title_parts).strip()),
                "canonical_urls": sorted(set(parser.canonical_urls)),
                "meta_timestamp_candidates": parser.meta_timestamps,
                "time_datetime_candidates": sorted(set(parser.time_datetimes)),
                "json_ld_date_published_present": json_ld,
            }
            counts["html_200"] += 1
            counts["html_title_present"] += profile["html"]["title_present"]
            counts["html_canonical_present"] += bool(parser.canonical_urls)
            counts["html_meta_timestamp_present"] += bool(parser.meta_timestamps)
            counts["html_time_datetime_present"] += bool(parser.time_datetimes)
            counts["html_json_ld_date_published_present"] += json_ld
        counts["last_modified_header_present"] += profile["has_last_modified_header"]
        counts[f"redirect_count_{profile['redirect_count']}"] += 1
        records[key] = profile
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "status": "PROFILE_COMPLETE",
        "records": records,
        "counts": dict(sorted(counts.items())),
        "http_status_counts": dict(sorted(http_statuses.items())),
        "content_type_counts": dict(sorted(content_types.items())),
        "category_counts": dict(sorted(categories.items())),
        "timestamp_candidates_accepted": 0,
        "source_ownership_verified": False,
        "issuer_binding_verified": False,
        "causal_availability_verified": False,
        "primary_catalyst_verified": False,
        "target_outcomes_observed_or_derived": False,
    }


def derive(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, index = _verify_contract(manifest_path, store)
    private = derive_profile(store, index)
    private["manifest_sha256"] = manifest["manifest_sha256"]
    private_path = _private_path(store.root)
    capture._write_gzip(private_path, private)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "PROFILE_COMPLETE",
        "counts": private["counts"],
        "http_status_counts": private["http_status_counts"],
        "content_type_counts": private["content_type_counts"],
        "category_counts": private["category_counts"],
        "private_result_sha256": capture._sha256_file(private_path),
        "timestamp_candidates_accepted": 0,
        "symbols_articles_urls_content_and_rows_public": False,
        "primary_catalyst_verified": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(public_status_path, public)
    return public


def inspect(
    *, manifest_path: Path, env_path: Path, public_result_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, index = _verify_contract(manifest_path, store)
    private_path = _private_path(store.root)
    private = capture._read_gzip(private_path)
    rebuilt = derive_profile(store, index)
    for field in (
        "counts",
        "http_status_counts",
        "content_type_counts",
        "category_counts",
    ):
        if private.get(field) != rebuilt.get(field):
            raise CatalystSourceProfileError(f"profile inspection mismatch: {field}")
    if not (
        private.get("manifest_sha256") == manifest["manifest_sha256"]
        and private.get("status") == "PROFILE_COMPLETE"
        and private.get("timestamp_candidates_accepted") == 0
        and private.get("target_outcomes_observed_or_derived") is False
        and private.get("primary_catalyst_verified") is False
    ):
        raise CatalystSourceProfileError("source profile is incomplete")
    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "counts": private["counts"],
        "http_status_counts": private["http_status_counts"],
        "content_type_counts": private["content_type_counts"],
        "category_counts": private["category_counts"],
        "private_result_sha256": capture._sha256_file(private_path),
        "findings": {
            "format_and_timestamp_candidate_profile_complete": True,
            "timestamp_candidates_accepted": 0,
            "source_ownership_verified": False,
            "issuer_binding_verified": False,
            "causal_availability_verified": False,
            "primary_catalyst_verified": False,
            "production_rule_change_earned": False,
        },
        "next_required_stage": (
            "freeze format-specific timestamp parsing, issuer-domain binding, and "
            "source ownership verification before accepting any evidence"
        ),
        "claim_boundary": (
            "Offline response-structure profile only; timestamp fields and canonical "
            "links are candidates, not accepted causal or ownership evidence."
        ),
        "symbols_articles_urls_content_and_rows_public": False,
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
            raise CatalystSourceProfileError("--manifest is required")
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
    except (CatalystSourceProfileError, LearningDataError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
