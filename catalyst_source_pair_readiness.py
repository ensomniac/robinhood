"""Measure pair-level primary-source readiness without accepting evidence."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import catalyst_news_enrichment as news
import catalyst_primary_source_capture as capture
import catalyst_source_profile as profile
from historical_store import HistoricalDayStore
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-catalyst-source-pair-readiness-2026-07-19-expansion-v1"
EXPECTED_PAIRS = 1_987
EXPECTED_URLS = 134
SOURCE_RESULTS = (
    news.DEFAULT_PUBLIC_RESULT,
    capture.DEFAULT_PUBLIC_RESULT,
    profile.DEFAULT_PUBLIC_RESULT,
)
SOURCE_PROFILE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_source_profile"
    / "manifests"
    / (
        "dataset-catalyst-source-profile-2026-07-19-expansion-v1-"
        "411be9d083d118d06087c73d95ad3e8de386b6a701358c29feb618800cb3151f.json"
    )
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches" / "catalyst_source_pair_readiness" / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_source_pair_readiness"
    / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-source-pair-readiness.json"
)


class CatalystSourcePairReadinessError(RuntimeError):
    """The frozen pair-readiness contract or inputs are invalid."""


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
        raise CatalystSourcePairReadinessError(
            f"public path must be repository-relative: {path}"
        ) from exc


def _input_paths(store_root: Path) -> dict[str, Path]:
    return {
        "discovery": news._discovery_path(store_root),
        "capture_selection": capture._selection_path(store_root),
        "capture_index": capture._index_path(store_root),
        "source_profile": profile._private_path(store_root),
    }


def _private_path(store_root: Path) -> Path:
    return (
        store_root
        / "_derived"
        / "catalyst_source_pair_readiness"
        / DATASET_ID
        / "pair-readiness.json.gz"
    )


def _response_is(record: Mapping[str, Any], media: str) -> bool:
    content_type = str(record.get("headers", {}).get("content-type") or "").lower()
    return record.get("http_status") == 200 and media in content_type


def build_pair_readiness(
    discovery: Mapping[str, Any],
    selection: Mapping[str, Any],
    capture_index: Mapping[str, Any],
    source_profile: Mapping[str, Any],
) -> dict[str, Any]:
    selected_urls = selection.get("records", [])
    captured = capture_index.get("records", {})
    profiled = source_profile.get("records", {})
    selected_hashes = {str(row.get("url_sha256")) for row in selected_urls}
    if selected_hashes != set(captured) or selected_hashes != set(profiled):
        raise CatalystSourcePairReadinessError(
            "selection, capture, and profile URL sets differ"
        )

    by_article: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for selected in selected_urls:
        for article_key in selected.get("article_keys", []):
            by_article[str(article_key)].append(selected)

    records: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for pair in discovery.get("pair_records", []):
        urls = {
            str(selected["url_sha256"]): selected
            for article_key in pair.get("article_keys", [])
            for selected in by_article.get(str(article_key), [])
        }
        categories = sorted(
            {str(selected.get("category")) for selected in urls.values()}
        )
        flags = {
            "has_primary_route": bool(urls),
            "has_captured_response": False,
            "has_capture_error": False,
            "has_http_200": False,
            "has_html_200": False,
            "has_pdf_200": False,
            "has_xml_200": False,
            "has_canonical_candidate": False,
            "has_standard_timestamp_candidate": False,
        }
        for url_hash in urls:
            response = captured[url_hash]
            response_profile = profiled[url_hash]
            html = response_profile.get("html") or {}
            flags["has_captured_response"] |= (
                response.get("status") == "RESPONSE_CAPTURED"
            )
            flags["has_capture_error"] |= response.get("status") == "CAPTURE_ERROR"
            flags["has_http_200"] |= response.get("http_status") == 200
            flags["has_html_200"] |= _response_is(response, "html")
            flags["has_pdf_200"] |= _response_is(response, "pdf")
            flags["has_xml_200"] |= _response_is(response, "xml")
            flags["has_canonical_candidate"] |= bool(html.get("canonical_urls"))
            flags["has_standard_timestamp_candidate"] |= bool(
                html.get("meta_timestamp_candidates")
                or html.get("time_datetime_candidates")
                or html.get("json_ld_date_published_present")
            )
        for flag, value in flags.items():
            counts[f"pairs_{flag}"] += bool(value)
        for category in categories:
            counts[f"pairs_{category.lower()}"] += 1
        records.append(
            {
                "date": str(pair.get("date")),
                "symbol": str(pair.get("symbol")),
                "route_url_hashes": sorted(urls),
                "route_categories": categories,
                **flags,
            }
        )

    counts["selected_pairs"] = len(records)
    counts["pairs_without_primary_route"] = (
        len(records) - counts["pairs_has_primary_route"]
    )
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "status": "DERIVATION_COMPLETE",
        "counts": dict(sorted(counts.items())),
        "pair_records": records,
        "timestamp_candidates_accepted": 0,
        "source_ownership_verified": False,
        "issuer_binding_verified": False,
        "causal_availability_verified": False,
        "primary_catalyst_verified": False,
        "target_outcomes_observed_or_derived": False,
    }


def freeze_inputs(*, env_path: Path, output_root: Path) -> tuple[Path, dict[str, Any]]:
    store = HistoricalDayStore.from_env(env_path)
    source_manifest = load_frozen_dataset_contract(SOURCE_PROFILE_MANIFEST)
    for result_path in SOURCE_RESULTS:
        result = capture._read_json(result_path)
        if result.get("status") != "READY" or result.get("inspected") is not True:
            raise CatalystSourcePairReadinessError(
                f"source result is not inspected READY: {result_path.name}"
            )
    paths = _input_paths(store.root)
    discovery = capture._read_gzip(paths["discovery"])
    selection = capture._read_gzip(paths["capture_selection"])
    capture_index = capture._read_json(paths["capture_index"])
    source_profile = capture._read_gzip(paths["source_profile"])
    if discovery.get("pair_count") != EXPECTED_PAIRS:
        raise CatalystSourcePairReadinessError("discovery is not exactly 1,987 pairs")
    if not (
        len(selection.get("records", []))
        == len(capture_index.get("records", {}))
        == len(source_profile.get("records", {}))
        == EXPECTED_URLS
    ):
        raise CatalystSourcePairReadinessError("source inputs are not exactly 134 URLs")
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
                _repo_path(SOURCE_PROFILE_MANIFEST),
                *[_repo_path(path) for path in SOURCE_RESULTS],
                "CATALYST_EVIDENCE_ACQUISITION.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            "source_profile_manifest_sha256": source_manifest["manifest_sha256"],
            **{
                f"{name}_sha256": capture._sha256_file(path)
                for name, path in paths.items()
            },
            "selected_pairs": EXPECTED_PAIRS,
            "routed_urls": EXPECTED_URLS,
            "symbols_urls_articles_and_rows_public": False,
        },
        "derivation_contract": {
            "implementation_sha256": capture._sha256_file(Path(__file__)),
            "pair_join_key": "article_keys",
            "url_join_key": "url_sha256",
            "http_success_status": 200,
            "profile_candidate_fields": [
                "canonical_urls",
                "meta_timestamp_candidates",
                "time_datetime_candidates",
                "json_ld_date_published_present",
            ],
            "timestamp_candidates_are_causal_evidence": False,
            "route_categories_establish_source_ownership": False,
            "network_access_allowed": False,
            "target_outcomes_observed_or_derived": False,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def _verify_contract(
    manifest_path: Path, store: HistoricalDayStore
) -> tuple[dict[str, Any], dict[str, Path]]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise CatalystSourcePairReadinessError("unexpected pair-readiness dataset")
    paths = _input_paths(store.root)
    selection = manifest["selection_contract"]
    for name, path in paths.items():
        if capture._sha256_file(path) != selection.get(f"{name}_sha256"):
            raise CatalystSourcePairReadinessError(f"{name} source changed")
    if capture._sha256_file(Path(__file__)) != manifest["derivation_contract"].get(
        "implementation_sha256"
    ):
        raise CatalystSourcePairReadinessError(
            "pair-readiness implementation differs from frozen contract"
        )
    return manifest, paths


def _derive_from_paths(paths: Mapping[str, Path]) -> dict[str, Any]:
    return build_pair_readiness(
        capture._read_gzip(paths["discovery"]),
        capture._read_gzip(paths["capture_selection"]),
        capture._read_json(paths["capture_index"]),
        capture._read_gzip(paths["source_profile"]),
    )


def derive(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, paths = _verify_contract(manifest_path, store)
    private = _derive_from_paths(paths)
    private["manifest_sha256"] = manifest["manifest_sha256"]
    private_path = _private_path(store.root)
    capture._write_gzip(private_path, private)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "DERIVATION_COMPLETE",
        "counts": private["counts"],
        "private_result_sha256": capture._sha256_file(private_path),
        "timestamp_candidates_accepted": 0,
        "symbols_urls_articles_and_rows_public": False,
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
    private_path = _private_path(store.root)
    private = capture._read_gzip(private_path)
    rebuilt = _derive_from_paths(paths)
    if not (
        private.get("manifest_sha256") == manifest["manifest_sha256"]
        and private.get("counts") == rebuilt.get("counts")
        and private.get("timestamp_candidates_accepted") == 0
        and private.get("primary_catalyst_verified") is False
        and private.get("target_outcomes_observed_or_derived") is False
    ):
        raise CatalystSourcePairReadinessError("pair readiness failed inspection")
    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "counts": private["counts"],
        "private_result_sha256": capture._sha256_file(private_path),
        "findings": {
            "pair_readiness_join_complete": True,
            "timestamp_candidates_accepted": 0,
            "source_ownership_verified": False,
            "issuer_binding_verified": False,
            "causal_availability_verified": False,
            "primary_catalyst_verified": False,
            "production_rule_change_earned": False,
        },
        "next_required_stage": (
            "freeze source-type-specific ownership, issuer binding, timestamp "
            "precedence, and unresolved-source handling"
        ),
        "claim_boundary": (
            "Pair-level routing and format readiness only; a successful response or "
            "timestamp-shaped field is not accepted causal catalyst evidence."
        ),
        "symbols_urls_articles_and_rows_public": False,
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
            raise CatalystSourcePairReadinessError("--manifest is required")
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
    except (
        CatalystSourcePairReadinessError,
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
