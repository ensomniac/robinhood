"""Retry the exact frozen primary-source transport failures without substitution."""

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
from collections import Counter, defaultdict
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

import catalyst_primary_source_capture as primary
from historical_store import HistoricalDayStore
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-catalyst-transport-recovery-2026-07-19-expansion-v1"
SOURCE_CAPTURE_DATASET_ID = primary.DATASET_ID
SOURCE_CAPTURE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_primary_source_capture"
    / "manifests"
    / (
        "dataset-catalyst-primary-source-capture-2026-07-19-expansion-v1-"
        "409a807f1cae100ced8b9018aeb99612a2efc9d5f496e5981c26654762b41ef4.json"
    )
)
SOURCE_CAPTURE_RESULT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-19-catalyst-primary-source-capture.json"
)
SOURCE_IDENTITY_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "selected_candidate_fidelity_expansion"
    / "manifests"
    / (
        "dataset-selected-candidate-fidelity-2026-07-19-expansion-v1-"
        "ee9ba6f3a2d48f3337ee654c545ea3c02287e23f4a4db5a3fb86adf1b0c780ce.json"
    )
)
SOURCE_SEMANTICS_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-source-semantics.json"
)
SEC_SEMANTICS_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-sec-semantics.json"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches" / "catalyst_transport_recovery" / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_transport_recovery"
    / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-transport-recovery.json"
)

EXPECTED_SOURCES = 13
EXPECTED_PAIR_SOURCE_JOINS = 13
EXPECTED_PAIRS = 6
EXPECTED_CATEGORY = "ISSUER_HOST_CANDIDATE"
EXPECTED_ERROR = "source request transport failure"
MINIMUM_INTERVAL_SECONDS = 0.5
MAXIMUM_ATTEMPTS = 3


class CatalystTransportRecoveryError(RuntimeError):
    """The frozen transport-recovery contract cannot be honored."""


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
        raise CatalystTransportRecoveryError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CatalystTransportRecoveryError(f"{path} must contain an object")
    return value


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise CatalystTransportRecoveryError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CatalystTransportRecoveryError(f"{path} must contain an object")
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


def _write_bytes(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(value)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _timestamp_now() -> str:
    return datetime.now(UTC).isoformat()


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise CatalystTransportRecoveryError(
            f"public path must be repository-relative: {path}"
        ) from exc


def _capture_root(store_root: Path) -> Path:
    return (
        store_root
        / "_derived"
        / "catalyst_primary_source_capture"
        / SOURCE_CAPTURE_DATASET_ID
    )


def _private_root(store_root: Path) -> Path:
    return store_root / "_derived" / "catalyst_transport_recovery" / DATASET_ID


def _selection_path(store_root: Path) -> Path:
    return _private_root(store_root) / "selection.json.gz"


def _index_path(store_root: Path) -> Path:
    return _private_root(store_root) / "capture-index.json"


def _response_path(store_root: Path, source_hash: str) -> Path:
    return _private_root(store_root) / "responses" / f"{source_hash}.bin"


def _discovery_path(store_root: Path) -> Path:
    return (
        store_root
        / "_derived"
        / "catalyst_news_enrichment"
        / "dataset-catalyst-news-enrichment-2026-07-19-expansion-v1"
        / "discovery-index.json.gz"
    )


def _identity_path(store_root: Path) -> Path:
    return (
        store_root
        / "_derived"
        / "selected_candidate_fidelity"
        / "dataset-selected-candidate-fidelity-2026-07-19-expansion-v1"
        / "selection-and-cik-map.json.gz"
    )


def build_selection(
    *,
    capture_selection: Mapping[str, Any],
    capture_index: Mapping[str, Any],
    discovery: Mapping[str, Any],
    identities: Mapping[str, Any],
) -> dict[str, Any]:
    pairs_by_article: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for pair in discovery.get("pair_records", []):
        key = (str(pair["date"]), str(pair["symbol"]))
        for article_key in pair.get("article_keys", []):
            pairs_by_article[str(article_key)].append(key)
    identity_by_pair = {
        (str(row["date"]), str(row["symbol"])): row
        for row in identities.get("pairs", [])
    }
    records = []
    unique_pairs: set[tuple[str, str]] = set()
    joins = 0
    for source in capture_selection.get("records", []):
        source_hash = str(source["url_sha256"])
        original = capture_index.get("records", {}).get(source_hash, {})
        if not (
            original.get("status") == "CAPTURE_ERROR"
            and original.get("error") == EXPECTED_ERROR
        ):
            continue
        if source.get("category") != EXPECTED_CATEGORY:
            raise CatalystTransportRecoveryError(
                "transport failure has unexpected source category"
            )
        pairs = sorted(
            {
                pair
                for article_key in source.get("article_keys", [])
                for pair in pairs_by_article.get(str(article_key), [])
            }
        )
        if not pairs:
            raise CatalystTransportRecoveryError(
                "transport source has no selected-pair join"
            )
        bound_pairs = []
        for pair in pairs:
            identity = identity_by_pair.get(pair)
            if identity is None:
                raise CatalystTransportRecoveryError(
                    "transport source pair lacks frozen identity"
                )
            bound_pairs.append(
                {
                    "date": pair[0],
                    "symbol": pair[1],
                    "instrument_id": identity["instrument_id"],
                    "primary_exchange": identity["primary_exchange"],
                    "issuer_name": identity["issuer_name"],
                    "cik": identity["cik"],
                }
            )
            unique_pairs.add(pair)
        records.append(
            {
                "source_sha256": source_hash,
                "source_url": source["url"],
                "category": source["category"],
                "article_keys": sorted(set(source.get("article_keys", []))),
                "pairs": bound_pairs,
                "original_error": original["error"],
            }
        )
        joins += len(bound_pairs)
    records.sort(key=lambda row: row["source_sha256"])
    if (
        len(records) != EXPECTED_SOURCES
        or joins != EXPECTED_PAIR_SOURCE_JOINS
        or len(unique_pairs) != EXPECTED_PAIRS
    ):
        raise CatalystTransportRecoveryError("transport recovery counts differ")
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "status": "FROZEN",
        "records": records,
        "counts": {
            "transport_failure_sources": len(records),
            "pair_source_joins": joins,
            "pairs": len(unique_pairs),
        },
        "symbols_dates_urls_articles_responses_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }


def freeze_inputs(*, env_path: Path, output_root: Path) -> tuple[Path, dict[str, Any]]:
    store = HistoricalDayStore.from_env(env_path)
    if shutil.disk_usage(store.root).free < store.min_free_bytes:
        raise CatalystTransportRecoveryError(
            "historical store disk reserve is not ready"
        )
    capture_manifest = load_frozen_dataset_contract(SOURCE_CAPTURE_MANIFEST)
    identity_manifest = load_frozen_dataset_contract(SOURCE_IDENTITY_MANIFEST)
    capture_result = _read_json(SOURCE_CAPTURE_RESULT)
    source_semantics = _read_json(SOURCE_SEMANTICS_RESULT)
    sec_semantics = _read_json(SEC_SEMANTICS_RESULT)
    if not (
        capture_result.get("status") == "READY"
        and capture_result.get("inspected") is True
        and capture_result.get("capture_status_counts", {}).get("CAPTURE_ERROR")
        == EXPECTED_SOURCES
    ):
        raise CatalystTransportRecoveryError("source capture is not inspected READY")
    if not (
        source_semantics.get("next_phase") == "SOURCE_RECOVERY"
        and sec_semantics.get("next_phase") == "SOURCE_RECOVERY"
        and sec_semantics.get("capacity_gate_passed") is False
        and source_semantics.get("target_outcomes_observed_or_derived") is False
        and sec_semantics.get("target_outcomes_observed_or_derived") is False
    ):
        raise CatalystTransportRecoveryError("semantic evidence did not earn recovery")
    capture_root = _capture_root(store.root)
    inputs = {
        "capture_selection": capture_root / "capture-selection.json.gz",
        "capture_index": capture_root / "capture-index.json",
        "discovery": _discovery_path(store.root),
        "identities": _identity_path(store.root),
    }
    selection = build_selection(
        capture_selection=_read_gzip(inputs["capture_selection"]),
        capture_index=_read_json(inputs["capture_index"]),
        discovery=_read_gzip(inputs["discovery"]),
        identities=_read_gzip(inputs["identities"]),
    )
    selection_path = _selection_path(store.root)
    _write_gzip(selection_path, selection)
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": _timestamp_now(),
        "requested_dates": list(capture_manifest["requested_dates"]),
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(SOURCE_CAPTURE_MANIFEST),
                _repo_path(SOURCE_CAPTURE_RESULT),
                _repo_path(SOURCE_IDENTITY_MANIFEST),
                _repo_path(SOURCE_SEMANTICS_RESULT),
                _repo_path(SEC_SEMANTICS_RESULT),
                "CATALYST_SOURCE_RECOVERY.md",
                "PRODUCTION_STRATEGY_VALIDATION.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            "capture_manifest_sha256": capture_manifest["manifest_sha256"],
            "capture_result_sha256": _sha256_file(SOURCE_CAPTURE_RESULT),
            "identity_manifest_sha256": identity_manifest["manifest_sha256"],
            "source_semantics_result_sha256": _sha256_file(SOURCE_SEMANTICS_RESULT),
            "sec_semantics_result_sha256": _sha256_file(SEC_SEMANTICS_RESULT),
            "private_input_sha256": {
                name: _sha256_file(path) for name, path in sorted(inputs.items())
            },
            "private_selection_sha256": _sha256_file(selection_path),
            **selection["counts"],
            "original_terminal_status": "CAPTURE_ERROR",
            "original_error": EXPECTED_ERROR,
            "source_category": EXPECTED_CATEGORY,
            "symbols_dates_urls_articles_responses_and_rows_public": False,
        },
        "collection_contract": {
            "collector_sha256": _sha256_file(Path(__file__)),
            "request_dependency_sha256": _sha256_file(Path(primary.__file__)),
            "same_exact_url_only": True,
            "user_agent": primary.USER_AGENT,
            "minimum_interval_seconds": MINIMUM_INTERVAL_SECONDS,
            "maximum_attempts": MAXIMUM_ATTEMPTS,
            "retryable_failures": ["transport", "HTTP_429", "HTTP_5XX"],
            "timeout_seconds": primary.REQUEST_TIMEOUT_SECONDS,
            "maximum_redirects": primary.MAX_REDIRECTS,
            "maximum_response_bytes": primary.MAX_RESPONSE_BYTES,
            "private_or_non_global_addresses_blocked": True,
            "every_redirect_revalidated": True,
            "historical_store_reserve_enforced": True,
            "checkpoint_unit": "source URL",
            "provider_substitution_allowed": False,
            "secondary_source_substitution_allowed": False,
            "source_semantics_classification_allowed": False,
            "target_outcomes_observed_or_derived": False,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def _verify_contract(
    manifest_path: Path, store: HistoricalDayStore
) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise CatalystTransportRecoveryError("unexpected transport-recovery dataset")
    selection_path = _selection_path(store.root)
    selection = _read_gzip(selection_path)
    if _sha256_file(selection_path) != manifest["selection_contract"].get(
        "private_selection_sha256"
    ):
        raise CatalystTransportRecoveryError("transport selection changed")
    dependencies = {
        "collector_sha256": Path(__file__),
        "request_dependency_sha256": Path(primary.__file__),
    }
    for key, path in dependencies.items():
        if _sha256_file(path) != manifest["collection_contract"].get(key):
            raise CatalystTransportRecoveryError(f"{key} differs from contract")
    return manifest, selection


def _capture_with_retry(session: requests.Session, url: str) -> dict[str, Any]:
    last_error = ""
    for attempt in range(MAXIMUM_ATTEMPTS):
        try:
            result = primary._request_once(session, url)
            status = int(result["http_status"])
            if (status == 429 or status >= 500) and attempt + 1 < MAXIMUM_ATTEMPTS:
                last_error = f"HTTP {status}"
                time.sleep(2.0**attempt)
                continue
            return result
        except primary.PrimarySourceCaptureError as exc:
            last_error = str(exc)
            if "transport" in last_error and attempt + 1 < MAXIMUM_ATTEMPTS:
                time.sleep(2.0**attempt)
                continue
            break
    return {"status": "CAPTURE_ERROR", "error": last_error}


def _public_summary(
    manifest: Mapping[str, Any], selection: Mapping[str, Any], index: Mapping[str, Any]
) -> dict[str, Any]:
    records = index.get("records", {})
    statuses = Counter(str(row.get("status")) for row in records.values())
    http_statuses = Counter(
        str(row.get("http_status"))
        for row in records.values()
        if row.get("http_status") is not None
    )
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": index["status"],
        "selection_counts": selection["counts"],
        "terminal_sources": len(records),
        "capture_status_counts": dict(sorted(statuses.items())),
        "http_status_counts": dict(sorted(http_statuses.items())),
        "response_bytes": sum(
            int(row.get("body_bytes") or 0) for row in records.values()
        ),
        "symbols_dates_urls_articles_responses_and_rows_public": False,
        "source_semantics_classified": False,
        "target_outcomes_observed_or_derived": False,
    }


def collect(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, selection = _verify_contract(manifest_path, store)
    if shutil.disk_usage(store.root).free < store.min_free_bytes:
        raise CatalystTransportRecoveryError(
            "historical store disk reserve is not ready"
        )
    index_path = _index_path(store.root)
    index = (
        _read_json(index_path)
        if index_path.exists()
        else {
            "schema_version": 1,
            "dataset_id": DATASET_ID,
            "manifest_sha256": manifest["manifest_sha256"],
            "status": "COLLECTING",
            "records": {},
            "target_outcomes_observed_or_derived": False,
        }
    )
    if index.get("manifest_sha256") != manifest["manifest_sha256"]:
        raise CatalystTransportRecoveryError("checkpoint belongs to another manifest")
    session = requests.Session()
    last_started = 0.0
    try:
        for selected in selection["records"]:
            source_hash = str(selected["source_sha256"])
            if source_hash in index["records"]:
                continue
            if shutil.disk_usage(store.root).free < store.min_free_bytes:
                raise CatalystTransportRecoveryError(
                    "historical store disk reserve is not ready"
                )
            remaining = MINIMUM_INTERVAL_SECONDS - (time.monotonic() - last_started)
            if remaining > 0:
                time.sleep(remaining)
            last_started = time.monotonic()
            result = _capture_with_retry(session, str(selected["source_url"]))
            body = result.pop("body", None)
            if body is not None:
                path = _response_path(store.root, source_hash)
                if (
                    shutil.disk_usage(store.root).free - len(body)
                    < store.min_free_bytes
                ):
                    raise CatalystTransportRecoveryError(
                        "response would breach historical store disk reserve"
                    )
                _write_bytes(path, body)
                result["body_sha256"] = _sha256_file(path)
                result["body_bytes"] = len(body)
            result.update(
                {
                    "source_sha256": source_hash,
                    "captured_at": _timestamp_now(),
                }
            )
            index["records"][source_hash] = result
            _write_json(index_path, index)
            _write_json(public_status_path, _public_summary(manifest, selection, index))
    finally:
        session.close()
    index["status"] = "COLLECTION_COMPLETE"
    _write_json(index_path, index)
    public = _public_summary(manifest, selection, index)
    _write_json(public_status_path, public)
    return public


def inspect(
    *, manifest_path: Path, env_path: Path, public_result_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, selection = _verify_contract(manifest_path, store)
    index_path = _index_path(store.root)
    index = _read_json(index_path)
    if not (
        index.get("manifest_sha256") == manifest["manifest_sha256"]
        and index.get("status") == "COLLECTION_COMPLETE"
        and len(index.get("records", {})) == EXPECTED_SOURCES
        and index.get("target_outcomes_observed_or_derived") is False
    ):
        raise CatalystTransportRecoveryError("transport recovery is incomplete")
    expected = {row["source_sha256"] for row in selection["records"]}
    if set(index["records"]) != expected:
        raise CatalystTransportRecoveryError("transport recovery row set differs")
    for source_hash, record in index["records"].items():
        if record.get("status") != "RESPONSE_CAPTURED":
            continue
        path = _response_path(store.root, source_hash)
        if not path.exists() or _sha256_file(path) != record.get("body_sha256"):
            raise CatalystTransportRecoveryError("recovered response hash mismatch")
    summary = _public_summary(manifest, selection, index)
    result = {
        **summary,
        "status": "READY",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "private_index_sha256": _sha256_file(index_path),
        "inspection": {
            "selection_rebuilt": True,
            "terminal_counts_rebuilt": True,
            "response_hashes_verified": True,
            "no_substitution_verified": True,
        },
        "next_required_stage": (
            "recover canonical issuer-host evidence through the exact referring-page "
            "and document chains, then freeze semantics separately"
        ),
        "claim_boundary": (
            "Exact retry of frozen transport-failure URLs only; not proof of source "
            "ownership, issuer binding, causality, event direction, alpha, or a "
            "strategy change."
        ),
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
            raise CatalystTransportRecoveryError("--manifest is required")
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
        CatalystTransportRecoveryError,
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
