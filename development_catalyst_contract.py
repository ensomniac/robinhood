"""Freeze and inspect the disjoint tranche's primary-source semantics contract.

This module is deliberately network free. It binds the exact selected-pair
surface and the existing source-semantics rules before any target source,
selected-symbol detail, or outcome input may be accessed.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import importlib.metadata
import json
import os
import shutil
import sys
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import catalyst_source_semantics as semantics
from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-primary-source-semantics-contract-2026-07-19-development-v2"
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v2/selected_pair_manifests"
    / (
        "dataset-selected-candidate-contract-2026-07-19-development-v2-"
        "a1362e198a6a6f05adf0da966236d5f2bec6bb82a395703255aa13f8299c834f.json"
    )
)
SCANNER_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v2/scanner_manifests"
    / (
        "dataset-production-scanner-replay-2026-07-19-development-v2-"
        "e500cf2a9a3f63f97496b76d31ffaf98d7e849696835c707fb7daf4fe85a3643.json"
    )
)
SCANNER_SUMMARY = (
    PROJECT_ROOT / "research_results/2026-07-19-development-tranche-scanner.json"
)
SCANNER_INSPECTION = (
    PROJECT_ROOT
    / "research_results/2026-07-19-development-tranche-scanner-inspection.json"
)
SECURITY_MASTER_SOURCE = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v2/security-master-source.json"
)
STRATEGY_SOURCE = (
    PROJECT_ROOT / "historical_batches/scanner_expansion/production-strategy-source.json"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches/development_tranche_v2/catalyst_manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v2/catalyst-contract-status.json"
)
DEFAULT_SELECTION_DOC = PROJECT_ROOT / "DEVELOPMENT_SELECTED_PAIRS.md"
PRIVATE_NAMESPACE = "_derived/development_catalyst_sources"


class DevelopmentCatalystContractError(RuntimeError):
    """The outcome-blind source contract is invalid or has drifted."""


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


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise DevelopmentCatalystContractError(
            f"public path must be repository relative: {path}"
        ) from exc


def _read_gzip_object(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise DevelopmentCatalystContractError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DevelopmentCatalystContractError(f"{path} must contain an object")
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


def _selection_private_path(store_root: Path, dataset_id: str) -> Path:
    return (
        store_root
        / "_derived/scanner_selected_pairs"
        / dataset_id
        / "selected-pairs.json.gz"
    )


def _target_source_root(store_root: Path, dataset_id: str) -> Path:
    return store_root / PRIVATE_NAMESPACE / dataset_id


def _target_artifact_count(store_root: Path, dataset_id: str) -> int:
    root = _target_source_root(store_root, dataset_id)
    return sum(1 for path in root.rglob("*") if path.is_file()) if root.exists() else 0


def _rebuild_selection(
    source_manifest: Mapping[str, Any], store_root: Path
) -> dict[str, Any]:
    source_id = str(source_manifest.get("dataset_id") or "")
    if not source_id.startswith("dataset-selected-candidate-contract-"):
        raise DevelopmentCatalystContractError(
            "source is not a selected-candidate contract"
        )
    selection = source_manifest.get("selection_contract")
    downstream = source_manifest.get("downstream_contract")
    if not isinstance(selection, Mapping) or not isinstance(downstream, Mapping):
        raise DevelopmentCatalystContractError("source selection contract is incomplete")
    if (
        downstream.get("source_outcomes_observed_or_derived") is not False
        or downstream.get("substitutions_allowed") is not False
    ):
        raise DevelopmentCatalystContractError("source selection lock is not closed")
    private = _read_gzip_object(_selection_private_path(store_root, source_id))
    expected_private_hash = str(selection.get("private_selection_content_sha256") or "")
    if _sha256_json(private) != expected_private_hash:
        raise DevelopmentCatalystContractError("private selected-pair content drifted")
    if private.get("dataset_id") != source_id:
        raise DevelopmentCatalystContractError("private selection identity differs")
    pairs = private.get("selected_pairs")
    requested = [str(item) for item in source_manifest.get("requested_dates", [])]
    daily = selection.get("daily_shortlists")
    if (
        not isinstance(pairs, list)
        or not isinstance(daily, list)
        or not requested
        or requested != [str(item.get("date")) for item in daily]
        or len(requested) != len(set(requested))
    ):
        raise DevelopmentCatalystContractError("selected-pair date surface is malformed")

    by_day: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    identities: set[tuple[str, str]] = set()
    for row in pairs:
        if not isinstance(row, Mapping):
            raise DevelopmentCatalystContractError("selected pair must be an object")
        day = str(row.get("date") or "")
        symbol = str(row.get("symbol") or "")
        instrument_id = str(row.get("instrument_id") or "")
        exchange = str(row.get("primary_exchange") or "")
        scanner_fields = row.get("scanner_fields")
        date.fromisoformat(day)
        if (
            day not in requested
            or not symbol
            or not instrument_id
            or not exchange
            or not isinstance(scanner_fields, Mapping)
        ):
            raise DevelopmentCatalystContractError("selected pair identity is incomplete")
        key = (day, symbol)
        if key in identities:
            raise DevelopmentCatalystContractError("selected pair repeats")
        identities.add(key)
        by_day[day].append(row)

    rebuilt_daily: list[dict[str, Any]] = []
    for public_day in daily:
        day = str(public_day["date"])
        ordered = sorted(by_day[day], key=lambda row: int(row.get("rank", -1)))
        if [int(row.get("rank", -1)) for row in ordered] != list(
            range(1, len(ordered) + 1)
        ):
            raise DevelopmentCatalystContractError(f"selected ranks differ for {day}")
        shortlist = [
            {
                "symbol": str(row["symbol"]),
                "instrument_id": str(row["instrument_id"]),
                "opening_relative_volume": row["scanner_fields"][
                    "opening_relative_volume"
                ],
                "opening_return": row["scanner_fields"]["opening_return"],
                "rank": int(row["rank"]),
            }
            for row in ordered
        ]
        shortlist_hash = _sha256_json(shortlist)
        if (
            len(shortlist) != int(public_day.get("shortlist_count", -1))
            or shortlist_hash != public_day.get("shortlist_sha256")
        ):
            raise DevelopmentCatalystContractError(
                f"selected shortlist differs for {day}"
            )
        rebuilt_daily.append(
            {
                "date": day,
                "shortlist_count": len(shortlist),
                "shortlist_sha256": shortlist_hash,
            }
        )
    if len(pairs) != int(selection.get("selected_pair_count", -1)):
        raise DevelopmentCatalystContractError("selected-pair count differs")
    return {
        "source_dataset_id": source_id,
        "source_manifest_sha256": str(source_manifest["manifest_sha256"]),
        "private_selection_content_sha256": expected_private_hash,
        "requested_date_count": len(requested),
        "selected_pair_count": len(pairs),
        "daily_shortlists": rebuilt_daily,
        "daily_shortlists_sha256": _sha256_json(rebuilt_daily),
        "dates_below_20": sum(
            1 for row in rebuilt_daily if row["shortlist_count"] < 20
        ),
        "minimum_shortlist_count": min(
            row["shortlist_count"] for row in rebuilt_daily
        ),
    }


def _source_rules() -> dict[str, Any]:
    return {
        "primary_evidence_only": True,
        "secondary_news_may_route_but_cannot_verify": True,
        "source_ownership_required": True,
        "issuer_binding_required": True,
        "issuer_binding_methods": sorted(semantics.ISSUER_BINDING_METHODS),
        "timestamp_precedence": list(semantics.TIMESTAMP_PRECEDENCE),
        "same_day_date_only_fails": True,
        "metadata_headers_capture_url_and_pdf_dates_cannot_independently_prove_publication": True,
        "event_taxonomy": list(semantics.EVENT_TAXONOMY),
        "financing_or_dilution_conflicts_classified_before_positive": True,
        "terminal_precedence": list(semantics.TERMINAL_PRECEDENCE),
        "one_terminal_disposition_per_pair_source_join": True,
        "selection_or_source_substitution_allowed": False,
    }


def _acquisition_contract(dataset_id: str = DATASET_ID) -> dict[str, Any]:
    return {
        "recovery_order": [
            "accession-bound SEC-operated endpoints with a compliant user agent",
            "captured transport failures under frozen pacing",
            "canonical issuer pages and document chains",
        ],
        "primary_source_replacement_with_secondary_news_allowed": False,
        "whole-source_fidelity_required": True,
        "missing_or_failed_sources_remain_terminal_rows": True,
        "paid_archive_policy": "decision memo and WAITING_SUBSCRIPTION; no automatic purchase or fabricated credential",
        "target_source_namespace": (
            "LOCAL_HISTORICAL_DATA_ROOT/"
            f"{PRIVATE_NAMESPACE}/{dataset_id}/"
        ),
    }


def _outcome_lock() -> dict[str, Any]:
    return {
        "post_entry_data_access_allowed": False,
        "return_fields_allowed": False,
        "target_outcomes_observed_or_derived": False,
        "minimum_verified_positive_pairs_before_any_outcome_contract": 20,
        "minimum_complete_unchanged_v3_non_return_survivors_before_outcomes": 20,
        "fewer_than_20_verified_positive_transition": "SOURCE_RECOVERY",
        "positive_but_fewer_than_20_survivors_transition": "DEVELOPMENT_ACQUISITION",
        "outcome_contract_must_be_separately_frozen": True,
    }


def _upstream_contract(
    source_manifest_path: Path,
    *,
    scanner_manifest_path: Path = SCANNER_MANIFEST,
    scanner_summary_path: Path = SCANNER_SUMMARY,
    scanner_inspection_path: Path = SCANNER_INSPECTION,
    security_master_source_path: Path = SECURITY_MASTER_SOURCE,
    strategy_source_path: Path = STRATEGY_SOURCE,
) -> dict[str, Any]:
    paths = {
        "selected_pair_manifest": source_manifest_path,
        "scanner_manifest": scanner_manifest_path,
        "scanner_summary": scanner_summary_path,
        "scanner_inspection": scanner_inspection_path,
        "security_master_source": security_master_source_path,
        "strategy_source": strategy_source_path,
    }
    return {
        name: {"path": _repo_path(path), "sha256": _sha256_file(path)}
        for name, path in paths.items()
    }


def _implementation_contract() -> dict[str, Any]:
    paths = {
        "contract_builder": Path(__file__),
        "source_semantics": Path(semantics.__file__),
    }
    return {
        "files": {
            name: {"path": _repo_path(path), "sha256": _sha256_file(path)}
            for name, path in paths.items()
        },
        "parser_version": semantics.PARSER_VERSION,
        "dependencies": {
            "python": sys.version.split()[0],
            "pypdf": importlib.metadata.version("pypdf"),
            "requests": importlib.metadata.version("requests"),
        },
    }


def _stable_contract(
    *,
    source_manifest_path: Path,
    env_path: Path,
    dataset_id: str = DATASET_ID,
    scanner_manifest_path: Path = SCANNER_MANIFEST,
    scanner_summary_path: Path = SCANNER_SUMMARY,
    scanner_inspection_path: Path = SCANNER_INSPECTION,
    security_master_source_path: Path = SECURITY_MASTER_SOURCE,
    strategy_source_path: Path = STRATEGY_SOURCE,
) -> tuple[dict[str, Any], HistoricalStoreConfig]:
    if not dataset_id.startswith("dataset-primary-source-semantics-contract-"):
        raise DevelopmentCatalystContractError(
            "source-semantics dataset namespace is invalid"
        )
    config = HistoricalStoreConfig.from_env(env_path)
    source_manifest = load_frozen_dataset_contract(source_manifest_path)
    selection = _rebuild_selection(source_manifest, config.root)
    artifact_count = _target_artifact_count(config.root, dataset_id)
    if artifact_count:
        raise DevelopmentCatalystContractError(
            "target source artifacts exist before the source contract freeze"
        )
    return (
        {
            "selection_contract": selection,
            "upstream_contract": _upstream_contract(
                source_manifest_path,
                scanner_manifest_path=scanner_manifest_path,
                scanner_summary_path=scanner_summary_path,
                scanner_inspection_path=scanner_inspection_path,
                security_master_source_path=security_master_source_path,
                strategy_source_path=strategy_source_path,
            ),
            "source_rules": _source_rules(),
            "acquisition_contract": _acquisition_contract(dataset_id),
            "private_record_contract": {
                "exact_rows_outside_git": True,
                "required_fields": [
                    "pair identity and rank",
                    "source hash and type",
                    "ownership evidence",
                    "issuer-binding methods",
                    "timestamp candidates and accepted UTC value",
                    "event direction and conflict",
                    "diagnostic failures",
                    "one terminal disposition",
                ],
                "public_fields": "aggregate counts, hashes, and claim boundaries only",
            },
            "outcome_lock": _outcome_lock(),
            "implementation_contract": _implementation_contract(),
            "pre_freeze_target_artifact_count": artifact_count,
        },
        config,
    )


def freeze_contract(
    *,
    source_manifest_path: Path,
    env_path: Path,
    output_root: Path,
    dataset_id: str = DATASET_ID,
    scanner_manifest_path: Path = SCANNER_MANIFEST,
    scanner_summary_path: Path = SCANNER_SUMMARY,
    scanner_inspection_path: Path = SCANNER_INSPECTION,
    security_master_source_path: Path = SECURITY_MASTER_SOURCE,
    strategy_source_path: Path = STRATEGY_SOURCE,
    selection_doc_path: Path = DEFAULT_SELECTION_DOC,
) -> tuple[Path, dict[str, Any]]:
    stable, config = _stable_contract(
        source_manifest_path=source_manifest_path,
        env_path=env_path,
        dataset_id=dataset_id,
        scanner_manifest_path=scanner_manifest_path,
        scanner_summary_path=scanner_summary_path,
        scanner_inspection_path=scanner_inspection_path,
        security_master_source_path=security_master_source_path,
        strategy_source_path=strategy_source_path,
    )
    matches = sorted(output_root.glob(f"{dataset_id}-*.json"))
    if len(matches) > 1:
        raise DevelopmentCatalystContractError("source contract has multiple manifests")
    if matches:
        existing = load_frozen_dataset_contract(matches[0])
        if any(existing.get(key) != value for key, value in stable.items()):
            raise DevelopmentCatalystContractError("existing source contract drifted")
        return matches[0], existing
    usage = shutil.disk_usage(config.root)
    if usage.free < config.min_free_bytes:
        raise DevelopmentCatalystContractError("historical-store reserve is unavailable")
    contract = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "registered_at": datetime.now(UTC).isoformat(),
        "requested_dates": [
            row["date"] for row in stable["selection_contract"]["daily_shortlists"]
        ],
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(selection_doc_path),
                _repo_path(source_manifest_path),
                _repo_path(scanner_inspection_path),
                "PRODUCTION_STRATEGY_VALIDATION.md",
            ],
            "inspected": False,
        },
        **stable,
        "capacity_contract": {
            "historical_store_outside_repository": not config.root.resolve().is_relative_to(
                PROJECT_ROOT.resolve()
            ),
            "free_bytes_at_freeze": usage.free,
            "reserve_bytes": config.min_free_bytes,
            "capacity_ready": True,
            "historical_deletion_allowed": False,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def inspect_contract(
    *,
    manifest_path: Path,
    source_manifest_path: Path,
    env_path: Path,
    status_path: Path,
    dataset_id: str = DATASET_ID,
    scanner_manifest_path: Path = SCANNER_MANIFEST,
    scanner_summary_path: Path = SCANNER_SUMMARY,
    scanner_inspection_path: Path = SCANNER_INSPECTION,
    security_master_source_path: Path = SECURITY_MASTER_SOURCE,
    strategy_source_path: Path = STRATEGY_SOURCE,
) -> dict[str, Any]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != dataset_id:
        raise DevelopmentCatalystContractError("unexpected source-contract dataset")
    stable, config = _stable_contract(
        source_manifest_path=source_manifest_path,
        env_path=env_path,
        dataset_id=dataset_id,
        scanner_manifest_path=scanner_manifest_path,
        scanner_summary_path=scanner_summary_path,
        scanner_inspection_path=scanner_inspection_path,
        security_master_source_path=security_master_source_path,
        strategy_source_path=strategy_source_path,
    )
    for key, value in stable.items():
        if manifest.get(key) != value:
            raise DevelopmentCatalystContractError(f"source contract {key} drifted")
    capacity = manifest.get("capacity_contract")
    if not isinstance(capacity, Mapping) or any(
        (
            capacity.get("capacity_ready") is not True,
            capacity.get("historical_store_outside_repository") is not True,
            capacity.get("historical_deletion_allowed") is not False,
            int(capacity.get("reserve_bytes", -1)) != config.min_free_bytes,
        )
    ):
        raise DevelopmentCatalystContractError("capacity contract is invalid")
    selection = stable["selection_contract"]
    status = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "status": "FROZEN_READY",
        "manifest_sha256": manifest["manifest_sha256"],
        "requested_dates": selection["requested_date_count"],
        "selected_pair_count": selection["selected_pair_count"],
        "daily_shortlists_sha256": selection["daily_shortlists_sha256"],
        "private_selection_content_sha256": selection[
            "private_selection_content_sha256"
        ],
        "pre_freeze_target_artifact_count": 0,
        "primary_evidence_only": True,
        "substitutions_allowed": False,
        "target_sources_accessed": False,
        "selected_symbol_detail_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "source_rules_sha256": _sha256_json(stable["source_rules"]),
        "implementation_contract_sha256": _sha256_json(
            stable["implementation_contract"]
        ),
        "valid": True,
    }
    _write_json(status_path, status)
    return status


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--dataset-id", default=DATASET_ID)
    parser.add_argument("--source-manifest", type=Path, default=SOURCE_MANIFEST)
    parser.add_argument("--scanner-manifest", type=Path, default=SCANNER_MANIFEST)
    parser.add_argument("--scanner-summary", type=Path, default=SCANNER_SUMMARY)
    parser.add_argument("--scanner-inspection", type=Path, default=SCANNER_INSPECTION)
    parser.add_argument(
        "--security-master-source", type=Path, default=SECURITY_MASTER_SOURCE
    )
    parser.add_argument("--strategy-source", type=Path, default=STRATEGY_SOURCE)
    parser.add_argument("--selection-doc", type=Path, default=DEFAULT_SELECTION_DOC)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("freeze")
    inspect = subparsers.add_parser("inspect")
    inspect.add_argument("manifest", type=Path)
    inspect.add_argument("--status", type=Path, default=DEFAULT_PUBLIC_STATUS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, manifest = freeze_contract(
                source_manifest_path=args.source_manifest,
                env_path=args.env,
                output_root=args.output_root,
                dataset_id=args.dataset_id,
                scanner_manifest_path=args.scanner_manifest,
                scanner_summary_path=args.scanner_summary,
                scanner_inspection_path=args.scanner_inspection,
                security_master_source_path=args.security_master_source,
                strategy_source_path=args.strategy_source,
                selection_doc_path=args.selection_doc,
            )
            value = {
                "dataset_id": args.dataset_id,
                "manifest_sha256": manifest["manifest_sha256"],
                "path": str(path),
                "selected_pair_count": manifest["selection_contract"][
                    "selected_pair_count"
                ],
            }
        else:
            value = inspect_contract(
                manifest_path=args.manifest,
                source_manifest_path=args.source_manifest,
                env_path=args.env,
                status_path=args.status,
                dataset_id=args.dataset_id,
                scanner_manifest_path=args.scanner_manifest,
                scanner_summary_path=args.scanner_summary,
                scanner_inspection_path=args.scanner_inspection,
                security_master_source_path=args.security_master_source,
                strategy_source_path=args.strategy_source,
            )
        print(json.dumps(value, indent=2, sort_keys=True))
        return 0
    except (
        DevelopmentCatalystContractError,
        HistoricalStoreError,
        LearningDataError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
