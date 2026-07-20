"""Independent entrypoint for the challenger acquisition zero-state audit."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from collections.abc import Mapping
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import challenger_orb_retest_acquisition as acquisition
from historical_store import HistoricalDayStore, HistoricalStoreConfig
from learning_data import (
    LearningDataError,
    load_frozen_dataset_contract,
    load_security_master,
    security_master_sha256,
)
from scanner_replay import ScannerReplayError


class ChallengerAcquisitionInspectionError(RuntimeError):
    """Independent acquisition reconstruction found a mismatch."""


def inspect_reference() -> dict[str, Any]:
    selection = acquisition._selection()
    expected_dates = sorted(selection["selected_dates"])
    expected_names = {f"{day}.json.gz" for day in expected_dates}
    all_files = sorted(
        path for path in acquisition.REFERENCE_ROOT.rglob("*") if path.is_file()
    )
    temporary = sorted(
        path.relative_to(acquisition.REFERENCE_ROOT).as_posix()
        for path in all_files
        if path.suffix == ".tmp"
    )
    observed_paths = sorted(
        path
        for path in all_files
        if path.parent == acquisition.REFERENCE_ROOT and path.name in expected_names
    )
    unexpected = sorted(
        path.relative_to(acquisition.REFERENCE_ROOT).as_posix()
        for path in all_files
        if path not in observed_paths and path.suffix != ".tmp"
    )
    if unexpected or temporary:
        raise ChallengerAcquisitionInspectionError(
            "reference cache contains unexpected or temporary artifacts"
        )

    raw_snapshots: list[dict[str, Any]] = []
    logical_snapshots: list[dict[str, Any]] = []
    ready_dates: set[str] = set()
    for path in observed_paths:
        day = path.name.removesuffix(".json.gz")
        rows = acquisition._read_gzip_array(path)
        symbols = [str(row.get("ticker") or "").strip().upper() for row in rows]
        if (
            not rows
            or any(not symbol for symbol in symbols)
            or len(symbols) != len(set(symbols))
        ):
            raise ChallengerAcquisitionInspectionError(
                f"reference snapshot is empty or ambiguous: {day}"
            )
        ready_dates.add(day)
        raw_snapshots.append(
            {"date": day, "rows": len(rows), "sha256": acquisition._sha256_file(path)}
        )
        logical_snapshots.append(
            {
                "date": day,
                "rows": len(rows),
                "content_sha256": acquisition._sha256_json(rows),
            }
        )

    missing = sorted(set(expected_dates) - ready_dates)
    controller = acquisition.reference_status()
    rebuilt = {
        "ready": len(raw_snapshots),
        "missing": len(missing),
        "snapshot_set_sha256": acquisition._sha256_json(raw_snapshots),
        "logical_snapshot_set_sha256": acquisition._sha256_json(logical_snapshots),
    }
    if any(controller.get(key) != value for key, value in rebuilt.items()):
        raise ChallengerAcquisitionInspectionError(
            "controller reference aggregate does not independently rebuild"
        )
    return {
        "schema_version": 1,
        "dataset_id": acquisition.tranche.DATASET_ID,
        "status": "REFERENCE_READY" if not missing else "REFERENCE_PARTIAL",
        "requested": len(expected_dates),
        **rebuilt,
        "unexpected_snapshots": 0,
        "temporary_artifacts": 0,
        "date_substitution_allowed": False,
        "target_market_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "valid": True,
    }


def inspect_inputs(*, env_path: Path) -> dict[str, Any]:
    selection = acquisition._selection()
    expected_dates = sorted(selection["selected_dates"])
    source = acquisition._read_object(acquisition.SECURITY_SOURCE)
    snapshot_rows = source.get("snapshots")
    if (
        source.get("requested_dates") != expected_dates
        or not isinstance(snapshot_rows, list)
        or [str(row.get("date") or "") for row in snapshot_rows] != expected_dates
    ):
        raise ChallengerAcquisitionInspectionError(
            "security-master source dates differ"
        )
    snapshot_hashes: list[dict[str, Any]] = []
    logical_snapshot_hashes: list[dict[str, Any]] = []
    for public in snapshot_rows:
        day = str(public["date"])
        path = acquisition.REFERENCE_ROOT / f"{day}.json.gz"
        rows = acquisition._read_gzip_array(path)
        symbols = [str(row.get("ticker") or "").strip().upper() for row in rows]
        observed = {
            "date": day,
            "rows": len(rows),
            "sha256": acquisition._sha256_file(path),
        }
        if (
            observed != public
            or not rows
            or any(not symbol for symbol in symbols)
            or len(symbols) != len(set(symbols))
        ):
            raise ChallengerAcquisitionInspectionError(
                f"reference snapshot differs: {day}"
            )
        snapshot_hashes.append(observed)
        logical_snapshot_hashes.append(
            {
                "date": day,
                "rows": len(rows),
                "content_sha256": acquisition._sha256_json(rows),
            }
        )

    master = load_security_master(acquisition.SECURITY_MASTER)
    master_public = source.get("security_master")
    if not isinstance(master_public, Mapping) or any(
        (
            master_public.get("sha256")
            != security_master_sha256(acquisition.SECURITY_MASTER),
            master_public.get("records") != len(master),
            master_public.get("instruments")
            != len({str(row["instrument_id"]) for row in master}),
        )
    ):
        raise ChallengerAcquisitionInspectionError("security master differs")

    split_source = acquisition._read_object(acquisition.SPLIT_SOURCE)
    split_rows = acquisition._read_gzip_array(acquisition.SPLITS)
    split_artifact = split_source.get("artifact")
    split_provider = split_source.get("source")
    ordered: list[tuple[str, str]] = []
    for row in split_rows:
        execution = str(row.get("execution_date") or "")
        ticker = str(row.get("ticker") or "").strip().upper()
        try:
            split_from = float(row["split_from"])
            split_to = float(row["split_to"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ChallengerAcquisitionInspectionError(
                "split action is malformed"
            ) from exc
        if (
            not "2023-01-04" <= execution <= "2024-12-24"
            or not ticker
            or split_from <= 0
            or split_to <= 0
        ):
            raise ChallengerAcquisitionInspectionError(
                "split action is outside the frozen query"
            )
        ordered.append((execution, ticker))
    if (
        not split_rows
        or ordered != sorted(ordered)
        or not isinstance(split_artifact, Mapping)
        or not isinstance(split_provider, Mapping)
        or split_artifact.get("events") != len(split_rows)
        or split_artifact.get("sha256") != acquisition._sha256_file(acquisition.SPLITS)
        or split_provider.get("provider") != "Massive"
        or split_provider.get("endpoint") != "https://api.massive.com/stocks/v1/splits"
        or split_provider.get("query_range")
        != {
            "execution_date_gte": "2023-01-04",
            "execution_date_lte": "2024-12-24",
        }
    ):
        raise ChallengerAcquisitionInspectionError("split actions differ")

    config = HistoricalStoreConfig.from_env(env_path)
    store = HistoricalDayStore(config.root)
    market_root = acquisition.alpaca.index_root(store, acquisition.SCANNER_DATASET_ID)
    market_artifacts = (
        sum(1 for path in market_root.rglob("*") if path.is_file())
        if market_root.exists()
        else 0
    )
    if market_artifacts or shutil.disk_usage(config.root).free < config.min_free_bytes:
        raise ChallengerAcquisitionInspectionError(
            "market zero-state or reserve differs"
        )
    return {
        "snapshot_count": len(snapshot_hashes),
        "snapshot_set_sha256": acquisition._sha256_json(snapshot_hashes),
        "logical_snapshot_set_sha256": acquisition._sha256_json(
            logical_snapshot_hashes
        ),
        "security_master_sha256": master_public["sha256"],
        "security_master_records": len(master),
        "split_actions_sha256": split_artifact["sha256"],
        "split_action_events": len(split_rows),
        "pre_freeze_target_market_artifacts": market_artifacts,
        "capacity_ready": True,
    }


def inspect(
    *, manifest_path: Path, scanner_manifest_path: Path, env_path: Path
) -> dict[str, Any]:
    input_evidence = inspect_inputs(env_path=env_path)
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != acquisition.DATASET_ID:
        raise ChallengerAcquisitionInspectionError(
            "acquisition dataset identity differs"
        )
    implementations = manifest.get("implementation_contract")
    upstream = manifest.get("upstream_contract")
    if not isinstance(implementations, Mapping) or not isinstance(upstream, Mapping):
        raise ChallengerAcquisitionInspectionError("acquisition bindings are missing")
    for binding in (*implementations.values(), *upstream.values()):
        if not isinstance(binding, Mapping):
            raise ChallengerAcquisitionInspectionError(
                "acquisition binding is malformed"
            )
        acquisition._verify_binding(binding)
    scanner_binding = upstream.get("scanner_manifest")
    if (
        not isinstance(scanner_binding, Mapping)
        or (acquisition.PROJECT_ROOT / str(scanner_binding.get("path"))).resolve()
        != scanner_manifest_path.resolve()
    ):
        raise ChallengerAcquisitionInspectionError(
            "scanner manifest path differs from its binding"
        )

    config = HistoricalStoreConfig.from_env(env_path)
    store = HistoricalDayStore(config.root)
    scanner_manifest, scanner_status = acquisition._validate_scanner_manifest(
        scanner_manifest_path, store=store
    )
    zero_state = {
        "ready_sessions": scanner_status["session_files"]["ready"],
        "provider_requests": scanner_status["provider_requests"],
        "provider_retries": scanner_status["provider_retries"],
        "derived_rows": scanner_status["derived_rows"],
        "canonical_day_merges": scanner_status["canonical_day_merges"],
    }
    if any(zero_state.values()):
        raise ChallengerAcquisitionInspectionError(
            "target scanner artifacts exist before collection"
        )

    selection = acquisition._selection()
    requested = sorted(selection["selected_dates"])
    market = manifest.get("full_universe_market_contract")
    source = manifest.get("primary_source_contract")
    detail = manifest.get("selected_symbol_contract")
    outcome_lock = manifest.get("outcome_lock")
    capacity = manifest.get("capacity_contract")
    reference = manifest.get("reference_identity_contract")
    expected_source = acquisition._source_rules_contract()
    expected_provider_config = acquisition._provider_config_contract(env_path)
    if any(
        (
            manifest.get("requested_dates") != requested,
            not isinstance(market, Mapping),
            not isinstance(source, Mapping),
            not isinstance(detail, Mapping),
            not isinstance(outcome_lock, Mapping),
            not isinstance(capacity, Mapping),
            not isinstance(reference, Mapping),
            source != expected_source,
            market.get("scanner_dataset_id") != acquisition.SCANNER_DATASET_ID,
            market.get("scanner_manifest_sha256")
            != scanner_manifest.get("manifest_sha256"),
            market.get("required_sessions_sha256")
            != selection.get("required_sessions_sha256"),
            market.get("complete_universe") is not True,
            market.get("substitutions_allowed") is not False,
            reference.get("requested_dates_sha256")
            != selection.get("selected_dates_sha256"),
            reference.get("requested_date_count") != len(requested),
            reference.get("provider_config") != expected_provider_config["reference"],
            market.get("provider_config") != expected_provider_config["market"],
            detail.get("full_universe_detail_forbidden") is not True,
            detail.get("scanner_selected_symbols_only") is not True,
            detail.get("exact_pair_graph_requires_separate_post-scanner_freeze")
            is not True,
            outcome_lock.get("post_entry_data_access_allowed") is not False,
            outcome_lock.get("return_fields_allowed") is not False,
            outcome_lock.get("target_outcomes_observed_or_derived") is not False,
            capacity.get("capacity_ready") is not True,
            capacity.get("historical_store_outside_repository") is not True,
            capacity.get("historical_deletion_allowed") is not False,
            capacity.get("reserve_bytes") != config.min_free_bytes,
            shutil.disk_usage(config.root).free < config.min_free_bytes,
        )
    ):
        raise ChallengerAcquisitionInspectionError(
            "acquisition contract does not independently rebuild"
        )
    return {
        "schema_version": 1,
        "dataset_id": acquisition.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "FROZEN_READY",
        "inspected": True,
        "requested_dates": len(requested),
        "required_sessions": market["required_session_count"],
        "pre_freeze_target_market_artifacts": sum(zero_state.values()),
        "input_evidence": input_evidence,
        "source_rules_sha256": expected_source["rules_sha256"],
        "substitutions_allowed": False,
        "target_outcomes_observed_or_derived": False,
        "valid": True,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path, nargs="?")
    parser.add_argument("scanner_manifest", type=Path, nargs="?")
    parser.add_argument("--env", type=Path, default=acquisition.PROJECT_ROOT / ".env")
    parser.add_argument("--reference-only", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.reference_only:
            if args.manifest is not None or args.scanner_manifest is not None:
                raise ChallengerAcquisitionInspectionError(
                    "reference-only inspection does not accept manifests"
                )
            result = inspect_reference()
        else:
            if args.manifest is None or args.scanner_manifest is None:
                raise ChallengerAcquisitionInspectionError(
                    "outer and scanner manifests are required"
                )
            result = inspect(
                manifest_path=args.manifest,
                scanner_manifest_path=args.scanner_manifest,
                env_path=args.env,
            )
            acquisition._write_json(acquisition.DEFAULT_STATUS, result)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        ChallengerAcquisitionInspectionError,
        acquisition.ChallengerAcquisitionError,
        LearningDataError,
        ScannerReplayError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
