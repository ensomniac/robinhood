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
from learning_data import LearningDataError, load_frozen_dataset_contract
from scanner_replay import ScannerReplayError


class ChallengerAcquisitionInspectionError(RuntimeError):
    """Independent acquisition reconstruction found a mismatch."""


def inspect(
    *, manifest_path: Path, scanner_manifest_path: Path, env_path: Path
) -> dict[str, Any]:
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
        "source_rules_sha256": expected_source["rules_sha256"],
        "substitutions_allowed": False,
        "target_outcomes_observed_or_derived": False,
        "valid": True,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("scanner_manifest", type=Path)
    parser.add_argument("--env", type=Path, default=acquisition.PROJECT_ROOT / ".env")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
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
