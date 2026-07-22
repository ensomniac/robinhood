"""Independently inspect the timezone-compatible qualification collection."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from collections.abc import Mapping
from datetime import datetime, time
from pathlib import Path
from typing import Any

import challenger_orb_retest_qualification_collection as collection
import challenger_orb_retest_qualification_collection_compatibility as compat
from historical_store import HistoricalStoreError
from learning_data import LearningDataError


class ChallengerQualificationCompatibilityInspectionError(RuntimeError):
    """The compatibility manifest or full causal collection does not rebuild."""


def _verify_binding(binding: Any) -> None:
    if not isinstance(binding, Mapping):
        raise ChallengerQualificationCompatibilityInspectionError(
            "implementation binding is malformed"
        )
    path = compat.PROJECT_ROOT / str(binding.get("path"))
    if compat._sha256_file(path) != binding.get("sha256"):
        raise ChallengerQualificationCompatibilityInspectionError(
            "compatibility implementation binding differs"
        )


def inspect(
    *,
    manifest_path: Path,
    env_path: Path,
    compatibility_status_path: Path,
    collection_status_path: Path,
    result_path: Path,
) -> dict[str, Any]:
    manifest = compat.load_manifest(manifest_path)
    compatibility_status = compat._read_json(compatibility_status_path)
    source = manifest["source_contract"]
    if not (
        compatibility_status.get("manifest_sha256")
        == manifest["manifest_sha256"]
        and compatibility_status.get("status") == "FROZEN_WAITING_INSPECTION"
        and compatibility_status.get("inspected") is False
        and compatibility_status.get("post_entry_data_access_allowed") is False
        and compatibility_status.get("target_outcomes_observed_or_derived") is False
        and compat._sha256_file(compat.SOURCE_MANIFEST)
        == source["collection_manifest_file_sha256"]
        and compat._sha256_file(compat.SOURCE_STATUS)
        == source["collection_status_file_sha256"]
        and not result_path.exists()
    ):
        raise ChallengerQualificationCompatibilityInspectionError(
            "compatibility zero-result state differs"
        )
    for binding in manifest["implementation_contract"].values():
        _verify_binding(binding)
    rebuilt_snapshot = compat.build_compatibility_snapshot(env_path)
    if rebuilt_snapshot != manifest["incident_snapshot"]:
        raise ChallengerQualificationCompatibilityInspectionError(
            "timezone compatibility incident snapshot differs"
        )

    source_manifest, _status, pairs, states, store = compat._source_state(env_path)
    index_path = compat._index_path(store.root)
    index = collection.base._read_gzip(index_path)
    indexed = {
        str(row["pair_key"]): str(row["sha256"])
        for row in index.get("pair_files", [])
        if isinstance(row, Mapping)
    }
    terminal: Counter[str] = Counter()
    origins: Counter[str] = Counter()
    total_rows = 0
    reconciled = 0
    if not (
        index.get("manifest_sha256") == compat.SOURCE_MANIFEST_SHA256
        and index.get("pairs_expected") == collection.EXPECTED_PAIRS
        and len(indexed) == collection.EXPECTED_PAIRS
        and index.get("target_outcomes_observed_or_derived") is False
        and len(states) == collection.EXPECTED_PAIRS
    ):
        raise ChallengerQualificationCompatibilityInspectionError(
            "qualification collection index differs"
        )

    for pair, state in zip(pairs, states, strict=True):
        path = compat._pair_path(store.root, pair)
        trigger = pair["trigger"]
        observed = state.get("clean_cross", {}).get("observed_at_et")
        decision = state.get("final_decision_at_et")
        if not (
            indexed.get(compat._pair_key(pair))
            == collection.base._sha256_file(path)
            and state.get("manifest_sha256")
            == source_manifest["manifest_sha256"]
            and state.get("status") == "TERMINAL"
            and state.get("terminal_disposition") == "PREENTRY_INPUTS_COLLECTED"
            and state.get("target_outcome_observed_or_derived") is False
            and state.get("provider_rows_after_final_decision") is False
            and state.get("frozen_retest_trigger") == trigger
            and compat.same_instant(observed, trigger["rebreak_at_et"])
            and float(state.get("clean_cross", {}).get("price", 0))
            == float(trigger["rebreak_price"])
            and compat.same_instant(decision, trigger["decision_at_et"])
            and state.get("search_windows_complete") == 0
            and state.get("search_minute_files") == []
            and state.get("active_search_minute") is None
        ):
            raise ChallengerQualificationCompatibilityInspectionError(
                "terminal pair differs beyond timezone representation"
            )
        final_at = compat.parse_aware(decision)
        if final_at.astimezone(collection.base.EASTERN).time() > time(10, 30):
            raise ChallengerQualificationCompatibilityInspectionError(
                "qualification decision exceeds the frozen cutoff"
            )
        requests = state.get("requests")
        if not isinstance(requests, Mapping):
            raise ChallengerQualificationCompatibilityInspectionError(
                "terminal pair request evidence is missing"
            )
        for name in (
            "opening_bars",
            "decision_trade_prefix",
            "quote_window",
            "premarket",
            "history",
        ):
            request = requests.get(name)
            rows = request.get("observations") if isinstance(request, Mapping) else None
            if not (
                isinstance(rows, list)
                and collection.base._sha256_json(rows) == request.get("sha256")
                and len(rows) == request.get("rows")
            ):
                raise ChallengerQualificationCompatibilityInspectionError(
                    f"{name} observations do not rehash"
                )
            total_rows += len(rows)
            origins[str(request.get("origin"))] += 1
        completed = requests.get("completed_bars")
        if not isinstance(completed, Mapping) or set(completed) != {
            str(pair["symbol"]),
            "SPY",
            "QQQ",
        }:
            raise ChallengerQualificationCompatibilityInspectionError(
                "completed decision-bar prefixes are incomplete"
            )
        for request in completed.values():
            rows = request.get("observations") if isinstance(request, Mapping) else None
            if not (
                isinstance(rows, list)
                and collection.base._sha256_json(rows) == request.get("sha256")
                and len(rows) == request.get("rows")
            ):
                raise ChallengerQualificationCompatibilityInspectionError(
                    "completed decision bars do not rehash"
                )
            total_rows += len(rows)
            origins[str(request.get("origin"))] += 1
        quote = requests["quote_window"]
        if collection.base.quote_snapshots(
            quote["observations"], compat.parse_aware(trigger["rebreak_at_et"])
        ) != quote.get("snapshots"):
            raise ChallengerQualificationCompatibilityInspectionError(
                "three causal quote snapshots do not rebuild"
            )
        if int(requests["history"].get("required_sessions", 0)) != 252:
            raise ChallengerQualificationCompatibilityInspectionError(
                "prior-session denominator differs"
            )
        halts = requests.get("halts")
        if not (
            isinstance(halts, Mapping)
            and isinstance(halts.get("causal_records"), list)
            and len(halts["causal_records"]) == halts.get("causal_record_count")
            and all(
                datetime.fromisoformat(str(row["halted_at_et"])) <= final_at
                for row in halts["causal_records"]
            )
        ):
            raise ChallengerQualificationCompatibilityInspectionError(
                "causal halt evidence differs"
            )
        terminal[str(state["terminal_disposition"])] += 1
        reconciled += 1

    if terminal != Counter(
        {"PREENTRY_INPUTS_COLLECTED": collection.EXPECTED_PAIRS}
    ):
        raise ChallengerQualificationCompatibilityInspectionError(
            "terminal qualification denominator differs"
        )
    result = {
        "schema_version": 1,
        "dataset_id": collection.DATASET_ID,
        "source_manifest_sha256": compat.SOURCE_MANIFEST_SHA256,
        "inspection_contract_id": compat.DATASET_ID,
        "inspection_manifest_sha256": manifest["manifest_sha256"],
        "status": "COLLECTION_INSPECTED",
        "inspected": True,
        "pairs_expected": collection.EXPECTED_PAIRS,
        "pairs_terminal": collection.EXPECTED_PAIRS,
        "distinct_trigger_sessions": collection.EXPECTED_DATES,
        "terminal_counts": dict(sorted(terminal.items())),
        "request_origin_counts": dict(sorted(origins.items())),
        "causal_rows_rehashed": total_rows,
        "timezone_representation_pairs_reconciled": reconciled,
        "maximum_absolute_timestamp_delta_seconds": 0,
        "private_index_sha256": collection.base._sha256_file(index_path),
        "inspection": {
            "selection_rebuilt": True,
            "all_pair_files_rehashed": True,
            "all_causal_requests_rehashed": True,
            "frozen_retest_triggers_reconciled": True,
            "timestamps_compared_as_exact_utc_instants": True,
            "three_snapshot_quotes_rebuilt": True,
            "prior_session_denominators_rebuilt": True,
            "privacy_and_outcome_locks_rebuilt": True,
        },
        "symbols_dates_instrument_ids_raw_rows_and_requests_public": False,
        "provider_rows_after_final_decision": False,
        "post_entry_data_access_allowed": False,
        "target_outcomes_observed_or_derived": False,
        "valid": True,
    }
    compatibility_ready = {
        "schema_version": 1,
        "dataset_id": compat.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "inspected": True,
        "source_collection_status": "COLLECTION_INSPECTED",
        "source_pairs": collection.EXPECTED_PAIRS,
        "semantically_equal_timestamp_pairs": reconciled,
        "maximum_absolute_timestamp_delta_seconds": 0,
        "result_path": collection.base._repo_path(result_path),
        "post_entry_data_access_allowed": False,
        "target_outcomes_observed_or_derived": False,
        "valid": True,
    }
    compat._write_json(result_path, result)
    compat._write_json(collection_status_path, result)
    compat._write_json(compatibility_status_path, compatibility_ready)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--env-file", type=Path, default=compat.PROJECT_ROOT / ".env")
    parser.add_argument("--compatibility-status", type=Path, default=compat.DEFAULT_STATUS)
    parser.add_argument("--collection-status", type=Path, default=compat.SOURCE_STATUS)
    parser.add_argument("--result", type=Path, default=compat.DEFAULT_RESULT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        value = inspect(
            manifest_path=args.manifest,
            env_path=args.env_file,
            compatibility_status_path=args.compatibility_status,
            collection_status_path=args.collection_status,
            result_path=args.result,
        )
    except (
        ChallengerQualificationCompatibilityInspectionError,
        compat.ChallengerQualificationCompatibilityError,
        collection.ChallengerQualificationCollectionError,
        collection.base.DevelopmentNonReturnCollectionError,
        HistoricalStoreError,
        LearningDataError,
        OSError,
        TypeError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
