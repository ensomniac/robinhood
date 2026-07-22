"""Inspect the frozen retest entry-qualification collection without outcomes."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from collections.abc import Mapping
from datetime import datetime, time
from pathlib import Path
from typing import Any

import challenger_orb_retest_qualification_collection as challenger
from historical_store import HistoricalStoreError
from learning_data import LearningDataError, load_frozen_dataset_contract


class ChallengerQualificationCollectionInspectionError(RuntimeError):
    """The qualification collection contract or terminal data does not rebuild."""


def inspect_contract(
    *, manifest_path: Path, env_path: Path, status_path: Path
) -> dict[str, Any]:
    challenger.configure_base()
    return challenger.base.inspect_contract(
        manifest_path=manifest_path,
        env_path=env_path,
        public_status_path=status_path,
    )


def inspect_collection(
    *, manifest_path: Path, env_path: Path, status_path: Path
) -> dict[str, Any]:
    challenger.configure_base()
    collection = challenger.base
    manifest = load_frozen_dataset_contract(manifest_path)
    base_manifest, _public, private, store = challenger._load_base(env_path)
    expected = collection._expected_contract(
        base_manifest=base_manifest,
        provider_contract=collection._provider_contract(env_path),
        free_bytes=int(manifest["capacity_contract"]["observed_free_bytes_at_freeze"]),
    )
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise ChallengerQualificationCollectionInspectionError(
                f"qualification collection manifest drifted at {key}"
            )
    index_path = collection._index_path(store.root)
    index = collection._read_gzip(index_path)
    pairs = private["selection"]["positive_pairs"]
    states = [collection._read_gzip(collection._pair_path(store.root, pair)) for pair in pairs]
    indexed = {
        str(row["pair_key"]): str(row["sha256"])
        for row in index.get("pair_files", [])
        if isinstance(row, Mapping)
    }
    if not (
        index.get("manifest_sha256") == manifest["manifest_sha256"]
        and index.get("pairs_expected") == challenger.EXPECTED_PAIRS
        and len(indexed) == challenger.EXPECTED_PAIRS
        and index.get("target_outcomes_observed_or_derived") is False
        and len(states) == challenger.EXPECTED_PAIRS
    ):
        raise ChallengerQualificationCollectionInspectionError(
            "qualification collection index differs"
        )
    terminal = Counter()
    total_rows = 0
    origins: Counter[str] = Counter()
    for pair, state in zip(pairs, states, strict=True):
        path = collection._pair_path(store.root, pair)
        trigger = pair["trigger"]
        if not (
            indexed.get(collection._pair_key(pair)) == collection._sha256_file(path)
            and state.get("manifest_sha256") == manifest["manifest_sha256"]
            and state.get("status") == "TERMINAL"
            and state.get("terminal_disposition") == "PREENTRY_INPUTS_COLLECTED"
            and state.get("target_outcome_observed_or_derived") is False
            and state.get("provider_rows_after_final_decision") is False
            and state.get("frozen_retest_trigger") == trigger
            and state.get("clean_cross", {}).get("observed_at_et")
            == trigger["rebreak_at_et"]
            and float(state.get("clean_cross", {}).get("price", 0))
            == float(trigger["rebreak_price"])
            and state.get("final_decision_at_et") == trigger["decision_at_et"]
            and state.get("search_windows_complete") == 0
            and state.get("search_minute_files") == []
            and state.get("active_search_minute") is None
        ):
            raise ChallengerQualificationCollectionInspectionError(
                "terminal pair does not match its frozen retest trigger"
            )
        final_at = datetime.fromisoformat(str(state["final_decision_at_et"]))
        if final_at.timetz().replace(tzinfo=None) > time(10, 30):
            raise ChallengerQualificationCollectionInspectionError(
                "qualification decision exceeds the frozen cutoff"
            )
        requests = state.get("requests")
        if not isinstance(requests, Mapping):
            raise ChallengerQualificationCollectionInspectionError(
                "terminal pair request evidence is missing"
            )
        for name in ("opening_bars", "decision_trade_prefix", "quote_window", "premarket", "history"):
            request = requests.get(name)
            rows = request.get("observations") if isinstance(request, Mapping) else None
            if not (
                isinstance(rows, list)
                and collection._sha256_json(rows) == request.get("sha256")
                and len(rows) == request.get("rows")
            ):
                raise ChallengerQualificationCollectionInspectionError(
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
            raise ChallengerQualificationCollectionInspectionError(
                "completed decision-bar prefixes are incomplete"
            )
        for request in completed.values():
            rows = request.get("observations") if isinstance(request, Mapping) else None
            if not (
                isinstance(rows, list)
                and collection._sha256_json(rows) == request.get("sha256")
                and len(rows) == request.get("rows")
            ):
                raise ChallengerQualificationCollectionInspectionError(
                    "completed decision bars do not rehash"
                )
            total_rows += len(rows)
            origins[str(request.get("origin"))] += 1
        quote = requests["quote_window"]
        clean_at = datetime.fromisoformat(str(trigger["rebreak_at_et"]))
        if collection.quote_snapshots(quote["observations"], clean_at) != quote.get(
            "snapshots"
        ):
            raise ChallengerQualificationCollectionInspectionError(
                "three causal quote snapshots do not rebuild"
            )
        history = requests["history"]
        if int(history.get("required_sessions", 0)) != 252:
            raise ChallengerQualificationCollectionInspectionError(
                "prior-session denominator differs"
            )
        terminal[str(state["terminal_disposition"])] += 1
    if terminal != Counter({"PREENTRY_INPUTS_COLLECTED": challenger.EXPECTED_PAIRS}):
        raise ChallengerQualificationCollectionInspectionError(
            "terminal qualification denominator differs"
        )
    result = {
        "schema_version": 1,
        "dataset_id": challenger.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "COLLECTION_INSPECTED",
        "inspected": True,
        "pairs_expected": challenger.EXPECTED_PAIRS,
        "pairs_terminal": challenger.EXPECTED_PAIRS,
        "distinct_trigger_sessions": challenger.EXPECTED_DATES,
        "terminal_counts": dict(sorted(terminal.items())),
        "request_origin_counts": dict(sorted(origins.items())),
        "causal_rows_rehashed": total_rows,
        "private_index_sha256": collection._sha256_file(index_path),
        "inspection": {
            "selection_rebuilt": True,
            "all_pair_files_rehashed": True,
            "all_causal_requests_rehashed": True,
            "frozen_retest_triggers_reconciled": True,
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
    collection._write_json(status_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect-contract", "inspect-collection"))
    parser.add_argument("manifest", type=Path)
    parser.add_argument(
        "--env-file", type=Path, default=challenger.PROJECT_ROOT / ".env"
    )
    parser.add_argument(
        "--contract-status", type=Path, default=challenger.DEFAULT_CONTRACT_STATUS
    )
    parser.add_argument(
        "--collection-status", type=Path, default=challenger.DEFAULT_COLLECTION_STATUS
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "inspect-contract":
            value = inspect_contract(
                manifest_path=args.manifest,
                env_path=args.env_file,
                status_path=args.contract_status,
            )
        else:
            value = inspect_collection(
                manifest_path=args.manifest,
                env_path=args.env_file,
                status_path=args.collection_status,
            )
    except (
        ChallengerQualificationCollectionInspectionError,
        challenger.ChallengerQualificationCollectionError,
        challenger.base.DevelopmentNonReturnCollectionError,
        HistoricalStoreError,
        LearningDataError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
