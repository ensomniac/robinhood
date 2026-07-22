"""Collect outcome-blind entry-qualification inputs for all frozen retest triggers."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import challenger_orb_retest_cumulative_capacity as capacity
import development_non_return_collection as base
from historical_providers import HistoricalProviderError
from historical_store import HistoricalDayStore, HistoricalStoreError
from learning_data import LearningDataError


PROJECT_ROOT = Path(__file__).resolve().parent
BASE_PATH = Path(base.__file__).resolve()
BASE_IMPLEMENTATION_SHA256 = (
    "168d17dd1a233c6ccf14b775a41cc64e177f0ca16ac600f3849d8111b86aca23"
)
DATASET_ID = (
    "dataset-challenger-orb-retest-entry-qualification-collection-2026-07-22-v1"
)
CAPACITY_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/"
    "cumulative_capacity_manifests/"
    "dataset-challenger-orb-retest-cumulative-trigger-capacity-2026-07-22-v1-"
    "94a6e83110427b5c7d7d371e5a36292f7a657b9f80d955b5e30982cccd3af21a.json"
)
CAPACITY_RESULT = capacity.DEFAULT_RESULT
CAPACITY_STATUS = capacity.DEFAULT_STATUS
INSPECTOR = PROJECT_ROOT / "challenger_orb_retest_qualification_collection_inspection.py"
DEFAULT_MANIFEST_ROOT = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/"
    "qualification_collection_manifests"
)
DEFAULT_CONTRACT_STATUS = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/"
    "qualification-collection-contract-status.json"
)
DEFAULT_COLLECTION_STATUS = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/"
    "qualification-collection-status.json"
)
PRIVATE_NAMESPACE = "_derived/challenger_orb_retest_qualification_collection"
EXPECTED_PAIRS = 60
EXPECTED_DATES = 52
CALENDAR_QUERY_START = "2021-01-01"
CALENDAR_QUERY_END = "2026-07-17"
MINIMUM_RESERVE_BYTES = 20 * 1024**3


class ChallengerQualificationCollectionError(RuntimeError):
    """The retest qualification collection adapter or source evidence differs."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class _SourceContract:
    DATASET_ID = capacity.DATASET_ID
    CALENDAR_QUERY_START = CALENDAR_QUERY_START
    CALENDAR_QUERY_END = CALENDAR_QUERY_END
    MINIMUM_RESERVE_BYTES = MINIMUM_RESERVE_BYTES

    @staticmethod
    def _sha256_json(value: Any) -> str:
        return capacity._sha256_json(value)


def build_trigger_selection(store_root: Path) -> dict[str, Any]:
    """Rebuild all 60 causal trigger pairs without exposing their identities."""

    summary, _trigger_sets, _corpus_sets = capacity.build_capacity_state(store_root)
    if not (
        summary["minimum_development_signal_capacity_passed"] is True
        and summary["cumulative_distinct_trigger_sessions"] == EXPECTED_DATES
        and summary["cumulative_trigger_pairs"] == EXPECTED_PAIRS
        and summary["pairwise_source_session_intersections"] == [0, 0, 0]
        and summary["pairwise_trigger_session_intersections"] == [0, 0, 0]
    ):
        raise ChallengerQualificationCollectionError(
            "inspected cumulative trigger capacity differs"
        )
    pairs: list[dict[str, Any]] = []
    for spec in capacity.SOURCE_SPECS:
        private_path = capacity._private_trigger_path(
            store_root, str(spec["dataset_id"])
        )
        private = capacity._read_gzip(private_path)
        if capacity._sha256_file(private_path) != spec["expected_private_sha256"]:
            raise ChallengerQualificationCollectionError(
                "private trigger source hash differs"
            )
        for row in private["records"]:
            result = row.get("result", {})
            if result.get("terminal_reason") != "TRIGGER_FOUND":
                continue
            trigger = result.get("trigger")
            if not isinstance(trigger, Mapping):
                raise ChallengerQualificationCollectionError(
                    "trigger source lacks a causal trigger record"
                )
            pairs.append(
                {
                    "date": str(row["date"]),
                    "symbol": str(row["symbol"]),
                    "instrument_id": str(row["instrument_id"]),
                    "rank": int(row["rank"]),
                    "scanner_fields": dict(row["scanner_fields"]),
                    "trigger": dict(trigger),
                    "source_dataset_id": str(spec["dataset_id"]),
                    "source_pair_hash": str(row["pair_hash"]),
                    "source_private_trigger_sha256": str(
                        spec["expected_private_sha256"]
                    ),
                }
            )
    pairs.sort(
        key=lambda row: (
            row["date"],
            int(row["rank"]),
            row["instrument_id"],
        )
    )
    dates = sorted({row["date"] for row in pairs})
    identities = [(row["date"], row["instrument_id"]) for row in pairs]
    if not (
        len(pairs) == EXPECTED_PAIRS
        and len(dates) == EXPECTED_DATES
        and len(set(identities)) == EXPECTED_PAIRS
    ):
        raise ChallengerQualificationCollectionError(
            "retest trigger selection denominator differs"
        )
    request_graph = [
        {
            "pair_hash": capacity._sha256_json(identity),
            "request_kinds": [
                "opening_bars",
                "decision_trade_prefix",
                "quote_window",
                "candidate_completed_bars",
                "spy_completed_bars",
                "qqq_completed_bars",
                "premarket_bars",
                "prior_252_session_bars",
                "nasdaq_halt_prefix",
            ],
            "rebreak_at_et": row["trigger"]["rebreak_at_et"],
            "decision_at_et": row["trigger"]["decision_at_et"],
        }
        for row, identity in zip(pairs, identities, strict=True)
    ]
    return {
        "schema_version": 1,
        "positive_pairs": pairs,
        "positive_pair_count": len(pairs),
        "distinct_trigger_dates": len(dates),
        "pair_identity_sha256": capacity._sha256_json(identities),
        "private_trigger_selection_sha256": capacity._sha256_json(pairs),
        "request_graph": request_graph,
        "request_graph_sha256": capacity._sha256_json(request_graph),
        "exact_dates_symbols_instrument_ids_and_rows_public": False,
        "post_entry_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
    }


def _load_base(
    env_path: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], HistoricalDayStore]:
    manifest = capacity.load_manifest(CAPACITY_MANIFEST)
    result = capacity._read_json(CAPACITY_RESULT)
    status = capacity._read_json(CAPACITY_STATUS)
    if not (
        manifest.get("manifest_sha256")
        == "94a6e83110427b5c7d7d371e5a36292f7a657b9f80d955b5e30982cccd3af21a"
        and result.get("manifest_sha256") == manifest["manifest_sha256"]
        and result.get("status") == "READY"
        and result.get("inspected") is True
        and result.get("minimum_development_signal_capacity_passed") is True
        and result.get("next_phase") == "FREEZE_OUTCOME_CONTRACT"
        and result.get("post_entry_data_accessed") is False
        and result.get("target_outcomes_observed_or_derived") is False
        and status == result
    ):
        raise ChallengerQualificationCollectionError(
            "cumulative capacity is not ready for qualification collection"
        )
    store = HistoricalDayStore.from_env(env_path)
    selection = build_trigger_selection(store.root)
    requested_dates = sorted(
        {str(row["date"]) for row in selection["positive_pairs"]}
    )
    pseudo_manifest = {
        "dataset_id": capacity.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "requested_dates": requested_dates,
        "selection_contract": {
            "private_positive_selection_sha256": selection[
                "private_trigger_selection_sha256"
            ],
            "positive_pair_identity_sha256": selection["pair_identity_sha256"],
        },
        "acquisition_contract": {
            "private_request_graph_sha256": selection["request_graph_sha256"],
        },
    }
    private = {
        "selection": {
            "positive_pairs": selection["positive_pairs"],
            "positive_pair_count": selection["positive_pair_count"],
        },
        "request_graph": selection["request_graph"],
        "target_outcomes_observed_or_derived": False,
    }
    return pseudo_manifest, result, private, store


def _initial_pair_state(
    pair: Mapping[str, Any], manifest_sha256: str
) -> dict[str, Any]:
    trigger = pair.get("trigger")
    if not isinstance(trigger, Mapping):
        raise ChallengerQualificationCollectionError("pair lacks frozen retest trigger")
    state = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest_sha256,
        "collector_sha256": _sha256_file(Path(__file__)),
        "pair_key": _SourceContract._sha256_json(
            (pair["date"], pair["instrument_id"])
        ),
        "date": pair["date"],
        "symbol": pair["symbol"],
        "instrument_id": pair["instrument_id"],
        "rank": pair["rank"],
        "scanner_fields": pair["scanner_fields"],
        "frozen_retest_trigger": dict(trigger),
        "status": "COLLECTING",
        "next_search_at_et": str(pair["date"]) + "T09:35:00-04:00",
        "search_windows_complete": 0,
        "search_minute_files": [],
        "active_search_minute": None,
        "clean_cross": {
            "observed_at_et": trigger["rebreak_at_et"],
            "price": float(trigger["rebreak_price"]),
            "conditions": ["FROZEN_CONDITION_VALID_TRIGGER"],
            "tape": None,
        },
        "requests": {},
        "terminal_disposition": None,
        "target_outcome_observed_or_derived": False,
    }
    return state


def _implementation_contract() -> dict[str, Any]:
    names = (
        "challenger_orb_retest_qualification_collection.py",
        "challenger_orb_retest_qualification_collection_inspection.py",
        "development_non_return_collection.py",
        "challenger_orb_retest_cumulative_capacity.py",
        "historical_providers.py",
        "historical_service.py",
        "historical_store.py",
        "nasdaq_halts.py",
        "scanner_replay.py",
        "sip_bar_aggregation.py",
        "sip_trade_conditions.py",
    )
    return {name: base._binding(PROJECT_ROOT / name) for name in names}


def configure_base() -> None:
    """Install the frozen retest-trigger graph into the generic collector."""

    if _sha256_file(BASE_PATH) != BASE_IMPLEMENTATION_SHA256:
        raise ChallengerQualificationCollectionError(
            "frozen qualification collector base drifted"
        )
    values: dict[str, Any] = {
        "DATASET_ID": DATASET_ID,
        "BASE_MANIFEST": CAPACITY_RESULT,
        "BASE_PUBLIC_STATUS": CAPACITY_STATUS,
        "DEFAULT_MANIFEST_ROOT": DEFAULT_MANIFEST_ROOT,
        "DEFAULT_CONTRACT_STATUS": DEFAULT_CONTRACT_STATUS,
        "DEFAULT_COLLECTION_STATUS": DEFAULT_COLLECTION_STATUS,
        "PRIVATE_NAMESPACE": PRIVATE_NAMESPACE,
        "EXPECTED_PAIRS": EXPECTED_PAIRS,
        "SEARCH_SECONDS_PER_PAIR": 0,
        "source_contract": _SourceContract,
        "_load_base": _load_base,
        "_initial_pair_state": _initial_pair_state,
        "_implementation_contract": _implementation_contract,
        "__file__": str(Path(__file__).resolve()),
    }
    for name, value in values.items():
        setattr(base, name, value)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "collect", "status"))
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_MANIFEST_ROOT)
    parser.add_argument("--contract-status", type=Path, default=DEFAULT_CONTRACT_STATUS)
    parser.add_argument(
        "--collection-status", type=Path, default=DEFAULT_COLLECTION_STATUS
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        configure_base()
        if args.command == "freeze":
            path, manifest = base.freeze(
                env_path=args.env_file,
                output_root=args.output_root,
                public_status_path=args.contract_status,
            )
            value = {"manifest": base._repo_path(path), **manifest}
        elif args.command == "status":
            value = base._read_json(args.collection_status)
        elif args.manifest is None:
            raise ChallengerQualificationCollectionError("--manifest is required")
        else:
            value = base.collect(
                manifest_path=args.manifest,
                env_path=args.env_file,
                public_status_path=args.collection_status,
            )
    except (
        ChallengerQualificationCollectionError,
        base.DevelopmentNonReturnCollectionError,
        HistoricalProviderError,
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
