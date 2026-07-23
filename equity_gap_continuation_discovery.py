"""Freeze the selection-aware equity-gap-continuation successor.

This is an exact version inside an already tested mechanism family, so it
consumes no new-family weekly slot. The 120-session representative corpus is
explicitly contaminated training; the later 75-session reserve stays untouched
until one immutable development winner is selected.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import equity_gap_continuation_validation as gap
import outcome_exposure
import portfolio_maturity
import strategy_discovery
from historical_store import HistoricalDayStore, canonical_sha256, sha256_file
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.EQUITY_GAP_CONTINUATION_FAMILY
MECHANISM_FAMILY = "equity-gap-continuation"
STRATEGY_ID = MECHANISM_FAMILY
SUCCESSOR_ID = "equity-gap-continuation-v2-development-search"
RESEARCH_GENERATION = "existing_family_successor"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"

PREDECESSOR_VARIANT_ID = "equity-gap-continuation-v1"
STAGE0_RESULT = (
    PROJECT_ROOT
    / "research_results/"
    "2026-07-21-equity-gap-continuation-stage0-"
    "1f447fb4e066b041463e62e26d7e752a12e1b90da7c13b6881ea42e10dda9e94.json"
)
STAGE0_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/inspections/"
    "equity-gap-continuation-v1-result-"
    "66ede02688e090973e482ecc9c491349238d14e2af63fd02c336b99a8564c7be.json"
)
SOURCE_SELECTION_MANIFEST = (
    PROJECT_ROOT
    / "strategy_validation/equity_gap_continuation/manifests/"
    "equity-gap-continuation-v1-"
    "0145f77948ff8d7398c69ec7ae2229b72cfb7a07bab055c3dcfb15762d5cea43.json"
)
SOURCE_FREEZE_INSPECTION = (
    PROJECT_ROOT
    / "strategy_validation/equity_gap_continuation/inspections/"
    "equity-gap-continuation-v1-freeze-"
    "1aa8fad408438bf19824ae1c01ede6ea35c446238d75113666e337e0fdecec67.json"
)
SOURCE_DEVELOPMENT_INPUT_INSPECTION = (
    PROJECT_ROOT
    / "strategy_validation/equity_gap_continuation/inspections/"
    "equity-gap-continuation-v1-development-inputs-"
    "36ef7b4db655573630d0e407e35d3a1e9ed43a44c91da6bf9646a5a991066761.json"
)
SOURCE_DEVELOPMENT_RESULT = (
    PROJECT_ROOT
    / "research_results/"
    "2026-07-21-equity-gap-continuation-development-"
    "3add7a7bdbd3a47282663a0daa41e17d1b6d3e6e780f08d959cf81ca4f2dba92.json"
)
SOURCE_DEVELOPMENT_RESULT_INSPECTION = (
    PROJECT_ROOT
    / "strategy_validation/equity_gap_continuation/inspections/"
    "equity-gap-continuation-v1-development-result-"
    "bf1050d3d88b2402890befd5c583f2b623b14fa1b33f2edc9f89692e130db8c6.json"
)


class EquityGapDiscoveryError(RuntimeError):
    """The successor's evidence, version, or frozen identity is invalid."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EquityGapDiscoveryError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise EquityGapDiscoveryError(f"{path} must contain an object")
    return value


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise EquityGapDiscoveryError(
            f"path escaped repository: {path}"
        ) from exc


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") == rendered:
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(rendered, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _timestamp(value: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EquityGapDiscoveryError("created_at is invalid") from exc
    if parsed.tzinfo is None:
        raise EquityGapDiscoveryError("created_at needs a timezone")


def _source_graph(
    *, enforce_commit: bool
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    paths = (
        STAGE0_RESULT,
        STAGE0_INSPECTION,
        SOURCE_SELECTION_MANIFEST,
        SOURCE_FREEZE_INSPECTION,
        SOURCE_DEVELOPMENT_INPUT_INSPECTION,
        SOURCE_DEVELOPMENT_RESULT,
        SOURCE_DEVELOPMENT_RESULT_INSPECTION,
    )
    if enforce_commit:
        for path in paths:
            strategy_discovery.require_committed(path)
    stage0 = _read(STAGE0_RESULT)
    stage0_inspection = _read(STAGE0_INSPECTION)
    source_manifest = _read(SOURCE_SELECTION_MANIFEST)
    freeze_inspection = _read(SOURCE_FREEZE_INSPECTION)
    input_inspection = _read(SOURCE_DEVELOPMENT_INPUT_INSPECTION)
    development = _read(SOURCE_DEVELOPMENT_RESULT)
    development_inspection = _read(
        SOURCE_DEVELOPMENT_RESULT_INSPECTION
    )
    if not (
        stage0.get("stage0_survived") is True
        and stage0.get("development_evidence_eligible") is False
        and stage0.get("confirmation_evidence_eligible") is False
        and stage0_inspection.get("result_sha256")
        == stage0.get("result_sha256")
        and stage0_inspection.get("stage0_survived") is True
        and stage0_inspection.get("valid") is True
        and source_manifest.get("private_selection", {}).get(
            "contains_target_returns"
        )
        is False
        and source_manifest.get("denominator", {}).get(
            "development_candidate_symbol_sessions"
        )
        == 4_833
        and source_manifest.get("denominator", {}).get(
            "confirmation_candidate_symbol_sessions"
        )
        == 3_192
        and freeze_inspection.get("valid") is True
        and input_inspection.get("sample_phase") == "development"
        and input_inspection.get("candidate_symbol_sessions") == 4_833
        and input_inspection.get("returns_computed") == 0
        and input_inspection.get("valid") is True
        and development.get("development_passed") is False
        and development_inspection.get("result_sha256")
        == development.get("result_sha256")
        and development_inspection.get("phase_passed") is False
        and development_inspection.get("valid") is True
    ):
        raise EquityGapDiscoveryError(
            "contaminated equity-gap source graph is invalid"
        )
    return stage0, source_manifest, input_inspection


def _scope(
    dates: list[str],
    candidates: Mapping[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    return {
        "dates": dates,
        "symbols_by_date": {
            day: sorted(str(row["symbol"]) for row in candidates[day])
            for day in dates
        },
    }


def freeze_successor_contract(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any], Path]:
    """Freeze the complete grid, split, implementation, and failure history."""

    _timestamp(created_at)
    stage0, source_manifest, input_inspection = _source_graph(
        enforce_commit=enforce_commit
    )
    if enforce_commit:
        for path in (
            Path(__file__).resolve(),
            PROJECT_ROOT / "equity_gap_continuation_plugin.py",
            PROJECT_ROOT / "dense_strategy_runtime.py",
        ):
            strategy_discovery.require_committed(path)
    source = store or HistoricalDayStore.from_env()
    selection = gap._load_gzip(gap._selection_path(source))
    if canonical_sha256(selection) != source_manifest["private_selection"][
        "content_sha256"
    ]:
        raise EquityGapDiscoveryError(
            "private point-in-time selection graph drifted"
        )
    development = list(selection["phases"]["development"]["dates"])
    embargo = list(selection["embargo_dates"])
    confirmation = list(selection["phases"]["confirmation"]["dates"])
    development_candidates = selection["phases"]["development"][
        "candidates_by_date"
    ]
    confirmation_candidates = selection["phases"]["confirmation"][
        "candidates_by_date"
    ]
    if not (
        len(development) == 120
        and len(embargo) == 5
        and len(confirmation) == 75
        and development[-1] < embargo[0] < confirmation[0]
    ):
        raise EquityGapDiscoveryError(
            "equity-gap evidence chronology drifted"
        )
    development_scope = _scope(development, development_candidates)
    confirmation_scope = _scope(confirmation, confirmation_candidates)
    index = outcome_exposure.read_index()
    if not outcome_exposure.find_overlaps(development_scope, index):
        raise EquityGapDiscoveryError(
            "development is not explicitly contaminated in the global index"
        )
    outcome_exposure.assert_untouched(confirmation_scope, index)
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    evidence_paths = [
        _repo_path(STAGE0_RESULT),
        _repo_path(STAGE0_INSPECTION),
        _repo_path(SOURCE_SELECTION_MANIFEST),
        _repo_path(SOURCE_FREEZE_INSPECTION),
        _repo_path(SOURCE_DEVELOPMENT_INPUT_INSPECTION),
        _repo_path(SOURCE_DEVELOPMENT_RESULT),
        _repo_path(SOURCE_DEVELOPMENT_RESULT_INSPECTION),
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
    ]
    capacity_path, _capacity = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-development",
            "registered_at": created_at,
            "requested_dates": development,
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": evidence_paths,
                "inspected": True,
                "point_in_time_evidence": True,
                "gap_continuation_capacity": {
                    "family_id": FAMILY_ID,
                    "mechanism_family": MECHANISM_FAMILY,
                    "formal_capacity": 4_833,
                    "capacity_unit": "frozen candidate symbol-sessions",
                    "development_sessions": 120,
                    "embargo_sessions": 5,
                    "confirmation_sessions": 75,
                    "confirmation_candidate_symbol_sessions": 3_192,
                    "development_training_contaminated": True,
                    "confirmation_access_permitted": False,
                    "provider_requests": 0,
                },
                "gap_continuation_runtime": {
                    "family_id": FAMILY_ID,
                    "sample_phase": "development",
                    "source_selection_manifest_path": _repo_path(
                        SOURCE_SELECTION_MANIFEST
                    ),
                    "source_selection_manifest_file_sha256": sha256_file(
                        SOURCE_SELECTION_MANIFEST
                    ),
                    "input_inspection_path": _repo_path(
                        SOURCE_DEVELOPMENT_INPUT_INSPECTION
                    ),
                    "input_inspection_file_sha256": sha256_file(
                        SOURCE_DEVELOPMENT_INPUT_INSPECTION
                    ),
                    "private_input_index_content_sha256": input_inspection[
                        "private_input_index_content_sha256"
                    ],
                    "provider_requests": 0,
                },
            },
        },
        root / SUCCESSOR_ID / "capacity",
    )
    contract: dict[str, Any] = {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "experiment_id": f"experiment-{SUCCESSOR_ID}",
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "strategy_id": STRATEGY_ID,
        "parent_experiment_id": PREDECESSOR_VARIANT_ID,
        "created_at": created_at,
        "status": "INVENTED",
        "research_generation": RESEARCH_GENERATION,
        "successor_id": SUCCESSOR_ID,
        "new_mechanism_family_slot_consumed": False,
        "prior_family_attempt_count": 1,
        "predecessor": {
            "variant_id": PREDECESSOR_VARIANT_ID,
            "stage0_result_path": _repo_path(STAGE0_RESULT),
            "stage0_result_file_sha256": sha256_file(STAGE0_RESULT),
            "stage0_result_sha256": stage0["result_sha256"],
            "stage0_inspection_path": _repo_path(STAGE0_INSPECTION),
            "stage0_inspection_file_sha256": sha256_file(
                STAGE0_INSPECTION
            ),
            "development_result_path": _repo_path(
                SOURCE_DEVELOPMENT_RESULT
            ),
            "development_result_file_sha256": sha256_file(
                SOURCE_DEVELOPMENT_RESULT
            ),
            "development_result_sha256": _read(
                SOURCE_DEVELOPMENT_RESULT
            )["result_sha256"],
            "development_inspection_path": _repo_path(
                SOURCE_DEVELOPMENT_RESULT_INSPECTION
            ),
            "development_inspection_file_sha256": sha256_file(
                SOURCE_DEVELOPMENT_RESULT_INSPECTION
            ),
            "promotion_evidence_reused": False,
        },
        "mechanism": (
            "Continuation after a completed high-volume opening-range breakout "
            "inside a point-in-time liquid positive-gap common-stock universe."
        ),
        "expected_holding_behavior": (
            "Long only, next-minute entry, and flat by 15:50 ET on the signal day."
        ),
        "dataset_lane": "development",
        "universe_requirements": {
            "security_type": "point-in-time active U.S. common stocks",
            "opening_price_minimum": 5.0,
            "opening_gap_fraction": [0.02, 0.08],
            "selection_time_et": "09:35:00",
            "complete_candidate_denominator": True,
        },
        "entry_rule": (
            "After the frozen opening range, require a completed close above "
            "the range high and cumulative session VWAP with the frozen "
            "prior-15-bar volume multiple, then enter at the next minute open."
        ),
        "stop_rule": (
            "Use the frozen opening-range low; a nonpositive structural stop "
            "or missing next bar produces a rejected or missed trade."
        ),
        "exit_rule": (
            "Resolve the frozen raw-R target or stop with stop-first same-minute "
            "ambiguity and otherwise force flat at the 15:50 bar open."
        ),
        "ranking_rule": (
            "Earliest next-minute entry, then highest breakout-volume multiple, "
            "largest opening gap, and lexical symbol."
        ),
        "selection_rule": (
            "At most one new family entry per day under the authoritative "
            "portfolio risk, notional, entry, and capital-contention caps."
        ),
        "parameter_grid": {
            "minimum_gap_fraction": [0.02, 0.04],
            "opening_range_minutes": [5, 15],
            "breakout_volume_multiple": [1.5, 2.5],
            "signal_cutoff_minutes": [60, 120],
            "target_r": [1.5, 2.0],
        },
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "primary_outcome": (
            "Selection-adjusted chronological account log growth after costs."
        ),
        "execution_assumptions": {
            "next_observable_fill": True,
            "ambiguity": "stop_first",
            "maximum_hold_sessions": 1,
            "missing_data": "retained_denominator_no_signal",
            "minimum_gross_to_primary_round_trip_cost": 5.0,
            "volume_lookback_completed_bars": 15,
            "force_flat_et": "15:50:00",
        },
        "falsification_criteria": {
            "minimum_20bps_log_growth": 0.0,
            "minimum_stressed_profit_factor": 1.2,
            "maximum_drawdown_r": 6.0,
            "minimum_deflated_sharpe_probability": 0.9,
            "maximum_pbo_probability": 0.5,
        },
        "minimum_evidence": {
            "configured_floor": 50,
            "confirmation_floor": 20,
            "power": 0.8,
            "alpha": 0.1,
        },
        "contamination_risks": [
            "The exact v1 Stage 0 and representative development outcomes are hypothesis evidence only.",
            "The 120-session development corpus has prior outcome exposure and is labeled contaminated training.",
            "The 75-session confirmation reserve must remain unopened until one exact winner is frozen.",
            "Any global date-symbol exposure disqualifies confirmation.",
        ],
        "production_compatibility_risks": [
            "Live completeness of the 09:35 gap universe, fresh quote, spread, depth, halt, tradability, timing, protection, and reconciliation remain mandatory."
        ],
        "material_difference_rationale": (
            "This version retains the tested equity-gap-continuation mechanism "
            "but prospectively searches only declared opening-range, volume, "
            "gap-strength, cutoff, and target choices under family-wise controls."
        ),
        "development_dates": development,
        "embargo_dates": embargo,
        "confirmation_dates": confirmation,
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "universe": {
            "identity": (
                "complete point-in-time active U.S. common-stock 2-8% opening-gap "
                "candidates at 09:35 ET"
            ),
            "source_manifest": _repo_path(SOURCE_SELECTION_MANIFEST),
            "development_candidate_symbol_sessions": 4_833,
            "confirmation_candidate_symbol_sessions": 3_192,
            "point_in_time": True,
        },
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "development_training_contaminated": True,
        },
        "falsifiers": [
            "nonpositive stressed log growth",
            "unstable parameter neighbors",
            "selection-aware statistical rejection",
            "incomplete execution or trial accounting",
            "insufficient frozen confirmation power capacity",
        ],
        "implementation_files": [
            "equity_gap_continuation_discovery.py",
            "equity_gap_continuation_plugin.py",
            "dense_strategy_runtime.py",
            "equity_gap_continuation_validation.py",
            "learning_statistics.py",
            "learning_experiment.py",
            "strategy_discovery.py",
            "outcome_exposure.py",
            "portfolio_maturity.py",
            "portfolio_config.toml",
        ],
        "plugin": {
            "module": "equity_gap_continuation_plugin",
            "preflight": "preflight",
            "evaluate_development": "evaluate_development",
            "evaluate_confirmation": "evaluate_confirmation",
            "evaluate_production": "evaluate_production",
        },
        "capacity_policy": {"retire_below": 50, "fast_lane_at": 100},
        "capacity_manifest": _repo_path(capacity_path),
        "dataset_manifest": _repo_path(capacity_path),
    }
    strategy_discovery._validate_family_contract(contract)
    digest = hashlib.sha256(_canonical(contract)).hexdigest()
    path = (
        root
        / SUCCESSOR_ID
        / "family-contract"
        / f"contract-{digest}.json"
    )
    _write_json(path, contract)
    return path, contract, capacity_path


def status(*, root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    contracts = sorted(
        (root / SUCCESSOR_ID / "family-contract").glob("contract-*.json")
    )
    discovery_root = strategy_discovery.DEFAULT_ROOT / FAMILY_ID
    return {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "successor_id": SUCCESSOR_ID,
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "calendar_wait_required": False,
        "new_mechanism_family_slot_consumed": False,
        "development_training_contaminated": True,
        "confirmation_outcomes_accessed": False,
        "family_contracts": len(contracts),
        "state": (
            "READY_TO_FREEZE"
            if not contracts
            else "DISCOVERY_ACTIVE"
            if discovery_root.exists()
            else "CONTRACT_FROZEN"
        ),
        "next_action": (
            "freeze the exact 32-trial successor"
            if not contracts
            else "run generic preflight and the development transition chain"
        ),
        "broker_actions_permitted": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status")
    freeze = sub.add_parser("freeze")
    freeze.add_argument("--created-at", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "status":
            result = status()
        else:
            path, contract, capacity = freeze_successor_contract(
                created_at=args.created_at
            )
            result = {
                "path": _repo_path(path),
                "capacity_manifest": _repo_path(capacity),
                "state": contract["status"],
                "trial_count": 32,
                "calendar_wait_required": False,
            }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        OSError,
        EquityGapDiscoveryError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
