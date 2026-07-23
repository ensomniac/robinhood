"""Freeze the immediate oversold-reversal successor on reusable historical data.

The weekly campaign ceiling governs new mechanisms, not new exact versions of an
existing family. This lane treats the predecessor's favorable ten-signal screen
as contaminated hypothesis evidence, uses the already exposed 2025 gap-candidate
development corpus only for training, and keeps the later 75-session reserve
untouched until one selection-aware winner is immutable.
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
FAMILY_ID = runtime.OVERSOLD_REVERSAL_FAMILY
MECHANISM_FAMILY = "short-horizon-oversold-reversal"
STRATEGY_ID = MECHANISM_FAMILY
SUCCESSOR_ID = "short-horizon-oversold-reversal-v3-gap-universe"
RESEARCH_GENERATION = "existing_family_successor"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"

PREDECESSOR_VARIANT_ID = "short-horizon-oversold-reversal-v1"
PREDECESSOR_RESULT = (
    PROJECT_ROOT
    / "research_results/"
    "2026-07-21-short-horizon-oversold-reversal-stage0-"
    "bb2152f3f18aef92bac5c7d5dcffc15bdbe2967525e63fa6678c6c553aeb75c6.json"
)
PREDECESSOR_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/inspections/"
    "short-horizon-oversold-reversal-v1-result-"
    "be56bcf7bf8fe2fd16e223f510a8236135fbc39f1a27e7531af9a4649a706e25.json"
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
FAILED_V2_SEARCH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/gap-universe-oversold-reversal/search/"
    "gap-universe-oversold-reversal-search-"
    "f914253ee7f58d8d6abaab028fbe6d09dc1a561ed87b341876a04ec9ad6b6dd7.json"
)
FAILED_V2_TRANSITION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/gap-universe-oversold-reversal/"
    "development-failures/gap-universe-oversold-reversal-development-failure-"
    "9f907241783e940b84e6fcdfa08529a495539ea05daaf2da6d86fe091a9820bc.json"
)


class OversoldDiscoveryError(RuntimeError):
    """The immediate successor's evidence or identity is invalid."""


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
        raise OversoldDiscoveryError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise OversoldDiscoveryError(f"{path} must contain an object")
    return value


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise OversoldDiscoveryError(f"path escaped repository: {path}") from exc


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
        raise OversoldDiscoveryError("created_at is invalid") from exc
    if parsed.tzinfo is None:
        raise OversoldDiscoveryError("created_at needs a timezone")


def _source_graph(
    *, enforce_commit: bool
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    paths = (
        PREDECESSOR_RESULT,
        PREDECESSOR_INSPECTION,
        SOURCE_SELECTION_MANIFEST,
        SOURCE_FREEZE_INSPECTION,
        SOURCE_DEVELOPMENT_INPUT_INSPECTION,
        SOURCE_DEVELOPMENT_RESULT,
        SOURCE_DEVELOPMENT_RESULT_INSPECTION,
        FAILED_V2_SEARCH,
        FAILED_V2_TRANSITION,
    )
    if enforce_commit:
        for path in paths:
            strategy_discovery.require_committed(path)
    predecessor = _read(PREDECESSOR_RESULT)
    predecessor_inspection = _read(PREDECESSOR_INSPECTION)
    if not (
        predecessor.get("variant_id") == PREDECESSOR_VARIANT_ID
        and predecessor.get("mechanism_family") == MECHANISM_FAMILY
        and predecessor.get("stage0_survived") is False
        and predecessor.get("stage0_blockers")
        == ["closed signals are below the Stage 0 minimum"]
        and predecessor.get("development_evidence_eligible") is False
        and predecessor.get("confirmation_evidence_eligible") is False
        and predecessor_inspection.get("result_sha256")
        == predecessor.get("result_sha256")
        and predecessor_inspection.get("stage0_survived") is False
        and predecessor_inspection.get("valid") is True
    ):
        raise OversoldDiscoveryError("retired oversold predecessor binding is invalid")
    source_manifest = _read(SOURCE_SELECTION_MANIFEST)
    source_input_inspection = _read(SOURCE_DEVELOPMENT_INPUT_INSPECTION)
    source_result = _read(SOURCE_DEVELOPMENT_RESULT)
    source_result_inspection = _read(SOURCE_DEVELOPMENT_RESULT_INSPECTION)
    failed_transition = strategy_discovery.load_artifact(
        FAILED_V2_TRANSITION,
        expected_kind="development-evaluation-failure",
    )
    if not (
        source_manifest.get("private_selection", {}).get(
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
        and source_input_inspection.get("sample_phase") == "development"
        and source_input_inspection.get("candidate_symbol_sessions") == 4_833
        and source_input_inspection.get("returns_computed") == 0
        and source_input_inspection.get("valid") is True
        and source_result.get("development_passed") is False
        and source_result_inspection.get("result_sha256")
        == source_result.get("result_sha256")
        and source_result_inspection.get("valid") is True
        and failed_transition.get("state")
        == "FAILED_ARTIFACT_BINDING_AFTER_OUTCOME_COMPUTATION"
        and failed_transition.get("search_path") == _repo_path(FAILED_V2_SEARCH)
        and failed_transition.get("trial_metrics_surfaced") is False
        and failed_transition.get("confirmation_accessed") is False
        and failed_transition.get(
            "strategy_grid_dates_rules_costs_changed_after_failure"
        )
        is False
    ):
        raise OversoldDiscoveryError("contaminated source graph is invalid")
    return predecessor, source_manifest, source_input_inspection


def _scope(
    dates: list[str], candidates: Mapping[str, list[dict[str, Any]]]
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
    """Freeze the complete grid, evidence split, and contaminated-data boundary."""

    _timestamp(created_at)
    predecessor, source_manifest, source_input_inspection = _source_graph(
        enforce_commit=enforce_commit
    )
    if enforce_commit:
        for path in (
            Path(__file__).resolve(),
            PROJECT_ROOT / "oversold_reversal_plugin.py",
            PROJECT_ROOT / "dense_strategy_runtime.py",
        ):
            strategy_discovery.require_committed(path)
    source = store or HistoricalDayStore.from_env()
    selection = gap._load_gzip(gap._selection_path(source))
    if canonical_sha256(selection) != source_manifest["private_selection"][
        "content_sha256"
    ]:
        raise OversoldDiscoveryError("private point-in-time selection graph drifted")
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
        raise OversoldDiscoveryError("oversold evidence chronology drifted")
    development_scope = _scope(development, development_candidates)
    confirmation_scope = _scope(confirmation, confirmation_candidates)
    index = outcome_exposure.read_index()
    if not outcome_exposure.find_overlaps(development_scope, index):
        raise OversoldDiscoveryError(
            "development is not explicitly contaminated in the global index"
        )
    outcome_exposure.assert_untouched(confirmation_scope, index)
    outcome_exposure.assert_disjoint([development_scope, confirmation_scope])
    evidence_paths = [
        _repo_path(PREDECESSOR_RESULT),
        _repo_path(PREDECESSOR_INSPECTION),
        _repo_path(SOURCE_SELECTION_MANIFEST),
        _repo_path(SOURCE_FREEZE_INSPECTION),
        _repo_path(SOURCE_DEVELOPMENT_INPUT_INSPECTION),
        _repo_path(SOURCE_DEVELOPMENT_RESULT),
        _repo_path(SOURCE_DEVELOPMENT_RESULT_INSPECTION),
        _repo_path(FAILED_V2_SEARCH),
        _repo_path(FAILED_V2_TRANSITION),
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
                "oversold_capacity": {
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
                "oversold_runtime": {
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
                    "private_input_index_content_sha256": (
                        source_input_inspection[
                            "private_input_index_content_sha256"
                        ]
                    ),
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
        "prior_family_attempt_count": 2,
        "supersedes_failed_transition": {
            "successor_id": "short-horizon-oversold-reversal-v2-gap-universe",
            "search_path": _repo_path(FAILED_V2_SEARCH),
            "search_file_sha256": sha256_file(FAILED_V2_SEARCH),
            "failure_path": _repo_path(FAILED_V2_TRANSITION),
            "failure_file_sha256": sha256_file(FAILED_V2_TRANSITION),
            "failure_sha256": strategy_discovery.load_artifact(
                FAILED_V2_TRANSITION,
                expected_kind="development-evaluation-failure",
            )["artifact_sha256"],
            "trial_metrics_surfaced": False,
            "confirmation_accessed": False,
            "strategy_grid_dates_rules_costs_changed": False,
            "implementation_change": (
                "return the already bound dataset manifest using its contract-"
                "canonical repository-relative path"
            ),
        },
        "pre_freeze_training_diagnostic": {
            "purpose": (
                "I/O and semantic dry run on the already contaminated development "
                "corpus; disclosed before formal search and ineligible as untouched evidence"
            ),
            "parameters": {
                "lookback_minutes": 30,
                "selloff_threshold": -0.03,
                "rsi_period": 5,
                "rsi_maximum": 20.0,
                "target_r": 1.5,
            },
            "rolling_origin_account_days": 72,
            "filled_signals": 13,
            "stress_20bps_total_log_growth": -0.0027368370161519346,
            "provider_requests": 0,
            "broker_actions": 0,
            "search_grid_changed_after_observation": False,
            "promotion_evidence_eligible": False,
        },
        "predecessor": {
            "variant_id": PREDECESSOR_VARIANT_ID,
            "result_path": _repo_path(PREDECESSOR_RESULT),
            "result_file_sha256": sha256_file(PREDECESSOR_RESULT),
            "result_sha256": predecessor["result_sha256"],
            "inspection_path": _repo_path(PREDECESSOR_INSPECTION),
            "inspection_file_sha256": sha256_file(PREDECESSOR_INSPECTION),
            "inspection_sha256": _read(PREDECESSOR_INSPECTION)[
                "inspection_sha256"
            ],
            "promotion_evidence_reused": False,
        },
        "mechanism": (
            "Intraday mean reversion after a completed short-horizon oversold "
            "selloff inside a point-in-time liquid opening-gap universe."
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
            "After completed lookback selloff, simple RSI, bullish prior-high and "
            "session-VWAP reclaim gates, enter the selected symbol at the next "
            "observed one-minute open."
        ),
        "stop_rule": (
            "Use the lowest completed session low through the trigger bar; a "
            "nonpositive structural stop produces a rejected trade."
        ),
        "exit_rule": (
            "Resolve the frozen raw-R target or stop with stop-first same-minute "
            "ambiguity and otherwise force flat at the 15:50 bar open."
        ),
        "ranking_rule": (
            "Earliest next-minute entry, then deepest selloff, lowest RSI, and "
            "lexical symbol."
        ),
        "selection_rule": (
            "At most one new family entry per day under the authoritative "
            "portfolio risk, notional, entry, and capital-contention caps."
        ),
        "parameter_grid": {
            "lookback_minutes": [15, 30],
            "selloff_threshold": [-0.02, -0.03],
            "rsi_period": [3, 5],
            "rsi_maximum": [15.0, 20.0],
            "target_r": [1.0, 1.5],
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
            "The predecessor's ten favorable signals are hypothesis evidence only.",
            "The 120-session development corpus has prior outcome exposure and is labeled contaminated training.",
            "One unchanged predecessor-like parameter combination was exercised as an adverse pre-freeze I/O diagnostic and is disclosed in this contract.",
            "The 75-session confirmation reserve must remain unopened until one exact winner is frozen.",
            "Any global date-symbol exposure disqualifies confirmation.",
        ],
        "production_compatibility_risks": [
            "Live completeness of the 09:35 gap universe, fresh quote, spread, depth, halt, tradability, timing, protection, and reconciliation remain mandatory."
        ],
        "material_difference_rationale": (
            "This exact version retains the existing oversold-reversal mechanism "
            "while replacing the catalyst shortlist with a prospectively frozen "
            "point-in-time opening-gap denominator and a complete declared grid."
        ),
        "development_dates": development,
        "embargo_dates": embargo,
        "confirmation_dates": confirmation,
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "outcome_exposure_index_sha256": outcome_exposure.audit()["index_sha256"],
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
            "predecessor_corpus_disjoint": True,
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
            "oversold_reversal_discovery.py",
            "oversold_reversal_plugin.py",
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
            "module": "oversold_reversal_plugin",
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
    path = root / SUCCESSOR_ID / "family-contract" / f"contract-{digest}.json"
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
            else "run the generic preflight and development transition chain"
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
        OversoldDiscoveryError,
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
