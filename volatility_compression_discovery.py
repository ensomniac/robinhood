"""Freeze the existing-family volatility-compression development successor.

The exact v1 rule remains retired. This successor uses a prospectively declared
32-cell grid on a disjoint, already inspected minute corpus as contaminated
training while preserving the later 75-session reserve as untouched evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import equity_gap_continuation_discovery as shared
import equity_gap_continuation_validation as gap
import outcome_exposure
import portfolio_maturity
import strategy_discovery
from historical_store import HistoricalDayStore, canonical_sha256, sha256_file
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.VOLATILITY_COMPRESSION_FAMILY
MECHANISM_FAMILY = "volatility-compression-breakout"
STRATEGY_ID = MECHANISM_FAMILY
SUCCESSOR_ID = "volatility-compression-breakout-v2-gap-universe-search"
RESEARCH_GENERATION = "existing_family_successor"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"

PREDECESSOR_VARIANT_ID = "volatility-compression-breakout-v1"
PREDECESSOR_RESULT = (
    PROJECT_ROOT
    / "research_results/"
    "2026-07-21-volatility-compression-breakout-stage0-"
    "5959e922a6cdaf117521a2a70877984497077f3c79acd5aedf2ac87eea0bbdfe.json"
)
PREDECESSOR_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/inspections/"
    "volatility-compression-breakout-v1-result-"
    "67792983095a62d5811046d8d15a095faa75da7566d7957492d6c52213824f53.json"
)


class VolatilityCompressionDiscoveryError(RuntimeError):
    """The successor evidence graph or frozen identity is invalid."""


def _source_graph(
    *, enforce_commit: bool
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    if enforce_commit:
        strategy_discovery.require_committed(PREDECESSOR_RESULT)
        strategy_discovery.require_committed(PREDECESSOR_INSPECTION)
    predecessor = shared._read(PREDECESSOR_RESULT)
    inspection = shared._read(PREDECESSOR_INSPECTION)
    if not (
        predecessor.get("variant_id") == PREDECESSOR_VARIANT_ID
        and predecessor.get("mechanism_family") == MECHANISM_FAMILY
        and predecessor.get("stage0_survived") is False
        and predecessor.get("development_evidence_eligible") is False
        and predecessor.get("confirmation_evidence_eligible") is False
        and predecessor.get("provider_requests") == 0
        and predecessor.get("broker_actions") == 0
        and len(predecessor.get("records", [])) == 95
        and inspection.get("result_sha256")
        == predecessor.get("result_sha256")
        and inspection.get("stage0_survived") is False
        and inspection.get("valid") is True
    ):
        raise VolatilityCompressionDiscoveryError(
            "retired compression predecessor graph is invalid"
        )
    _stage0, source_manifest, input_inspection = shared._source_graph(
        enforce_commit=enforce_commit
    )
    return predecessor, source_manifest, input_inspection


def freeze_successor_contract(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any], Path]:
    """Freeze all trials, evidence partitions, semantics, and falsifiers."""

    shared._timestamp(created_at)
    predecessor, source_manifest, input_inspection = _source_graph(
        enforce_commit=enforce_commit
    )
    if enforce_commit:
        for path in (
            Path(__file__).resolve(),
            PROJECT_ROOT / "volatility_compression_plugin.py",
            PROJECT_ROOT / "equity_gap_continuation_plugin.py",
            PROJECT_ROOT / "dense_strategy_runtime.py",
        ):
            strategy_discovery.require_committed(path)
    source = store or HistoricalDayStore.from_env()
    selection = gap._load_gzip(gap._selection_path(source))
    if canonical_sha256(selection) != source_manifest["private_selection"][
        "content_sha256"
    ]:
        raise VolatilityCompressionDiscoveryError(
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
    predecessor_dates = {
        str(row["date"]) for row in predecessor["records"]
    }
    if not (
        len(development) == 120
        and len(embargo) == 5
        and len(confirmation) == 75
        and development[-1] < embargo[0] < confirmation[0]
        and predecessor_dates.isdisjoint(development)
        and predecessor_dates.isdisjoint(confirmation)
    ):
        raise VolatilityCompressionDiscoveryError(
            "compression evidence chronology or predecessor disjointness drifted"
        )
    development_scope = shared._scope(
        development, development_candidates
    )
    confirmation_scope = shared._scope(
        confirmation, confirmation_candidates
    )
    index = outcome_exposure.read_index()
    if not outcome_exposure.find_overlaps(development_scope, index):
        raise VolatilityCompressionDiscoveryError(
            "development is not explicitly contaminated"
        )
    outcome_exposure.assert_untouched(confirmation_scope, index)
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    evidence_paths = [
        shared._repo_path(PREDECESSOR_RESULT),
        shared._repo_path(PREDECESSOR_INSPECTION),
        shared._repo_path(shared.SOURCE_SELECTION_MANIFEST),
        shared._repo_path(shared.SOURCE_FREEZE_INSPECTION),
        shared._repo_path(shared.SOURCE_DEVELOPMENT_INPUT_INSPECTION),
        shared._repo_path(shared.SOURCE_DEVELOPMENT_RESULT),
        shared._repo_path(shared.SOURCE_DEVELOPMENT_RESULT_INSPECTION),
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
                "volatility_compression_capacity": {
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
                    "source_selection_manifest_path": shared._repo_path(
                        shared.SOURCE_SELECTION_MANIFEST
                    ),
                    "source_selection_manifest_file_sha256": sha256_file(
                        shared.SOURCE_SELECTION_MANIFEST
                    ),
                    "input_inspection_path": shared._repo_path(
                        shared.SOURCE_DEVELOPMENT_INPUT_INSPECTION
                    ),
                    "input_inspection_file_sha256": sha256_file(
                        shared.SOURCE_DEVELOPMENT_INPUT_INSPECTION
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
            "result_path": shared._repo_path(PREDECESSOR_RESULT),
            "result_file_sha256": sha256_file(PREDECESSOR_RESULT),
            "result_sha256": predecessor["result_sha256"],
            "inspection_path": shared._repo_path(PREDECESSOR_INSPECTION),
            "inspection_file_sha256": sha256_file(PREDECESSOR_INSPECTION),
            "promotion_evidence_reused": False,
            "adverse_finding": (
                "The exact 20-bar, 0.60-ratio, 1.50-volume, 2R v1 rule "
                "lost 19.259R at primary cost and remains retired."
            ),
        },
        "source_training": {
            "selection_manifest_path": shared._repo_path(
                shared.SOURCE_SELECTION_MANIFEST
            ),
            "input_inspection_path": shared._repo_path(
                shared.SOURCE_DEVELOPMENT_INPUT_INSPECTION
            ),
            "development_result_path": shared._repo_path(
                shared.SOURCE_DEVELOPMENT_RESULT
            ),
            "development_outcomes_exposed": True,
            "promotion_evidence_reused": False,
            "predecessor_date_overlap": 0,
        },
        "mechanism": (
            "Buy a point-in-time liquid positive-gap common stock after a "
            "completed low-range consolidation breaks above its high and "
            "cumulative VWAP on exceptional completed-bar volume."
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
            "From 10:15 ET through the frozen cutoff, require the completed "
            "10- or 20-bar range to be no more than the frozen fraction of the "
            "first-30-minute range, then require a completed close above that "
            "compression high and cumulative VWAP with the frozen volume "
            "multiple; enter at the next minute open."
        ),
        "stop_rule": (
            "Use the completed compression-window low; a nonpositive or "
            "non-protective stop and a missing next bar cannot be substituted."
        ),
        "exit_rule": (
            "Resolve the frozen raw-R target or stop with stop-first same-minute "
            "ambiguity and otherwise force flat at the 15:50 bar open."
        ),
        "ranking_rule": (
            "Earliest next-minute entry, then lowest compression ratio, highest "
            "breakout-volume multiple, and lexical symbol."
        ),
        "selection_rule": (
            "At most one new family entry per day under the authoritative "
            "portfolio risk, notional, entry, and capital-contention caps."
        ),
        "parameter_grid": {
            "compression_bars": [10, 20],
            "maximum_compression_ratio": [0.4, 0.6],
            "breakout_volume_multiple": [1.5, 2.5],
            "signal_cutoff_minutes": [120, 300],
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
            "signal_start_et": "10:15:00",
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
            "The exact v1 outcome remains adverse history and cannot promote this version.",
            "The 120-session development corpus is already exposed and is training only.",
            "The 75-session reserve stays unopened until an exact winner is frozen.",
            "Any global date-symbol exposure disqualifies confirmation.",
        ],
        "production_compatibility_risks": [
            "Live denominator completeness, synchronized completed bars, fresh quote, spread, depth, halt, tradability, protection, and reconciliation remain mandatory."
        ],
        "material_difference_rationale": (
            "This exact successor preserves the compression-breakout mechanism "
            "but moves to a wholly disjoint point-in-time gap universe and "
            "prospectively freezes family-wise selection across compression "
            "length, tightness, volume, cutoff, and reward choices."
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
            "source_manifest": shared._repo_path(
                shared.SOURCE_SELECTION_MANIFEST
            ),
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
            "volatility_compression_discovery.py",
            "volatility_compression_plugin.py",
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
            "module": "volatility_compression_plugin",
            "preflight": "preflight",
            "evaluate_development": "evaluate_development",
            "evaluate_confirmation": "evaluate_confirmation",
            "evaluate_production": "evaluate_production",
        },
        "capacity_policy": {"retire_below": 50, "fast_lane_at": 100},
        "capacity_manifest": shared._repo_path(capacity_path),
        "dataset_manifest": shared._repo_path(capacity_path),
    }
    validated = strategy_discovery._validate_family_contract(contract)
    digest = hashlib.sha256(shared._canonical(validated)).hexdigest()
    path = (
        root
        / SUCCESSOR_ID
        / "family-contract"
        / f"contract-{digest}.json"
    )
    shared._write_json(path, validated)
    return path, validated, capacity_path


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
            else "run generic preflight and development transitions"
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
                "path": shared._repo_path(path),
                "capacity_manifest": shared._repo_path(capacity),
                "state": contract["status"],
                "trial_count": len(contract["trial_family"]),
                "calendar_wait_required": False,
            }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        OSError,
        VolatilityCompressionDiscoveryError,
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
