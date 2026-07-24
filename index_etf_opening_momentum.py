"""Freeze a new liquid-index-ETF opening-momentum development family."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import dense_data_collection as collection
import dense_strategy_runtime as runtime
import liquid_index_etf_opening_reversal as reversal
import outcome_exposure
import portfolio_maturity
import sector_etf_gap_drift as artifact_support
import strategy_discovery
from historical_store import sha256_file
from learning_data import (
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.INDEX_ETF_OPENING_MOMENTUM_FAMILY
MECHANISM_FAMILY = "intraday-index-etf-opening-momentum"
STRATEGY_ID = "liquid-index-etf-opening-momentum"
SUCCESSOR_ID = "liquid-index-etf-opening-momentum-v1"
RESEARCH_GENERATION = "new_mechanism_family"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
SYMBOLS = list(reversal.SYMBOLS)
BASE_CONTRACT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "liquid-index-etf-opening-reversal-v2-post2016/family-contract/"
    "contract-f78116ad4af574edd43adff8f38b0ab75bdc3b0b69bbd84b"
    "3453b161eed2768e.json"
)
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-index-etf-opening-reversal-post2016/development-dataset/"
    "dataset-liquid-index-etf-opening-reversal-post2016-development-"
    "c50c1847502f733a-"
    "7929c6a2ed9e498bd0797e53867c601a4a58f5f3d1d22f45e9c8f9fe0b60dd4b.json"
)
SOURCE_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-index-etf-opening-reversal-post2016/"
    "development-collection-inspection/"
    "liquid-index-etf-opening-reversal-post2016-development-collection-"
    "inspection-"
    "c3fd42e6a84448c5d267c0d1c1e4627eaf2db00cd864c9d11996f5582ab9df51.json"
)


class IndexEtfOpeningMomentumError(ValueError):
    """The opening-momentum contract or evidence boundary drifted."""


def _repo_path(path: Path) -> str:
    return artifact_support._repo_path(path)


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise IndexEtfOpeningMomentumError(
            f"cannot read frozen predecessor: {path.name}"
        ) from exc
    if not isinstance(value, dict):
        raise IndexEtfOpeningMomentumError(
            f"frozen predecessor is malformed: {path.name}"
        )
    return value


def _source_evidence(
    *, enforce_commit: bool
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    for path in (BASE_CONTRACT, SOURCE_MANIFEST, SOURCE_INSPECTION):
        if enforce_commit:
            strategy_discovery.require_committed(path)
    base = _read(BASE_CONTRACT)
    manifest = load_frozen_dataset_contract(SOURCE_MANIFEST)
    inspection = strategy_discovery.load_artifact(
        SOURCE_INSPECTION,
        expected_kind="dense-data-collection-inspection",
    )
    binding = manifest["dataset_payload"].get("dense_runtime")
    if not (
        base.get("family_id")
        == runtime.LIQUID_INDEX_ETF_OPENING_REVERSAL_POST2016_FAMILY
        and isinstance(binding, Mapping)
        and binding.get("family_id")
        == runtime.LIQUID_INDEX_ETF_OPENING_REVERSAL_POST2016_FAMILY
        and manifest["dataset_payload"].get("inspected") is True
        and manifest["dataset_payload"].get("lane") == "development"
        and inspection.get("state") == "DATASET_INSPECTED_READY"
        and inspection.get("dataset_sha256")
        == binding.get("dataset_sha256")
    ):
        raise IndexEtfOpeningMomentumError(
            "opening-momentum source evidence graph drifted"
        )
    return base, manifest, inspection


def _scope(dates: list[str]) -> dict[str, Any]:
    return {"dates": list(dates), "symbols": list(SYMBOLS)}


def _validate_exposure_state(contract: Mapping[str, Any]) -> None:
    records = outcome_exposure.read_index()
    proposed = outcome_exposure.scope_pairs(
        contract["development_scope"]
    )
    covered = reversal._covered_pairs(
        contract["development_scope"], records
    )
    if covered != proposed:
        raise IndexEtfOpeningMomentumError(
            "opening-momentum development must remain fully labeled "
            "as contaminated training"
        )
    outcome_exposure.assert_untouched(
        contract["confirmation_scope"], records
    )
    outcome_exposure.assert_disjoint(
        [
            contract["development_scope"],
            contract["confirmation_scope"],
        ]
    )


def freeze_contract(
    *, created_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any], Path]:
    artifact_support._timestamp(created_at, "created_at")
    base, source_manifest, source_inspection = _source_evidence(
        enforce_commit=enforce_commit
    )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
        strategy_discovery.require_committed(
            PROJECT_ROOT / "index_etf_opening_momentum_plugin.py"
        )
    (
        warmup,
        development,
        embargo,
        confirmation_warmup,
        confirmation,
    ) = reversal._post2016_partitions()
    if source_manifest["requested_dates"] != development:
        raise IndexEtfOpeningMomentumError(
            "source manifest dates drifted from the frozen development block"
        )
    development_scope = _scope(development)
    confirmation_scope = _scope(confirmation)
    _validate_exposure_state(
        {
            "development_scope": development_scope,
            "confirmation_scope": confirmation_scope,
        }
    )
    binding = source_manifest["dataset_payload"]["dense_runtime"]
    capacity_path, _capacity = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-capacity",
            "registered_at": created_at,
            "requested_dates": sorted(
                {
                    *warmup,
                    *development,
                    *embargo,
                    *confirmation_warmup,
                    *confirmation,
                }
            ),
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": [
                    _repo_path(SOURCE_MANIFEST),
                    _repo_path(SOURCE_INSPECTION),
                    "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
                ],
                "inspected": True,
                "point_in_time_evidence": True,
                "dense_capacity": {
                    "family_id": FAMILY_ID,
                    "mechanism_family": MECHANISM_FAMILY,
                    "formal_capacity": len(development) * len(SYMBOLS),
                    "capacity_unit": (
                        "frozen instrument-session observations"
                    ),
                    "development_sessions": len(development),
                    "embargo_sessions": len(embargo),
                    "confirmation_sessions": len(confirmation),
                    "calendar_sha256": sha256_file(
                        reversal.CALENDAR_PATH
                    ),
                    "provider_requests": 0,
                    "market_prices_accessed": False,
                    "outcomes_accessed": False,
                    "source_market_prices_previously_accessed": True,
                    "source_provider_requests": 266,
                    "development_scope_previously_exposed": True,
                    "confirmation_scope_untouched": True,
                },
            },
        },
        DEFAULT_ROOT / SUCCESSOR_ID / "capacity",
    )
    contract = copy.deepcopy(base)
    for field in (
        "implementation_hashes",
        "primary_trial_id",
        "rolling_origin_plan",
        "trial_family",
    ):
        contract.pop(field, None)
    contract.update(
        {
            "campaign_id": CAMPAIGN_ID,
            "experiment_id": f"experiment-{SUCCESSOR_ID}",
            "family_id": FAMILY_ID,
            "mechanism_family": MECHANISM_FAMILY,
            "strategy_id": STRATEGY_ID,
            "parent_experiment_id": base["experiment_id"],
            "created_at": created_at,
            "status": "INVENTED",
            "research_generation": RESEARCH_GENERATION,
            "successor_id": SUCCESSOR_ID,
            "new_mechanism_family_slot_consumed": True,
            "prior_family_attempt_count": 0,
            "source_evidence": {
                "manifest_path": _repo_path(SOURCE_MANIFEST),
                "manifest_sha256": sha256_file(SOURCE_MANIFEST),
                "dataset_sha256": binding["dataset_sha256"],
                "inspection_path": _repo_path(SOURCE_INSPECTION),
                "inspection_sha256": source_inspection[
                    "artifact_sha256"
                ],
                "input_reused_only": True,
                "strategy_metrics_reused": False,
            },
            "mechanism": (
                "After a positive 30- or 60-minute index-ETF opening "
                "impulse, require one or two additional completed closes "
                "above cumulative session VWAP, enter on the next minute, "
                "and exit at the frozen ATR stop, R target, or session close."
            ),
            "parameter_grid": {
                "opening_window_minutes": [30, 60],
                "minimum_opening_return": [0.005, 0.01],
                "vwap_confirmation_completed_bars": [1, 2],
                "stop_intraday_atr": [1.0, 1.5],
                "target_r": [1.0, 1.5],
            },
            "entry_rule": (
                "Require the frozen positive opening return and completed "
                "above-VWAP confirmation, then enter only at the exact next "
                "observable minute open."
            ),
            "exit_rule": (
                "Exit stop-first on same-bar ambiguity, at the frozen R "
                "target, or at the regular-session final close."
            ),
            "stop_rule": (
                "Freeze a long stop one or one-and-a-half completed "
                "intraday ATR below entry."
            ),
            "ranking_rule": (
                "Earliest confirmation first, then strongest completed "
                "opening return, then canonical symbol."
            ),
            "falsifiers": [
                "nonpositive stressed log growth",
                "unstable parameter neighbors",
                "selection-aware statistical rejection",
                "incomplete execution or trial accounting",
                "opening momentum fails to persist after VWAP confirmation",
            ],
            "universe_requirements": {
                "symbols": list(SYMBOLS),
                "data": "complete SIP regular-session minute bars",
                "selection_basis": (
                    "Four fixed highly liquid broad U.S. index ETFs; an "
                    "incomplete symbol-session makes the entire entry date "
                    "a missed zero-return day."
                ),
            },
            "universe": {
                "symbols": list(SYMBOLS),
                "data": "complete SIP regular-session minute bars",
            },
            "development_evidence_classification": (
                "CONTAMINATED_TRAINING_ONLY"
            ),
            "development_outcomes_previously_exposed": True,
            "development_outcomes_eligible_for_confirmation": False,
            "dataset_manifest": _repo_path(SOURCE_MANIFEST),
            "source_dataset_manifest_sha256": sha256_file(
                SOURCE_MANIFEST
            ),
            "source_dataset_sha256": binding["dataset_sha256"],
            "contamination_risks": [
                "All development inputs are previously exposed and cannot satisfy confirmation maturity.",
                "No predecessor strategy metric or trial ranking is reused.",
                "The 310-session confirmation target block is globally untouched at freeze.",
                "The first five post-development full sessions are an immutable embargo.",
                "Incomplete sessions are explicit missed zero-return dates with no interpolation.",
            ],
            "development_dates": development,
            "development_warmup_dates": warmup,
            "confirmation_warmup_dates": confirmation_warmup,
            "embargo_dates": embargo,
            "confirmation_dates": confirmation,
            "confirmation_signal_capacity": len(confirmation),
            "development_scope": development_scope,
            "confirmation_scope": confirmation_scope,
            "outcome_exposure_index_sha256": outcome_exposure.audit()[
                "index_sha256"
            ],
            "calendar_path": _repo_path(reversal.CALENDAR_PATH),
            "historical_data_contract": {
                "minute_provider": "alpaca",
                "minute_feed": "sip",
                "minute_adjustment": "raw",
                "minute_request_mode": "symbol_range",
                "minute_missing_session_policy": (
                    collection.INTRADAY_FIXED_UNIVERSE_MISS_POLICY
                ),
                "session": "09:30-16:00 America/New_York",
                "provider_substitutions_allowed": False,
                "development_input_alias": _repo_path(SOURCE_MANIFEST),
            },
            "partitions": {
                "rolling_origin": True,
                "confirmation_untouched": True,
                "development_previously_exposed": True,
                "development_training_only": True,
                "five_session_embargo": True,
            },
            "plugin": {
                "module": "index_etf_opening_momentum_plugin",
                "preflight": "preflight",
                "evaluate_development": "evaluate_development",
                "evaluate_confirmation": "evaluate_confirmation",
                "evaluate_production": "evaluate_production",
            },
            "implementation_files": [
                "index_etf_opening_momentum.py",
                "index_etf_opening_momentum_plugin.py",
                "dense_data_collection.py",
                "dense_strategy_plugin.py",
                "dense_strategy_runtime.py",
                "learning_statistics.py",
                "learning_experiment.py",
                "strategy_discovery.py",
                "outcome_exposure.py",
                "portfolio_maturity.py",
                "portfolio_config.toml",
            ],
            "capacity_manifest": _repo_path(capacity_path),
        }
    )
    contract = strategy_discovery._validate_family_contract(contract)
    validate_contract(contract, enforce_commit=enforce_commit)
    digest = hashlib.sha256(
        artifact_support._canonical(contract)
    ).hexdigest()
    path = (
        DEFAULT_ROOT
        / SUCCESSOR_ID
        / "family-contract"
        / f"contract-{digest}.json"
    )
    artifact_support._write_json(path, contract)
    return path, contract, capacity_path


def validate_contract(
    contract: Mapping[str, Any], *, enforce_commit: bool = True
) -> None:
    _base, source_manifest, source_inspection = _source_evidence(
        enforce_commit=enforce_commit
    )
    (
        warmup,
        development,
        embargo,
        confirmation_warmup,
        confirmation,
    ) = reversal._post2016_partitions()
    binding = source_manifest["dataset_payload"]["dense_runtime"]
    if not (
        contract.get("campaign_id") == CAMPAIGN_ID
        and contract.get("family_id") == FAMILY_ID
        and contract.get("mechanism_family") == MECHANISM_FAMILY
        and contract.get("successor_id") == SUCCESSOR_ID
        and contract.get("research_generation") == RESEARCH_GENERATION
        and contract.get("new_mechanism_family_slot_consumed") is True
        and contract.get("prior_family_attempt_count") == 0
        and len(contract.get("trial_family", [])) == 32
        and contract.get("development_warmup_dates") == warmup
        and contract.get("development_dates") == development
        and contract.get("embargo_dates") == embargo
        and contract.get("confirmation_warmup_dates")
        == confirmation_warmup
        and contract.get("confirmation_dates") == confirmation
        and contract.get("development_scope") == _scope(development)
        and contract.get("confirmation_scope") == _scope(confirmation)
        and contract.get("universe", {}).get("symbols") == SYMBOLS
        and contract.get("dataset_manifest")
        == _repo_path(SOURCE_MANIFEST)
        and contract.get("source_dataset_manifest_sha256")
        == sha256_file(SOURCE_MANIFEST)
        and contract.get("source_dataset_sha256")
        == binding["dataset_sha256"]
        and contract.get("source_evidence", {}).get(
            "inspection_sha256"
        )
        == source_inspection["artifact_sha256"]
        and contract.get("development_evidence_classification")
        == "CONTAMINATED_TRAINING_ONLY"
        and contract.get("development_outcomes_eligible_for_confirmation")
        is False
        and contract.get("confirmation_signal_capacity")
        == len(confirmation)
        and contract.get("historical_data_contract", {}).get(
            "minute_missing_session_policy"
        )
        == collection.INTRADAY_FIXED_UNIVERSE_MISS_POLICY
    ):
        raise IndexEtfOpeningMomentumError(
            "index-ETF opening-momentum contract drifted"
        )
    _validate_exposure_state(contract)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze",))
    parser.add_argument("--created-at", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        path, contract, capacity = freeze_contract(
            created_at=args.created_at
        )
        print(
            json.dumps(
                {
                    "path": _repo_path(path),
                    "state": contract["status"],
                    "trial_count": len(contract["trial_family"]),
                    "development_sessions": len(
                        contract["development_dates"]
                    ),
                    "confirmation_sessions": len(
                        contract["confirmation_dates"]
                    ),
                    "capacity_manifest": _repo_path(capacity),
                    "confirmation_access_permitted": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        IndexEtfOpeningMomentumError,
        OSError,
        ValueError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
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
