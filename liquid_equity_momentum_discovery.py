"""Freeze a liquidity-constrained common-stock momentum successor immediately."""

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
import outcome_exposure
import portfolio_maturity
import strategy_discovery
from historical_store import sha256_file
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.LIQUID_EQUITY_MOMENTUM_FAMILY
MECHANISM_FAMILY = "cross-sectional-momentum"
STRATEGY_ID = "liquid-equity-cross-sectional-momentum"
SUCCESSOR_ID = "cross-sectional-momentum-v4-liquid-common-stock"
RESEARCH_GENERATION = "existing_family_successor"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
DISCOVERY_ROOT = PROJECT_ROOT / "strategy_tournament/v2/discovery"

SOURCE_SELECTIONS = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v2/"
    "selection-2026-07-19-100-days.json",
    PROJECT_ROOT
    / "historical_batches/development_tranche_v3/"
    "selection-2026-07-20-100-days.json",
)
SOURCE_DETAILS = (
    PROJECT_ROOT / "learning_runs/scanner_expansion_v2/scanner-replay-detail.json",
    PROJECT_ROOT
    / "learning_runs/development_tranche_v3/scanner_replay/"
    "scanner-replay-detail.json",
)
CALENDAR_PATH = (
    PROJECT_ROOT
    / "historical_batches/preentry_structure/"
    "session-calendar-2024-12-through-2026-06.json"
)
SOURCE_STATUS = (
    PROJECT_ROOT / "strategy_tournament/cross_sectional_momentum/collection-status.json"
)
PREDECESSOR_ACTIVATION = (
    PROJECT_ROOT
    / "strategy_tournament/activations/"
    "cross-sectional-momentum-v1-"
    "a8970ad2bfb0045f54404da985178686ec7cd0068c3a6a23f59165c7e415520a.json"
)
PREDECESSOR_RESULT = (
    PROJECT_ROOT
    / "research_results/2026-07-21-cross-sectional-momentum-stage0-"
    "9a165ae149b2f9220a2d78c2251a34617ad63ed2a468ded8749fb5b0cc0a8869.json"
)
PREDECESSOR_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/inspections/cross-sectional-momentum-v1-result-"
    "fa04429130158e31d221151b1d00d4dd3432ec43a8c88595dd0f8bc4a3c0421b.json"
)
SOURCE_EXTERNAL_RELATIVE = (
    "_derived/cross_sectional_momentum_stage0/"
    "dataset-cross-sectional-momentum-stage0-2026-07-21-v1/daily-bars.json.gz"
)
DEVELOPMENT_SIGNAL_COUNT = 80
LOOKBACK_SESSIONS = 60
MAXIMUM_HOLD_SESSIONS = 5
MINIMUM_CONFIRMATION_SIGNAL_CAPACITY = 20


class LiquidEquityMomentumDiscoveryError(RuntimeError):
    """The successor source graph or evidence boundary is invalid."""


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
        raise LiquidEquityMomentumDiscoveryError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise LiquidEquityMomentumDiscoveryError(
            f"{path} must contain an object"
        )
    return value


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise LiquidEquityMomentumDiscoveryError(
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
        raise LiquidEquityMomentumDiscoveryError(
            "created_at is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise LiquidEquityMomentumDiscoveryError(
            "created_at needs a timezone"
        )


def _calendar() -> list[str]:
    try:
        raw = json.loads(CALENDAR_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LiquidEquityMomentumDiscoveryError(
            "frozen session calendar is unavailable"
        ) from exc
    if not isinstance(raw, list):
        raise LiquidEquityMomentumDiscoveryError(
            "frozen session calendar is malformed"
        )
    dates = [
        str(row.get("date"))
        for row in raw
        if isinstance(row, Mapping)
        and "2024-12-16" <= str(row.get("date")) <= "2025-12-22"
    ]
    if dates != sorted(set(dates)):
        raise LiquidEquityMomentumDiscoveryError(
            "frozen session calendar is not chronological"
        )
    return dates


def _source_dates(*, enforce_commit: bool) -> list[str]:
    paths = (
        *SOURCE_SELECTIONS,
        CALENDAR_PATH,
        SOURCE_STATUS,
        PREDECESSOR_ACTIVATION,
        PREDECESSOR_RESULT,
        PREDECESSOR_INSPECTION,
    )
    if enforce_commit:
        for path in paths:
            strategy_discovery.require_committed(path)
    dates: list[str] = []
    for path in SOURCE_SELECTIONS:
        selection = _read(path)
        selected = selection.get("selected_dates")
        if not (
            isinstance(selected, list)
            and len(selected) == 100
            and len(selected) == len(set(selected))
            and selection.get("target_outcomes_observed_or_derived") is False
        ):
            raise LiquidEquityMomentumDiscoveryError(
                f"{path}: source selection is not outcome locked"
            )
        dates.extend(map(str, selected))
    if len(dates) != 200 or len(set(dates)) != 200:
        raise LiquidEquityMomentumDiscoveryError(
            "point-in-time source dates overlap or are incomplete"
        )
    status = _read(SOURCE_STATUS)
    activation = _read(PREDECESSOR_ACTIVATION)
    predecessor = _read(PREDECESSOR_RESULT)
    inspection = _read(PREDECESSOR_INSPECTION)
    expected_detail_hashes = {
        str(binding.get("detail_path")): str(binding.get("detail_sha256"))
        for binding in activation.get("source_bindings", [])
        if isinstance(binding, Mapping)
    }
    observed_detail_hashes = {
        _repo_path(path): sha256_file(path) for path in SOURCE_DETAILS
    }
    if not (
        status.get("status") == "READY"
        and status.get("returns_computed") == 0
        and status.get("broker_actions") == 0
        and status.get("private_daily_file_sha256")
        == "ce532df2278db7e1368f9b88d795e23656806a92149c757d7c338735aa2a156f"
        and predecessor.get("variant_id") == "cross-sectional-momentum-v1"
        and predecessor.get("stage0_survived") is False
        and predecessor.get("stage0_blockers")
        == ["primary drawdown exceeds the Stage 0 maximum"]
        and inspection.get("result_sha256") == predecessor.get("result_sha256")
        and inspection.get("valid") is True
        and observed_detail_hashes == expected_detail_hashes
    ):
        raise LiquidEquityMomentumDiscoveryError(
            "predecessor or cached source evidence drifted"
        )
    return sorted(dates)


def _partitions(
    *, enforce_commit: bool
) -> tuple[list[str], list[str], list[str], list[str], list[str]]:
    calendar = _calendar()
    positions = {day: index for index, day in enumerate(calendar)}
    source_dates = _source_dates(enforce_commit=enforce_commit)
    eligible = [
        day
        for day in source_dates
        if day in positions
        and positions[day] >= LOOKBACK_SESSIONS
        and positions[day] + MAXIMUM_HOLD_SESSIONS < len(calendar)
    ]
    if len(eligible) < DEVELOPMENT_SIGNAL_COUNT + MINIMUM_CONFIRMATION_SIGNAL_CAPACITY:
        raise LiquidEquityMomentumDiscoveryError(
            "source has insufficient chronological signal capacity"
        )
    development_signals = eligible[:DEVELOPMENT_SIGNAL_COUNT]
    final_development_index = (
        positions[development_signals[-1]] + MAXIMUM_HOLD_SESSIONS
    )
    development_dates = [
        day
        for day in calendar
        if "2025-01-02" <= day <= calendar[final_development_index]
    ]
    embargo = calendar[
        final_development_index + 1 : final_development_index + 6
    ]
    if len(embargo) != 5:
        raise LiquidEquityMomentumDiscoveryError(
            "five-session successor embargo is unavailable"
        )
    index = outcome_exposure.read_index()
    confirmation_signals = [
        day
        for day in eligible[DEVELOPMENT_SIGNAL_COUNT:]
        if day > embargo[-1]
        and not outcome_exposure.find_overlaps(
            {"dates": [day], "symbols": ["*"]}, index
        )
    ]
    if len(confirmation_signals) < MINIMUM_CONFIRMATION_SIGNAL_CAPACITY:
        raise LiquidEquityMomentumDiscoveryError(
            "INSUFFICIENT_POWER_CAPACITY: fewer than 20 untouched signal dates"
        )
    final_confirmation_index = (
        positions[confirmation_signals[-1]] + MAXIMUM_HOLD_SESSIONS
    )
    confirmation_dates = [
        day
        for day in calendar
        if embargo[-1] < day <= calendar[final_confirmation_index]
    ]
    return (
        development_dates,
        development_signals,
        embargo,
        confirmation_dates,
        confirmation_signals,
    )


def _source_evidence_paths() -> list[str]:
    return [
        *[_repo_path(path) for path in SOURCE_SELECTIONS],
        _repo_path(CALENDAR_PATH),
        _repo_path(SOURCE_STATUS),
        _repo_path(PREDECESSOR_ACTIVATION),
        _repo_path(PREDECESSOR_RESULT),
        _repo_path(PREDECESSOR_INSPECTION),
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
    ]


def _source_binding(
    *,
    signal_dates: list[str],
    formal_capacity: int,
) -> dict[str, Any]:
    status = _read(SOURCE_STATUS)
    return {
        "family_id": FAMILY_ID,
        "external_relative_path": SOURCE_EXTERNAL_RELATIVE,
        "external_file_sha256": status["private_daily_file_sha256"],
        "format": "json.gz",
        "signal_dates": signal_dates,
        "formal_capacity": formal_capacity,
        "provider_requests": 0,
        "source_status_path": _repo_path(SOURCE_STATUS),
        "source_status_file_sha256": sha256_file(SOURCE_STATUS),
    }


def freeze_successor_contract(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any], Path]:
    """Freeze all 32 trials without opening the cached daily price file."""

    _timestamp(created_at)
    (
        development,
        development_signals,
        embargo,
        confirmation,
        confirmation_signals,
    ) = _partitions(enforce_commit=enforce_commit)
    development_scope = {
        "dates": development_signals,
        "symbols": ["*"],
    }
    confirmation_scope = {
        "dates": confirmation_signals,
        "symbols": ["*"],
    }
    index = outcome_exposure.read_index()
    if not outcome_exposure.find_overlaps(development_scope, index):
        raise LiquidEquityMomentumDiscoveryError(
            "development is not explicitly contaminated training"
        )
    outcome_exposure.assert_untouched(confirmation_scope, index)
    outcome_exposure.assert_disjoint([development_scope, confirmation_scope])
    capacity_path, _capacity = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-development",
            "registered_at": created_at,
            "requested_dates": development,
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": _source_evidence_paths(),
                "inspected": True,
                "point_in_time_evidence": True,
                "development_training_contaminated": True,
                "confirmation_access_permitted": False,
                "liquid_equity_momentum_source": _source_binding(
                    signal_dates=development_signals,
                    formal_capacity=len(development_signals) * 250,
                ),
            },
        },
        root / SUCCESSOR_ID / "capacity",
    )
    universe = {
        "point_in_time": True,
        "security_type": "CS",
        "minimum_prior_close": 10.0,
        "minimum_median_20_session_dollar_volume": 50_000_000.0,
        "liquidity_ranking_sessions": 60,
        "maximum_names": 250,
        "split_affected_windows": "excluded",
    }
    contract: dict[str, Any] = {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "experiment_id": f"experiment-{SUCCESSOR_ID}",
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "strategy_id": STRATEGY_ID,
        "parent_experiment_id": "cross-sectional-momentum-v1",
        "created_at": created_at,
        "status": "INVENTED",
        "research_generation": RESEARCH_GENERATION,
        "successor_id": SUCCESSOR_ID,
        "new_mechanism_family_slot_consumed": False,
        "prior_family_attempt_count": 3,
        "mechanism": (
            "Rank the complete point-in-time liquid common-stock universe by "
            "completed trailing return and buy the strongest cost-clearing "
            "leader that remains above its frozen trend filter."
        ),
        "expected_holding_behavior": (
            "Long only, next-session-open entry, and flat within five sessions."
        ),
        "dataset_lane": "development",
        "universe_requirements": universe,
        "entry_rule": (
            "After a completed close, select the top 250 qualified common stocks "
            "by prior 60-session median dollar volume, rank trailing returns, "
            "require a 1% or 2% excess over the eligible median and the cost "
            "floor, then enter the deterministic leader at the next open."
        ),
        "stop_rule": (
            "Place the frozen 1.5 or 2.0 ATR14 structural stop below entry; "
            "invalid stops, missing bars, or split-affected windows are missed."
        ),
        "exit_rule": (
            "Resolve stop first on daily ambiguity and otherwise exit at the "
            "completed close after three or five sessions."
        ),
        "ranking_rule": (
            "Highest completed trailing return, then lexical symbol; at most "
            "one new family entry per session."
        ),
        "selection_rule": (
            "Portfolio risk, concurrent-position, aggregate-risk, daily-entry, "
            "gross-notional, and capital-contention caps remain authoritative."
        ),
        "parameter_grid": {
            "return_lookback_sessions": [20, 60],
            "trend_sma_sessions": [50, 100],
            "minimum_excess_return_fraction": [0.01, 0.02],
            "stop_atr14": [1.5, 2.0],
            "maximum_hold_sessions": [3, 5],
        },
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "primary_outcome": (
            "Selection-adjusted chronological account log growth after costs."
        ),
        "execution_assumptions": {
            "next_observable_fill": True,
            "ambiguity": "stop_first",
            "maximum_hold_sessions": 5,
            "missing_data": "missed_trade_no_substitute",
            "minimum_gross_to_primary_round_trip_cost": 5.0,
            "shared_signal_contention": (
                "the 20-bps path freezes the conservative filled-signal set "
                "used unchanged by all cost scenarios"
            ),
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
            "The development decision dates include inspected v1 outcomes and are training only.",
            "The predecessor positive mean and excessive drawdown are hypothesis-generating only.",
            "No confirmation decision date may enter development or selection.",
        ],
        "production_compatibility_risks": [
            "Live ranking needs a complete point-in-time common-stock reference snapshot, sixty completed daily sessions, corporate actions, and fresh execution facts."
        ],
        "material_difference_rationale": (
            "This exact version retains the existing cross-sectional-momentum "
            "mechanism while prospectively replacing the predecessor's thin "
            "top-decile exposure with a top-250, fifty-million-dollar liquidity "
            "floor, one-entry limit, and a selection-aware 32-trial search."
        ),
        "development_dates": development,
        "development_signal_dates": development_signals,
        "embargo_dates": embargo,
        "confirmation_dates": confirmation,
        "confirmation_signal_dates": confirmation_signals,
        "confirmation_signal_capacity": len(confirmation_signals),
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "outcome_exposure_index_sha256": outcome_exposure.audit()["index_sha256"],
        "universe": universe,
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "development_training_contaminated": True,
            "account_calendar_includes_zero_and_mark_to_market_days": True,
        },
        "falsifiers": [
            "nonpositive stressed log growth",
            "unstable parameter neighbors",
            "selection-aware statistical rejection",
            "incomplete execution or trial accounting",
            "insufficient frozen confirmation power capacity",
        ],
        "implementation_files": [
            "liquid_equity_momentum_discovery.py",
            "liquid_equity_momentum_plugin.py",
            "dense_strategy_plugin.py",
            "dense_strategy_runtime.py",
            "learning_statistics.py",
            "learning_experiment.py",
            "strategy_discovery.py",
            "outcome_exposure.py",
            "portfolio_maturity.py",
            "portfolio_config.toml",
        ],
        "plugin": {
            "module": "liquid_equity_momentum_plugin",
            "preflight": "preflight",
            "evaluate_development": "evaluate_development",
            "evaluate_confirmation": "evaluate_confirmation",
            "evaluate_production": "evaluate_production",
        },
        "capacity_policy": {"retire_below": 50, "fast_lane_at": 100},
        "capacity_manifest": _repo_path(capacity_path),
        "dataset_manifest": _repo_path(capacity_path),
    }
    validated = strategy_discovery._validate_family_contract(contract)
    digest = hashlib.sha256(_canonical(validated)).hexdigest()
    path = root / SUCCESSOR_ID / "family-contract" / f"contract-{digest}.json"
    _write_json(path, validated)
    return path, validated, capacity_path


def freeze_confirmation_dataset(
    winner_path: Path,
    *,
    created_at: str,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    """Bind the cached file to one committed winner without opening outcomes."""

    _timestamp(created_at)
    if enforce_commit:
        strategy_discovery.require_committed(winner_path)
    winner = strategy_discovery.load_artifact(
        winner_path, expected_kind="frozen-strategy-winner"
    )
    if not (
        winner.get("family_id") == FAMILY_ID
        and winner.get("state") == "WINNER_FROZEN"
        and winner.get("confirmation_access_permitted") is True
    ):
        raise LiquidEquityMomentumDiscoveryError(
            "winner does not authorize exact confirmation"
        )
    outcome_exposure.assert_untouched(
        winner["confirmation_scope"], outcome_exposure.read_index()
    )
    return freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": (
                f"dataset-{winner['strategy_version']}-confirmation"
            ),
            "registered_at": created_at,
            "requested_dates": winner["confirmation_dates"],
            "dataset_payload": {
                "lane": "confirmation",
                "claim_scope": "EXACT_PREREGISTERED_CONTRACT_ONLY",
                "evidence_paths": [
                    _repo_path(winner_path),
                    *_source_evidence_paths(),
                ],
                "inspected": True,
                "point_in_time_evidence": True,
                "preregistration_sha256": winner["rules_hash"],
                "capture_after_preregistration_attested": True,
                "liquid_equity_momentum_source": _source_binding(
                    signal_dates=winner["confirmation_signal_dates"],
                    formal_capacity=winner["confirmation_signal_capacity"],
                ),
            },
        },
        DISCOVERY_ROOT / FAMILY_ID / "confirmation-dataset",
    )


def status(*, root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    contracts = sorted(
        (root / SUCCESSOR_ID / "family-contract").glob("contract-*.json")
    )
    return {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "successor_id": SUCCESSOR_ID,
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "calendar_wait_required": False,
        "new_mechanism_family_slot_consumed": False,
        "contracts": len(contracts),
        "confirmation_outcomes_accessed": False,
        "broker_actions_permitted": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status")
    freeze = sub.add_parser("freeze")
    freeze.add_argument("--created-at", required=True)
    confirmation = sub.add_parser("freeze-confirmation")
    confirmation.add_argument("winner", type=Path)
    confirmation.add_argument("--created-at", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "status":
            result = status(root=args.root)
        elif args.command == "freeze":
            path, artifact, capacity = freeze_successor_contract(
                created_at=args.created_at,
                root=args.root,
            )
            result = {
                "path": _repo_path(path),
                "capacity_manifest": _repo_path(capacity),
                "state": artifact["status"],
                "trial_count": len(artifact["trial_family"]),
                "development_signal_dates": len(
                    artifact["development_signal_dates"]
                ),
                "confirmation_signal_capacity": artifact[
                    "confirmation_signal_capacity"
                ],
                "calendar_wait_required": False,
            }
        else:
            path, artifact = freeze_confirmation_dataset(
                args.winner,
                created_at=args.created_at,
            )
            result = {
                "path": _repo_path(path),
                "rules_hash": artifact["dataset_payload"][
                    "preregistration_sha256"
                ],
                "state": "CONFIRMATION_DATASET_FROZEN",
            }
    except (
        LiquidEquityMomentumDiscoveryError,
        strategy_discovery.StrategyDiscoveryError,
        outcome_exposure.OutcomeExposureError,
    ) as exc:
        print(json.dumps({"error": str(exc)}, indent=2, sort_keys=True))
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
