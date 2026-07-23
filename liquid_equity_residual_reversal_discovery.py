"""Freeze a liquid-common-stock successor to cross-sectional reversal.

The weekly ceiling governs new mechanism families. This exact version remains
inside the already evaluated two-to-three-day cross-sectional-reversal
mechanism, uses contaminated development dates only as training, and keeps the
later confirmation dates unopened until an exact winner is frozen.
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
import liquid_equity_momentum_discovery as source
import outcome_exposure
import portfolio_maturity
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.EQUITY_RESIDUAL_FAMILY
MECHANISM_FAMILY = "two-to-three-day-cross-sectional-reversal"
STRATEGY_ID = MECHANISM_FAMILY
SUCCESSOR_ID = (
    "two-to-three-day-cross-sectional-reversal-v5-"
    "liquid-common-stock-residual-spy"
)
RESEARCH_GENERATION = "existing_family_successor"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
DISCOVERY_ROOT = PROJECT_ROOT / "strategy_tournament/v2/discovery"

V1_RESULT = (
    PROJECT_ROOT
    / "research_results/"
    "2026-07-21-two-to-three-day-cross-sectional-reversal-stage0-"
    "3b54a1c87305fe1e808f3852779900728d741d9062a42fe56d8677108bcca057.json"
)
V1_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/second_wave/inspections/"
    "two-to-three-day-cross-sectional-reversal-v1-result-"
    "21ad67029e44d87612e862031a8bbace20bdae2d64cade6dc266c3677b0132c7.json"
)
V2_CONTRACT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "two-to-three-day-cross-sectional-reversal-v2-liquid-index-etf/"
    "family-contract/"
    "contract-ec87155722f2a1c8e180dd23f9bae581139a20af097589ca68ffa4f59f0f56e8.json"
)
V2_RESULT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-etf-cross-sectional-reversal/development/"
    "liquid-etf-cross-sectional-reversal-development-"
    "69c755055673961de87040e5a87df95d42118e46c8d57a6aee8e96fe8599c3eb.json"
)
V2_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-etf-cross-sectional-reversal/development-inspection/"
    "liquid-etf-cross-sectional-reversal-development-inspection-"
    "0abea388cc121ca801cd2c353fb2fa2da069f9397ba499b930a471d9cab29734.json"
)
V3_FAILURE = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-equity-market-residual-reversal/development-failures/"
    "liquid-equity-market-residual-reversal-development-failure-"
    "f5596e4e0aaca3208541fb3ab1ee4995e5b6bc9c7bfbd735637635622125a081.json"
)
V4_DIAGNOSTIC = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-equity-market-residual-reversal/development-diagnostics/"
    "liquid-equity-market-residual-reversal-development-diagnostic-"
    "dfd84e2e18f05c2891ed75713f5d7ee0dd42fc198136daa3ee60213cb639544f.json"
)
SPY_REFERENCE_START = "2024-01-02"


class LiquidEquityResidualDiscoveryError(RuntimeError):
    """The predecessor, source graph, or evidence boundary is invalid."""


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
        raise LiquidEquityResidualDiscoveryError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise LiquidEquityResidualDiscoveryError(
            f"{path} must contain an object"
        )
    return value


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise LiquidEquityResidualDiscoveryError(
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
        raise LiquidEquityResidualDiscoveryError(
            "created_at is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise LiquidEquityResidualDiscoveryError(
            "created_at needs a timezone"
        )


def _predecessor_paths() -> tuple[Path, ...]:
    return (
        V1_RESULT,
        V1_INSPECTION,
        V2_CONTRACT,
        V2_RESULT,
        V2_INSPECTION,
        V3_FAILURE,
        V4_DIAGNOSTIC,
    )


def _predecessors(*, enforce_commit: bool) -> None:
    if enforce_commit:
        for path in _predecessor_paths():
            strategy_discovery.require_committed(path)
    v1 = _read(V1_RESULT)
    v1_inspection = _read(V1_INSPECTION)
    v2_contract = _read(V2_CONTRACT)
    v2_result = strategy_discovery.load_artifact(
        V2_RESULT,
        expected_kind="development-search-result",
    )
    v2_inspection = strategy_discovery.load_artifact(
        V2_INSPECTION,
        expected_kind="development-search-inspection",
    )
    v3_failure = strategy_discovery.load_artifact(
        V3_FAILURE,
        expected_kind="development-evaluation-failure",
    )
    v4_diagnostic = strategy_discovery.load_artifact(
        V4_DIAGNOSTIC,
        expected_kind="development-engineering-diagnostic",
    )
    if not (
        v1.get("variant_id")
        == "two-to-three-day-cross-sectional-reversal-v1"
        and v1.get("stage0_survived") is False
        and "closed signals are below the Stage 0 minimum"
        in v1.get("stage0_blockers", [])
        and v1_inspection.get("result_sha256") == v1.get("result_sha256")
        and v1_inspection.get("stage0_survived") is False
        and v1_inspection.get("valid") is True
        and v2_contract.get("mechanism_family") == MECHANISM_FAMILY
        and v2_contract.get("successor_id")
        == "two-to-three-day-cross-sectional-reversal-v2-liquid-index-etf"
        and v2_result.get("family_id")
        == runtime.ETF_CROSS_SECTIONAL_REVERSAL_FAMILY
        and v2_result.get("state") == "DEVELOPMENT_EVALUATED"
        and v2_inspection.get("result_sha256")
        == v2_result.get("artifact_sha256")
        and v2_inspection.get("state") == "REJECTED"
        and v2_inspection.get("selection", {}).get("status") == "REJECTED"
        and v3_failure.get("state")
        == "FAILED_MISSING_SPY_REFERENCE_BOUNDARY"
        and v3_failure.get("candidate_outcomes_computed") is False
        and v3_failure.get("trial_metrics_surfaced") is False
        and v3_failure.get("confirmation_accessed") is False
        and v3_failure.get(
            "strategy_grid_dates_rules_costs_changed_after_failure"
        )
        is False
        and v4_diagnostic.get("state")
        == "CONTAMINATED_NOT_PROMOTION_EVIDENCE"
        and v4_diagnostic.get("implementation_hash_frozen_before_access")
        is False
        and v4_diagnostic.get("formal_development_result_created") is False
        and v4_diagnostic.get("independent_selection_executed") is False
        and v4_diagnostic.get("promotion_eligible") is False
        and v4_diagnostic.get("confirmation_accessed") is False
        and v4_diagnostic.get(
            "rules_grid_dates_costs_selection_changed_after_diagnostic"
        )
        is False
        and v4_diagnostic.get("permitted_successor", {}).get("successor_id")
        == SUCCESSOR_ID
    ):
        raise LiquidEquityResidualDiscoveryError(
            "cross-sectional-reversal predecessor graph is invalid"
        )


def _source_evidence_paths() -> list[str]:
    return [
        *[_repo_path(path) for path in source.SOURCE_SELECTIONS],
        _repo_path(source.CALENDAR_PATH),
        _repo_path(source.SOURCE_STATUS),
        _repo_path(source.PREDECESSOR_ACTIVATION),
        *[_repo_path(path) for path in _predecessor_paths()],
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
    ]


def _source_binding(
    *,
    signal_dates: list[str],
    formal_capacity: int,
) -> dict[str, Any]:
    status = _read(source.SOURCE_STATUS)
    return {
        "family_id": FAMILY_ID,
        "external_relative_path": source.SOURCE_EXTERNAL_RELATIVE,
        "external_file_sha256": status["private_daily_file_sha256"],
        "format": "json.gz",
        "signal_dates": signal_dates,
        "formal_capacity": formal_capacity,
        "provider_requests": 0,
        "source_status_path": _repo_path(source.SOURCE_STATUS),
        "source_status_file_sha256": sha256_file(source.SOURCE_STATUS),
    }


def _spy_dataset(
    store: HistoricalDayStore,
    day: str,
) -> dict[str, Any] | None:
    value = store.select_dataset(
        "SPY",
        day,
        kind="bars",
        channel="trades",
        timeframe="1d",
        providers=("ibkr",),
        require_complete=True,
        feed="smart",
        adjustment="provider_adjusted_unknown_basis",
    )
    if value is None or value.get("quality", {}).get("row_count") != 1:
        return None
    return value


def _spy_reference_binding(*, end_date: str) -> dict[str, Any]:
    """Content-address SPY metadata without deriving a price or return."""

    store = HistoricalDayStore.from_env()
    dates = [
        day
        for day in store.dates("SPY")
        if SPY_REFERENCE_START <= day <= end_date
    ]
    if (
        not dates
        or dates != sorted(set(dates))
        or dates[0] != SPY_REFERENCE_START
        or dates[-1] != end_date
        or len(dates) < 300
    ):
        raise LiquidEquityResidualDiscoveryError(
            "SPY reference calendar is incomplete"
        )
    rows: list[dict[str, Any]] = []
    for day in dates:
        path = store.path_for("SPY", day)
        dataset = _spy_dataset(store, day)
        if not path.is_file() or dataset is None:
            raise LiquidEquityResidualDiscoveryError(
                f"SPY reference is incomplete on {day}"
            )
        rows.append(
            {
                "date": day,
                "store_relative_path": str(path.relative_to(store.root)),
                "document_file_sha256": sha256_file(path),
                "dataset_id": dataset["id"],
                "dataset_content_sha256": dataset["content_sha256"],
            }
        )
    return {
        "symbol": "SPY",
        "provider": "ibkr",
        "channel": "trades",
        "timeframe": "1d",
        "feed": "smart",
        "adjustment": "provider_adjusted_unknown_basis",
        "session": "regular",
        "scope": "full_session",
        "reference_start": dates[0],
        "reference_end": dates[-1],
        "reference_sessions": len(dates),
        "rows": rows,
        "prices_or_returns_derived": False,
        "provider_requests": 0,
    }


def _partitions(
    *, enforce_commit: bool
) -> tuple[list[str], list[str], list[str], list[str], list[str]]:
    _predecessors(enforce_commit=enforce_commit)
    return source._partitions(enforce_commit=enforce_commit)


def freeze_successor_contract(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any], Path]:
    """Freeze all 48 trials without opening the cached daily price file."""

    _timestamp(created_at)
    (
        development,
        development_signals,
        embargo,
        confirmation,
        confirmation_signals,
    ) = _partitions(enforce_commit=enforce_commit)
    if enforce_commit:
        for path in (
            Path(__file__).resolve(),
            PROJECT_ROOT / "liquid_equity_momentum_plugin.py",
            PROJECT_ROOT / "dense_strategy_plugin.py",
            PROJECT_ROOT / "dense_strategy_runtime.py",
        ):
            strategy_discovery.require_committed(path)
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
        raise LiquidEquityResidualDiscoveryError(
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
                "liquid_equity_daily_source": _source_binding(
                    signal_dates=development_signals,
                    formal_capacity=len(development_signals) * 250,
                ),
                "spy_reference_source": _spy_reference_binding(
                    end_date=development[-1],
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
        "parent_experiment_id": (
            "two-to-three-day-cross-sectional-reversal-v4-"
            "liquid-common-stock-residual-spy"
        ),
        "created_at": created_at,
        "status": "INVENTED",
        "research_generation": RESEARCH_GENERATION,
        "successor_id": SUCCESSOR_ID,
        "new_mechanism_family_slot_consumed": False,
        "prior_family_attempt_count": 4,
        "predecessors": [
            {
                "path": _repo_path(path),
                "file_sha256": sha256_file(path),
            }
            for path in _predecessor_paths()
        ],
        "supersedes_failed_transition": {
            "successor_id": (
                "two-to-three-day-cross-sectional-reversal-v3-"
                "liquid-common-stock-residual"
            ),
            "failure_path": _repo_path(V3_FAILURE),
            "failure_file_sha256": sha256_file(V3_FAILURE),
            "failure_sha256": strategy_discovery.load_artifact(
                V3_FAILURE,
                expected_kind="development-evaluation-failure",
            )["artifact_sha256"],
            "trial_metrics_surfaced": False,
            "confirmation_accessed": False,
            "strategy_grid_dates_rules_costs_changed": False,
            "implementation_change": (
                "bind the complete IBKR SPY daily reference series needed by "
                "the already frozen market-residual and trend calculations"
            ),
        },
        "supersedes_diagnostic_transition": {
            "successor_id": (
                "two-to-three-day-cross-sectional-reversal-v4-"
                "liquid-common-stock-residual-spy"
            ),
            "diagnostic_path": _repo_path(V4_DIAGNOSTIC),
            "diagnostic_file_sha256": sha256_file(V4_DIAGNOSTIC),
            "diagnostic_sha256": strategy_discovery.load_artifact(
                V4_DIAGNOSTIC,
                expected_kind="development-engineering-diagnostic",
            )["artifact_sha256"],
            "promotion_eligible": False,
            "confirmation_accessed": False,
            "strategy_grid_dates_rules_costs_changed": False,
            "implementation_change": (
                "freeze the already verified SPY boundary and exact runtime "
                "with semantics-preserving index, feature, and candidate caches"
            ),
        },
        "mechanism": (
            "Buy the largest short-horizon downside idiosyncratic overshoot "
            "inside the complete point-in-time liquid common-stock universe."
        ),
        "expected_holding_behavior": (
            "Long only, next-session-open entry, and flat within five sessions."
        ),
        "dataset_lane": "development",
        "universe_requirements": universe,
        "entry_rule": (
            "After the completed close, compute one- or three-session returns "
            "less SPY, standardize each residual on the latest completed "
            "history, require the frozen downside z threshold, an open SPY "
            "trend gate, and the five-times-cost floor, then enter the most "
            "negative eligible residual at the next open."
        ),
        "stop_rule": (
            "Place the frozen 1.0 or 1.5 ATR14 structural stop below entry; "
            "invalid stops, missing bars, or split-affected windows are missed."
        ),
        "exit_rule": (
            "Resolve stop first on daily ambiguity and otherwise exit at the "
            "completed close after two or five sessions."
        ),
        "ranking_rule": (
            "Most negative completed residual z-score, then lexical symbol; "
            "at most one new family entry per session."
        ),
        "selection_rule": (
            "Portfolio risk, concurrent-position, aggregate-risk, daily-entry, "
            "gross-notional, and capital-contention caps remain authoritative."
        ),
        "parameter_grid": {
            "prior_return_sessions": [1, 3],
            "residual_z_threshold": [-1.5, -2.0, -2.5],
            "market_trend_gate": ["SPY>SMA100", "SPY>SMA200"],
            "stop_atr14": [1.0, 1.5],
            "hold_sessions": [2, 5],
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
            "All development decision dates are exposed training only.",
            "The ETF predecessor and momentum results are hypothesis evidence only.",
            "No confirmation decision date may enter development or selection.",
        ],
        "production_compatibility_risks": [
            "Live evaluation needs a complete common-stock reference snapshot, "
            "two hundred completed SPY sessions, sixty completed liquidity "
            "sessions, corporate actions, and fresh execution facts."
        ],
        "material_difference_rationale": (
            "V5 preserves the exact V3 short-horizon cross-sectional-reversal "
            "mechanism, grid, partitions, costs, and selection rule, binds the "
            "content-addressed SPY reference series whose absence stopped V3, "
            "and freezes the semantics-preserving runtime caches tested only "
            "as non-promotable V4 engineering diagnostics."
        ),
        "development_dates": development,
        "development_signal_dates": development_signals,
        "embargo_dates": embargo,
        "confirmation_dates": confirmation,
        "confirmation_signal_dates": confirmation_signals,
        "confirmation_signal_capacity": len(confirmation_signals),
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
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
            "liquid_equity_residual_reversal_discovery.py",
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
    """Bind the cached file to one committed residual-reversal winner."""

    _timestamp(created_at)
    if enforce_commit:
        strategy_discovery.require_committed(winner_path)
    winner = strategy_discovery.load_artifact(
        winner_path,
        expected_kind="frozen-strategy-winner",
    )
    if not (
        winner.get("family_id") == FAMILY_ID
        and winner.get("state") == "WINNER_FROZEN"
        and winner.get("confirmation_access_permitted") is True
    ):
        raise LiquidEquityResidualDiscoveryError(
            "winner does not authorize exact confirmation"
        )
    outcome_exposure.assert_untouched(
        winner["confirmation_scope"],
        outcome_exposure.read_index(),
    )
    return freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{winner['strategy_version']}-confirmation",
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
                "liquid_equity_daily_source": _source_binding(
                    signal_dates=winner["confirmation_signal_dates"],
                    formal_capacity=winner["confirmation_signal_capacity"],
                ),
                "spy_reference_source": _spy_reference_binding(
                    end_date=winner["confirmation_dates"][-1],
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
            path, contract, capacity = freeze_successor_contract(
                created_at=args.created_at,
                root=args.root,
            )
            result = {
                "state": "FAMILY_FROZEN",
                "family_id": contract["family_id"],
                "trial_count": len(contract["trial_family"]),
                "confirmation_signal_capacity": contract[
                    "confirmation_signal_capacity"
                ],
                "written": _repo_path(path),
                "capacity_manifest": _repo_path(capacity),
                "broker_actions_permitted": False,
            }
        else:
            path, manifest = freeze_confirmation_dataset(
                args.winner,
                created_at=args.created_at,
            )
            result = {
                "state": "CONFIRMATION_DATASET_FROZEN",
                "manifest_sha256": manifest["manifest_sha256"],
                "written": _repo_path(path),
                "broker_actions_permitted": False,
            }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        LiquidEquityResidualDiscoveryError,
        strategy_discovery.StrategyDiscoveryError,
        outcome_exposure.OutcomeExposureError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                },
                indent=2,
                sort_keys=True,
            ),
            file=os.sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
