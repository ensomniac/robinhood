"""Freeze the exact disjoint long-history residual-reversal family contract."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

import outcome_exposure
import residual_replication_data as data
import strategy_discovery
from historical_store import canonical_json_bytes, canonical_sha256, sha256_file
from learning_data import load_frozen_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
ROOT = data.ROOT
V5_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-equity-market-residual-reversal/development-inspection/"
    "liquid-equity-market-residual-reversal-development-inspection-"
    "c5fba1719ef3c26e2bd9fda0fe06fd4cbd1da58c3d604260adc53ebf95f9e138.json"
)


class ResidualReplicationDiscoveryError(RuntimeError):
    """The disjoint replication source graph or exact contract is invalid."""


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise ResidualReplicationDiscoveryError(
            f"path escaped repository: {path}"
        ) from exc


def _timestamp(value: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ResidualReplicationDiscoveryError(
            "created_at is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise ResidualReplicationDiscoveryError(
            "created_at needs a timezone"
        )


def _write_contract(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != rendered:
            raise ResidualReplicationDiscoveryError(
                "immutable family contract drifted"
            )
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(rendered, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _single(path: Path, pattern: str) -> Path:
    matches = sorted(path.glob(pattern))
    if len(matches) != 1:
        raise ResidualReplicationDiscoveryError(
            f"expected exactly one artifact under {path}; found {len(matches)}"
        )
    return matches[0]


def freeze_family(
    *,
    created_at: str,
    root: Path = ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    _timestamp(created_at)
    inspection_path = _single(
        root / "development-data-inspection", "*.json"
    )
    manifest_path = _single(root / "development-dataset", "dataset-*.json")
    if enforce_commit:
        for path in (inspection_path, manifest_path, V5_INSPECTION):
            strategy_discovery.require_committed(path)
    inspection = strategy_discovery.load_artifact(
        inspection_path, expected_kind=data.COLLECTION_INSPECTION_KIND
    )
    if not (
        inspection.get("state") == data.COLLECTION_INSPECTION_STATE
        and isinstance(inspection.get("checks"), Mapping)
        and all(inspection["checks"].values())
        and inspection.get("strategy_metrics_accessed") is False
        and inspection.get("confirmation_prices_accessed") is False
    ):
        raise ResidualReplicationDiscoveryError(
            "development data is not independently inspected"
        )
    contract_path = PROJECT_ROOT / str(inspection["contract_path"])
    contract = data._load_contract(
        contract_path, enforce_commit=enforce_commit
    )
    manifest = load_frozen_dataset_contract(manifest_path)
    binding = manifest["dataset_payload"].get("dense_runtime")
    if not (
        isinstance(binding, Mapping)
        and binding.get("family_id") == data.FAMILY_ID
        and binding.get("formal_capacity")
        == len(contract["development_signal_dates"])
        and manifest.get("requested_dates") == contract["development_dates"]
        and manifest["dataset_payload"].get("collection_inspection_sha256")
        == inspection["artifact_sha256"]
    ):
        raise ResidualReplicationDiscoveryError(
            "development dataset manifest drifted"
        )
    predecessor = strategy_discovery.load_artifact(
        V5_INSPECTION, expected_kind="development-search-inspection"
    )
    if not (
        predecessor.get("state") == "REJECTED"
        and predecessor.get("family_id")
        == "liquid-equity-market-residual-reversal"
    ):
        raise ResidualReplicationDiscoveryError(
            "V5 adverse predecessor is unavailable"
        )
    development_scope = dict(contract["development_scope"])
    confirmation_scope = dict(contract["confirmation_scope"])
    index = outcome_exposure.read_index()
    if outcome_exposure.find_overlaps(development_scope, index):
        raise ResidualReplicationDiscoveryError(
            "replication development evidence is no longer disjoint"
        )
    outcome_exposure.assert_untouched(confirmation_scope, index)
    family: dict[str, Any] = {
        "schema_version": 1,
        "campaign_id": data.CAMPAIGN_ID,
        "experiment_id": (
            "experiment-two-to-three-day-cross-sectional-reversal-v6-"
            "disjoint-long-history"
        ),
        "family_id": data.FAMILY_ID,
        "mechanism_family": data.MECHANISM_FAMILY,
        "strategy_id": data.MECHANISM_FAMILY,
        "parent_experiment_id": (
            "experiment-two-to-three-day-cross-sectional-reversal-v5-"
            "liquid-common-stock-residual-spy"
        ),
        "created_at": created_at,
        "status": "INVENTED",
        "research_generation": "existing_family_disjoint_replication",
        "successor_id": data.SUCCESSOR_ID,
        "new_mechanism_family_slot_consumed": False,
        "predecessor": {
            "path": _repo_path(V5_INSPECTION),
            "file_sha256": sha256_file(V5_INSPECTION),
            "artifact_sha256": predecessor["artifact_sha256"],
            "state": predecessor["state"],
            "evaluated_corpus_reused": False,
            "outcome_guided_parameter_change": False,
        },
        "mechanism": (
            "Buy the largest short-horizon idiosyncratic downside overshoot "
            "inside the complete point-in-time top-250 liquid common-stock "
            "universe."
        ),
        "expected_holding_behavior": (
            "Long only, next-session-open entry, and flat within five sessions."
        ),
        "dataset_lane": "development",
        "universe_requirements": dict(contract["universe_contract"]),
        "entry_rule": (
            "After the completed close, calculate one- or three-session stock "
            "return less SPY return, standardize it on the latest sixty "
            "completed observations, require the frozen downside z threshold, "
            "the frozen SPY trend gate, and the five-times-cost floor, then "
            "enter the most negative eligible residual at the next open."
        ),
        "stop_rule": (
            "Freeze a stop one or one-and-a-half completed ATR14 below entry; "
            "invalid stops, missing bars, and split-affected windows are missed."
        ),
        "exit_rule": (
            "Resolve the frozen stop first on daily ambiguity and otherwise "
            "exit at the completed close after two or five sessions."
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
            "V5 outcomes are adverse predecessor evidence only.",
            "The 200 development dates are disjoint from every V5 date.",
            "No confirmation price may be opened before winner freeze.",
        ],
        "production_compatibility_risks": [
            "Live evaluation requires a complete dated common-stock reference, "
            "two hundred SPY sessions, sixty liquidity sessions, fresh quotes, "
            "tradability, and confirmed overnight protection."
        ],
        "material_difference_rationale": (
            "This replication preserves the complete prospectively declared "
            "48-trial family while replacing the failed 80-date corpus with "
            "200 disjoint outcome-blind dates, 96 untouched confirmation dates, "
            "true top-250 liquidity selection, and explicit split-window "
            "exclusion. No evaluated V5 date or parameter repair is reused."
        ),
        "development_dates": list(contract["development_dates"]),
        "development_signal_dates": list(
            contract["development_signal_dates"]
        ),
        "embargo_dates": list(contract["embargo_dates"]),
        "confirmation_dates": list(contract["confirmation_dates"]),
        "confirmation_signal_dates": list(
            contract["confirmation_signal_dates"]
        ),
        "confirmation_signal_capacity": len(
            contract["confirmation_signal_dates"]
        ),
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "universe": {
            "point_in_time": True,
            "security_type": "CS",
            "maximum_names": 250,
            "minimum_prior_close": 10.0,
            "minimum_median_20_session_dollar_volume": 50_000_000.0,
            "liquidity_ranking_sessions": 60,
        },
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "development_disjoint_replication": True,
            "account_calendar_includes_zero_and_mark_to_market_days": True,
        },
        "falsifiers": [
            "nonpositive stressed log growth",
            "stressed drawdown above 6R",
            "unstable parameter neighbors",
            "selection-aware statistical rejection",
            "incomplete execution or trial accounting",
            "insufficient frozen confirmation power capacity",
        ],
        "implementation_files": [
            "residual_replication_data.py",
            "residual_replication_inspection.py",
            "residual_replication_discovery.py",
            "residual_replication_plugin.py",
            "dense_strategy_runtime.py",
            "dense_strategy_plugin.py",
            "learning_statistics.py",
            "learning_experiment.py",
            "strategy_discovery.py",
            "outcome_exposure.py",
            "portfolio_maturity.py",
            "portfolio_config.toml",
        ],
        "plugin": {
            "module": "residual_replication_plugin",
            "preflight": "preflight",
            "evaluate_development": "evaluate_development",
            "evaluate_confirmation": "evaluate_confirmation",
            "evaluate_production": "evaluate_production",
        },
        "capacity_policy": {"retire_below": 50, "fast_lane_at": 100},
        "capacity_manifest": _repo_path(manifest_path),
        "dataset_manifest": _repo_path(manifest_path),
        "data_contract_path": _repo_path(contract_path),
        "data_contract_sha256": contract["artifact_sha256"],
        "data_inspection_path": _repo_path(inspection_path),
        "data_inspection_sha256": inspection["artifact_sha256"],
        "confirmation_access_permitted": False,
    }
    validated = strategy_discovery._validate_family_contract(family)
    digest = hashlib.sha256(canonical_json_bytes(validated)).hexdigest()
    path = (
        root
        / "family-contract"
        / f"contract-{digest}.json"
    )
    _write_contract(path, validated)
    return path, validated


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--created-at", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        path, contract = freeze_family(
            created_at=args.created_at,
            root=args.root,
        )
        print(
            json.dumps(
                {
                    "written": _repo_path(path),
                    "contract_sha256": canonical_sha256(contract),
                    "family_id": contract["family_id"],
                    "trial_count": 48,
                    "confirmation_signal_capacity": contract[
                        "confirmation_signal_capacity"
                    ],
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        ResidualReplicationDiscoveryError,
        data.ResidualReplicationDataError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
