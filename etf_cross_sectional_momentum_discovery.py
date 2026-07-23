"""Freeze an existing-family liquid-ETF cross-sectional momentum successor."""

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
from learning_data import freeze_dataset_contract, load_frozen_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.ETF_CROSS_SECTIONAL_MOMENTUM_FAMILY
MECHANISM_FAMILY = "cross-sectional-momentum"
STRATEGY_ID = MECHANISM_FAMILY
SUCCESSOR_ID = "cross-sectional-momentum-v2-liquid-index-etf"
RESEARCH_GENERATION = "existing_family_successor"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"

PREDECESSOR_VARIANT_ID = "cross-sectional-momentum-v1"
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
SOURCE_SEARCH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/liquid-etf-trend-pullback-cost-floor/"
    "search/liquid-etf-trend-pullback-cost-floor-search-"
    "b9003c468f6d21316482ae2e5d5527cb538636bddb41efabdfdfaf409aa78fb9.json"
)
SOURCE_RESULT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/liquid-etf-trend-pullback-cost-floor/"
    "development/liquid-etf-trend-pullback-cost-floor-development-"
    "b68f6c1b16339bd176b500f765b64e9646e99f4fb9eb3aff20fabff89b74f119.json"
)
SOURCE_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/liquid-etf-trend-pullback-cost-floor/"
    "development-inspection/liquid-etf-trend-pullback-cost-floor-"
    "development-inspection-"
    "650511bc77dc31fa3a9aeeab0029f9df7d6ffb99dfd165fbeb36b9908987665b.json"
)
SOURCE_DATASET_MANIFEST = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/liquid-etf-trend-pullback-cost-floor/"
    "development-dataset/dataset-liquid-etf-trend-pullback-cost-floor-"
    "development-b9003c468f6d2131-"
    "223d642cb7b475ea57a7d0f47192d45aea311e1170091f4af7e863e8ef304d0f.json"
)


class EtfCrossSectionalMomentumDiscoveryError(RuntimeError):
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
        raise EtfCrossSectionalMomentumDiscoveryError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise EtfCrossSectionalMomentumDiscoveryError(
            f"{path} must contain an object"
        )
    return value


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise EtfCrossSectionalMomentumDiscoveryError(
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
        raise EtfCrossSectionalMomentumDiscoveryError(
            "created_at is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise EtfCrossSectionalMomentumDiscoveryError(
            "created_at needs a timezone"
        )


def _source_graph(*, enforce_commit: bool) -> tuple[dict[str, Any], dict[str, Any]]:
    paths = (
        PREDECESSOR_RESULT,
        PREDECESSOR_INSPECTION,
        SOURCE_SEARCH,
        SOURCE_RESULT,
        SOURCE_INSPECTION,
        SOURCE_DATASET_MANIFEST,
    )
    if enforce_commit:
        for path in paths:
            strategy_discovery.require_committed(path)
    predecessor = _read(PREDECESSOR_RESULT)
    predecessor_inspection = _read(PREDECESSOR_INSPECTION)
    search = strategy_discovery.load_artifact(
        SOURCE_SEARCH, expected_kind="frozen-development-search"
    )
    result = strategy_discovery.load_artifact(
        SOURCE_RESULT, expected_kind="development-search-result"
    )
    inspection = strategy_discovery.load_artifact(
        SOURCE_INSPECTION, expected_kind="development-search-inspection"
    )
    load_frozen_dataset_contract(SOURCE_DATASET_MANIFEST)
    if not (
        predecessor.get("variant_id") == PREDECESSOR_VARIANT_ID
        and predecessor.get("mechanism_family") == MECHANISM_FAMILY
        and predecessor.get("stage0_survived") is False
        and predecessor.get("stage0_blockers")
        == ["primary drawdown exceeds the Stage 0 maximum"]
        and predecessor_inspection.get("result_sha256")
        == predecessor.get("result_sha256")
        and predecessor_inspection.get("valid") is True
        and search.get("artifact_sha256")
        == result.get("search_sha256")
        and result.get("artifact_sha256")
        == inspection.get("result_sha256")
        and inspection.get("state") == "REJECTED"
        and inspection.get("inspection", {}).get("valid") is True
        and result["evaluation"].get("dataset_manifest")
        == str(SOURCE_DATASET_MANIFEST)
    ):
        raise EtfCrossSectionalMomentumDiscoveryError(
            "bound predecessor or source development graph is invalid"
        )
    source_contract = search["family_contract"]
    if not (
        source_contract.get("family_id") == runtime.ETF_PULLBACK_FAMILY
        and source_contract.get("universe", {}).get("symbols")
        == ["SPY", "QQQ", "IWM", "DIA"]
        and len(source_contract.get("development_dates", [])) == 1_000
        and len(source_contract.get("development_warmup_dates", [])) == 200
        and len(source_contract.get("embargo_dates", [])) == 5
        and len(source_contract.get("confirmation_dates", [])) == 500
    ):
        raise EtfCrossSectionalMomentumDiscoveryError(
            "source ETF partitions or universe drifted"
        )
    return predecessor, search


def freeze_successor_contract(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any], Path]:
    """Freeze the full 32-trial family without opening external price rows."""

    _timestamp(created_at)
    predecessor, source_search = _source_graph(enforce_commit=enforce_commit)
    source_contract = source_search["family_contract"]
    source_manifest = load_frozen_dataset_contract(SOURCE_DATASET_MANIFEST)
    source_binding = source_manifest["dataset_payload"]["dense_runtime"]
    development = list(source_contract["development_dates"])
    warmup = list(source_contract["development_warmup_dates"])
    embargo = list(source_contract["embargo_dates"])
    confirmation = list(source_contract["confirmation_dates"])
    development_scope = dict(source_contract["development_scope"])
    confirmation_scope = dict(source_contract["confirmation_scope"])
    index = outcome_exposure.read_index()
    if not outcome_exposure.find_overlaps(development_scope, index):
        raise EtfCrossSectionalMomentumDiscoveryError(
            "development is not explicitly contaminated"
        )
    outcome_exposure.assert_untouched(confirmation_scope, index)
    outcome_exposure.assert_disjoint([development_scope, confirmation_scope])
    evidence_paths = [
        _repo_path(PREDECESSOR_RESULT),
        _repo_path(PREDECESSOR_INSPECTION),
        _repo_path(SOURCE_SEARCH),
        _repo_path(SOURCE_RESULT),
        _repo_path(SOURCE_INSPECTION),
        _repo_path(SOURCE_DATASET_MANIFEST),
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
                "development_training_contaminated": True,
                "confirmation_access_permitted": False,
                "etf_cross_sectional_momentum_source": {
                    "source_family_id": runtime.ETF_PULLBACK_FAMILY,
                    "target_family_id": FAMILY_ID,
                    "source_manifest_path": _repo_path(SOURCE_DATASET_MANIFEST),
                    "source_manifest_file_sha256": sha256_file(
                        SOURCE_DATASET_MANIFEST
                    ),
                    "external_relative_path": source_binding[
                        "external_relative_path"
                    ],
                    "external_file_sha256": source_binding[
                        "external_file_sha256"
                    ],
                    "dataset_sha256": source_binding["dataset_sha256"],
                    "format": source_binding["format"],
                    "formal_capacity": len(development) * 4,
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
            "result_path": _repo_path(PREDECESSOR_RESULT),
            "result_file_sha256": sha256_file(PREDECESSOR_RESULT),
            "result_sha256": predecessor["result_sha256"],
            "inspection_path": _repo_path(PREDECESSOR_INSPECTION),
            "inspection_file_sha256": sha256_file(PREDECESSOR_INSPECTION),
            "promotion_evidence_reused": False,
            "adverse_finding": "primary drawdown exceeded the Stage 0 maximum",
        },
        "source_training": {
            "search_path": _repo_path(SOURCE_SEARCH),
            "search_sha256": source_search["artifact_sha256"],
            "result_path": _repo_path(SOURCE_RESULT),
            "inspection_path": _repo_path(SOURCE_INSPECTION),
            "dataset_manifest_path": _repo_path(SOURCE_DATASET_MANIFEST),
            "development_outcomes_exposed": True,
            "promotion_evidence_reused": False,
        },
        "mechanism": (
            "Rank the complete liquid index-ETF universe by trailing return and "
            "buy the strongest cost-clearing leader only while SPY remains in a "
            "completed long-term uptrend."
        ),
        "expected_holding_behavior": (
            "Long only, next-session-open entry, and flat within five sessions."
        ),
        "dataset_lane": "development",
        "universe_requirements": {
            "symbols": ["DIA", "IWM", "QQQ", "SPY"],
            "complete_frozen_daily_history": True,
        },
        "entry_rule": (
            "At each completed close, rank all four ETFs by the frozen trailing "
            "return, require the leader's return to clear the cross-sectional "
            "median and five-times-cost floor, then enter at the next open."
        ),
        "stop_rule": (
            "Place the exact 1.5 or 2.0 ATR14 structural stop below entry; invalid "
            "or missing stops produce missed trades."
        ),
        "exit_rule": (
            "Resolve stop first on daily ambiguity and otherwise exit at the "
            "completed close after three or five sessions."
        ),
        "ranking_rule": (
            "Highest completed trailing return, then lexical symbol; at most one "
            "new family entry per session."
        ),
        "selection_rule": (
            "Portfolio risk, concurrent-position, aggregate-risk, daily-entry, "
            "gross-notional, and capital-contention caps remain authoritative."
        ),
        "parameter_grid": {
            "return_lookback_sessions": [20, 60],
            "market_trend_sma": [100, 200],
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
            "The 2016-2020 ETF partition is already outcome exposed and is training only.",
            "The predecessor equity result is hypothesis-generating only.",
            "No confirmation date or symbol may enter selection.",
        ],
        "production_compatibility_risks": [
            "Live quote, spread, depth, halt, tradability, timestamp, protection, and reconciliation gates remain required."
        ],
        "material_difference_rationale": (
            "This exact version retains the existing cross-sectional-momentum "
            "mechanism but replaces concentrated microcap equity exposure with a "
            "complete four-index-ETF universe and prospectively frozen risk, trend, "
            "lookback, cost, and hold rules."
        ),
        "development_dates": development,
        "development_warmup_dates": warmup,
        "confirmation_warmup_dates": list(
            source_contract["confirmation_warmup_dates"]
        ),
        "embargo_dates": embargo,
        "confirmation_dates": confirmation,
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "outcome_exposure_index_sha256": outcome_exposure.audit()["index_sha256"],
        "universe": {
            "symbols": ["DIA", "IWM", "QQQ", "SPY"],
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
            "etf_cross_sectional_momentum_discovery.py",
            "etf_cross_sectional_momentum_plugin.py",
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
            "module": "etf_cross_sectional_momentum_plugin",
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
        "contracts": len(contracts),
        "discovery_started": discovery_root.exists(),
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
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "status":
            result = status(root=args.root)
        else:
            path, artifact, capacity = freeze_successor_contract(
                created_at=args.created_at,
                root=args.root,
            )
            result = {
                "path": _repo_path(path),
                "capacity_manifest": _repo_path(capacity),
                "state": artifact["status"],
                "trial_count": len(artifact["trial_family"]),
                "calendar_wait_required": False,
            }
    except (
        EtfCrossSectionalMomentumDiscoveryError,
        strategy_discovery.StrategyDiscoveryError,
    ) as exc:
        print(json.dumps({"error": str(exc)}, indent=2, sort_keys=True))
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
