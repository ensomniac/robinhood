"""Freeze an outcome-clean macro-ETF pullback replication on 2016-2022 data."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from datetime import date, datetime
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
FAMILY_ID = runtime.ETF_PULLBACK_FAMILY
MECHANISM_FAMILY = "broad-etf-trend-pullback"
STRATEGY_ID = "macro-etf-trend-pullback"
SUCCESSOR_ID = "macro-etf-trend-pullback-v4-outcome-clean-replication"
RESEARCH_GENERATION = "existing_family_successor"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
CALENDAR_PATH = (
    PROJECT_ROOT
    / "historical_batches/continuous_v2/"
    "session-calendar-2014-01-through-2022-12.json"
)
CALENDAR_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "broad-etf-trend-pullback-v2-cost-floor/calendar/data-inspection/"
    "continuous-successor-calendar-data-inspection-"
    "f4da078f7a8b857d42b727efc543ec766b15f34d3f2d54d526e5f326a72e42b8.json"
)
V2_SEARCH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/liquid-etf-trend-pullback-cost-floor/"
    "search/liquid-etf-trend-pullback-cost-floor-search-"
    "b9003c468f6d21316482ae2e5d5527cb538636bddb41efabdfdfaf409aa78fb9.json"
)
V2_RESULT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/liquid-etf-trend-pullback-cost-floor/"
    "development/liquid-etf-trend-pullback-cost-floor-development-"
    "b68f6c1b16339bd176b500f765b64e9646e99f4fb9eb3aff20fabff89b74f119.json"
)
V2_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/liquid-etf-trend-pullback-cost-floor/"
    "development-inspection/"
    "liquid-etf-trend-pullback-cost-floor-development-inspection-"
    "650511bc77dc31fa3a9aeeab0029f9df7d6ffb99dfd165fbeb36b9908987665b.json"
)
PRE2016_FAILURE = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/liquid-etf-trend-pullback-cost-floor/"
    "development-collection-failure/"
    "liquid-etf-trend-pullback-cost-floor-development-collection-failure-"
    "cf63e21defadebd483bfa88e57fd63e600cd37c1b3bee77227b9602b093ff503.json"
)
PRE2016_FAILURE_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/liquid-etf-trend-pullback-cost-floor/"
    "development-collection-failure-inspection/"
    "liquid-etf-trend-pullback-cost-floor-development-collection-failure-"
    "inspection-c0f06e96acee2f96239ee545abada40d18d0bffcdd207514e18d9224d1d0a0d9.json"
)
SYMBOLS = ["DBC", "EEM", "EFA", "GLD", "IEF", "TLT"]
DEVELOPMENT_WARMUP_SESSIONS = 200
DEVELOPMENT_SESSIONS = 1_000
EMBARGO_SESSIONS = 5
CONFIRMATION_SESSIONS = 500
TOTAL_SESSIONS = (
    DEVELOPMENT_WARMUP_SESSIONS
    + DEVELOPMENT_SESSIONS
    + EMBARGO_SESSIONS
    + CONFIRMATION_SESSIONS
)


class EtfMacroPullbackReplicationError(ValueError):
    """The macro-ETF replication contract or evidence graph drifted."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError as exc:
        raise EtfMacroPullbackReplicationError(
            f"path escaped repository: {path}"
        ) from exc


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EtfMacroPullbackReplicationError(
            f"{field} is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise EtfMacroPullbackReplicationError(
            f"{field} needs a timezone"
        )
    if parsed.date() > date.today():
        raise EtfMacroPullbackReplicationError(
            f"{field} cannot be future-dated"
        )
    return parsed


def _write_json(path: Path, value: Any) -> None:
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


def _full_sessions() -> list[str]:
    try:
        rows = json.loads(CALENDAR_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EtfMacroPullbackReplicationError(
            "calendar cannot be loaded"
        ) from exc
    if not isinstance(rows, list):
        raise EtfMacroPullbackReplicationError("calendar is malformed")
    dates = [
        str(row["date"])
        for row in rows
        if isinstance(row, Mapping)
        and row.get("open_et") == "09:30"
        and row.get("close_et") == "16:00"
    ]
    if (
        len(dates) < TOTAL_SESSIONS
        or dates != sorted(dates)
        or len(dates) != len(set(dates))
    ):
        raise EtfMacroPullbackReplicationError(
            "calendar lacks exact full-session capacity"
        )
    return dates


def _scope(dates: Sequence[str]) -> dict[str, Any]:
    return {"dates": list(dates), "symbols": list(SYMBOLS)}


def _implementation_hashes() -> dict[str, str]:
    paths = (
        Path(__file__).resolve(),
        PROJECT_ROOT / "dense_data_collection.py",
        PROJECT_ROOT / "dense_data_collection_inspection.py",
        PROJECT_ROOT / "dense_strategy_plugin.py",
        PROJECT_ROOT / "dense_strategy_runtime.py",
    )
    if any(not path.is_file() for path in paths):
        raise EtfMacroPullbackReplicationError(
            "macro replication implementation is incomplete"
        )
    return {_repo_path(path): sha256_file(path) for path in paths}


def _adverse_predecessors(
    *, enforce_commit: bool
) -> dict[str, dict[str, Any]]:
    paths = (
        CALENDAR_PATH,
        CALENDAR_INSPECTION,
        V2_SEARCH,
        V2_RESULT,
        V2_INSPECTION,
        PRE2016_FAILURE,
        PRE2016_FAILURE_INSPECTION,
    )
    if enforce_commit:
        for path in paths:
            strategy_discovery.require_committed(path)
    calendar = strategy_discovery.load_artifact(
        CALENDAR_INSPECTION,
        expected_kind="continuous-successor-calendar-data-inspection",
    )
    search = strategy_discovery.load_artifact(
        V2_SEARCH, expected_kind="frozen-development-search"
    )
    result = strategy_discovery.load_artifact(
        V2_RESULT, expected_kind="development-search-result"
    )
    inspection = strategy_discovery.load_artifact(
        V2_INSPECTION, expected_kind="development-search-inspection"
    )
    failure = strategy_discovery.load_artifact(
        PRE2016_FAILURE, expected_kind="dense-data-collection-failure"
    )
    failure_inspection = strategy_discovery.load_artifact(
        PRE2016_FAILURE_INSPECTION,
        expected_kind="dense-data-collection-failure-inspection",
    )
    if not (
        calendar.get("state") == "CALENDAR_INSPECTED_READY"
        and calendar.get("calendar_sha256") == sha256_file(CALENDAR_PATH)
        and search["family_contract"].get("family_id") == FAMILY_ID
        and result.get("search_sha256") == search["artifact_sha256"]
        and inspection.get("result_sha256") == result["artifact_sha256"]
        and inspection.get("state") == "REJECTED"
        and failure.get("data_outcomes_accessed") is False
        and failure.get("market_price_rows_accessed") == 0
        and failure_inspection.get("failure_sha256")
        == failure["artifact_sha256"]
        and failure_inspection.get("state")
        == "COLLECTION_FAILURE_INSPECTED"
    ):
        raise EtfMacroPullbackReplicationError(
            "adverse predecessor evidence graph drifted"
        )
    return {
        "calendar": calendar,
        "search": search,
        "result": result,
        "inspection": inspection,
        "failure": failure,
        "failure_inspection": failure_inspection,
    }


def freeze_contract(
    *, created_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any], Path]:
    _timestamp(created_at, "created_at")
    predecessors = _adverse_predecessors(enforce_commit=enforce_commit)
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    selected = _full_sessions()[-TOTAL_SESSIONS:]
    warmup = selected[:DEVELOPMENT_WARMUP_SESSIONS]
    development = selected[
        DEVELOPMENT_WARMUP_SESSIONS:
        DEVELOPMENT_WARMUP_SESSIONS + DEVELOPMENT_SESSIONS
    ]
    embargo_start = DEVELOPMENT_WARMUP_SESSIONS + DEVELOPMENT_SESSIONS
    embargo = selected[embargo_start:embargo_start + EMBARGO_SESSIONS]
    confirmation = selected[-CONFIRMATION_SESSIONS:]
    confirmation_warmup = selected[
        -CONFIRMATION_SESSIONS - DEVELOPMENT_WARMUP_SESSIONS:
        -CONFIRMATION_SESSIONS
    ]
    development_scope = _scope([*warmup, *development])
    confirmation_scope = _scope(confirmation)
    records = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(development_scope, records)
    outcome_exposure.assert_untouched(confirmation_scope, records)
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    evidence_paths = [
        _repo_path(CALENDAR_PATH),
        _repo_path(CALENDAR_INSPECTION),
        _repo_path(V2_SEARCH),
        _repo_path(V2_RESULT),
        _repo_path(V2_INSPECTION),
        _repo_path(PRE2016_FAILURE),
        _repo_path(PRE2016_FAILURE_INSPECTION),
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
    ]
    capacity_path, _capacity = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-capacity",
            "registered_at": created_at,
            "requested_dates": selected,
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": evidence_paths,
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
                    "calendar_sha256": sha256_file(CALENDAR_PATH),
                    "provider_requests": 0,
                    "market_prices_accessed": False,
                    "outcomes_accessed": False,
                },
            },
        },
        DEFAULT_ROOT / SUCCESSOR_ID / "capacity",
    )
    contract: dict[str, Any] = {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "experiment_id": f"experiment-{SUCCESSOR_ID}",
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "strategy_id": STRATEGY_ID,
        "parent_experiment_id": (
            "experiment-broad-etf-trend-pullback-v2-cost-floor"
        ),
        "created_at": created_at,
        "status": "INVENTED",
        "research_generation": RESEARCH_GENERATION,
        "successor_id": SUCCESSOR_ID,
        "existing_successor_validator": {
            "module": "etf_macro_pullback_replication",
            "function": "validate_existing_successor_contract",
        },
        "new_mechanism_family_slot_consumed": False,
        "prior_family_attempt_count": 4,
        "predecessor": {
            "search_path": _repo_path(V2_SEARCH),
            "search_sha256": predecessors["search"]["artifact_sha256"],
            "result_path": _repo_path(V2_RESULT),
            "result_sha256": predecessors["result"]["artifact_sha256"],
            "inspection_path": _repo_path(V2_INSPECTION),
            "inspection_sha256": predecessors["inspection"][
                "artifact_sha256"
            ],
            "pre2016_failure_path": _repo_path(PRE2016_FAILURE),
            "pre2016_failure_sha256": predecessors["failure"][
                "artifact_sha256"
            ],
            "pre2016_failure_inspection_path": _repo_path(
                PRE2016_FAILURE_INSPECTION
            ),
            "pre2016_failure_inspection_sha256": predecessors[
                "failure_inspection"
            ]["artifact_sha256"],
            "promotion_evidence_reused": False,
        },
        "mechanism": (
            "Buy a cost-clearing short pullback in a liquid macro ETF that "
            "remains above its completed long-term trend."
        ),
        "expected_holding_behavior": (
            "Long only, next-session-open entry, and flat within five sessions."
        ),
        "dataset_lane": "development",
        "universe_requirements": {
            "symbols": list(SYMBOLS),
            "complete_frozen_daily_history": True,
            "selection_basis": (
                "Authorized liquid ETFs excluding every symbol with prior "
                "2016-2022 pullback, cross-sectional, or sector outcome exposure."
            ),
        },
        "entry_rule": (
            "After completed SMA, RSI2, and three-session-decline "
            "qualification, rank by lowest RSI2, deepest decline, then symbol, "
            "and enter at the next observable session open."
        ),
        "stop_rule": (
            "Use the exact one or one-and-a-half completed ATR14 stop below "
            "entry; invalid or missing stops are missed trades."
        ),
        "exit_rule": (
            "Resolve stop first on daily ambiguity and otherwise exit at the "
            "completed close after three or five sessions."
        ),
        "ranking_rule": (
            "Lowest RSI2, deepest three-session decline, then symbol."
        ),
        "selection_rule": (
            "At most one new family entry per day under all portfolio risk, "
            "notional, daily-entry, and capital-contention caps."
        ),
        "parameter_grid": {
            "trend_sma": [100, 200],
            "rsi2_maximum": [5, 10],
            "three_session_decline_fraction": [0.02, 0.03],
            "stop_atr14": [1.0, 1.5],
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
            "The prior four-ETF outcomes motivate replication only and cannot count toward promotion.",
            "Every selected macro ETF is absent from prior 2016-2022 uniform-scope outcome records.",
            "Warmup is feature-only and cannot count as target evidence.",
        ],
        "production_compatibility_risks": [
            "Fresh quote, spread, depth, halt, tradability, timestamp, protection, and reconciliation remain mandatory."
        ],
        "material_difference_rationale": (
            "This existing-family replication preserves the exact 32-rule grid "
            "but uses the six authorized macro ETFs whose 2016-2022 pairs are "
            "globally outcome-clean. The universe is exposure-derived before "
            "price access, not selected from strategy returns."
        ),
        "development_dates": development,
        "development_warmup_dates": warmup,
        "confirmation_warmup_dates": confirmation_warmup,
        "embargo_dates": embargo,
        "confirmation_dates": confirmation,
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "universe": {"symbols": list(SYMBOLS), "point_in_time": True},
        "historical_data_contract": {
            "daily_provider": "alpaca",
            "daily_endpoint": "/v2/stocks/{symbol}/bars",
            "daily_feed": "sip",
            "daily_adjustment": "raw",
            "split_provider": "massive",
            "provider_substitutions_allowed": False,
        },
        "calendar_path": _repo_path(CALENDAR_PATH),
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "predecessor_corpora_disjoint": True,
        },
        "falsifiers": [
            "nonpositive stressed log growth",
            "unstable parameter neighbors",
            "selection-aware statistical rejection",
            "incomplete execution or trial accounting",
            "insufficient frozen confirmation power capacity",
        ],
        "implementation_files": [
            "etf_macro_pullback_replication.py",
            "dense_data_collection.py",
            "dense_data_collection_inspection.py",
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
            "module": "dense_strategy_plugin",
            "preflight": "preflight",
            "evaluate_development": "evaluate_development",
            "evaluate_confirmation": "evaluate_confirmation",
            "evaluate_production": "evaluate_production",
        },
        "capacity_policy": {"retire_below": 50, "fast_lane_at": 100},
        "capacity_manifest": _repo_path(capacity_path),
    }
    contract = strategy_discovery._validate_family_contract(contract)
    validate_existing_successor_contract(
        contract, enforce_commit=enforce_commit
    )
    digest = hashlib.sha256(_canonical(contract)).hexdigest()
    path = (
        DEFAULT_ROOT
        / SUCCESSOR_ID
        / "family-contract"
        / f"contract-{digest}.json"
    )
    _write_json(path, contract)
    return path, contract, capacity_path


def validate_existing_successor_contract(
    contract: Mapping[str, Any], *, enforce_commit: bool = True
) -> None:
    expected_provider = {
        "daily_provider": "alpaca",
        "daily_endpoint": "/v2/stocks/{symbol}/bars",
        "daily_feed": "sip",
        "daily_adjustment": "raw",
        "split_provider": "massive",
        "provider_substitutions_allowed": False,
    }
    if not (
        contract.get("campaign_id") == CAMPAIGN_ID
        and contract.get("family_id") == FAMILY_ID
        and contract.get("mechanism_family") == MECHANISM_FAMILY
        and contract.get("strategy_id") == STRATEGY_ID
        and contract.get("successor_id") == SUCCESSOR_ID
        and contract.get("research_generation") == RESEARCH_GENERATION
        and contract.get("new_mechanism_family_slot_consumed") is False
        and contract.get("prior_family_attempt_count") == 4
        and contract.get("selection_mode") == "development_search"
        and len(contract.get("trial_family", [])) == 32
        and contract.get("universe", {}).get("symbols") == SYMBOLS
        and contract.get("existing_successor_validator")
        == {
            "module": "etf_macro_pullback_replication",
            "function": "validate_existing_successor_contract",
        }
        and contract.get("historical_data_contract") == expected_provider
        and contract.get("calendar_path") == _repo_path(CALENDAR_PATH)
        and contract.get("outcome_exposure_index_sha256")
        == outcome_exposure.audit()["index_sha256"]
    ):
        raise EtfMacroPullbackReplicationError(
            "macro-ETF replication contract drifted"
        )
    _adverse_predecessors(enforce_commit=enforce_commit)
    records = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(
        contract["development_scope"], records
    )
    outcome_exposure.assert_untouched(
        contract["confirmation_scope"], records
    )


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
        EtfMacroPullbackReplicationError,
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
