"""Freeze an outcome-clean sector-ETF post-gap drift development search."""

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
FAMILY_ID = runtime.SECTOR_ETF_GAP_DRIFT_FAMILY
MECHANISM_FAMILY = "equity-gap-continuation"
STRATEGY_ID = "sector-etf-gap-drift"
SUCCESSOR_ID = "sector-etf-gap-drift-v1-outcome-clean"
RESEARCH_GENERATION = "existing_family_successor"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
CALENDAR_PATH = (
    PROJECT_ROOT
    / "historical_batches/continuous_v2/"
    "session-calendar-2014-01-through-2021-12.json"
)
CALENDAR_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "broad-etf-trend-pullback-v2-cost-floor/calendar/data-inspection/"
    "continuous-successor-calendar-data-inspection-"
    "9d2d20e32fe51fd7352395eaffaf50955e4f005b4a2deb80c69c8341dad6db99.json"
)
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
PREDECESSOR_SEARCH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "equity-gap-continuation-development-search/search/"
    "equity-gap-continuation-development-search-search-"
    "172558745037c05f40bdce814e32b56df2bea08420cf86e451415d926611ae14.json"
)
PREDECESSOR_RESULT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "equity-gap-continuation-development-search/development/"
    "equity-gap-continuation-development-search-development-"
    "da3c6b3763ae6cc8e3dd588c8c0e9bc0826e8b0753a3f1c0cd6f15b9cbdc5d02.json"
)
PREDECESSOR_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "equity-gap-continuation-development-search/development-inspection/"
    "equity-gap-continuation-development-search-development-inspection-"
    "e8ed1d2b6704814ca3a1acd3b48c2b6d32e8be03c9c8265b8423e7cc3ef8a7fd.json"
)
SYMBOLS = ["XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY"]
DEVELOPMENT_WARMUP_SESSIONS = 200
DEVELOPMENT_SESSIONS = 1_000
EMBARGO_SESSIONS = 5
CONFIRMATION_SESSIONS = 250
TOTAL_SESSIONS = (
    DEVELOPMENT_WARMUP_SESSIONS
    + DEVELOPMENT_SESSIONS
    + EMBARGO_SESSIONS
    + CONFIRMATION_SESSIONS
)


class SectorEtfGapDriftError(ValueError):
    """The exact gap-drift contract or evidence graph drifted."""


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
        raise SectorEtfGapDriftError(
            f"path escaped repository: {path}"
        ) from exc


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SectorEtfGapDriftError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise SectorEtfGapDriftError(f"{field} needs a timezone")
    if parsed.date() > date.today():
        raise SectorEtfGapDriftError(f"{field} cannot be future-dated")
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
        raise SectorEtfGapDriftError("calendar cannot be loaded") from exc
    if not isinstance(rows, list):
        raise SectorEtfGapDriftError("calendar is malformed")
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
        raise SectorEtfGapDriftError(
            "calendar lacks exact full-session capacity"
        )
    return dates


def _scope(dates: Sequence[str]) -> dict[str, Any]:
    return {"dates": list(dates), "symbols": list(SYMBOLS)}


def _read_plain(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SectorEtfGapDriftError(
            f"adverse evidence cannot be loaded: {path.name}"
        ) from exc
    if not isinstance(value, dict):
        raise SectorEtfGapDriftError(
            f"adverse evidence is malformed: {path.name}"
        )
    return value


def _adverse_predecessors(
    *, enforce_commit: bool
) -> dict[str, dict[str, Any]]:
    paths = (
        CALENDAR_PATH,
        CALENDAR_INSPECTION,
        STAGE0_RESULT,
        STAGE0_INSPECTION,
        PREDECESSOR_SEARCH,
        PREDECESSOR_RESULT,
        PREDECESSOR_INSPECTION,
    )
    if enforce_commit:
        for path in paths:
            strategy_discovery.require_committed(path)
    calendar = strategy_discovery.load_artifact(
        CALENDAR_INSPECTION,
        expected_kind="continuous-successor-calendar-data-inspection",
    )
    stage0 = _read_plain(STAGE0_RESULT)
    stage0_inspection = _read_plain(STAGE0_INSPECTION)
    search = strategy_discovery.load_artifact(
        PREDECESSOR_SEARCH,
        expected_kind="frozen-development-search",
    )
    result = strategy_discovery.load_artifact(
        PREDECESSOR_RESULT,
        expected_kind="development-search-result",
    )
    inspection = strategy_discovery.load_artifact(
        PREDECESSOR_INSPECTION,
        expected_kind="development-search-inspection",
    )
    if not (
        calendar.get("state") == "CALENDAR_INSPECTED_READY"
        and calendar.get("calendar_sha256") == sha256_file(CALENDAR_PATH)
        and stage0_inspection.get("valid") is True
        and stage0_inspection.get("stage0_survived") is True
        and stage0_inspection.get("result_file_sha256")
        == sha256_file(STAGE0_RESULT)
        and stage0_inspection.get("result_sha256")
        == stage0.get("result_sha256")
        and search["family_contract"].get("mechanism_family")
        == MECHANISM_FAMILY
        and result.get("search_sha256") == search["artifact_sha256"]
        and inspection.get("result_sha256") == result["artifact_sha256"]
        and inspection.get("state") == "REJECTED"
    ):
        raise SectorEtfGapDriftError(
            "gap-continuation adverse evidence graph drifted"
        )
    return {
        "calendar": calendar,
        "stage0": stage0,
        "stage0_inspection": stage0_inspection,
        "search": search,
        "result": result,
        "inspection": inspection,
    }


def _validate_exposure_state(contract: Mapping[str, Any]) -> None:
    records = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(
        contract["confirmation_scope"], records
    )
    overlaps = outcome_exposure.find_overlaps(
        contract["development_scope"], records
    )
    if not overlaps:
        return
    overlap_ids = {item["exposure_id"] for item in overlaps}
    matching = [
        record
        for record in records
        if record["exposure_id"] in overlap_ids
    ]
    expected_prefix = (
        f"strategy_tournament/v2/discovery/{FAMILY_ID}/development/"
    )
    if not (
        len(matching) == 1
        and matching[0]["lane"] == "development"
        and matching[0]["source_path"].startswith(expected_prefix)
        and matching[0]["scope"] == contract["development_scope"]
    ):
        raise SectorEtfGapDriftError(
            "development scope has foreign or partial outcome exposure"
        )


def freeze_contract(
    *, created_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any], Path]:
    _timestamp(created_at, "created_at")
    predecessors = _adverse_predecessors(
        enforce_commit=enforce_commit
    )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    selected = _full_sessions()[-TOTAL_SESSIONS:]
    warmup = selected[:DEVELOPMENT_WARMUP_SESSIONS]
    development = selected[
        DEVELOPMENT_WARMUP_SESSIONS:
        DEVELOPMENT_WARMUP_SESSIONS + DEVELOPMENT_SESSIONS
    ]
    embargo_start = (
        DEVELOPMENT_WARMUP_SESSIONS + DEVELOPMENT_SESSIONS
    )
    embargo = selected[
        embargo_start:embargo_start + EMBARGO_SESSIONS
    ]
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
        _repo_path(STAGE0_RESULT),
        _repo_path(STAGE0_INSPECTION),
        _repo_path(PREDECESSOR_SEARCH),
        _repo_path(PREDECESSOR_RESULT),
        _repo_path(PREDECESSOR_INSPECTION),
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
                    "formal_capacity": (
                        len(development) * len(SYMBOLS)
                    ),
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
            "experiment-equity-gap-protection-continuation-v3-"
            "development-search"
        ),
        "created_at": created_at,
        "status": "INVENTED",
        "research_generation": RESEARCH_GENERATION,
        "successor_id": SUCCESSOR_ID,
        "existing_successor_validator": {
            "module": "sector_etf_gap_drift",
            "function": "validate_existing_successor_contract",
        },
        "new_mechanism_family_slot_consumed": False,
        "prior_family_attempt_count": 3,
        "predecessor": {
            "stage0_result_path": _repo_path(STAGE0_RESULT),
            "stage0_result_sha256": stage0_result_sha256(),
            "stage0_inspection_path": _repo_path(STAGE0_INSPECTION),
            "stage0_inspection_file_sha256": sha256_file(
                STAGE0_INSPECTION
            ),
            "search_path": _repo_path(PREDECESSOR_SEARCH),
            "search_sha256": predecessors["search"]["artifact_sha256"],
            "result_path": _repo_path(PREDECESSOR_RESULT),
            "result_sha256": predecessors["result"]["artifact_sha256"],
            "inspection_path": _repo_path(PREDECESSOR_INSPECTION),
            "inspection_sha256": predecessors["inspection"][
                "artifact_sha256"
            ],
            "promotion_evidence_reused": False,
        },
        "mechanism": (
            "Buy persistent post-gap drift in a liquid sector ETF only after "
            "a positive completed gap session closes nonnegative and above "
            "its completed long-term trend."
        ),
        "expected_holding_behavior": (
            "Long only, next-session-open entry, and flat within five sessions."
        ),
        "dataset_lane": "development",
        "universe_requirements": {
            "symbols": list(SYMBOLS),
            "complete_frozen_daily_history": True,
            "selection_basis": (
                "The nine legacy sector SPDRs, fixed before price access and "
                "globally outcome-clean on the exact 2016-2021 partition."
            ),
        },
        "entry_rule": (
            "After a completed sector-ETF session opens between the frozen "
            "positive gap bounds, closes at or above that open and above its "
            "completed trend SMA, rank by largest gap, strongest session "
            "return, then symbol, and enter at the next session open."
        ),
        "stop_rule": (
            "Use one or one-and-a-half completed ATR14 below entry; missing or "
            "structurally invalid protection is a missed trade."
        ),
        "exit_rule": (
            "Resolve stop first on daily ambiguity and otherwise exit at the "
            "completed close after two or five sessions."
        ),
        "ranking_rule": (
            "Largest completed opening gap, strongest completed session "
            "return, then canonical symbol."
        ),
        "selection_rule": (
            "At most one new family entry per day under all portfolio risk, "
            "notional, daily-entry, and capital-contention caps."
        ),
        "parameter_grid": {
            "minimum_gap_fraction": [0.01, 0.02],
            "maximum_gap_fraction": [0.04, 0.08],
            "trend_sma": [100, 200],
            "stop_atr14": [1.0, 1.5],
            "maximum_hold_sessions": [2, 5],
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
            "Prior common-stock gap outcomes motivate the mechanism only and cannot count toward promotion.",
            "The exact sector-ETF date-symbol pairs are absent from the global exposure index before development.",
            "Warmup is feature-only and cannot count as target evidence.",
        ],
        "production_compatibility_risks": [
            "Fresh quote, spread, depth, halt, tradability, timestamp, GTC protection, and before-open reconciliation remain mandatory."
        ],
        "material_difference_rationale": (
            "This existing-mechanism successor replaces exposed single-stock "
            "intraday rules with a fixed, liquid sector-ETF universe and "
            "completed-session post-gap drift on a disjoint historical corpus."
        ),
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
        "universe": {
            "symbols": list(SYMBOLS),
            "point_in_time": True,
        },
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
            "sector_etf_gap_drift.py",
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


def stage0_result_sha256() -> str:
    stage0 = _read_plain(STAGE0_RESULT)
    value = stage0.get("result_sha256")
    if not isinstance(value, str) or len(value) != 64:
        raise SectorEtfGapDriftError(
            "stage-0 result hash is missing"
        )
    return value


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
    selected = _full_sessions()[-TOTAL_SESSIONS:]
    development = selected[
        DEVELOPMENT_WARMUP_SESSIONS:
        DEVELOPMENT_WARMUP_SESSIONS + DEVELOPMENT_SESSIONS
    ]
    embargo_start = (
        DEVELOPMENT_WARMUP_SESSIONS + DEVELOPMENT_SESSIONS
    )
    if not (
        contract.get("campaign_id") == CAMPAIGN_ID
        and contract.get("family_id") == FAMILY_ID
        and contract.get("mechanism_family") == MECHANISM_FAMILY
        and contract.get("strategy_id") == STRATEGY_ID
        and contract.get("successor_id") == SUCCESSOR_ID
        and contract.get("research_generation") == RESEARCH_GENERATION
        and contract.get("new_mechanism_family_slot_consumed") is False
        and contract.get("prior_family_attempt_count") == 3
        and contract.get("selection_mode") == "development_search"
        and len(contract.get("trial_family", [])) == 32
        and contract.get("universe", {}).get("symbols") == SYMBOLS
        and contract.get("development_dates") == development
        and contract.get("embargo_dates")
        == selected[embargo_start:embargo_start + EMBARGO_SESSIONS]
        and contract.get("confirmation_dates")
        == selected[-CONFIRMATION_SESSIONS:]
        and contract.get("existing_successor_validator")
        == {
            "module": "sector_etf_gap_drift",
            "function": "validate_existing_successor_contract",
        }
        and contract.get("historical_data_contract") == expected_provider
        and contract.get("calendar_path") == _repo_path(CALENDAR_PATH)
        and isinstance(
            contract.get("outcome_exposure_index_sha256"), str
        )
        and len(contract["outcome_exposure_index_sha256"]) == 64
    ):
        raise SectorEtfGapDriftError(
            "sector-ETF gap-drift contract drifted"
        )
    _adverse_predecessors(enforce_commit=enforce_commit)
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
        SectorEtfGapDriftError,
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
