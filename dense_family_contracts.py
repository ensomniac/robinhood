"""Freeze the authorized W31 dense-family contracts from outcome-blind inventory."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping
from datetime import date, datetime
from pathlib import Path
from typing import Any

import next_week_discovery_batch as batch
import outcome_exposure
import strategy_discovery
from learning_data import LearningDataError, load_frozen_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/family-contracts"
DEFAULT_STATUS_PATH = PROJECT_ROOT / "strategy_tournament/v2/next_batch/status.json"
SCHEMA_VERSION = 1


class DenseFamilyContractError(RuntimeError):
    """The weekly gate, capacity inventory, or untouched scope is invalid."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DenseFamilyContractError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DenseFamilyContractError(f"{path} must contain an object")
    return value


def _write(value: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") != rendered:
        raise DenseFamilyContractError(f"content-addressed artifact drifted: {path}")
    if not path.exists():
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(path)


def _manifest(
    path_text: Any, *, enforce_commit: bool
) -> tuple[Path, dict[str, Any]]:
    if not isinstance(path_text, str) or not path_text:
        raise DenseFamilyContractError("capacity_manifest is missing")
    path = Path(path_text)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    try:
        if enforce_commit:
            strategy_discovery.require_committed(path)
        return path, load_frozen_dataset_contract(path)
    except (LearningDataError, OSError) as exc:
        raise DenseFamilyContractError(f"capacity manifest is invalid: {exc}") from exc


def _validate_inventory(
    value: Mapping[str, Any],
    *,
    as_of: date,
    index_path: Path,
    enforce_commit: bool,
) -> dict[str, Any]:
    inventory = dict(value)
    supplied = inventory.pop("inventory_sha256", None)
    if supplied != _hash(inventory):
        raise DenseFamilyContractError("inventory hash is invalid")
    if as_of < batch.ACTIVATION_NOT_BEFORE:
        raise DenseFamilyContractError(
            f"ISO-week budget does not reset until {batch.ACTIVATION_NOT_BEFORE}"
        )
    if not (
        inventory.get("schema_version") == SCHEMA_VERSION
        and inventory.get("campaign_id") == batch.CAMPAIGN_ID
        and inventory.get("target_iso_week") == batch.TARGET_ISO_WEEK
        and inventory.get("outcomes_accessed") is False
        and inventory.get("provider_requests") == 0
        and inventory.get("broker_actions") == 0
    ):
        raise DenseFamilyContractError("inventory authority or zero-state drifted")
    created_at = inventory.get("created_at")
    if not isinstance(created_at, str):
        raise DenseFamilyContractError("inventory created_at is missing")
    try:
        timestamp = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DenseFamilyContractError("inventory created_at is invalid") from exc
    if timestamp.tzinfo is None:
        raise DenseFamilyContractError("inventory created_at needs a timezone")
    families = inventory.get("families")
    plan = batch.build_plan()
    plan_families = {item["family_id"]: item for item in plan["families"]}
    if not isinstance(families, list) or {
        item.get("family_id") for item in families if isinstance(item, Mapping)
    } != set(plan_families):
        raise DenseFamilyContractError("inventory must cover the exact three families")
    confirmation_scopes: list[dict[str, Any]] = []
    development_scopes: list[dict[str, Any]] = []
    records = outcome_exposure.read_index(index_path)
    for family in families:
        if not isinstance(family, Mapping):
            raise DenseFamilyContractError("family inventory entries must be objects")
        family_id = str(family["family_id"])
        path, manifest = _manifest(
            family.get("capacity_manifest"), enforce_commit=enforce_commit
        )
        payload = manifest["dataset_payload"]
        capacity = payload.get("dense_capacity")
        if not (
            payload.get("lane") == "development"
            and payload.get("point_in_time_evidence") is True
            and isinstance(capacity, Mapping)
            and capacity.get("family_id") == family_id
            and isinstance(capacity.get("formal_capacity"), int)
            and not isinstance(capacity.get("formal_capacity"), bool)
            and int(capacity["formal_capacity"]) >= 100
        ):
            raise DenseFamilyContractError(
                f"{family_id} lacks 100 outcome-blind capacity observations"
            )
        development_dates = family.get("development_dates")
        development_warmup_dates = family.get("development_warmup_dates")
        confirmation_warmup_dates = family.get("confirmation_warmup_dates")
        embargo_dates = family.get("embargo_dates")
        confirmation_dates = family.get("confirmation_dates")
        for values, name in (
            (development_warmup_dates, "development_warmup_dates"),
            (confirmation_warmup_dates, "confirmation_warmup_dates"),
            (development_dates, "development_dates"),
            (embargo_dates, "embargo_dates"),
            (confirmation_dates, "confirmation_dates"),
        ):
            strategy_discovery._date_list(values, name)
        if len(embargo_dates) < 5:
            raise DenseFamilyContractError(f"{family_id} needs a five-session embargo")
        expected_warmup = 60 if family_id == "intraday-index-etf-opening-reversal" else 200
        if len(development_warmup_dates) != expected_warmup or len(
            confirmation_warmup_dates
        ) != expected_warmup:
            raise DenseFamilyContractError(f"{family_id} warmup capacity is incomplete")
        if not (
            development_warmup_dates[-1] < development_dates[0]
            and confirmation_warmup_dates[-1] < confirmation_dates[0]
            and confirmation_warmup_dates[-1] == embargo_dates[-1]
        ):
            raise DenseFamilyContractError(f"{family_id} warmup chronology drifted")
        if not (
            set(manifest["requested_dates"]) >= set(development_dates)
            and set(manifest["requested_dates"]) >= set(confirmation_dates)
        ):
            raise DenseFamilyContractError(
                f"{family_id} capacity inventory does not cover frozen dates"
            )
        development_scope = outcome_exposure.validate_scope(
            family.get("development_scope")
        )
        confirmation_scope = outcome_exposure.validate_scope(
            family.get("confirmation_scope")
        )
        if development_scope["dates"] != [
            *development_warmup_dates,
            *development_dates,
        ]:
            raise DenseFamilyContractError("development exposure scope dates drifted")
        if confirmation_scope["dates"] != confirmation_dates:
            raise DenseFamilyContractError("confirmation exposure scope dates drifted")
        try:
            outcome_exposure.assert_untouched(confirmation_scope, records)
        except outcome_exposure.OutcomeExposureError as exc:
            raise DenseFamilyContractError(str(exc)) from exc
        development_scopes.append(development_scope)
        confirmation_scopes.append(confirmation_scope)
        family["capacity_manifest"] = str(path)
        family["outcome_exposure_index_sha256"] = inventory[
            "outcome_exposure_index_sha256"
        ]
        if plan_families[family_id]["trial_count"] > 64:
            raise DenseFamilyContractError("family exceeds the 64-trial ceiling")
    try:
        outcome_exposure.assert_disjoint(development_scopes)
        outcome_exposure.assert_disjoint(confirmation_scopes)
    except outcome_exposure.OutcomeExposureError as exc:
        raise DenseFamilyContractError(str(exc)) from exc
    observed_index_hash = outcome_exposure.audit(index_path)["index_sha256"]
    if inventory.get("outcome_exposure_index_sha256") != observed_index_hash:
        raise DenseFamilyContractError("global outcome-exposure index drifted")
    return {**inventory, "inventory_sha256": supplied}


def _semantics(family_id: str) -> dict[str, str]:
    if family_id == "liquid-equity-market-residual-reversal":
        return {
            "mechanism": "Short-horizon idiosyncratic overshoot mean reversion in the most liquid common equities.",
            "entry_rule": "Rank residual downside z-scores after the completed close and enter the highest-ranked eligible symbol at the next session open.",
            "stop_rule": "Freeze a long stop one or one-and-a-half completed ATR14 below the next-open entry; invalid or missing stops are missed trades.",
            "exit_rule": "Resolve the frozen stop first and otherwise exit at the completed close after two or five sessions.",
            "ranking_rule": "Most negative residual z-score first, then canonical symbol.",
        }
    if family_id == "intraday-index-etf-opening-reversal":
        return {
            "mechanism": "Opening downside dislocation in liquid index and sector ETFs followed by exact cumulative-VWAP reclaim.",
            "entry_rule": "After the frozen opening window and completed reclaim bars, enter at the next observable minute-bar open.",
            "stop_rule": "Freeze a long stop one or one-and-a-half completed intraday ATR below entry.",
            "exit_rule": "Exit stop-first on same-bar ambiguity, at the frozen R target, or at the regular-session final close.",
            "ranking_rule": "Most negative opening-return z-score first, then canonical symbol.",
        }
    return {
        "mechanism": "Cost-clearing short pullback inside a persistent liquid-ETF uptrend.",
        "entry_rule": "After completed SMA, RSI2, and three-session-decline qualification, enter the highest-ranked ETF at the next session open.",
        "stop_rule": "Freeze a long stop one or one-and-a-half completed ATR14 below the next-open entry.",
        "exit_rule": "Resolve the frozen stop first and otherwise exit at the completed close after three or five sessions.",
        "ranking_rule": "Lowest RSI2, deepest three-session decline, then canonical symbol.",
    }


def _contract(
    family: Mapping[str, Any], plan_family: Mapping[str, Any], created_at: str
) -> dict[str, Any]:
    family_id = str(family["family_id"])
    semantics = _semantics(family_id)
    return {
        "schema_version": 1,
        "campaign_id": batch.CAMPAIGN_ID,
        "experiment_id": f"experiment-{family_id}-2026-w31",
        "family_id": family_id,
        "strategy_id": family_id,
        "parent_experiment_id": None,
        "created_at": created_at,
        "status": "INVENTED",
        **semantics,
        "expected_holding_behavior": "Long only and flat no later than five trading sessions after entry.",
        "dataset_lane": "development",
        "universe_requirements": dict(plan_family["universe"]),
        "selection_rule": "At most one new family entry per day under the portfolio-wide risk, notional, and capital-contention caps.",
        "parameter_grid": dict(plan_family["parameter_grid"]),
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "primary_outcome": "Selection-adjusted chronological account log growth after costs.",
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
            "Development outcomes may train selection but can never become untouched confirmation.",
            "Any global date-symbol outcome exposure disqualifies that confirmation pair.",
        ],
        "production_compatibility_risks": [
            "Live quotes, spread, depth, halt, tradability, timing, protection, and reconciliation remain required."
        ],
        "material_difference_rationale": (
            f"{semantics['mechanism']} This is causally and operationally distinct "
            "from the retired disclosure and legacy ORB mechanisms."
        ),
        "development_dates": list(family["development_dates"]),
        "development_warmup_dates": list(family["development_warmup_dates"]),
        "confirmation_warmup_dates": list(family["confirmation_warmup_dates"]),
        "embargo_dates": list(family["embargo_dates"]),
        "confirmation_dates": list(family["confirmation_dates"]),
        "development_scope": dict(family["development_scope"]),
        "confirmation_scope": dict(family["confirmation_scope"]),
        "outcome_exposure_index_sha256": family["outcome_exposure_index_sha256"],
        "universe": dict(plan_family["universe"]),
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "disjoint_family_evidence": True,
        },
        "falsifiers": [
            "nonpositive stressed log growth",
            "unstable parameter neighbors",
            "selection-aware statistical rejection",
            "incomplete execution or trial accounting",
        ],
        "implementation_files": [
            "dense_strategy_plugin.py",
            "dense_strategy_runtime.py",
            "learning_statistics.py",
            "learning_experiment.py",
            "strategy_discovery.py",
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
        "capacity_manifest": str(family["capacity_manifest"]),
    }


def freeze_batch(
    inventory_path: Path,
    *,
    as_of: date | None = None,
    index_path: Path = outcome_exposure.DEFAULT_INDEX,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    status_path: Path = DEFAULT_STATUS_PATH,
    enforce_commit: bool = True,
) -> tuple[list[Path], dict[str, Any]]:
    current = as_of or date.today()
    if current >= batch.ACTIVATION_NOT_BEFORE and enforce_commit:
        strategy_discovery.require_committed(inventory_path)
    inventory = _validate_inventory(
        _read(inventory_path),
        as_of=current,
        index_path=index_path,
        enforce_commit=enforce_commit,
    )
    plan = batch.build_plan()
    by_id = {item["family_id"]: item for item in plan["families"]}
    paths: list[Path] = []
    hashes: dict[str, str] = {}
    for family in sorted(inventory["families"], key=lambda item: item["family_id"]):
        contract = _contract(
            family, by_id[str(family["family_id"])], inventory["created_at"]
        )
        strategy_discovery._validate_family_contract(contract)
        digest = _hash(contract)
        path = output_root / str(family["family_id"]) / f"contract-{digest}.json"
        _write(contract, path)
        paths.append(path)
        hashes[str(family["family_id"])] = digest
    status = {
        "schema_version": SCHEMA_VERSION,
        "campaign_id": batch.CAMPAIGN_ID,
        "target_iso_week": batch.TARGET_ISO_WEEK,
        "activation_not_before": batch.ACTIVATION_NOT_BEFORE.isoformat(),
        "as_of": current.isoformat(),
        "state": "THREE_FAMILY_CONTRACTS_FROZEN",
        "family_contracts_frozen": 3,
        "family_contract_sha256": hashes,
        "inventory_sha256": inventory["inventory_sha256"],
        "outcome_exposure_index_sha256": inventory[
            "outcome_exposure_index_sha256"
        ],
        "provider_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "valid": True,
    }
    _write(status, status_path)
    return paths, status


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inventory", type=Path)
    parser.add_argument("--index", type=Path, default=outcome_exposure.DEFAULT_INDEX)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--status", type=Path, default=DEFAULT_STATUS_PATH)
    parser.add_argument("--as-of", type=date.fromisoformat)
    return parser


def main() -> int:
    args = _parser().parse_args()
    paths, status = freeze_batch(
        args.inventory,
        as_of=args.as_of,
        index_path=args.index,
        output_root=args.output_root,
        status_path=args.status,
    )
    print(
        json.dumps(
            {**status, "paths": [str(path) for path in paths]},
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
