"""Falsify frozen cross-sectional variants whose sample cannot meet Stage 0."""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import etf_or_momentum_stage0 as common
import sector_etf_rotation_stage0 as daily
from historical_store import sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = Path("/Users/ensomniac/trade/historical_data")
SLATE_PATH = daily.SLATE_PATH
SLATE_INSPECTION_PATH = daily.SLATE_INSPECTION_PATH
SCHEMA_VERSION = 1
VARIANT_ORDINALS = {
    "two-to-three-day-cross-sectional-reversal-v1": 4,
    "five-day-52-week-high-continuation-v1": 5,
}


class CrossSectionalCapacityError(RuntimeError):
    """A frozen capacity contract is missing, mutable, or inconsistent."""


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CrossSectionalCapacityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CrossSectionalCapacityError(f"{path} must contain an object")
    return value


def _load_gzip_json(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise CrossSectionalCapacityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CrossSectionalCapacityError(f"{path} must contain an object")
    return value


def _variant(variant_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    if variant_id not in VARIANT_ORDINALS:
        raise CrossSectionalCapacityError(f"unsupported variant: {variant_id}")
    slate = _load_json(SLATE_PATH)
    if slate.get("manifest_sha256") != common._self_hash(slate, "manifest_sha256"):
        raise CrossSectionalCapacityError("second-wave slate hash is invalid")
    inspection = _load_json(SLATE_INSPECTION_PATH)
    if (
        inspection.get("inspection_sha256")
        != common._self_hash(inspection, "inspection_sha256")
        or inspection.get("manifest_sha256") != slate["manifest_sha256"]
        or inspection.get("return_evaluation_authorized") is not True
    ):
        raise CrossSectionalCapacityError("second-wave slate inspection is invalid")
    matches = [item for item in slate["variants"] if item["variant_id"] == variant_id]
    if len(matches) != 1:
        raise CrossSectionalCapacityError(f"{variant_id} is missing from the slate")
    variant = dict(matches[0])
    expected_ordinal = [item["variant_id"] for item in slate["variants"]].index(
        variant_id
    ) + 1
    if expected_ordinal != VARIANT_ORDINALS[variant_id]:
        raise CrossSectionalCapacityError(f"{variant_id} ordinal drifted")
    return variant, slate


def _membership(variant: Mapping[str, Any]) -> tuple[Path, dict[str, Any]]:
    contract = variant["data_contract"]
    path = DATA_ROOT / str(contract["membership_path"])
    expected_file_hash = str(contract["membership_file_sha256"])
    if sha256_file(path) != expected_file_hash:
        raise CrossSectionalCapacityError("frozen membership file hash drifted")
    membership = _load_gzip_json(path)
    target_dates = list(contract["target_dates"])
    if membership.get("target_dates") != target_dates:
        raise CrossSectionalCapacityError("frozen membership target dates drifted")
    if membership.get("target_outcomes_observed_or_derived") is not False:
        raise CrossSectionalCapacityError("membership source is outcome-derived")
    members_by_date = membership.get("members_by_date")
    if (
        not isinstance(members_by_date, Mapping)
        or list(members_by_date) != target_dates
    ):
        raise CrossSectionalCapacityError("membership date denominator drifted")
    for day, rows in members_by_date.items():
        if not isinstance(rows, list) or not rows:
            raise CrossSectionalCapacityError(f"membership is empty for {day}")
        symbols = [str(row.get("symbol", "")) for row in rows]
        if any(not symbol for symbol in symbols) or len(symbols) != len(set(symbols)):
            raise CrossSectionalCapacityError(
                f"membership symbols are invalid for {day}"
            )
    return path, membership


def _capacity(variant: Mapping[str, Any], slate: Mapping[str, Any]) -> dict[str, Any]:
    target_dates = list(variant["data_contract"]["target_dates"])
    entries_per_day = int(variant["execution"]["maximum_new_entries_per_day"])
    minimum = int(slate["stage0_falsification"]["minimum_closed_signals"])
    maximum = len(target_dates) * entries_per_day
    return {
        "target_date_count": len(target_dates),
        "maximum_new_entries_per_target_date": entries_per_day,
        "maximum_possible_closed_signals": maximum,
        "minimum_required_closed_signals": minimum,
        "closed_signal_shortfall": max(0, minimum - maximum),
        "stage0_capacity_possible": maximum >= minimum,
        "proof": (
            "target_date_count multiplied by maximum_new_entries_per_day; "
            "no market outcome can increase this preregistered upper bound"
        ),
    }


def build_activation(variant_id: str) -> dict[str, Any]:
    variant, slate = _variant(variant_id)
    membership_path, membership = _membership(variant)
    capacity = _capacity(variant, slate)
    if capacity["stage0_capacity_possible"]:
        raise CrossSectionalCapacityError(
            f"{variant_id} is not eligible for capacity-only falsification"
        )
    members_by_date = membership["members_by_date"]
    unique_symbols = {
        str(row["symbol"]) for rows in members_by_date.values() for row in rows
    }
    activation: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_kind": "stage0-capacity-activation",
        "campaign_id": "multi-strategy-portfolio-validation-v1",
        "tournament_wave": 2,
        "variant_ordinal": VARIANT_ORDINALS[variant_id],
        "variant_id": variant_id,
        "strategy_version": variant["version"],
        "mechanism_family": variant["mechanism_family"],
        "base_rules_hash": variant["rules_hash"],
        "slate_manifest_sha256": slate["manifest_sha256"],
        "slate_inspection_sha256": _load_json(SLATE_INSPECTION_PATH)[
            "inspection_sha256"
        ],
        "implementation_sha256": sha256_file(Path(__file__).resolve()),
        "claim_scope": "FALSIFICATION_ONLY",
        "outcomes_previously_accessed_for_exact_rules": False,
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "provider_requests_authorized": False,
        "broker_actions_authorized": False,
        "return_evaluation_authorized_before_inspection": False,
        "source_selection": {
            "provider": variant["data_contract"]["provider"],
            "provider_substitution": False,
            "membership_path": str(membership_path.relative_to(DATA_ROOT)),
            "membership_file_sha256": sha256_file(membership_path),
            "membership_dates": len(members_by_date),
            "membership_rows": sum(len(rows) for rows in members_by_date.values()),
            "unique_symbols": len(unique_symbols),
            "market_data_requested": False,
            "market_outcomes_accessed": False,
        },
        "selection_contract": dict(variant["signal"]),
        "outcome_contract": {
            **variant["execution"],
            **variant["exit"],
            "maximum_holding_trading_days": variant["maximum_holding_trading_days"],
        },
        "stage0_gate": dict(slate["stage0_falsification"]),
        "capacity_contract": capacity,
        "denominator": {
            "decision_dates": len(variant["data_contract"]["target_dates"]),
            "maximum_strategy_entries_per_day": int(
                variant["execution"]["maximum_new_entries_per_day"]
            ),
            "preserve_unevaluated_dates": True,
        },
        "maturity_effect": "NONE",
    }
    activation["activation_rules_hash"] = common._hash(
        {
            "base_rules_hash": activation["base_rules_hash"],
            "source_selection": activation["source_selection"],
            "selection_contract": activation["selection_contract"],
            "outcome_contract": activation["outcome_contract"],
            "stage0_gate": activation["stage0_gate"],
            "capacity_contract": activation["capacity_contract"],
        }
    )
    activation["manifest_sha256"] = common._self_hash(activation, "manifest_sha256")
    return activation


def default_activation_path(activation: Mapping[str, Any]) -> Path:
    return (
        PROJECT_ROOT / "strategy_tournament/second_wave/activations/"
        f"{activation['variant_id']}-{activation['manifest_sha256']}.json"
    )


def inspect_activation(path: Path) -> dict[str, Any]:
    recorded = _load_json(path)
    if recorded.get("manifest_sha256") != common._self_hash(
        recorded, "manifest_sha256"
    ):
        raise CrossSectionalCapacityError("activation content hash is invalid")
    variant_id = str(recorded.get("variant_id"))
    if recorded != build_activation(variant_id):
        raise CrossSectionalCapacityError("activation does not rebuild")
    capacity = recorded["capacity_contract"]
    if capacity["stage0_capacity_possible"] is not False:
        raise CrossSectionalCapacityError("capacity proof does not falsify Stage 0")
    inspection: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-activation-capacity-inspection",
        "variant_id": variant_id,
        "manifest_sha256": recorded["manifest_sha256"],
        "manifest_file_sha256": sha256_file(path),
        "membership_file_sha256": recorded["source_selection"][
            "membership_file_sha256"
        ],
        "target_date_count": capacity["target_date_count"],
        "maximum_possible_closed_signals": capacity["maximum_possible_closed_signals"],
        "minimum_required_closed_signals": capacity["minimum_required_closed_signals"],
        "closed_signal_shortfall": capacity["closed_signal_shortfall"],
        "provider_requests": 0,
        "broker_actions": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "capacity_evaluation_authorized": True,
        "return_evaluation_authorized": False,
        "maturity_effect": "NONE",
        "valid": True,
    }
    inspection["inspection_sha256"] = common._self_hash(inspection, "inspection_sha256")
    return inspection


def build_result(
    activation_path: Path,
    inspection_path: Path,
    *,
    require_published: bool = True,
) -> dict[str, Any]:
    activation = _load_json(activation_path)
    inspection = _load_json(inspection_path)
    if inspection != inspect_activation(activation_path):
        raise CrossSectionalCapacityError("capacity inspection does not rebuild")
    if inspection.get("capacity_evaluation_authorized") is not True:
        raise CrossSectionalCapacityError("capacity evaluation is not authorized")
    if require_published:
        daily._require_published((activation_path, inspection_path))
    empty_metrics = common._metrics([])
    blockers = [
        "closed signals are below the Stage 0 minimum",
        "primary expectancy is not positive",
        "primary profit factor is below the Stage 0 minimum",
        "20 bps-per-side total R is not positive",
    ]
    capacity = activation["capacity_contract"]
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "result_kind": "stage0-capacity-falsification",
        "variant_id": activation["variant_id"],
        "strategy_version": activation["strategy_version"],
        "mechanism_family": activation["mechanism_family"],
        "base_rules_hash": activation["base_rules_hash"],
        "activation_rules_hash": activation["activation_rules_hash"],
        "manifest_sha256": activation["manifest_sha256"],
        "inspection_sha256": inspection["inspection_sha256"],
        "implementation_sha256": activation["implementation_sha256"],
        "claim_scope": "FALSIFICATION_ONLY",
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "provider_requests": 0,
        "broker_actions": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "denominator": {
            "decision_dates": capacity["target_date_count"],
            "closed_signals": 0,
            "maximum_possible_closed_signals": capacity[
                "maximum_possible_closed_signals"
            ],
            "minimum_required_closed_signals": capacity[
                "minimum_required_closed_signals"
            ],
            "closed_signal_shortfall": capacity["closed_signal_shortfall"],
            "unevaluated_capacity_blocked_dates": capacity["target_date_count"],
            "rule_violations": 0,
        },
        "primary_5bps": dict(empty_metrics),
        "stress": {"10": dict(empty_metrics), "20": dict(empty_metrics)},
        "stage0_survived": False,
        "stage0_blockers": blockers,
        "structural_falsification": {
            **capacity,
            "provider_collection_skipped": True,
            "outcome_evaluation_skipped": True,
        },
        "next_action": (
            "retire this exact capacity-infeasible variant and advance to the "
            "next frozen mechanism"
        ),
        "maturity_effect": "NONE",
        "records": [],
    }
    result["result_sha256"] = common._self_hash(result, "result_sha256")
    return result


def inspect_result(
    activation_path: Path, inspection_path: Path, result_path: Path
) -> dict[str, Any]:
    recorded = _load_json(result_path)
    if recorded.get("result_sha256") != common._self_hash(recorded, "result_sha256"):
        raise CrossSectionalCapacityError("result content hash is invalid")
    if recorded != build_result(
        activation_path, inspection_path, require_published=False
    ):
        raise CrossSectionalCapacityError("result does not independently rebuild")
    denominator = recorded["denominator"]
    inspection: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-result-inspection",
        "variant_id": recorded["variant_id"],
        "manifest_sha256": recorded["manifest_sha256"],
        "input_inspection_sha256": recorded["inspection_sha256"],
        "result_sha256": recorded["result_sha256"],
        "result_file_sha256": sha256_file(result_path),
        "closed_signals": denominator["closed_signals"],
        "maximum_possible_closed_signals": denominator[
            "maximum_possible_closed_signals"
        ],
        "minimum_required_closed_signals": denominator[
            "minimum_required_closed_signals"
        ],
        "stage0_survived": False,
        "provider_requests": 0,
        "broker_actions": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "maturity_effect": "NONE",
        "valid": True,
    }
    inspection["inspection_sha256"] = common._self_hash(inspection, "inspection_sha256")
    return inspection


def _one(pattern: str, description: str) -> Path:
    matches = sorted(PROJECT_ROOT.glob(pattern))
    if len(matches) != 1:
        raise CrossSectionalCapacityError(
            f"expected one {description}; found {len(matches)}"
        )
    return matches[0]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("freeze", "inspect-inputs", "evaluate", "inspect-result")
    )
    parser.add_argument("variant_id", choices=tuple(VARIANT_ORDINALS))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    variant_id = args.variant_id
    try:
        if args.command == "freeze":
            result = build_activation(variant_id)
            path = default_activation_path(result)
            daily._write_json(result, path)
            output = {
                "manifest_sha256": result["manifest_sha256"],
                "maximum_possible_closed_signals": result["capacity_contract"][
                    "maximum_possible_closed_signals"
                ],
                "written": str(path.relative_to(PROJECT_ROOT)),
            }
        elif args.command == "inspect-inputs":
            activation_path = _one(
                f"strategy_tournament/second_wave/activations/{variant_id}-*.json",
                f"{variant_id} capacity activation",
            )
            result = inspect_activation(activation_path)
            path = (
                PROJECT_ROOT / "strategy_tournament/second_wave/inspections/"
                f"{variant_id}-input-{result['inspection_sha256']}.json"
            )
            daily._write_json(result, path)
            output = {
                "inspection_sha256": result["inspection_sha256"],
                "written": str(path.relative_to(PROJECT_ROOT)),
            }
        elif args.command == "evaluate":
            activation_path = _one(
                f"strategy_tournament/second_wave/activations/{variant_id}-*.json",
                f"{variant_id} capacity activation",
            )
            inspection_path = _one(
                f"strategy_tournament/second_wave/inspections/{variant_id}-input-*.json",
                f"{variant_id} capacity inspection",
            )
            result = build_result(activation_path, inspection_path)
            path = (
                PROJECT_ROOT / "research_results/"
                f"2026-07-21-{result['mechanism_family']}-stage0-"
                f"{result['result_sha256']}.json"
            )
            daily._write_json(result, path)
            output = {
                "result_sha256": result["result_sha256"],
                "stage0_survived": result["stage0_survived"],
                "closed_signals": result["denominator"]["closed_signals"],
                "written": str(path.relative_to(PROJECT_ROOT)),
            }
        else:
            activation_path = _one(
                f"strategy_tournament/second_wave/activations/{variant_id}-*.json",
                f"{variant_id} capacity activation",
            )
            inspection_path = _one(
                f"strategy_tournament/second_wave/inspections/{variant_id}-input-*.json",
                f"{variant_id} capacity inspection",
            )
            candidates = [
                path
                for path in PROJECT_ROOT.glob(
                    "research_results/2026-07-21-*-stage0-*.json"
                )
                if _load_json(path).get("variant_id") == variant_id
            ]
            if len(candidates) != 1:
                raise CrossSectionalCapacityError(
                    f"expected one {variant_id} Stage 0 result; found {len(candidates)}"
                )
            result_path = candidates[0]
            result = inspect_result(activation_path, inspection_path, result_path)
            path = (
                PROJECT_ROOT / "strategy_tournament/second_wave/inspections/"
                f"{variant_id}-result-{result['inspection_sha256']}.json"
            )
            daily._write_json(result, path)
            output = {
                "inspection_sha256": result["inspection_sha256"],
                "written": str(path.relative_to(PROJECT_ROOT)),
            }
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    except (OSError, RuntimeError, ValueError) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
