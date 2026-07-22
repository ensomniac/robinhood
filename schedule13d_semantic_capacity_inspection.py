"""Inspect Schedule 13D semantic activation and capacity result."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import schedule13d_capacity as capacity
import schedule13d_semantic_capacity as semantic
from historical_store import sha256_file


class Schedule13dSemanticInspectionError(RuntimeError):
    """The semantic activation or result does not independently rebuild."""


def inspect_activation(
    path: Path, *, status_path: Path = semantic.ACTIVATION_STATUS_PATH
) -> dict[str, Any]:
    recorded = semantic.load_activation(path)
    expected = semantic.build_activation()
    status = semantic._read_object(status_path)
    pending = status.get("status") == "SEMANTIC_ACTIVATION_PENDING_INSPECTION"
    inspected = (
        status.get("status") == "SEMANTIC_ACTIVATION_INSPECTED"
        and status.get("inspection_sha256")
        == capacity.successor._self_hash(status, "inspection_sha256")
    )
    for relative, digest in recorded["implementation_hashes"].items():
        if sha256_file(semantic.PROJECT_ROOT / relative) != digest:
            raise Schedule13dSemanticInspectionError(
                f"semantic implementation drifted: {relative}"
            )
    if not (
        recorded == expected
        and (pending or inspected)
        and status.get("activation_sha256") == recorded["activation_sha256"]
        and recorded["denominator"]["indexed_initial_sc13d_filings"] == 6852
        and recorded["denominator"]["verified_event_count"] is None
        and recorded["parser_contract"]["same_paragraph_intent_required"] is True
        and recorded["parser_contract"]["cooldown_calendar_days"] == 63
        and recorded["parser_contract"][
            "unresolved_symbol_conservative_cooldown_anchor"
        ]
        is True
        and recorded["access_contract"][
            "classification_before_activation_inspection_permitted"
        ]
        is False
        and recorded["access_contract"]["issuer_symbol_supplemental_access_permitted"]
        is False
        and recorded["access_contract"]["market_price_access_permitted"] is False
        and recorded["access_contract"]["stage0_outcome_access_permitted"] is False
        and recorded["verified_event_count"] is None
        and recorded["capacity_passed"] is None
        and recorded["returns_computed"] == 0
        and recorded["market_outcomes_accessed"] is False
    ):
        raise Schedule13dSemanticInspectionError(
            "semantic parser, denominator, or outcome boundary differs"
        )
    result: dict[str, Any] = {
        "schema_version": semantic.SCHEMA_VERSION,
        "inspection_kind": "outcome-blind-filing-semantic-activation-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "dataset_id": semantic.documents.DATASET_ID,
        "contract_sha256": semantic.documents.indexes.CONTRACT_SHA256,
        "document_collection_inspection_sha256": semantic.DOCUMENT_INSPECTION_SHA256,
        "activation_sha256": recorded["activation_sha256"],
        "activation_file_sha256": sha256_file(path),
        "status": "SEMANTIC_ACTIVATION_INSPECTED",
        "denominator": 6852,
        "parser_patterns_rebuilt": True,
        "implementation_hashes_rebuilt": True,
        "complete_attrition_rule_rebuilt": True,
        "classification_permitted": True,
        "issuer_symbol_supplemental_access_permitted": False,
        "verified_event_count": None,
        "capacity_passed": None,
        "market_price_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = capacity.successor._self_hash(
        result, "inspection_sha256"
    )
    semantic._write_json(result, status_path)
    return result


def inspect_result(
    activation: Path,
    result_path: Path,
) -> dict[str, Any]:
    recorded = semantic._read_object(result_path)
    if recorded.get("result_sha256") != capacity.successor._self_hash(
        recorded, "result_sha256"
    ):
        raise Schedule13dSemanticInspectionError("semantic result hash is invalid")
    expected = semantic.build_result(
        activation, require_published=False, write_private=False
    )
    if recorded != expected:
        raise Schedule13dSemanticInspectionError(
            "semantic capacity result does not independently rebuild"
        )
    result: dict[str, Any] = {
        "schema_version": semantic.SCHEMA_VERSION,
        "inspection_kind": "outcome-blind-filing-semantic-result-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "dataset_id": semantic.documents.DATASET_ID,
        "contract_sha256": recorded["contract_sha256"],
        "activation_sha256": recorded["activation_sha256"],
        "activation_inspection_sha256": recorded["activation_inspection_sha256"],
        "result_sha256": recorded["result_sha256"],
        "result_file_sha256": sha256_file(result_path),
        "classification_sha256": recorded["classification_sha256"],
        "denominator": recorded["denominator"],
        "attrition_counts": recorded["attrition_counts"],
        "verified_event_lower_bound": recorded["verified_event_lower_bound"],
        "pending_causal_symbol_supplemental": recorded[
            "pending_causal_symbol_supplemental"
        ],
        "minimum_capacity_proven_without_supplemental": recorded[
            "minimum_capacity_proven_without_supplemental"
        ],
        "capacity_classification_complete": recorded[
            "capacity_classification_complete"
        ],
        "capacity_passed": recorded["capacity_passed"],
        "issuer_symbol_supplemental_access_permitted": (
            recorded["pending_causal_symbol_supplemental"] > 0
        ),
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = capacity.successor._self_hash(
        result, "inspection_sha256"
    )
    return result


def result_inspection_path(value: Mapping[str, Any]) -> Path:
    return semantic.RESULT_INSPECTION_ROOT / (
        f"{capacity.CANDIDATE_ID}-semantic-{value['inspection_sha256']}.json"
    )


def _one(pattern: str, description: str) -> Path:
    matches = sorted(semantic.PROJECT_ROOT.glob(pattern))
    if len(matches) != 1:
        raise Schedule13dSemanticInspectionError(
            f"expected one {description}; found {len(matches)}"
        )
    return matches[0]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect-activation", "inspect-result"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        activation = _one(
            "strategy_tournament/v2/schedule13d/semantic/activations/"
            f"{capacity.CANDIDATE_ID}-*.json",
            "semantic activation",
        )
        if args.command == "inspect-activation":
            result = inspect_activation(activation)
        else:
            result_path = _one(
                "strategy_tournament/v2/schedule13d/semantic/results/"
                f"{capacity.CANDIDATE_ID}-*.json",
                "semantic result",
            )
            result = inspect_result(activation, result_path)
            path = result_inspection_path(result)
            semantic._write_json(result, path)
            result = {**result, "written": semantic.documents._repo_relative(path)}
    except (
        Schedule13dSemanticInspectionError,
        semantic.Schedule13dSemanticCapacityError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
