"""Independently rebuild one frozen ASR single-rule capacity contract."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import asr_security_identity as identity
import asr_single_rule_preflight as preflight
import outcome_exposure
import strategy_discovery


DEFAULT_ROOT = preflight.DEFAULT_ROOT / "inspections"


class AsrSingleRulePreflightInspectionError(RuntimeError):
    """The frozen ASR single-rule capacity contract failed inspection."""


def _write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def inspect(
    contract_path: Path,
    *,
    inspected_at: str,
    output_root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    preflight._timestamp(inspected_at, "inspected_at")
    if enforce_commit:
        strategy_discovery.require_committed(contract_path)
    contract = preflight.load_contract(contract_path)
    rebuilt = preflight.build_contract(
        created_at=str(contract["created_at"]),
        enforce_commit=enforce_commit,
    )
    current_index = outcome_exposure.audit()["index_sha256"]
    inventory = rebuilt["inventory"]
    checks = {
        "contract_hash_valid": contract["contract_sha256"]
        == identity.self_hash(contract, "contract_sha256"),
        "contract_rebuilt_exactly": rebuilt == contract,
        "combined_result_rebuilt": inventory["combined_result_sha256"]
        == contract["source_lineage"]["combined_result_sha256"],
        "combined_inspection_rebuilt": inventory[
            "combined_inspection_sha256"
        ]
        == contract["source_lineage"]["combined_inspection_sha256"],
        "agreement_count_rebuilt": inventory["verified_agreement_count"] == 185,
        "independent_disclosures_rebuilt": inventory[
            "independent_disclosure_count"
        ]
        == 89,
        "daily_ranking_rebuilt": inventory["daily_ranked_signal_capacity"] == 86,
        "development_allocation_rebuilt": inventory[
            "development_candidate_count"
        ]
        == 50,
        "five_session_embargo_rebuilt": len(inventory["embargo_dates"]) == 5,
        "confirmation_candidates_rebuilt": inventory[
            "confirmation_candidate_count"
        ]
        == 35,
        "untouched_confirmation_capacity_rebuilt": inventory[
            "untouched_confirmation_signal_capacity"
        ]
        == 3,
        "outcome_index_current": inventory["outcome_exposure_index_sha256"]
        == current_index,
        "preselected_primary_only": contract["selection_mode"]
        == "preselected_primary"
        and contract["trial_count"] == 1,
        "capacity_disposition_rebuilt": contract["capacity_disposition"]
        == "INSUFFICIENT_POWER_CAPACITY",
        "zero_outcome_access_rebuilt": contract["market_price_values_accessed"]
        == 0
        and contract["returns_computed"] == 0
        and contract["market_outcomes_accessed"] is False,
        "all_external_actions_closed": contract["provider_access_permitted"]
        is False
        and contract["market_price_access_permitted"] is False
        and contract["confirmation_outcome_access_permitted"] is False
        and contract["broker_actions_permitted"] is False,
    }
    if not all(checks.values()):
        failed = sorted(name for name, passed in checks.items() if not passed)
        raise AsrSingleRulePreflightInspectionError(
            f"ASR single-rule capacity inspection failed: {failed}"
        )
    inspection: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "outcome-blind-asr-single-rule-capacity-inspection",
        "campaign_id": contract["campaign_id"],
        "family_id": contract["family_id"],
        "strategy_id": contract["strategy_id"],
        "strategy_version": contract["strategy_version"],
        "contract_path": (
            preflight._repo_path(contract_path)
            if enforce_commit
            else str(contract_path)
        ),
        "contract_sha256": contract["contract_sha256"],
        "inspected_at": inspected_at,
        "checks": checks,
        "verified_agreement_count": inventory["verified_agreement_count"],
        "independent_disclosure_count": inventory[
            "independent_disclosure_count"
        ],
        "daily_ranked_signal_capacity": inventory[
            "daily_ranked_signal_capacity"
        ],
        "development_candidate_count": inventory[
            "development_candidate_count"
        ],
        "confirmation_candidate_count": inventory[
            "confirmation_candidate_count"
        ],
        "required_confirmation_signals": (
            preflight.MINIMUM_CONFIRMATION_SIGNALS
        ),
        "untouched_confirmation_signal_capacity": inventory[
            "untouched_confirmation_signal_capacity"
        ],
        "capacity_disposition": "INSUFFICIENT_POWER_CAPACITY",
        "reason": (
            "Only three chronological ASR confirmation opportunities retain "
            "globally untouched warmup-through-exit price scopes, below the "
            "frozen minimum of twenty."
        ),
        "development_search_permitted": False,
        "single_rule_price_access_permitted": False,
        "confirmation_outcome_access_permitted": False,
        "provider_requests": 0,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions_permitted": False,
        "maturity_effect": "NONE",
        "state": "INSUFFICIENT_POWER_CAPACITY",
        "valid": True,
    }
    inspection["inspection_sha256"] = identity.self_hash(
        inspection, "inspection_sha256"
    )
    path = output_root / (
        f"asr-single-rule-{inspection['inspection_sha256']}.json"
    )
    _write(path, inspection)
    return path, inspection


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("contract", type=Path)
    parser.add_argument("--inspected-at", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        path, inspection = inspect(
            args.contract,
            inspected_at=args.inspected_at,
        )
    except (
        AsrSingleRulePreflightInspectionError,
        preflight.AsrSingleRulePreflightError,
        outcome_exposure.OutcomeExposureError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(
        json.dumps(
            {**inspection, "written": preflight._repo_path(path)},
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
