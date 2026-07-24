"""Freeze the range-collected country-ETF opening-reversal version."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import country_etf_opening_reversal as base
import outcome_exposure
import sector_etf_gap_drift as artifact_support
import strategy_discovery
from historical_store import sha256_file
from learning_data import freeze_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = base.CAMPAIGN_ID
FAMILY_ID = base.FAMILY_ID
MECHANISM_FAMILY = base.MECHANISM_FAMILY
STRATEGY_ID = base.STRATEGY_ID
SUCCESSOR_ID = "country-etf-opening-reversal-v2-symbol-range"
RESEARCH_GENERATION = base.RESEARCH_GENERATION
DEFAULT_ROOT = base.DEFAULT_ROOT
SYMBOLS = base.SYMBOLS
CALENDAR_PATH = base.CALENDAR_PATH
BASE_CONTRACT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "country-etf-opening-reversal-v1-long-history/family-contract/"
    "contract-e97f8797ec822d4163d05f02ff85816ef1797c3003cd09da5284d8bc5fd4529b.json"
)


class CountryEtfOpeningReversalRangeError(ValueError):
    """The range-collected exact version or its predecessor drifted."""


def _repo_path(path: Path) -> str:
    return artifact_support._repo_path(path)


def _read_plain(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CountryEtfOpeningReversalRangeError(
            f"frozen predecessor cannot be loaded: {path.name}"
        ) from exc
    if not isinstance(value, dict):
        raise CountryEtfOpeningReversalRangeError(
            "frozen predecessor is malformed"
        )
    return value


def _base_contract(*, enforce_commit: bool) -> dict[str, Any]:
    if enforce_commit:
        strategy_discovery.require_committed(BASE_CONTRACT)
    contract = _read_plain(BASE_CONTRACT)
    if not (
        contract.get("family_id") == FAMILY_ID
        and contract.get("successor_id") == base.SUCCESSOR_ID
        and contract.get("parameter_grid")
        == _read_plain(base.BASE_CONTRACT)["parameter_grid"]
        and contract.get("development_scope", {}).get("symbols") == SYMBOLS
    ):
        raise CountryEtfOpeningReversalRangeError(
            "country-ETF V1 contract drifted"
        )
    return contract


def _validate_exposure_state(contract: Mapping[str, Any]) -> None:
    records = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(contract["confirmation_scope"], records)
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
        raise CountryEtfOpeningReversalRangeError(
            "development scope has foreign or partial exposure"
        )


def freeze_contract(
    *, created_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any], Path]:
    artifact_support._timestamp(created_at, "created_at")
    previous = _base_contract(enforce_commit=enforce_commit)
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    outcome_exposure.assert_untouched(
        previous["development_scope"], outcome_exposure.read_index()
    )
    outcome_exposure.assert_untouched(
        previous["confirmation_scope"], outcome_exposure.read_index()
    )
    evidence_paths = [
        _repo_path(BASE_CONTRACT),
        _repo_path(CALENDAR_PATH),
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
    ]
    capacity_path, _capacity = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-capacity",
            "registered_at": created_at,
            "requested_dates": [
                *previous["development_warmup_dates"],
                *previous["development_dates"],
                *previous["embargo_dates"],
                *previous["confirmation_dates"],
            ],
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
                        len(previous["development_dates"]) * len(SYMBOLS)
                    ),
                    "capacity_unit": (
                        "frozen instrument-session observations"
                    ),
                    "development_sessions": len(
                        previous["development_dates"]
                    ),
                    "embargo_sessions": len(previous["embargo_dates"]),
                    "confirmation_sessions": len(
                        previous["confirmation_dates"]
                    ),
                    "calendar_sha256": sha256_file(CALENDAR_PATH),
                    "provider_requests": 0,
                    "market_prices_accessed": False,
                    "outcomes_accessed": False,
                },
            },
        },
        DEFAULT_ROOT / SUCCESSOR_ID / "capacity",
    )

    contract = copy.deepcopy(previous)
    for field in (
        "implementation_hashes",
        "primary_trial_id",
        "rolling_origin_plan",
        "trial_family",
    ):
        contract.pop(field, None)
    contract.update(
        {
            "experiment_id": f"experiment-{SUCCESSOR_ID}",
            "parent_experiment_id": previous["experiment_id"],
            "created_at": created_at,
            "status": "INVENTED",
            "successor_id": SUCCESSOR_ID,
            "existing_successor_validator": {
                "module": "country_etf_opening_reversal_range",
                "function": "validate_existing_successor_contract",
            },
            "predecessor": {
                "contract_path": _repo_path(BASE_CONTRACT),
                "contract_sha256": hashlib.sha256(
                    artifact_support._canonical(previous)
                ).hexdigest(),
                "promotion_evidence_reused": False,
                "outcomes_accessed": False,
            },
            "historical_data_contract": {
                **previous["historical_data_contract"],
                "minute_request_mode": "symbol_range",
            },
            "material_difference_rationale": (
                "This exact version changes only the outcome-blind provider "
                "request topology from 8,480 session-symbol tasks to eight "
                "resumable symbol-range tasks; every strategy and evidence "
                "semantic remains identical to committed V1."
            ),
            "contamination_risks": [
                *previous["contamination_risks"],
                "V1 opened no minute prices or outcomes before the range topology was frozen.",
            ],
            "implementation_files": [
                "country_etf_opening_reversal_range.py",
                "country_etf_opening_reversal.py",
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
            "capacity_manifest": _repo_path(capacity_path),
            "outcome_exposure_index_sha256": outcome_exposure.audit()[
                "index_sha256"
            ],
        }
    )
    contract = strategy_discovery._validate_family_contract(contract)
    validate_existing_successor_contract(
        contract, enforce_commit=enforce_commit
    )
    digest = hashlib.sha256(
        artifact_support._canonical(contract)
    ).hexdigest()
    path = (
        DEFAULT_ROOT
        / SUCCESSOR_ID
        / "family-contract"
        / f"contract-{digest}.json"
    )
    artifact_support._write_json(path, contract)
    return path, contract, capacity_path


def validate_existing_successor_contract(
    contract: Mapping[str, Any], *, enforce_commit: bool = True
) -> None:
    previous = _base_contract(enforce_commit=enforce_commit)
    semantic_previous = copy.deepcopy(previous)
    semantic_current = copy.deepcopy(dict(contract))
    for value in (semantic_previous, semantic_current):
        for field in (
            "capacity_manifest",
            "contamination_risks",
            "created_at",
            "existing_successor_validator",
            "experiment_id",
            "implementation_files",
            "implementation_hashes",
            "material_difference_rationale",
            "outcome_exposure_index_sha256",
            "parent_experiment_id",
            "predecessor",
            "primary_trial_id",
            "rolling_origin_plan",
            "status",
            "successor_id",
            "trial_family",
        ):
            value.pop(field, None)
    current_provider = semantic_current.get("historical_data_contract", {})
    if isinstance(current_provider, dict):
        current_provider.pop("minute_request_mode", None)
    if not (
        semantic_current == semantic_previous
        and contract.get("successor_id") == SUCCESSOR_ID
        and contract.get("historical_data_contract", {}).get(
            "minute_request_mode"
        )
        == "symbol_range"
        and len(contract.get("trial_family", [])) == 32
        and contract.get("existing_successor_validator")
        == {
            "module": "country_etf_opening_reversal_range",
            "function": "validate_existing_successor_contract",
        }
    ):
        raise CountryEtfOpeningReversalRangeError(
            "range version changed strategy or evidence semantics"
        )
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
        CountryEtfOpeningReversalRangeError,
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
