"""Freeze a long-history country-ETF opening-reversal replication."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import high_beta_etf_oversold as calendar_support
import outcome_exposure
import portfolio_maturity
import sector_etf_gap_drift as artifact_support
import strategy_discovery
from historical_store import sha256_file
from learning_data import freeze_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.COUNTRY_ETF_OPENING_REVERSAL_FAMILY
MECHANISM_FAMILY = "intraday-index-etf-opening-reversal"
STRATEGY_ID = "country-etf-opening-reversal"
SUCCESSOR_ID = "country-etf-opening-reversal-v1-long-history"
RESEARCH_GENERATION = "existing_family_successor"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
CALENDAR_PATH = calendar_support.CALENDAR_PATH
CALENDAR_INSPECTION = calendar_support.CALENDAR_INSPECTION
SYMBOLS = ["EWC", "EWG", "EWP", "EWQ", "EWT", "EWU", "EWW", "EWY"]
DEVELOPMENT_WARMUP_SESSIONS = 60
DEVELOPMENT_SESSIONS = 1_000
EMBARGO_SESSIONS = 5
CONFIRMATION_SESSIONS = 500
TOTAL_SESSIONS = (
    DEVELOPMENT_WARMUP_SESSIONS
    + DEVELOPMENT_SESSIONS
    + EMBARGO_SESSIONS
    + CONFIRMATION_SESSIONS
)
BASE_CONTRACT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/family-contracts/"
    "intraday-index-etf-opening-reversal/"
    "contract-97278779287891b951c07a57f4ba1e09c8d67c98bc937ead7077976072fa5a06.json"
)
PREDECESSOR_SEARCH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/intraday-index-etf-opening-reversal/"
    "search/intraday-index-etf-opening-reversal-search-"
    "ee47036462ba257fe88cb4d6c8a72ff0704e60bac6477792cd7be1aa577e95c5.json"
)
PREDECESSOR_RESULT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/intraday-index-etf-opening-reversal/"
    "development/intraday-index-etf-opening-reversal-development-"
    "ccfe990efe26e6771434f2d280cd6433383c6f159d22da432caf7471e25991da.json"
)
PREDECESSOR_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/intraday-index-etf-opening-reversal/"
    "development-inspection/"
    "intraday-index-etf-opening-reversal-development-inspection-"
    "d66c2a87cce85d780545521562e61f528987ccdcb5bb5d513910cab6d8d8ec21.json"
)


class CountryEtfOpeningReversalError(ValueError):
    """The country-ETF replication or evidence graph drifted."""


def _repo_path(path: Path) -> str:
    return artifact_support._repo_path(path)


def _full_sessions() -> list[str]:
    return calendar_support._full_sessions()


def _read_plain(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CountryEtfOpeningReversalError(
            f"adverse evidence cannot be loaded: {path.name}"
        ) from exc
    if not isinstance(value, dict):
        raise CountryEtfOpeningReversalError(
            f"adverse evidence is malformed: {path.name}"
        )
    return value


def _adverse_predecessor(*, enforce_commit: bool) -> dict[str, dict[str, Any]]:
    paths = (
        CALENDAR_PATH,
        CALENDAR_INSPECTION,
        BASE_CONTRACT,
        PREDECESSOR_SEARCH,
        PREDECESSOR_RESULT,
        PREDECESSOR_INSPECTION,
    )
    if enforce_commit:
        for path in paths:
            strategy_discovery.require_committed(path)
    base = _read_plain(BASE_CONTRACT)
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
        base.get("family_id") == runtime.INTRADAY_ETF_FAMILY
        and search.get("family_contract", {}).get("family_id")
        == runtime.INTRADAY_ETF_FAMILY
        and result.get("search_sha256") == search["artifact_sha256"]
        and inspection.get("result_sha256") == result["artifact_sha256"]
        and inspection.get("state") == "REJECTED"
    ):
        raise CountryEtfOpeningReversalError(
            "intraday opening-reversal adverse graph drifted"
        )
    return {
        "base": base,
        "search": search,
        "result": result,
        "inspection": inspection,
    }


def _scope(dates: list[str]) -> dict[str, Any]:
    return {"dates": list(dates), "symbols": list(SYMBOLS)}


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
        raise CountryEtfOpeningReversalError(
            "development scope has foreign or partial outcome exposure"
        )


def freeze_contract(
    *, created_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any], Path]:
    artifact_support._timestamp(created_at, "created_at")
    adverse = _adverse_predecessor(enforce_commit=enforce_commit)
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
        _repo_path(BASE_CONTRACT),
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

    contract = copy.deepcopy(adverse["base"])
    for field in (
        "implementation_hashes",
        "primary_trial_id",
        "rolling_origin_plan",
        "trial_family",
    ):
        contract.pop(field, None)
    contract.update(
        {
            "campaign_id": CAMPAIGN_ID,
            "experiment_id": f"experiment-{SUCCESSOR_ID}",
            "family_id": FAMILY_ID,
            "mechanism_family": MECHANISM_FAMILY,
            "strategy_id": STRATEGY_ID,
            "parent_experiment_id": adverse["base"]["experiment_id"],
            "created_at": created_at,
            "status": "INVENTED",
            "research_generation": RESEARCH_GENERATION,
            "successor_id": SUCCESSOR_ID,
            "existing_successor_validator": {
                "module": "country_etf_opening_reversal",
                "function": "validate_existing_successor_contract",
            },
            "new_mechanism_family_slot_consumed": False,
            "prior_family_attempt_count": 1,
            "predecessor": {
                "search_path": _repo_path(PREDECESSOR_SEARCH),
                "search_sha256": adverse["search"]["artifact_sha256"],
                "result_path": _repo_path(PREDECESSOR_RESULT),
                "result_sha256": adverse["result"]["artifact_sha256"],
                "inspection_path": _repo_path(PREDECESSOR_INSPECTION),
                "inspection_sha256": adverse["inspection"][
                    "artifact_sha256"
                ],
                "promotion_evidence_reused": False,
            },
            "mechanism": (
                "Replicate the frozen downside opening-z and completed VWAP "
                "reclaim reversal across liquid country ETFs over a much "
                "larger, outcome-clean history."
            ),
            "universe_requirements": {
                "symbols": list(SYMBOLS),
                "data": "complete SIP regular-session minute bars",
                "selection_basis": (
                    "Eight liquid developed-market country ETFs fixed before "
                    "minute-price access and globally outcome-clean on every "
                    "exact 2016-2022 date-symbol pair."
                ),
            },
            "universe": {
                "symbols": list(SYMBOLS),
                "data": "complete SIP regular-session minute bars",
            },
            "contamination_risks": [
                "The rejected 120-session index-ETF result cannot count toward this replication.",
                "The exact country-ETF date-symbol pairs are absent from the global exposure index before development.",
                "The complete 32-trial grid is unchanged so predecessor outcomes cannot repair the rule.",
                "Sixty warmup sessions estimate opening-return z-scores only and cannot count as target evidence.",
            ],
            "material_difference_rationale": (
                "This preregistered replication preserves the exact intraday "
                "rule grid while replacing an underpowered 120-session index-"
                "ETF corpus with 1,000 development sessions on a fixed, "
                "disjoint country-ETF universe."
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
            "calendar_path": _repo_path(CALENDAR_PATH),
            "historical_data_contract": {
                "minute_provider": "alpaca",
                "minute_feed": "sip",
                "minute_adjustment": "raw",
                "session": "09:30-16:00 America/New_York",
                "provider_substitutions_allowed": False,
            },
            "partitions": {
                "rolling_origin": True,
                "confirmation_untouched": True,
                "predecessor_corpora_disjoint": True,
            },
            "implementation_files": [
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
    base = _adverse_predecessor(
        enforce_commit=enforce_commit
    )["base"]
    expected_provider = {
        "minute_provider": "alpaca",
        "minute_feed": "sip",
        "minute_adjustment": "raw",
        "session": "09:30-16:00 America/New_York",
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
        and contract.get("selection_mode") == "development_search"
        and len(contract.get("trial_family", [])) == 32
        and contract.get("parameter_grid") == base["parameter_grid"]
        and contract.get("universe", {}).get("symbols") == SYMBOLS
        and contract.get("development_dates") == development
        and contract.get("embargo_dates")
        == selected[embargo_start:embargo_start + EMBARGO_SESSIONS]
        and contract.get("confirmation_dates")
        == selected[-CONFIRMATION_SESSIONS:]
        and contract.get("existing_successor_validator")
        == {
            "module": "country_etf_opening_reversal",
            "function": "validate_existing_successor_contract",
        }
        and contract.get("historical_data_contract") == expected_provider
        and contract.get("calendar_path") == _repo_path(CALENDAR_PATH)
        and isinstance(
            contract.get("outcome_exposure_index_sha256"), str
        )
        and len(contract["outcome_exposure_index_sha256"]) == 64
    ):
        raise CountryEtfOpeningReversalError(
            "country-ETF opening-reversal contract drifted"
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
        CountryEtfOpeningReversalError,
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
