"""Freeze a liquid-index-ETF long-history opening-reversal replication."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import dense_data_collection as collection
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
FAMILY_ID = runtime.LIQUID_INDEX_ETF_OPENING_REVERSAL_FAMILY
MECHANISM_FAMILY = "intraday-index-etf-opening-reversal"
STRATEGY_ID = "liquid-index-etf-opening-reversal"
SUCCESSOR_ID = "liquid-index-etf-opening-reversal-v1-long-history"
POST2016_FAMILY_ID = (
    runtime.LIQUID_INDEX_ETF_OPENING_REVERSAL_POST2016_FAMILY
)
POST2016_SUCCESSOR_ID = "liquid-index-etf-opening-reversal-v2-post2016"
RESEARCH_GENERATION = "existing_family_successor"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
CALENDAR_PATH = calendar_support.CALENDAR_PATH
SYMBOLS = ["DIA", "IWM", "QQQ", "SPY"]
DEVELOPMENT_WARMUP_SESSIONS = 60
BASE_CONTRACT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "country-etf-opening-reversal-v2-symbol-range/family-contract/"
    "contract-cd3389cc86bc8a944439a751124f3a072f1afd061b2383d0922f6ae611256f8f.json"
)
COUNTRY_FAILURE = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/country-etf-opening-reversal/"
    "development-collection-failure/"
    "country-etf-opening-reversal-development-collection-failure-"
    "7f91f4bcb837c01527cd01bf3edf2948fa84ad0f849b8a4e02f51fa763f2e730.json"
)
COUNTRY_FAILURE_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/country-etf-opening-reversal/"
    "development-collection-failure-inspection/"
    "country-etf-opening-reversal-development-collection-failure-inspection-"
    "b556d260b88efce62a27c4466f9a5ba3b572f142f3fd7ec595c0da64e13946c8.json"
)
V1_CONTRACT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "liquid-index-etf-opening-reversal-v1-long-history/family-contract/"
    "contract-d01b09773624db2cdf0b621ceb32b5e518ab5fe1f18b87bbf7c38605b51f943f.json"
)
V1_FAILURE = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-index-etf-opening-reversal-long-history/"
    "development-collection-failure/"
    "liquid-index-etf-opening-reversal-long-history-development-collection-"
    "failure-4a24c73d171b912d96f6caa6c9e3c990ad645b749322b843afcb6bd7b4da1c7d.json"
)
V1_FAILURE_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-index-etf-opening-reversal-long-history/"
    "development-collection-failure-inspection/"
    "liquid-index-etf-opening-reversal-long-history-development-collection-"
    "failure-inspection-b19536b9f958065100d4a61c81e2d291adde050432c90afb0874fdb9175e12c3.json"
)


class LiquidIndexEtfOpeningReversalError(ValueError):
    """The liquid-index replication or evidence graph drifted."""


def _repo_path(path: Path) -> str:
    return artifact_support._repo_path(path)


def _read_plain(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LiquidIndexEtfOpeningReversalError(
            f"frozen predecessor cannot be loaded: {path.name}"
        ) from exc
    if not isinstance(value, dict):
        raise LiquidIndexEtfOpeningReversalError(
            f"frozen predecessor is malformed: {path.name}"
        )
    return value


def _predecessors(
    *, enforce_commit: bool
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    paths = (BASE_CONTRACT, COUNTRY_FAILURE, COUNTRY_FAILURE_INSPECTION)
    if enforce_commit:
        for path in paths:
            strategy_discovery.require_committed(path)
    base = _read_plain(BASE_CONTRACT)
    failure = strategy_discovery.load_artifact(
        COUNTRY_FAILURE,
        expected_kind="dense-data-collection-failure",
    )
    inspection = strategy_discovery.load_artifact(
        COUNTRY_FAILURE_INSPECTION,
        expected_kind="dense-data-collection-failure-inspection",
    )
    if not (
        base.get("family_id")
        == runtime.COUNTRY_ETF_OPENING_REVERSAL_FAMILY
        and failure.get("failure_code")
        == "INCOMPLETE_SIP_RANGE_REGULAR_SESSION"
        and failure.get("strategy_metrics_accessed") is False
        and failure.get("confirmation_outcomes_accessed") is False
        and inspection.get("failure_sha256") == failure["artifact_sha256"]
        and inspection.get("state") == "COLLECTION_FAILURE_INSPECTED"
        and all(inspection.get("checks", {}).values())
    ):
        raise LiquidIndexEtfOpeningReversalError(
            "opening-reversal adverse evidence graph drifted"
        )
    return base, failure, inspection


def _partitions() -> tuple[list[str], ...]:
    sessions = calendar_support._full_sessions()
    early = [
        day
        for day in sessions
        if "2014-01-02" <= day <= "2016-03-07"
    ]
    if len(early) != 543:
        raise LiquidIndexEtfOpeningReversalError(
            "early clean session inventory drifted"
        )
    warmup = early[:DEVELOPMENT_WARMUP_SESSIONS]
    development = early[DEVELOPMENT_WARMUP_SESSIONS:]
    positions = {day: index for index, day in enumerate(sessions)}
    development_end = positions[development[-1]]
    embargo = sessions[development_end + 1 : development_end + 6]
    confirmation = [
        day
        for day in sessions
        if "2020-12-24" <= day <= "2022-03-29"
    ]
    if len(confirmation) != 315:
        raise LiquidIndexEtfOpeningReversalError(
            "confirmation session inventory drifted"
        )
    confirmation_start = positions[confirmation[0]]
    confirmation_warmup = sessions[
        confirmation_start
        - DEVELOPMENT_WARMUP_SESSIONS : confirmation_start
    ]
    if not (
        len(development) == 483
        and len(embargo) == 5
        and len(confirmation_warmup) == DEVELOPMENT_WARMUP_SESSIONS
        and max(development) < min(embargo) < min(confirmation)
    ):
        raise LiquidIndexEtfOpeningReversalError(
            "opening-reversal evidence partitions drifted"
        )
    return warmup, development, embargo, confirmation_warmup, confirmation


def _scope(dates: list[str]) -> dict[str, Any]:
    return {"dates": list(dates), "symbols": list(SYMBOLS)}


def _post2016_partitions() -> tuple[list[str], ...]:
    sessions = calendar_support._full_sessions()
    development_end = sessions.index("2020-12-23")
    development = sessions[development_end - 999 : development_end + 1]
    warmup = sessions[
        development_end - 1059 : development_end - 999
    ]
    embargo = sessions[development_end + 1 : development_end + 6]
    confirmation = [
        day
        for day in sessions
        if embargo[-1] < day <= "2022-03-29"
    ]
    confirmation_start = sessions.index(confirmation[0])
    confirmation_warmup = sessions[
        confirmation_start
        - DEVELOPMENT_WARMUP_SESSIONS : confirmation_start
    ]
    if not (
        len(warmup) == DEVELOPMENT_WARMUP_SESSIONS
        and len(development) == 1_000
        and development[0] == "2016-12-21"
        and development[-1] == "2020-12-23"
        and len(embargo) == 5
        and embargo == [
            "2020-12-28",
            "2020-12-29",
            "2020-12-30",
            "2020-12-31",
            "2021-01-04",
        ]
        and len(confirmation_warmup) == DEVELOPMENT_WARMUP_SESSIONS
        and len(confirmation) == 310
        and confirmation[0] == "2021-01-05"
        and confirmation[-1] == "2022-03-29"
        and max(development) < min(embargo) < min(confirmation)
    ):
        raise LiquidIndexEtfOpeningReversalError(
            "post-2016 opening-reversal evidence partitions drifted"
        )
    return warmup, development, embargo, confirmation_warmup, confirmation


def _post2016_predecessors(
    *, enforce_commit: bool
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    paths = (V1_CONTRACT, V1_FAILURE, V1_FAILURE_INSPECTION)
    if enforce_commit:
        for path in paths:
            strategy_discovery.require_committed(path)
    contract = _read_plain(V1_CONTRACT)
    failure = strategy_discovery.load_artifact(
        V1_FAILURE,
        expected_kind="dense-data-collection-failure",
    )
    inspection = strategy_discovery.load_artifact(
        V1_FAILURE_INSPECTION,
        expected_kind="dense-data-collection-failure-inspection",
    )
    if not (
        contract.get("family_id") == FAMILY_ID
        and failure.get("family_id") == FAMILY_ID
        and failure.get("failure_code")
        == "INCOMPLETE_SIP_RANGE_REGULAR_SESSION"
        and failure.get("strategy_metrics_accessed") is False
        and failure.get("confirmation_outcomes_accessed") is False
        and inspection.get("failure_sha256")
        == failure["artifact_sha256"]
        and inspection.get("state") == "COLLECTION_FAILURE_INSPECTED"
        and all(inspection.get("checks", {}).values())
    ):
        raise LiquidIndexEtfOpeningReversalError(
            "pre-2016 provider-capacity failure graph drifted"
        )
    return contract, failure, inspection


def _covered_pairs(
    scope: Mapping[str, Any],
    records: list[dict[str, Any]],
) -> set[tuple[str, str]]:
    proposed = outcome_exposure.scope_pairs(scope)
    proposed_symbols = {
        symbol for _day, symbol in proposed
    }
    covered: set[tuple[str, str]] = set()
    for record in records:
        for day, symbol in outcome_exposure.scope_pairs(record["scope"]):
            if symbol == "*":
                covered.update(
                    (day, candidate)
                    for candidate in proposed_symbols
                    if (day, candidate) in proposed
                )
            elif (day, symbol) in proposed:
                covered.add((day, symbol))
    return covered


def _validate_post2016_exposure_state(
    contract: Mapping[str, Any],
) -> None:
    records = outcome_exposure.read_index()
    proposed = outcome_exposure.scope_pairs(contract["development_scope"])
    covered = _covered_pairs(contract["development_scope"], records)
    if covered != proposed:
        raise LiquidIndexEtfOpeningReversalError(
            "post-2016 development scope must be fully labeled as "
            "previously exposed training data"
        )
    outcome_exposure.assert_untouched(
        contract["confirmation_scope"], records
    )
    outcome_exposure.assert_disjoint(
        [
            contract["development_scope"],
            contract["confirmation_scope"],
        ]
    )


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
        raise LiquidIndexEtfOpeningReversalError(
            "development scope has foreign or partial outcome exposure"
        )


def freeze_contract(
    *, created_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any], Path]:
    artifact_support._timestamp(created_at, "created_at")
    base, failure, inspection = _predecessors(
        enforce_commit=enforce_commit
    )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    (
        warmup,
        development,
        embargo,
        confirmation_warmup,
        confirmation,
    ) = _partitions()
    development_scope = _scope(development)
    confirmation_scope = _scope(confirmation)
    records = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(development_scope, records)
    outcome_exposure.assert_untouched(confirmation_scope, records)
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    evidence_paths = [
        _repo_path(CALENDAR_PATH),
        _repo_path(BASE_CONTRACT),
        _repo_path(COUNTRY_FAILURE),
        _repo_path(COUNTRY_FAILURE_INSPECTION),
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
    ]
    requested_dates = [
        *warmup,
        *development,
        *embargo,
        *confirmation_warmup,
        *confirmation,
    ]
    capacity_path, _capacity = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-capacity",
            "registered_at": created_at,
            "requested_dates": requested_dates,
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

    contract = copy.deepcopy(base)
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
            "parent_experiment_id": base["experiment_id"],
            "created_at": created_at,
            "status": "INVENTED",
            "research_generation": RESEARCH_GENERATION,
            "successor_id": SUCCESSOR_ID,
            "existing_successor_validator": {
                "module": "liquid_index_etf_opening_reversal",
                "function": "validate_existing_successor_contract",
            },
            "new_mechanism_family_slot_consumed": False,
            "prior_family_attempt_count": 2,
            "predecessor": {
                "contract_path": _repo_path(BASE_CONTRACT),
                "contract_sha256": hashlib.sha256(
                    artifact_support._canonical(base)
                ).hexdigest(),
                "failure_path": _repo_path(COUNTRY_FAILURE),
                "failure_sha256": failure["artifact_sha256"],
                "failure_inspection_path": _repo_path(
                    COUNTRY_FAILURE_INSPECTION
                ),
                "failure_inspection_sha256": inspection[
                    "artifact_sha256"
                ],
                "promotion_evidence_reused": False,
                "strategy_metrics_reused": False,
            },
            "mechanism": (
                "Replicate the frozen downside opening-z and completed VWAP "
                "reclaim reversal on four highly liquid index ETFs using "
                "long, globally untouched development and confirmation blocks."
            ),
            "universe_requirements": {
                "symbols": list(SYMBOLS),
                "data": "complete SIP regular-session minute bars",
                "selection_basis": (
                    "The four most liquid broad U.S. index ETFs are fixed "
                    "before price access; an incomplete symbol-session makes "
                    "the entire fixed-universe entry date a missed trade."
                ),
            },
            "universe": {
                "symbols": list(SYMBOLS),
                "data": "complete SIP regular-session minute bars",
            },
            "contamination_risks": [
                "The rejected 120-session predecessor cannot count toward this replication.",
                "The structurally incomplete country-ETF corpus contributes no strategy metric.",
                "Development and confirmation target pairs are globally untouched at freeze.",
                "Incomplete sessions are missed entry dates with zero interpolation or substitution.",
                "The complete 32-trial rule grid is unchanged from the adverse predecessor.",
            ],
            "material_difference_rationale": (
                "This preregistered replication addresses the predecessor's "
                "11-fill power weakness and the country universe's structural "
                "minute gaps by using 483 clean development sessions and 315 "
                "separate confirmation sessions on four highly liquid ETFs."
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
                "minute_request_mode": "symbol_range",
                "minute_missing_session_policy": (
                    collection.INTRADAY_FIXED_UNIVERSE_MISS_POLICY
                ),
                "session": "09:30-16:00 America/New_York",
                "provider_substitutions_allowed": False,
            },
            "partitions": {
                "rolling_origin": True,
                "confirmation_untouched": True,
                "predecessor_corpora_disjoint": True,
            },
            "implementation_files": [
                "liquid_index_etf_opening_reversal.py",
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


def freeze_post2016_contract(
    *, created_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any], Path]:
    """Freeze provider-compatible training with a disjoint untouched reserve."""

    artifact_support._timestamp(created_at, "created_at")
    base, failure, inspection = _post2016_predecessors(
        enforce_commit=enforce_commit
    )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    (
        warmup,
        development,
        embargo,
        confirmation_warmup,
        confirmation,
    ) = _post2016_partitions()
    development_scope = _scope(development)
    confirmation_scope = _scope(confirmation)
    records = outcome_exposure.read_index()
    proposed = outcome_exposure.scope_pairs(development_scope)
    covered = _covered_pairs(development_scope, records)
    if covered != proposed:
        raise LiquidIndexEtfOpeningReversalError(
            "post-2016 development scope is not fully covered by prior "
            "outcome exposure"
        )
    outcome_exposure.assert_untouched(confirmation_scope, records)
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    evidence_paths = [
        _repo_path(CALENDAR_PATH),
        _repo_path(V1_CONTRACT),
        _repo_path(V1_FAILURE),
        _repo_path(V1_FAILURE_INSPECTION),
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
    ]
    requested_dates = sorted(
        {
            *warmup,
            *development,
            *embargo,
            *confirmation_warmup,
            *confirmation,
        }
    )
    capacity_path, _capacity = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": (
                f"dataset-{POST2016_SUCCESSOR_ID}-capacity"
            ),
            "registered_at": created_at,
            "requested_dates": requested_dates,
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": evidence_paths,
                "inspected": True,
                "point_in_time_evidence": True,
                "dense_capacity": {
                    "family_id": POST2016_FAMILY_ID,
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
                    "development_scope_previously_exposed": True,
                    "previously_exposed_development_pairs": len(covered),
                    "confirmation_scope_untouched": True,
                },
            },
        },
        (
            DEFAULT_ROOT
            / POST2016_SUCCESSOR_ID
            / "capacity"
        ),
    )

    contract = copy.deepcopy(base)
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
            "experiment_id": (
                f"experiment-{POST2016_SUCCESSOR_ID}"
            ),
            "family_id": POST2016_FAMILY_ID,
            "mechanism_family": MECHANISM_FAMILY,
            "strategy_id": STRATEGY_ID,
            "parent_experiment_id": base["experiment_id"],
            "created_at": created_at,
            "status": "INVENTED",
            "research_generation": RESEARCH_GENERATION,
            "successor_id": POST2016_SUCCESSOR_ID,
            "existing_successor_validator": {
                "module": "liquid_index_etf_opening_reversal",
                "function": "validate_post2016_successor_contract",
            },
            "new_mechanism_family_slot_consumed": False,
            "prior_family_attempt_count": 3,
            "predecessor": {
                "contract_path": _repo_path(V1_CONTRACT),
                "contract_sha256": hashlib.sha256(
                    artifact_support._canonical(base)
                ).hexdigest(),
                "failure_path": _repo_path(V1_FAILURE),
                "failure_sha256": failure["artifact_sha256"],
                "failure_inspection_path": _repo_path(
                    V1_FAILURE_INSPECTION
                ),
                "failure_inspection_sha256": inspection[
                    "artifact_sha256"
                ],
                "promotion_evidence_reused": False,
                "strategy_metrics_reused": False,
            },
            "mechanism": (
                "Run the unchanged downside opening-z and completed VWAP "
                "reclaim reversal on four highly liquid index ETFs. Use the "
                "already exposed post-2016 block only for explicitly "
                "contaminated development selection and reserve a later "
                "disjoint block for untouched confirmation."
            ),
            "universe_requirements": {
                "symbols": list(SYMBOLS),
                "data": "complete SIP regular-session minute bars",
                "selection_basis": (
                    "The same four highly liquid broad U.S. index ETFs remain "
                    "fixed. An incomplete symbol-session makes the entire "
                    "fixed-universe entry date a missed trade."
                ),
            },
            "universe": {
                "symbols": list(SYMBOLS),
                "data": "complete SIP regular-session minute bars",
            },
            "development_evidence_classification": (
                "CONTAMINATED_TRAINING_ONLY"
            ),
            "development_outcomes_previously_exposed": True,
            "development_outcomes_eligible_for_confirmation": False,
            "contamination_risks": [
                "Every development pair was previously outcome-exposed and is training evidence only.",
                "No development statistic can satisfy untouched-confirmation maturity.",
                "The 310-session confirmation target block is globally untouched at freeze.",
                "The first five post-development full sessions are an immutable embargo.",
                "Incomplete sessions are missed entry dates with zero interpolation or substitution.",
                "The complete 32-trial rule grid is unchanged from the adverse predecessors.",
            ],
            "material_difference_rationale": (
                "The configured Alpaca SIP minute history begins in 2016. "
                "This exact version uses 1,000 provider-covered sessions for "
                "selection while explicitly treating them as contaminated "
                "training, then tests only a frozen later 310-session block "
                "for confirmation."
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
                "minute_request_mode": "symbol_range",
                "minute_missing_session_policy": (
                    collection.INTRADAY_FIXED_UNIVERSE_MISS_POLICY
                ),
                "session": "09:30-16:00 America/New_York",
                "provider_substitutions_allowed": False,
            },
            "partitions": {
                "rolling_origin": True,
                "confirmation_untouched": True,
                "predecessor_corpora_disjoint": False,
                "development_previously_exposed": True,
                "development_training_only": True,
                "five_session_embargo": True,
            },
            "implementation_files": [
                "liquid_index_etf_opening_reversal.py",
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
    validate_post2016_successor_contract(
        contract, enforce_commit=enforce_commit
    )
    digest = hashlib.sha256(
        artifact_support._canonical(contract)
    ).hexdigest()
    path = (
        DEFAULT_ROOT
        / POST2016_SUCCESSOR_ID
        / "family-contract"
        / f"contract-{digest}.json"
    )
    artifact_support._write_json(path, contract)
    return path, contract, capacity_path


def validate_existing_successor_contract(
    contract: Mapping[str, Any], *, enforce_commit: bool = True
) -> None:
    base, failure, inspection = _predecessors(
        enforce_commit=enforce_commit
    )
    (
        warmup,
        development,
        embargo,
        confirmation_warmup,
        confirmation,
    ) = _partitions()
    unchanged_fields = (
        "parameter_grid",
        "selection_mode",
        "selection_rule",
        "winner_selection",
        "costs_bps_per_side",
        "entry_rule",
        "exit_rule",
        "stop_rule",
        "execution_assumptions",
        "falsifiers",
        "ranking_rule",
        "primary_outcome",
        "plugin",
    )
    if not all(contract.get(field) == base.get(field) for field in unchanged_fields):
        raise LiquidIndexEtfOpeningReversalError(
            "opening-reversal strategy semantics changed"
        )
    predecessor = contract.get("predecessor")
    if not (
        contract.get("campaign_id") == CAMPAIGN_ID
        and contract.get("family_id") == FAMILY_ID
        and contract.get("mechanism_family") == MECHANISM_FAMILY
        and contract.get("successor_id") == SUCCESSOR_ID
        and contract.get("research_generation") == RESEARCH_GENERATION
        and contract.get("new_mechanism_family_slot_consumed") is False
        and contract.get("prior_family_attempt_count") == 2
        and contract.get("development_warmup_dates") == warmup
        and contract.get("development_dates") == development
        and contract.get("embargo_dates") == embargo
        and contract.get("confirmation_warmup_dates")
        == confirmation_warmup
        and contract.get("confirmation_dates") == confirmation
        and contract.get("development_scope") == _scope(development)
        and contract.get("confirmation_scope") == _scope(confirmation)
        and contract.get("universe", {}).get("symbols") == SYMBOLS
        and contract.get("historical_data_contract", {}).get(
            "minute_request_mode"
        )
        == "symbol_range"
        and contract.get("historical_data_contract", {}).get(
            "minute_missing_session_policy"
        )
        == collection.INTRADAY_FIXED_UNIVERSE_MISS_POLICY
        and isinstance(predecessor, Mapping)
        and predecessor.get("failure_sha256")
        == failure["artifact_sha256"]
        and predecessor.get("failure_inspection_sha256")
        == inspection["artifact_sha256"]
        and predecessor.get("promotion_evidence_reused") is False
        and predecessor.get("strategy_metrics_reused") is False
        and contract.get("confirmation_signal_capacity")
        == len(confirmation)
        and isinstance(contract.get("outcome_exposure_index_sha256"), str)
        and len(contract["outcome_exposure_index_sha256"]) == 64
    ):
        raise LiquidIndexEtfOpeningReversalError(
            "liquid-index opening-reversal contract drifted"
        )
    _validate_exposure_state(contract)


def validate_post2016_successor_contract(
    contract: Mapping[str, Any], *, enforce_commit: bool = True
) -> None:
    base, failure, inspection = _post2016_predecessors(
        enforce_commit=enforce_commit
    )
    (
        warmup,
        development,
        embargo,
        confirmation_warmup,
        confirmation,
    ) = _post2016_partitions()
    unchanged_fields = (
        "parameter_grid",
        "selection_mode",
        "selection_rule",
        "winner_selection",
        "costs_bps_per_side",
        "entry_rule",
        "exit_rule",
        "stop_rule",
        "execution_assumptions",
        "falsifiers",
        "ranking_rule",
        "primary_outcome",
        "plugin",
    )
    if not all(
        contract.get(field) == base.get(field)
        for field in unchanged_fields
    ):
        raise LiquidIndexEtfOpeningReversalError(
            "post-2016 opening-reversal strategy semantics changed"
        )
    predecessor = contract.get("predecessor")
    if not (
        contract.get("campaign_id") == CAMPAIGN_ID
        and contract.get("family_id") == POST2016_FAMILY_ID
        and contract.get("mechanism_family") == MECHANISM_FAMILY
        and contract.get("successor_id") == POST2016_SUCCESSOR_ID
        and contract.get("research_generation") == RESEARCH_GENERATION
        and contract.get("new_mechanism_family_slot_consumed") is False
        and contract.get("prior_family_attempt_count") == 3
        and contract.get("development_evidence_classification")
        == "CONTAMINATED_TRAINING_ONLY"
        and contract.get("development_outcomes_previously_exposed")
        is True
        and contract.get(
            "development_outcomes_eligible_for_confirmation"
        )
        is False
        and contract.get("development_warmup_dates") == warmup
        and contract.get("development_dates") == development
        and contract.get("embargo_dates") == embargo
        and contract.get("confirmation_warmup_dates")
        == confirmation_warmup
        and contract.get("confirmation_dates") == confirmation
        and contract.get("development_scope") == _scope(development)
        and contract.get("confirmation_scope") == _scope(confirmation)
        and contract.get("universe", {}).get("symbols") == SYMBOLS
        and contract.get("historical_data_contract", {}).get(
            "minute_request_mode"
        )
        == "symbol_range"
        and contract.get("historical_data_contract", {}).get(
            "minute_missing_session_policy"
        )
        == collection.INTRADAY_FIXED_UNIVERSE_MISS_POLICY
        and contract.get("partitions", {}).get(
            "development_previously_exposed"
        )
        is True
        and contract.get("partitions", {}).get(
            "development_training_only"
        )
        is True
        and isinstance(predecessor, Mapping)
        and predecessor.get("failure_sha256")
        == failure["artifact_sha256"]
        and predecessor.get("failure_inspection_sha256")
        == inspection["artifact_sha256"]
        and predecessor.get("promotion_evidence_reused") is False
        and predecessor.get("strategy_metrics_reused") is False
        and contract.get("confirmation_signal_capacity")
        == len(confirmation)
        and isinstance(
            contract.get("outcome_exposure_index_sha256"), str
        )
        and len(contract["outcome_exposure_index_sha256"]) == 64
    ):
        raise LiquidIndexEtfOpeningReversalError(
            "post-2016 liquid-index opening-reversal contract drifted"
        )
    _validate_post2016_exposure_state(contract)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("freeze", "freeze-post2016")
    )
    parser.add_argument("--created-at", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "freeze":
            path, contract, capacity = freeze_contract(
                created_at=args.created_at
            )
        else:
            path, contract, capacity = freeze_post2016_contract(
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
        LiquidIndexEtfOpeningReversalError,
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
