"""Freeze and collect the corrected-source 2012-2015 SEC PEAD inventory.

The v12 source root failed before retaining any archive or metadata row.  This
successor binds that inspected failure, corrects only the official archive
root, and reduces the metadata-only scope outcome-blind for collection cost.
Market prices and confirmation outcomes remain forbidden until independent
capacity inspection.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import requests

import earnings_sec_expansion_capacity as v12_capacity
import earnings_sec_expansion_collection as v12_collection
import outcome_exposure
import strategy_discovery
from historical_discovery import SecConfig
from historical_store import HistoricalDayStore, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = v12_capacity.CAMPAIGN_ID
FAMILY_ID = v12_capacity.FAMILY_ID
SUCCESSOR_ID = (
    "earnings-positive-surprise-drift-v13-sec-2012-2015-corrected-source"
)
DEFAULT_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/continuous" / SUCCESSOR_ID
)
V12_FAILURE_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "earnings-positive-surprise-drift-v12-sec-2012-2019-expansion/"
    "metadata-source-failure-inspection/"
    "inspection-"
    "7cb930f1d80a8b68ceb185d32dea527b332bba47feb2fc376b4293930b9fca48"
    ".json"
)
ARCHIVE_ROOT = (
    "https://www.sec.gov/files/dera/data/"
    "financial-statement-notes-data-sets"
)
DATASET_PAGE = v12_capacity.DATASET_PAGE
DOCUMENTATION_URL = v12_capacity.DOCUMENTATION_URL
DEVELOPMENT_START = "2012-01-01"
DEVELOPMENT_END = "2014-12-19"
EMBARGO_START = "2014-12-20"
EMBARGO_END = "2015-01-11"
CONFIRMATION_START = "2015-01-12"
CONFIRMATION_END = "2015-12-31"
MINIMUM_UNIQUE_EVENTS = 100
MINIMUM_DEVELOPMENT_DATES = 50
MINIMUM_CONFIRMATION_DATES = 20
MINIMUM_SPACING_SECONDS = 0.20
PRIOR_EVALUATED_TRIALS = 32
FUTURE_SEARCH_TRIAL_CAP = 32
PRIVATE_NAMESPACE = "_derived/earnings_sec_corrected_expansion"
ARCHIVES = tuple(
    (f"{year}q{quarter}", f"{year}q{quarter}_notes.zip")
    for year in range(2012, 2016)
    for quarter in range(1, 5)
)


class EarningsSecCorrectedExpansionError(RuntimeError):
    """The corrected-source SEC PEAD boundary drifted."""


def canonical_bytes(value: Any) -> bytes:
    return v12_capacity.canonical_bytes(value)


def self_hash(value: Mapping[str, Any], field: str) -> str:
    return v12_capacity.self_hash(value, field)


def _read(path: Path) -> dict[str, Any]:
    return v12_capacity._read(path)


def _write(path: Path, value: Mapping[str, Any]) -> None:
    v12_capacity._write(path, value)


def _timestamp(value: str, name: str) -> str:
    return v12_capacity._timestamp(value, name)


def _repo_path(path: Path) -> str:
    return v12_capacity._repo_path(path)


def requests_graph() -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for ordinal, (quarter, filename) in enumerate(ARCHIVES):
        row: dict[str, Any] = {
            "ordinal": ordinal,
            "quarter": quarter,
            "method": "GET",
            "url": f"{ARCHIVE_ROOT}/{filename}",
            "filename": filename,
        }
        row["request_sha256"] = hashlib.sha256(
            canonical_bytes(row)
        ).hexdigest()
        result.append(row)
    return result


def _v12_failure_lineage() -> dict[str, Any]:
    strategy_discovery.require_committed(V12_FAILURE_INSPECTION)
    value = _read(V12_FAILURE_INSPECTION)
    if not (
        value.get("inspection_sha256")
        == self_hash(value, "inspection_sha256")
        and value.get("state")
        == "SEC_EXPANSION_ARCHIVE_ROOT_404_INSPECTED_TERMINAL"
        and value.get("valid") is True
        and value.get("v12_resume_permitted") is False
        and value.get("corrected_source_successor_permitted") is True
        and value.get("market_price_access_authorized") is False
    ):
        raise EarningsSecCorrectedExpansionError(
            "v12 terminal source-failure lineage is invalid"
        )
    return {
        "inspection_path": _repo_path(V12_FAILURE_INSPECTION),
        "inspection_file_sha256": sha256_file(V12_FAILURE_INSPECTION),
        "inspection_sha256": value["inspection_sha256"],
        "state": value["state"],
        "v12_resume_permitted": False,
        "corrected_source_successor_permitted": True,
    }


def build_contract(*, created_at: str) -> dict[str, Any]:
    inspector = PROJECT_ROOT / "earnings_sec_corrected_expansion_inspection.py"
    for path in (
        Path(__file__).resolve(),
        inspector,
        PROJECT_ROOT / "earnings_sec_expansion_capacity.py",
        PROJECT_ROOT / "earnings_sec_expansion_collection.py",
        PROJECT_ROOT / "earnings_sec_legacy_capacity.py",
        PROJECT_ROOT / "earnings_sec_cover_identity.py",
    ):
        strategy_discovery.require_committed(path)
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-sec-corrected-expansion-capacity-contract"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "SEC_CORRECTED_EXPANSION_CAPACITY_CONTRACT_FROZEN",
        "created_at": _timestamp(created_at, "created_at"),
        "predecessor": _v12_failure_lineage(),
        "rolling_authority": v12_capacity.rolling_authority(),
        "provider": "U.S. SEC Financial Statement and Notes Data Sets",
        "dataset_page": DATASET_PAGE,
        "documentation_url": DOCUMENTATION_URL,
        "archive_root": ARCHIVE_ROOT,
        "requests": requests_graph(),
        "authorized_metadata_requests": len(ARCHIVES),
        "scope_reduction": {
            "original_archive_count": 32,
            "successor_archive_count": len(ARCHIVES),
            "basis": (
                "outcome-blind collection-cost reduction authorized by the "
                "inspected zero-row v12 source failure"
            ),
            "prices_or_returns_used": False,
            "same_archive_filename_graph_prefix": True,
        },
        "request_policy": {
            "exact_archives_only": True,
            "minimum_spacing_seconds": MINIMUM_SPACING_SECONDS,
            "retries_permitted": 0,
            "substitutions_permitted": 0,
            "interrupted_collection_may_reuse_hash_valid_zip": True,
            "unexpected_archive_members_fail_closed": True,
            "market_price_requests_permitted": 0,
        },
        "event_semantics": {
            "identical_to_v12": True,
            "forms": ["10-Q"],
            "amendments_excluded": True,
            "as_filed_acceptance_timestamp_required": True,
            "same_accession_trading_symbol_required": True,
            "same_accession_common_stock_shares_cover_fact_required": True,
            "external_or_current_ticker_mapping_permitted": False,
            "eps_tags_priority": [
                "EarningsPerShareDiluted",
                "EarningsPerShareBasicAndDiluted",
                "EarningsPerShareBasic",
            ],
            "quarter_duration": 1,
            "consolidated_nondimensional_only": True,
            "current_period_matches_submission_period": True,
            "prior_comparison_days": [300, 430],
            "positive_yoy_eps_change_required": True,
            "maximum_events_per_accepted_date": 3,
            "event_rank": [
                "descending EPS change ratio",
                "descending EPS absolute change",
                "canonical ticker",
                "accession",
            ],
            "duplicate_accession_or_event_key_receives_zero_credit": True,
            "event_key": ["accepted", "ticker", "adsh"],
            "reaction_session": (
                "first fully observable regular session after SEC acceptance"
            ),
            "entry_observation_boundary": (
                "next regular-session open after completed reaction session"
            ),
        },
        "partitions": {
            "development": [DEVELOPMENT_START, DEVELOPMENT_END],
            "embargo": [EMBARGO_START, EMBARGO_END],
            "confirmation": [CONFIRMATION_START, CONFIRMATION_END],
            "maximum_hold_sessions": 5,
            "five_complete_session_embargo_required": True,
            "development_and_confirmation_filtered_against_global_exposure": True,
            "confirmation_market_outcomes_remain_untouched": True,
        },
        "capacity_thresholds": {
            "minimum_unique_events": MINIMUM_UNIQUE_EVENTS,
            "minimum_development_event_dates": MINIMUM_DEVELOPMENT_DATES,
            "minimum_confirmation_event_dates": MINIMUM_CONFIRMATION_DATES,
            "required_total_signals_formula": "max(50, frozen_power_target)",
            "required_confirmation_signals_formula": (
                "max(20, ceil(required_total_signals * 0.30))"
            ),
        },
        "selection_accounting": {
            "prior_evaluated_trials_same_mechanism": PRIOR_EVALUATED_TRIALS,
            "future_search_trial_cap": FUTURE_SEARCH_TRIAL_CAP,
            "cumulative_trial_count_if_full_search": (
                PRIOR_EVALUATED_TRIALS + FUTURE_SEARCH_TRIAL_CAP
            ),
            "prior_trials_must_enter_deflated_sharpe_correction": True,
            "prior_trials_must_enter_family_overfitting_accounting": True,
            "confirmation_cannot_influence_trial_selection": True,
        },
        "implementation_hashes": {
            "earnings_sec_corrected_expansion.py": sha256_file(
                Path(__file__).resolve()
            ),
            "earnings_sec_corrected_expansion_inspection.py": sha256_file(
                inspector
            ),
            "earnings_sec_expansion_capacity.py": sha256_file(
                PROJECT_ROOT / "earnings_sec_expansion_capacity.py"
            ),
            "earnings_sec_expansion_collection.py": sha256_file(
                PROJECT_ROOT / "earnings_sec_expansion_collection.py"
            ),
            "earnings_sec_legacy_capacity.py": sha256_file(
                PROJECT_ROOT / "earnings_sec_legacy_capacity.py"
            ),
            "earnings_sec_cover_identity.py": sha256_file(
                PROJECT_ROOT / "earnings_sec_cover_identity.py"
            ),
            "outcome_exposure.py": sha256_file(
                PROJECT_ROOT / "outcome_exposure.py"
            ),
            "strategy_discovery.py": sha256_file(
                PROJECT_ROOT / "strategy_discovery.py"
            ),
        },
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "provider_requests_executed": 0,
        "metadata_rows_accessed": 0,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    value["contract_sha256"] = self_hash(value, "contract_sha256")
    return value


def freeze_contract(
    *, created_at: str, root: Path = DEFAULT_ROOT
) -> tuple[Path, dict[str, Any]]:
    value = build_contract(created_at=created_at)
    path = (
        root
        / "metadata-contract"
        / f"contract-{value['contract_sha256']}.json"
    )
    _write(path, value)
    return path, value


def _contract_lineage(
    contract_path: Path, inspection_path: Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = _read(contract_path)
    inspection = _read(inspection_path)
    rebuilt = build_contract(created_at=str(contract["created_at"]))
    if not (
        contract == rebuilt
        and contract.get("contract_sha256")
        == self_hash(contract, "contract_sha256")
        and inspection.get("inspection_sha256")
        == self_hash(inspection, "inspection_sha256")
        and inspection.get("contract_sha256") == contract["contract_sha256"]
        and inspection.get("state")
        == "SEC_CORRECTED_EXPANSION_CAPACITY_CONTRACT_INSPECTED_READY"
        and inspection.get("valid") is True
        and inspection.get("metadata_collection_authorized") is True
        and inspection.get("market_price_access_authorized") is False
    ):
        raise EarningsSecCorrectedExpansionError(
            "corrected expansion contract lineage is invalid"
        )
    return contract, inspection


def build_plan(
    contract_path: Path,
    contract_inspection_path: Path,
    *,
    created_at: str,
) -> dict[str, Any]:
    for path in (
        Path(__file__).resolve(),
        PROJECT_ROOT / "earnings_sec_corrected_expansion_inspection.py",
    ):
        strategy_discovery.require_committed(path)
    contract, inspection = _contract_lineage(
        contract_path, contract_inspection_path
    )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-sec-corrected-expansion-metadata-collection-plan"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "SEC_CORRECTED_EXPANSION_COLLECTION_PLAN_FROZEN",
        "created_at": _timestamp(created_at, "created_at"),
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_path": _repo_path(contract_inspection_path),
        "contract_inspection_file_sha256": sha256_file(
            contract_inspection_path
        ),
        "contract_inspection_sha256": inspection["inspection_sha256"],
        "requests": contract["requests"],
        "request_count": len(contract["requests"]),
        "transport": {
            "provider": contract["provider"],
            "minimum_spacing_seconds": MINIMUM_SPACING_SECONDS,
            "timeout_seconds": 180.0,
            "retries_permitted": 0,
            "substitutions_permitted": 0,
            "hash_valid_cache_resume_permitted": True,
        },
        "derivation": {
            "parser": (
                "legacy as-filed TradingSymbol and quarterly EPS facts, "
                "then same-accession common-stock-shares cover"
            ),
            "maximum_events_per_accepted_date": 3,
            "duplicate_event_keys_receive_zero_credit": True,
            "rank": contract["event_semantics"]["event_rank"],
            "partitions": contract["partitions"],
            "global_outcome_exposure_filter_required": True,
            "exposed_ranked_event_substitution_permitted": False,
        },
        "private_namespace": PRIVATE_NAMESPACE,
        "implementation_hashes": contract["implementation_hashes"],
        "provider_requests_executed": 0,
        "metadata_rows_accessed": 0,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    value["plan_sha256"] = self_hash(value, "plan_sha256")
    return value


def freeze_plan(
    contract_path: Path,
    contract_inspection_path: Path,
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    value = build_plan(
        contract_path,
        contract_inspection_path,
        created_at=created_at,
    )
    path = (
        root
        / "metadata-collection-plan"
        / f"plan-{value['plan_sha256']}.json"
    )
    _write(path, value)
    return path, value


def _archive_path(
    store: HistoricalDayStore, plan_sha256: str, request_sha256: str
) -> Path:
    return (
        store.root
        / PRIVATE_NAMESPACE
        / plan_sha256
        / "archives"
        / f"{request_sha256}.zip"
    )


def collect(
    plan_path: Path,
    inspection_path: Path,
    *,
    collected_at: str,
    store: HistoricalDayStore | None = None,
    session: requests.Session | None = None,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(plan_path)
    strategy_discovery.require_committed(inspection_path)
    plan = _read(plan_path)
    plan_inspection = _read(inspection_path)
    contract_path = PROJECT_ROOT / str(plan["contract_path"])
    contract_inspection_path = PROJECT_ROOT / str(
        plan["contract_inspection_path"]
    )
    rebuilt = build_plan(
        contract_path,
        contract_inspection_path,
        created_at=str(plan["created_at"]),
    )
    if not (
        plan == rebuilt
        and plan.get("plan_sha256") == self_hash(plan, "plan_sha256")
        and plan_inspection.get("inspection_sha256")
        == self_hash(plan_inspection, "inspection_sha256")
        and plan_inspection.get("plan_sha256") == plan["plan_sha256"]
        and plan_inspection.get("state")
        == "SEC_CORRECTED_EXPANSION_COLLECTION_PLAN_INSPECTED_READY"
        and plan_inspection.get("valid") is True
        and plan_inspection.get("metadata_collection_authorized") is True
    ):
        raise EarningsSecCorrectedExpansionError(
            "committed corrected expansion collection plan is invalid"
        )
    historical_store = store or HistoricalDayStore.from_env()
    config = SecConfig.from_env(
        PROJECT_ROOT / ".env",
        historical_store.root / PRIVATE_NAMESPACE,
        workers=1,
    )
    own_session = session is None
    http = session or requests.Session()
    http.headers.update(
        {"User-Agent": config.user_agent, "Accept-Encoding": "gzip, deflate"}
    )
    archives: list[dict[str, Any]] = []
    paths: list[Path] = []
    telemetry: dict[str, Any] = {
        "requests": 0,
        "cache_hits": 0,
        "failures": 0,
        "request_seconds": 0.0,
        "pacing_wait_seconds": 0.0,
    }
    try:
        for ordinal, request in enumerate(plan["requests"]):
            destination = _archive_path(
                historical_store,
                plan["plan_sha256"],
                request["request_sha256"],
            )
            info = v12_collection._archive_info(
                destination, request, historical_store
            )
            if info is not None:
                telemetry["cache_hits"] += 1
            else:
                if ordinal and telemetry["requests"]:
                    time.sleep(MINIMUM_SPACING_SECONDS)
                    telemetry["pacing_wait_seconds"] += (
                        MINIMUM_SPACING_SECONDS
                    )
                try:
                    info, elapsed = v12_collection._download(
                        request,
                        destination=destination,
                        store=historical_store,
                        session=http,
                        timeout_seconds=float(
                            plan["transport"]["timeout_seconds"]
                        ),
                    )
                except Exception:
                    telemetry["failures"] += 1
                    raise
                telemetry["requests"] += 1
                telemetry["request_seconds"] += elapsed
            archives.append(info)
            paths.append(destination)
    finally:
        if own_session:
            http.close()
    if telemetry["requests"] + telemetry["cache_hits"] != len(
        plan["requests"]
    ):
        raise EarningsSecCorrectedExpansionError(
            "corrected expansion request accounting is incomplete"
        )
    events, archive_counts, summary = v12_collection._rank_and_cover(paths)
    private: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "private-earnings-sec-corrected-expansion-capacity"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "plan_sha256": plan["plan_sha256"],
        "archives": archives,
        "archive_counts": archive_counts,
        "events": events,
        "derivation_summary": summary,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    private["content_sha256"] = self_hash(private, "content_sha256")
    raw = gzip.compress(canonical_bytes(private), mtime=0)
    relative = (
        Path(PRIVATE_NAMESPACE)
        / plan["plan_sha256"]
        / "events"
        / f"{private['content_sha256']}.json.gz"
    )
    private_path = historical_store.root / relative
    private_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = private_path.with_name(
        f".{private_path.name}.{os.getpid()}.tmp"
    )
    try:
        temporary.write_bytes(raw)
        os.replace(temporary, private_path)
    finally:
        temporary.unlink(missing_ok=True)
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-sec-corrected-expansion-metadata-collection"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "SEC_CORRECTED_EXPANSION_METADATA_COLLECTED_UNINSPECTED",
        "collected_at": _timestamp(collected_at, "collected_at"),
        "plan_path": _repo_path(plan_path),
        "plan_file_sha256": sha256_file(plan_path),
        "plan_sha256": plan["plan_sha256"],
        "inspection_path": _repo_path(inspection_path),
        "inspection_file_sha256": sha256_file(inspection_path),
        "inspection_sha256": plan_inspection["inspection_sha256"],
        "archive_count": len(archives),
        "archives": archives,
        "derivation_summary": summary,
        "private_artifact": {
            "cache_relative_path": str(relative),
            "content_sha256": private["content_sha256"],
            "file_sha256": hashlib.sha256(raw).hexdigest(),
            "compressed_bytes": len(raw),
        },
        "provider_telemetry": telemetry,
        "retries": 0,
        "substitutions": 0,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    value["collection_sha256"] = self_hash(value, "collection_sha256")
    output = (
        root
        / "metadata-collection"
        / f"collection-{value['collection_sha256']}.json"
    )
    _write(output, value)
    return output, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    contract = subparsers.add_parser("freeze-contract")
    contract.add_argument("--created-at", required=True)
    plan = subparsers.add_parser("freeze-plan")
    plan.add_argument("contract", type=Path)
    plan.add_argument("contract_inspection", type=Path)
    plan.add_argument("--created-at", required=True)
    collect_parser = subparsers.add_parser("collect")
    collect_parser.add_argument("plan", type=Path)
    collect_parser.add_argument("inspection", type=Path)
    collect_parser.add_argument("--collected-at", required=True)
    args = parser.parse_args(argv)
    if args.command == "freeze-contract":
        path, value = freeze_contract(created_at=args.created_at)
        digest = value["contract_sha256"]
        provider_requests = 0
    elif args.command == "freeze-plan":
        path, value = freeze_plan(
            args.contract,
            args.contract_inspection,
            created_at=args.created_at,
        )
        digest = value["plan_sha256"]
        provider_requests = 0
    else:
        path, value = collect(
            args.plan,
            args.inspection,
            collected_at=args.collected_at,
        )
        digest = value["collection_sha256"]
        provider_requests = value["provider_telemetry"]["requests"]
    print(
        json.dumps(
            {
                "path": _repo_path(path),
                "sha256": digest,
                "state": value["state"],
                "provider_requests": provider_requests,
                "market_prices_accessed": False,
                "broker_actions": 0,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
