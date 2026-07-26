"""Freeze, collect, and inspect the 2018-2019 SEC metadata reserve.

This controller is metadata-only.  It fixes the already-inspected v12 archive
root typo, preserves the same as-filed event semantics, and never reads market
prices, forward returns, or broker state.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import time
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import requests

import earnings_sec_corrected_expansion as corrected
import earnings_sec_expansion_collection as expansion
import earnings_sec_market_data as market
import outcome_exposure
import sec_broad_pead
import strategy_discovery
from historical_discovery import SecConfig
from historical_store import HistoricalDayStore, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = sec_broad_pead.CAMPAIGN_ID
FAMILY_ID = sec_broad_pead.FAMILY_ID
SUCCESSOR_ID = (
    "sec-yoy-eps-improvement-broad-drift-temporal-confirmation-metadata-v1"
)
DEFAULT_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/continuous" / SUCCESSOR_ID
)
PRIVATE_NAMESPACE = "_derived/sec_broad_pead_temporal_metadata"
CURRENT_REJECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery"
    / FAMILY_ID
    / "development-inspection"
    / (
        f"{FAMILY_ID}-development-inspection-"
        "0a02505e68a88dfdde79f6d3bbbbb26fe525dea99a4f08e78c8c6667c5850bde"
        ".json"
    )
)
ARCHIVE_ROOT = corrected.ARCHIVE_ROOT
RESERVE_START = "2018-01-08"
RESERVE_END = "2019-12-31"
RESERVE_SETTLEMENT_END = date(2020, 1, 15)
MINIMUM_SIGNAL_DATES = 20
MINIMUM_EVENTS = 50
MINIMUM_SPACING_SECONDS = 0.20
EXACT_PARAMETERS = {
    "maximum_hold_sessions": 5,
    "minimum_opening_gap_fraction": -0.02,
    "minimum_yoy_eps_change_ratio": 0.0,
    "security_trend_gate": "price>SMA200",
    "stop_atr14": 1.5,
}
ARCHIVES = tuple(
    (f"{year}q{quarter}", f"{year}q{quarter}_notes.zip")
    for year in range(2018, 2020)
    for quarter in range(1, 5)
)


class SecBroadPeadTemporalMetadataError(RuntimeError):
    """The exact metadata reserve or its authorization drifted."""


def _repo_path(path: Path) -> str:
    return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))


def _read(path: Path) -> dict[str, Any]:
    return corrected._read(path)


def _write(path: Path, value: Mapping[str, Any]) -> None:
    corrected._write(path, value)


def _timestamp(value: str, field: str) -> str:
    return corrected._timestamp(value, field)


def _self_hash(value: Mapping[str, Any], field: str) -> str:
    return corrected.self_hash(value, field)


def request_graph() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ordinal, (quarter, filename) in enumerate(ARCHIVES):
        row: dict[str, Any] = {
            "ordinal": ordinal,
            "quarter": quarter,
            "method": "GET",
            "url": f"{ARCHIVE_ROOT}/{filename}",
            "filename": filename,
        }
        row["request_sha256"] = hashlib.sha256(
            corrected.canonical_bytes(row)
        ).hexdigest()
        rows.append(row)
    return rows


def _rejection_lineage() -> dict[str, Any]:
    strategy_discovery.require_committed(CURRENT_REJECTION)
    value = strategy_discovery.load_artifact(
        CURRENT_REJECTION,
        expected_kind="development-search-inspection",
    )
    classifications = value.get("selection", {}).get(
        "trial_classifications", []
    )
    exact = next(
        (
            row
            for row in classifications
            if row.get("trial_id") == "trial-fd3c1a04ec4ffb49"
        ),
        None,
    )
    if not (
        value.get("state") == "REJECTED"
        and value.get("selection", {}).get("status") == "REJECTED"
        and len(classifications) == 16
        and isinstance(exact, Mapping)
        and exact.get("rebuilt_metrics", {}).get(
            "stress_20bps_total_log_growth"
        )
        == 0.04590529360374653
        and exact.get("rebuilt_metrics", {}).get(
            "stress_20bps_profit_factor"
        )
        == 1.4877476127123104
        and exact.get("rebuilt_metrics", {}).get(
            "stress_20bps_bootstrap_lower_mean_account_return"
        )
        == 9.183967813913968e-05
    ):
        raise SecBroadPeadTemporalMetadataError(
            "near-survivor rejection lineage drifted"
        )
    return {
        "inspection_path": _repo_path(CURRENT_REJECTION),
        "inspection_file_sha256": sha256_file(CURRENT_REJECTION),
        "inspection_sha256": value["artifact_sha256"],
        "state": "REJECTED",
        "selected_trial_id": "trial-fd3c1a04ec4ffb49",
        "exact_parameters": EXACT_PARAMETERS,
        "parameter_alternatives": 0,
    }


def price_scope_dates() -> list[str]:
    calendar = market._sessions(date(2017, 1, 3), RESERVE_SETTLEMENT_END)
    first = calendar.index(RESERVE_START)
    if first < 205:
        raise SecBroadPeadTemporalMetadataError(
            "reserve lacks frozen SMA200 warmup"
        )
    return calendar[first - 205 :]


def build_contract(
    *,
    created_at: str,
    enforce_commit: bool = True,
) -> dict[str, Any]:
    if enforce_commit:
        for path in (
            Path(__file__).resolve(),
            PROJECT_ROOT / "earnings_sec_corrected_expansion.py",
            PROJECT_ROOT / "earnings_sec_expansion_collection.py",
            PROJECT_ROOT / "earnings_sec_legacy_capacity.py",
            PROJECT_ROOT / "earnings_sec_cover_identity.py",
        ):
            strategy_discovery.require_committed(path)
    failure = corrected._v12_failure_lineage()
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "sec-broad-pead-temporal-metadata-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "TEMPORAL_METADATA_CONTRACT_FROZEN",
        "created_at": _timestamp(created_at, "created_at"),
        "source_failure_lineage": failure,
        "development_rejection_lineage": _rejection_lineage(),
        "purpose": (
            "establish a point-in-time 2018-2019 SEC metadata denominator "
            "for a separately sealed historical confirmation reserve"
        ),
        "provider": "U.S. SEC Financial Statement and Notes Data Sets",
        "archive_root": ARCHIVE_ROOT,
        "requests": request_graph(),
        "authorized_metadata_requests": len(ARCHIVES),
        "request_policy": {
            "exact_archives_only": True,
            "minimum_spacing_seconds": MINIMUM_SPACING_SECONDS,
            "timeout_seconds": 180.0,
            "retries_permitted": 0,
            "substitutions_permitted": 0,
            "hash_valid_cache_resume_permitted": True,
            "market_price_requests_permitted": 0,
        },
        "event_semantics": {
            "identical_to_inspected_corrected_source": True,
            "forms": ["10-Q"],
            "amendments_excluded": True,
            "as_filed_acceptance_timestamp_required": True,
            "same_accession_trading_symbol_required": True,
            "same_accession_common_stock_shares_cover_fact_required": True,
            "external_or_current_ticker_mapping_permitted": False,
            "positive_yoy_eps_change_required": True,
            "maximum_events_per_accepted_date": 3,
            "duplicate_event_keys_receive_zero_credit": True,
            "event_rank": [
                "descending EPS change ratio",
                "descending EPS absolute change",
                "canonical ticker",
                "accession",
            ],
        },
        "reserve": {
            "accepted_start": RESERVE_START,
            "accepted_end": RESERVE_END,
            "price_scope_dates": price_scope_dates(),
            "minimum_events": MINIMUM_EVENTS,
            "minimum_signal_dates": MINIMUM_SIGNAL_DATES,
            "development_symbol_disjoint_required": True,
            "global_outcome_exposure_filter_required": True,
            "exposed_event_or_symbol_substitution_permitted": False,
        },
        "exact_strategy": {
            "selected_trial_id": "trial-fd3c1a04ec4ffb49",
            "parameters": EXACT_PARAMETERS,
            "parameter_alternatives": 0,
            "maximum_hold_sessions": 5,
        },
        "implementation_hashes": {
            "sec_broad_pead_temporal_metadata.py": sha256_file(
                Path(__file__).resolve()
            ),
            "earnings_sec_corrected_expansion.py": sha256_file(
                PROJECT_ROOT / "earnings_sec_corrected_expansion.py"
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
    value["contract_sha256"] = _self_hash(value, "contract_sha256")
    return value


def freeze_contract(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    value = build_contract(created_at=created_at)
    path = (
        root
        / "metadata-contract"
        / f"contract-{value['contract_sha256']}.json"
    )
    _write(path, value)
    return path, value


def inspect_contract(
    contract_path: Path,
    *,
    inspected_at: str,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(contract_path)
    contract = _read(contract_path)
    rebuilt = build_contract(created_at=str(contract["created_at"]))
    checks = {
        "contract_hash_valid": contract.get("contract_sha256")
        == _self_hash(contract, "contract_sha256"),
        "contract_rebuilt_exactly": contract == rebuilt,
        "corrected_official_root": contract.get("archive_root")
        == corrected.ARCHIVE_ROOT
        and "-and-" not in str(contract.get("archive_root")),
        "exact_eight_archives": contract.get("requests")
        == request_graph()
        and contract.get("authorized_metadata_requests") == 8,
        "exact_rule_no_alternatives": contract.get(
            "exact_strategy", {}
        ).get("parameters")
        == EXACT_PARAMETERS
        and contract.get("exact_strategy", {}).get(
            "parameter_alternatives"
        )
        == 0,
        "reserve_preregistered": contract.get("reserve", {}).get(
            "accepted_start"
        )
        == RESERVE_START
        and contract.get("reserve", {}).get("accepted_end") == RESERVE_END
        and len(contract.get("reserve", {}).get("price_scope_dates", []))
        > 500,
        "zero_retry_or_substitution": contract.get(
            "request_policy", {}
        ).get("retries_permitted")
        == 0
        and contract.get("request_policy", {}).get(
            "substitutions_permitted"
        )
        == 0,
        "zero_price_outcome_or_broker": contract.get(
            "market_prices_accessed"
        )
        is False
        and contract.get("forward_returns_accessed") is False
        and contract.get("strategy_metrics_computed") == 0
        and contract.get("confirmation_outcomes_accessed") is False
        and contract.get("broker_actions") == 0,
    }
    if not all(checks.values()):
        raise SecBroadPeadTemporalMetadataError(
            "temporal metadata contract inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "sec-broad-pead-temporal-metadata-contract-inspection"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "TEMPORAL_METADATA_CONTRACT_INSPECTED_READY",
        "inspected_at": _timestamp(inspected_at, "inspected_at"),
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "checks": checks,
        "metadata_collection_authorized": True,
        "authorized_provider_requests": len(ARCHIVES),
        "market_price_access_authorized": False,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = _self_hash(
        value, "inspection_sha256"
    )
    path = (
        root
        / "metadata-contract-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    _write(path, value)
    return path, value


def _archive_path(
    store: HistoricalDayStore,
    contract_sha256: str,
    request_sha256: str,
) -> Path:
    return (
        store.root
        / PRIVATE_NAMESPACE
        / contract_sha256
        / "archives"
        / f"{request_sha256}.zip"
    )


def collect(
    contract_path: Path,
    inspection_path: Path,
    *,
    collected_at: str,
    store: HistoricalDayStore | None = None,
    session: requests.Session | None = None,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = _read(contract_path)
    inspection = _read(inspection_path)
    if not (
        contract == build_contract(created_at=str(contract["created_at"]))
        and inspection.get("inspection_sha256")
        == _self_hash(inspection, "inspection_sha256")
        and inspection.get("contract_sha256")
        == contract["contract_sha256"]
        and inspection.get("state")
        == "TEMPORAL_METADATA_CONTRACT_INSPECTED_READY"
        and inspection.get("valid") is True
        and inspection.get("metadata_collection_authorized") is True
        and inspection.get("market_price_access_authorized") is False
    ):
        raise SecBroadPeadTemporalMetadataError(
            "metadata provider access is not authorized"
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
        {
            "User-Agent": config.user_agent,
            "Accept-Encoding": "gzip, deflate",
        }
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
        for ordinal, request in enumerate(contract["requests"]):
            destination = _archive_path(
                historical_store,
                contract["contract_sha256"],
                request["request_sha256"],
            )
            info = expansion._archive_info(
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
                    info, elapsed = expansion._download(
                        request,
                        destination=destination,
                        store=historical_store,
                        session=http,
                        timeout_seconds=float(
                            contract["request_policy"][
                                "timeout_seconds"
                            ]
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
    if telemetry["requests"] + telemetry["cache_hits"] != len(ARCHIVES):
        raise SecBroadPeadTemporalMetadataError(
            "metadata request accounting is incomplete"
        )
    events, archive_counts, summary = expansion._rank_and_cover(paths)
    private: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "private-sec-broad-pead-temporal-metadata",
        "contract_sha256": contract["contract_sha256"],
        "events": events,
        "archive_counts": archive_counts,
        "derivation_summary": summary,
    }
    private["content_sha256"] = _self_hash(private, "content_sha256")
    relative = (
        Path(PRIVATE_NAMESPACE)
        / contract["contract_sha256"]
        / "metadata.json.gz"
    )
    private_path = historical_store.root / relative
    market._write_private(private_path, private)
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "sec-broad-pead-temporal-metadata-collection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "TEMPORAL_METADATA_COLLECTED_UNINSPECTED",
        "collected_at": _timestamp(collected_at, "collected_at"),
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "inspection_path": _repo_path(inspection_path),
        "inspection_file_sha256": sha256_file(inspection_path),
        "inspection_sha256": inspection["inspection_sha256"],
        "archives": archives,
        "provider_telemetry": telemetry,
        "derivation_summary": summary,
        "private_artifact": {
            "cache_relative_path": str(relative),
            "file_sha256": sha256_file(private_path),
            "content_sha256": private["content_sha256"],
        },
        "retries": 0,
        "substitutions": 0,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    value["collection_sha256"] = _self_hash(
        value, "collection_sha256"
    )
    path = (
        root
        / "metadata-collection"
        / f"collection-{value['collection_sha256']}.json"
    )
    _write(path, value)
    return path, value


def _read_private(
    collection: Mapping[str, Any],
    store: HistoricalDayStore,
) -> dict[str, Any]:
    info = collection["private_artifact"]
    path = store.root / str(info["cache_relative_path"])
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != info["file_sha256"]:
        raise SecBroadPeadTemporalMetadataError(
            "private temporal metadata bytes drifted"
        )
    value = json.loads(gzip.decompress(raw))
    if not (
        isinstance(value, dict)
        and value.get("content_sha256")
        == _self_hash(value, "content_sha256")
        == info["content_sha256"]
    ):
        raise SecBroadPeadTemporalMetadataError(
            "private temporal metadata content drifted"
        )
    return value


def _exposed_symbols(
    *,
    dates: Sequence[str],
    symbols: Sequence[str],
) -> list[str]:
    date_set = set(dates)
    symbol_set = set(symbols)
    exposed: set[str] = set()
    for raw in outcome_exposure.read_index():
        record = outcome_exposure.validate_record(raw)
        scope = record["scope"]
        for day in date_set.intersection(scope["dates"]):
            scoped = (
                set(scope["symbols_by_date"][day])
                if "symbols_by_date" in scope
                else set(scope["symbols"])
            )
            exposed.update(
                symbol_set if "*" in scoped else symbol_set.intersection(scoped)
            )
    return sorted(exposed)


def inspect_collection(
    collection_path: Path,
    *,
    inspected_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(collection_path)
    collection = _read(collection_path)
    contract_path = PROJECT_ROOT / collection["contract_path"]
    inspection_path = PROJECT_ROOT / collection["inspection_path"]
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = _read(contract_path)
    authorization = _read(inspection_path)
    historical_store = store or HistoricalDayStore.from_env()
    private = _read_private(collection, historical_store)
    archive_paths: list[Path] = []
    archives_valid = True
    for request, info in zip(
        contract["requests"], collection["archives"], strict=True
    ):
        path = historical_store.root / info["cache_relative_path"]
        if not (
            info["request_sha256"] == request["request_sha256"]
            and path.is_file()
            and sha256_file(path) == info["file_sha256"]
        ):
            archives_valid = False
        archive_paths.append(path)
    events, archive_counts, summary = expansion._rank_and_cover(
        archive_paths
    )
    sessions = market._sessions(date(2017, 1, 3), RESERVE_SETTLEMENT_END)
    raw_events: list[dict[str, Any]] = []
    for row in events:
        accepted_date = str(row["accepted"])[:10]
        if not RESERVE_START <= accepted_date <= RESERVE_END:
            continue
        reaction = market._reaction_date(str(row["accepted"]), sessions)
        if reaction is not None:
            raw_events.append({**row, "reaction_date": reaction})
    development_symbols = set(
        sec_broad_pead.selection()["confirmation_symbols"]
    )
    candidate_symbols = sorted(
        {
            str(row["ticker"])
            for row in raw_events
            if str(row["ticker"]) not in development_symbols
        }
    )
    exposed = _exposed_symbols(
        dates=contract["reserve"]["price_scope_dates"],
        symbols=candidate_symbols,
    )
    admitted = sorted(
        [
            row
            for row in raw_events
            if str(row["ticker"]) not in development_symbols
            and str(row["ticker"]) not in set(exposed)
        ],
        key=lambda row: (
            str(row["reaction_date"]),
            int(row["accepted_date_rank"]),
            str(row["ticker"]),
            str(row["adsh"]),
        ),
    )
    signal_dates = sorted(
        {str(row["reaction_date"]) for row in admitted}
    )
    symbols = sorted({str(row["ticker"]) for row in admitted})
    selection: dict[str, Any] = {
        "schema_version": 1,
        "successor_id": SUCCESSOR_ID,
        "accepted_start": RESERVE_START,
        "accepted_end": RESERVE_END,
        "price_scope_dates": contract["reserve"]["price_scope_dates"],
        "events": admitted,
        "signal_dates": signal_dates,
        "symbols": symbols,
        "development_symbols_excluded": sorted(development_symbols),
        "globally_exposed_symbols_excluded": exposed,
        "market_prices_accessed": False,
        "confirmation_outcomes_accessed": False,
    }
    selection["content_sha256"] = _self_hash(
        selection, "content_sha256"
    )
    relative = (
        Path(PRIVATE_NAMESPACE)
        / contract["contract_sha256"]
        / "inspected-selection.json.gz"
    )
    selection_path = historical_store.root / relative
    market._write_private(selection_path, selection)
    capacity_ready = (
        len(admitted) >= MINIMUM_EVENTS
        and len(signal_dates) >= MINIMUM_SIGNAL_DATES
    )
    checks = {
        "collection_hash_valid": collection.get("collection_sha256")
        == _self_hash(collection, "collection_sha256"),
        "contract_chain_valid": collection.get("contract_sha256")
        == contract.get("contract_sha256")
        == authorization.get("contract_sha256"),
        "contract_inspected": authorization.get("state")
        == "TEMPORAL_METADATA_CONTRACT_INSPECTED_READY"
        and authorization.get("valid") is True,
        "archives_valid": archives_valid
        and len(archive_paths) == len(ARCHIVES),
        "request_accounting_complete": collection.get(
            "provider_telemetry", {}
        ).get("requests", 0)
        + collection.get("provider_telemetry", {}).get("cache_hits", 0)
        == len(ARCHIVES),
        "derivation_rebuilt": private.get("events") == events
        and private.get("archive_counts") == archive_counts
        and private.get("derivation_summary") == summary,
        "development_symbol_disjoint": not development_symbols.intersection(
            symbols
        ),
        "zero_retry_or_substitution": collection.get("retries") == 0
        and collection.get("substitutions") == 0,
        "zero_price_outcome_or_broker": collection.get(
            "market_prices_accessed"
        )
        is False
        and collection.get("forward_returns_accessed") is False
        and collection.get("strategy_metrics_computed") == 0
        and collection.get("confirmation_outcomes_accessed") is False
        and collection.get("broker_actions") == 0,
    }
    if not all(checks.values()):
        raise SecBroadPeadTemporalMetadataError(
            "temporal metadata collection inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "sec-broad-pead-temporal-metadata-capacity-inspection"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "state": (
            "TEMPORAL_CONFIRMATION_METADATA_CAPACITY_READY"
            if capacity_ready
            else "INSUFFICIENT_TEMPORAL_CONFIRMATION_METADATA_CAPACITY"
        ),
        "inspected_at": _timestamp(inspected_at, "inspected_at"),
        "collection_path": _repo_path(collection_path),
        "collection_file_sha256": sha256_file(collection_path),
        "collection_sha256": collection["collection_sha256"],
        "checks": checks,
        "events_before_filters": len(raw_events),
        "events": len(admitted),
        "signal_dates": len(signal_dates),
        "symbols": len(symbols),
        "development_symbols_excluded": len(development_symbols),
        "globally_exposed_symbols_excluded": exposed,
        "selection": {
            "cache_relative_path": str(relative),
            "file_sha256": sha256_file(selection_path),
            "content_sha256": selection["content_sha256"],
        },
        "minimum_events": MINIMUM_EVENTS,
        "minimum_signal_dates": MINIMUM_SIGNAL_DATES,
        "market_price_access_authorized": False,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = _self_hash(
        value, "inspection_sha256"
    )
    path = (
        root
        / "metadata-collection-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    _write(path, value)
    return path, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze-contract")
    freeze.add_argument("--created-at", required=True)
    inspect = subparsers.add_parser("inspect-contract")
    inspect.add_argument("contract", type=Path)
    inspect.add_argument("--inspected-at", required=True)
    collect_parser = subparsers.add_parser("collect")
    collect_parser.add_argument("contract", type=Path)
    collect_parser.add_argument("inspection", type=Path)
    collect_parser.add_argument("--collected-at", required=True)
    inspect_collection_parser = subparsers.add_parser(
        "inspect-collection"
    )
    inspect_collection_parser.add_argument("collection", type=Path)
    inspect_collection_parser.add_argument("--inspected-at", required=True)
    args = parser.parse_args(argv)
    if args.command == "freeze-contract":
        path, value = freeze_contract(created_at=args.created_at)
        state = value["state"]
        provider_requests = 0
    elif args.command == "inspect-contract":
        path, value = inspect_contract(
            args.contract, inspected_at=args.inspected_at
        )
        state = value["state"]
        provider_requests = 0
    elif args.command == "collect":
        path, value = collect(
            args.contract,
            args.inspection,
            collected_at=args.collected_at,
        )
        state = value["state"]
        provider_requests = value["provider_telemetry"]["requests"]
    else:
        path, value = inspect_collection(
            args.collection, inspected_at=args.inspected_at
        )
        state = value["state"]
        provider_requests = 0
    print(
        json.dumps(
            {
                "path": _repo_path(path),
                "state": state,
                "provider_requests": provider_requests,
                "market_prices_accessed": False,
                "confirmation_outcomes_accessed": False,
                "broker_actions": 0,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
