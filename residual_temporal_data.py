"""Freeze and collect the combined residual-reversal v7 development dataset."""

from __future__ import annotations

import argparse
import gzip
import json
import time
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time as wall_time, timedelta, timezone
from pathlib import Path
from typing import Any

import outcome_exposure
import residual_replication_data as v6
import residual_temporal_reference as reference
import strategy_discovery
from historical_store import (
    DEFAULT_ENV_PATH,
    EASTERN,
    HistoricalStoreConfig,
    canonical_sha256,
    sha256_file,
)
from scanner_replay import (
    MassiveReferenceCollector,
    MassiveReferenceConfig,
    ScannerReplayError,
)
from scanner_replay_alpaca import AlpacaBulkBarsClient, AlpacaBulkConfig


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = reference.CAMPAIGN_ID
FAMILY_ID = reference.FAMILY_ID
MECHANISM_FAMILY = reference.MECHANISM_FAMILY
SUCCESSOR_ID = reference.SUCCESSOR_ID
ROOT = reference.ROOT
CONTRACT_KIND = "residual-temporal-data-contract"
CONTRACT_STATE = "DATA_CONTRACT_FROZEN"
CONTRACT_INSPECTION_KIND = "residual-temporal-data-contract-inspection"
CONTRACT_INSPECTION_STATE = "DATA_CONTRACT_INSPECTED"
COLLECTION_KIND = "residual-temporal-development-collection"
COLLECTION_STATE = "DEVELOPMENT_COLLECTED_UNINSPECTED"
COLLECTION_INSPECTION_KIND = "residual-temporal-development-inspection"
COLLECTION_INSPECTION_STATE = "DEVELOPMENT_DATA_INSPECTED"
DEVELOPMENT_WARMUP_SESSIONS = 200
CONTAMINATED_EXPOSURE_ID = (
    "development-liquid-equity-market-residual-reversal-replication-"
    "d4c9fb3b7ea04864"
)


class ResidualTemporalDataError(RuntimeError):
    """The combined v7 data boundary or provider result is unsafe."""


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ResidualTemporalDataError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise ResidualTemporalDataError(f"{field} needs a timezone")
    return parsed.astimezone(timezone.utc)


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise ResidualTemporalDataError(
            f"path escaped repository: {path}"
        ) from exc


def _read_gzip(path: Path) -> Any:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            return json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise ResidualTemporalDataError(f"cannot read {path}: {exc}") from exc


def _combined_calendar() -> list[str]:
    early = reference._calendar()
    later = v6._calendar()
    combined = sorted(set(early) | set(later))
    if (
        combined != sorted(combined)
        or len(combined) != len(set(combined))
        or max(early) >= min(later)
    ):
        raise ResidualTemporalDataError(
            "combined exchange calendar is not a clean chronology"
        )
    return combined


def _load_reference_inspection(
    path: Path, *, enforce_commit: bool
) -> tuple[dict[str, Any], dict[str, dict[str, str]]]:
    if enforce_commit:
        strategy_discovery.require_committed(path)
    inspection = strategy_discovery.load_artifact(
        path, expected_kind="residual-temporal-reference-inspection"
    )
    checks = inspection.get("checks")
    if not (
        inspection.get("state") == "REFERENCE_IDENTITIES_INSPECTED"
        and isinstance(checks, Mapping)
        and checks
        and all(checks.values())
        and inspection.get("market_prices_accessed") is False
        and inspection.get("strategy_outcomes_accessed") is False
        and inspection.get("broker_actions") == 0
    ):
        raise ResidualTemporalDataError(
            "reference identities are not independently inspected"
        )
    store = HistoricalStoreConfig.from_env(DEFAULT_ENV_PATH)
    identity_path = (
        store.root / str(inspection["identity_external_relative_path"])
    ).resolve()
    if (
        store.root.resolve() not in identity_path.parents
        or not identity_path.is_file()
        or sha256_file(identity_path)
        != inspection["identity_external_file_sha256"]
    ):
        raise ResidualTemporalDataError("reference identity graph drifted")
    identities = _read_gzip(identity_path)
    if (
        not isinstance(identities, dict)
        or canonical_sha256(identities)
        != inspection["identity_graph_sha256"]
    ):
        raise ResidualTemporalDataError(
            "reference identity graph content drifted"
        )
    normalized = {
        str(day): {
            str(symbol): str(identity)
            for symbol, identity in values.items()
        }
        for day, values in identities.items()
        if isinstance(values, Mapping)
    }
    if set(normalized) != set(reference.selected_dates()):
        raise ResidualTemporalDataError(
            "reference identity dates are incomplete"
        )
    return inspection, normalized


def _v6_development_identities() -> tuple[
    dict[str, Any], dict[str, dict[str, str]]
]:
    contract = strategy_discovery.load_artifact(
        reference.V6_CONTRACT,
        expected_kind="residual-replication-data-contract",
    )
    source_dates, identities, _bindings = v6._source_graph()
    if canonical_sha256(source_dates) != contract["source_dates_sha256"]:
        raise ResidualTemporalDataError("v6 source identity graph drifted")
    selected = {
        day: identities[day]
        for day in contract["development_signal_dates"]
    }
    if len(selected) != 200:
        raise ResidualTemporalDataError(
            "v6 contaminated development sample is incomplete"
        )
    return contract, selected


def _contaminated_record(
    records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    matches = [
        outcome_exposure.validate_record(record)
        for record in records
        if record.get("exposure_id") == CONTAMINATED_EXPOSURE_ID
    ]
    if len(matches) != 1:
        raise ResidualTemporalDataError(
            "v6 contaminated exposure record is unavailable"
        )
    return matches[0]


def _development_scopes(
    early_identities: Mapping[str, Mapping[str, str]],
    later_identities: Mapping[str, Mapping[str, str]],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    early_dates = sorted(early_identities)
    later_dates = sorted(later_identities)
    fresh_scope = {
        "dates": early_dates,
        "symbols_by_date": {
            day: sorted(early_identities[day]) for day in early_dates
        },
    }
    exact_scope = {
        "dates": early_dates + later_dates,
        "symbols_by_date": {
            **fresh_scope["symbols_by_date"],
            **{day: ["*"] for day in later_dates},
        },
    }
    exposure_scope = {
        "dates": early_dates + later_dates,
        "symbols": ["*"],
    }
    return fresh_scope, exact_scope, exposure_scope


def _partitions(
    early_dates: Sequence[str], v6_contract: Mapping[str, Any]
) -> tuple[list[str], list[str], list[str], list[str], list[str]]:
    calendar = _combined_calendar()
    positions = {day: index for index, day in enumerate(calendar)}
    early = list(early_dates)
    later = list(v6_contract["development_signal_dates"])
    if (
        len(early) != 200
        or len(later) != 200
        or max(early) >= min(later)
        or min(early) not in positions
        or positions[min(early)] < DEVELOPMENT_WARMUP_SESSIONS
    ):
        raise ResidualTemporalDataError(
            "combined development signal chronology is invalid"
        )
    development_signals = early + later
    start_index = positions[min(early)] - DEVELOPMENT_WARMUP_SESSIONS
    final_index = positions[str(v6_contract["development_dates"][-1])]
    development_dates = calendar[start_index : final_index + 1]
    embargo = list(v6_contract["embargo_dates"])
    confirmation_dates = list(v6_contract["confirmation_dates"])
    confirmation_signals = list(v6_contract["confirmation_signal_dates"])
    if not (
        max(development_dates) < min(embargo) < min(confirmation_dates)
        and len(embargo) == 5
        and len(confirmation_signals) == 93
    ):
        raise ResidualTemporalDataError(
            "preserved confirmation chronology drifted"
        )
    return (
        development_dates,
        development_signals,
        embargo,
        confirmation_dates,
        confirmation_signals,
    )


def freeze_contract(
    reference_inspection_path: Path,
    *,
    created_at: str,
    root: Path = ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    _timestamp(created_at, "created_at")
    reference_inspection, early_identities = _load_reference_inspection(
        reference_inspection_path, enforce_commit=enforce_commit
    )
    v6_contract, later_identities = _v6_development_identities()
    (
        development_dates,
        development_signals,
        embargo,
        confirmation_dates,
        confirmation_signals,
    ) = _partitions(sorted(early_identities), v6_contract)
    records = outcome_exposure.read_index()
    contaminated = _contaminated_record(records)
    if contaminated["scope"] != v6_contract["development_scope"]:
        raise ResidualTemporalDataError(
            "v6 contaminated exposure scope drifted"
        )
    fresh_scope, exact_development_scope, development_scope = (
        _development_scopes(early_identities, later_identities)
    )
    if outcome_exposure.find_overlaps(fresh_scope, records):
        raise ResidualTemporalDataError(
            "new early development scope is no longer untouched"
        )
    confirmation_scope = dict(v6_contract["confirmation_scope"])
    outcome_exposure.assert_untouched(confirmation_scope, records)
    all_identities = {**early_identities, **later_identities}
    if set(all_identities) != set(development_signals):
        raise ResidualTemporalDataError(
            "combined identity graph does not cover every signal date"
        )
    symbols = sorted(
        {
            "SPY",
            *{
                symbol
                for values in all_identities.values()
                for symbol in values
            },
        }
    )
    implementation_files = (
        "residual_temporal_data.py",
        "residual_temporal_data_inspection.py",
        "residual_temporal_discovery.py",
        "residual_temporal_plugin.py",
        "residual_replication_plugin.py",
        "dense_strategy_runtime.py",
        "strategy_discovery.py",
        "learning_statistics.py",
    )
    payload = {
        "schema_version": 1,
        "artifact_kind": CONTRACT_KIND,
        "state": CONTRACT_STATE,
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "successor_id": SUCCESSOR_ID,
        "created_at": created_at,
        "reference_inspection_path": _repo_path(reference_inspection_path),
        "reference_inspection_file_sha256": sha256_file(
            reference_inspection_path
        ),
        "reference_inspection_sha256": reference_inspection[
            "artifact_sha256"
        ],
        "reference_identity_graph_sha256": reference_inspection[
            "identity_graph_sha256"
        ],
        "v6_contract_path": _repo_path(reference.V6_CONTRACT),
        "v6_contract_file_sha256": sha256_file(reference.V6_CONTRACT),
        "v6_contract_sha256": v6_contract["artifact_sha256"],
        "contaminated_training_exposure": contaminated,
        "contaminated_training_signal_count": len(later_identities),
        "fresh_development_scope_sha256": canonical_sha256(fresh_scope),
        "fresh_development_identity_pairs": sum(
            len(symbols_by_date)
            for symbols_by_date in fresh_scope["symbols_by_date"].values()
        ),
        "fresh_development_signal_count": len(early_identities),
        "development_dates": development_dates,
        "development_signal_dates": development_signals,
        "embargo_dates": embargo,
        "confirmation_dates": confirmation_dates,
        "confirmation_signal_dates": confirmation_signals,
        "development_scope": development_scope,
        "exact_development_scope_sha256": canonical_sha256(
            exact_development_scope
        ),
        "confirmation_scope": confirmation_scope,
        "development_symbol_count": len(symbols),
        "development_symbols_sha256": canonical_sha256(symbols),
        "identity_graph_sha256": canonical_sha256(all_identities),
        "calendar_contract": {
            "early_path": _repo_path(reference.CALENDAR_PATH),
            "early_sha256": sha256_file(reference.CALENDAR_PATH),
            "later_path": _repo_path(v6.CALENDAR_PATH),
            "later_sha256": sha256_file(v6.CALENDAR_PATH),
            "warmup_sessions": DEVELOPMENT_WARMUP_SESSIONS,
        },
        "collection_contract": {
            "price_provider": "Alpaca historical SIP",
            "price_endpoint": "https://data.alpaca.markets/v2/stocks/bars",
            "feed": "sip",
            "adjustment": "raw",
            "timeframe": "1Day",
            "development_start_inclusive": development_dates[0],
            "development_end_inclusive": development_dates[-1],
            "split_provider": "Massive",
            "split_endpoint": "https://api.massive.com/stocks/v1/splits",
            "split_start_inclusive": development_dates[0],
            "split_end_inclusive": development_dates[-1],
            "confirmation_prices_locked": True,
            "provider_substitution_allowed": False,
            "interpolation_allowed": False,
            "resumable_symbol_batches": True,
        },
        "universe_contract": dict(v6_contract["universe_contract"]),
        "implementation_hashes": {
            name: sha256_file(PROJECT_ROOT / name)
            for name in implementation_files
            if (PROJECT_ROOT / name).is_file()
        },
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "market_prices_accessed_before_freeze": False,
        "confirmation_prices_accessed": False,
        "strategy_metrics_accessed_before_freeze": False,
        "rules_or_parameter_grid_changed": False,
        "substitutions": 0,
        "broker_actions": 0,
    }
    return strategy_discovery._write_artifact(
        payload,
        root / "data-contract",
        "residual-temporal-data-contract",
    )


def load_contract(
    path: Path, *, enforce_commit: bool = True
) -> dict[str, Any]:
    if enforce_commit:
        strategy_discovery.require_committed(path)
    contract = strategy_discovery.load_artifact(
        path, expected_kind=CONTRACT_KIND
    )
    if not (
        contract.get("state") == CONTRACT_STATE
        and contract.get("market_prices_accessed_before_freeze") is False
        and contract.get("confirmation_prices_accessed") is False
        and contract.get("strategy_metrics_accessed_before_freeze") is False
        and contract.get("rules_or_parameter_grid_changed") is False
        and contract.get("substitutions") == 0
        and contract.get("broker_actions") == 0
    ):
        raise ResidualTemporalDataError(
            "development data contract is not collection-ready"
        )
    for name, expected in contract.get("implementation_hashes", {}).items():
        implementation = PROJECT_ROOT / str(name)
        if (
            not implementation.is_file()
            or sha256_file(implementation) != expected
        ):
            raise ResidualTemporalDataError(
                f"frozen development implementation drifted: {name}"
            )
        if enforce_commit:
            strategy_discovery.require_committed(implementation)
    fresh_scope, exact_scope, exposure_scope = _rebuild_development_scopes(
        contract
    )
    if not (
        canonical_sha256(fresh_scope)
        == contract.get("fresh_development_scope_sha256")
        and canonical_sha256(exact_scope)
        == contract.get("exact_development_scope_sha256")
        and exposure_scope == contract.get("development_scope")
    ):
        raise ResidualTemporalDataError(
            "development exposure scopes drifted"
        )
    records = outcome_exposure.read_index()
    if outcome_exposure.find_overlaps(fresh_scope, records):
        raise ResidualTemporalDataError(
            "fresh early outcomes were exposed before collection"
        )
    outcome_exposure.assert_untouched(
        contract["confirmation_scope"], records
    )
    return contract


def _load_all_identities(
    contract: Mapping[str, Any],
) -> dict[str, dict[str, str]]:
    reference_path = PROJECT_ROOT / str(
        contract["reference_inspection_path"]
    )
    inspection, early = _load_reference_inspection(
        reference_path, enforce_commit=True
    )
    v6_contract, later = _v6_development_identities()
    combined = {**early, **later}
    if (
        inspection["artifact_sha256"]
        != contract["reference_inspection_sha256"]
        or v6_contract["artifact_sha256"] != contract["v6_contract_sha256"]
        or canonical_sha256(combined) != contract["identity_graph_sha256"]
    ):
        raise ResidualTemporalDataError(
            "combined development identities drifted"
        )
    return combined


def _rebuild_development_scopes(
    contract: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    combined = _load_all_identities(contract)
    early_dates = set(reference.selected_dates())
    later_dates = set(contract["development_signal_dates"]) - early_dates
    early = {
        day: combined[day]
        for day in sorted(early_dates)
    }
    later = {
        day: combined[day]
        for day in sorted(later_dates)
    }
    return _development_scopes(early, later)


def _load_contract_inspection(
    path: Path, *, enforce_commit: bool
) -> dict[str, Any]:
    if enforce_commit:
        strategy_discovery.require_committed(path)
    inspection = strategy_discovery.load_artifact(
        path, expected_kind=CONTRACT_INSPECTION_KIND
    )
    contract_path = PROJECT_ROOT / str(inspection.get("contract_path", ""))
    contract = load_contract(contract_path, enforce_commit=enforce_commit)
    checks = inspection.get("checks")
    if not (
        inspection.get("state") == CONTRACT_INSPECTION_STATE
        and inspection.get("contract_sha256") == contract["artifact_sha256"]
        and isinstance(checks, Mapping)
        and checks
        and all(checks.values())
    ):
        raise ResidualTemporalDataError(
            "development data contract inspection is incomplete"
        )
    return contract


def _collect_splits(
    contract: Mapping[str, Any],
    *,
    private_root: Path,
    store: HistoricalStoreConfig,
) -> tuple[dict[str, list[str]], dict[str, Any]]:
    output = private_root / "split-actions.json.gz"
    if output.is_file():
        rows = _read_gzip(output)
        disposition = "cached"
        requests = 0
        elapsed = 0.0
    else:
        config = MassiveReferenceConfig.from_env(DEFAULT_ENV_PATH)
        started = time.monotonic()
        with MassiveReferenceCollector(config) as collector:
            rows = collector.fetch_splits(
                contract["collection_contract"]["split_start_inclusive"],
                contract["collection_contract"]["split_end_inclusive"],
            )
        elapsed = time.monotonic() - started
        requests = 1
        v6._write_external(output, rows, store)
        disposition = "collected"
    if not isinstance(rows, list):
        raise ResidualTemporalDataError("split action rows are malformed")
    mapped: dict[str, set[str]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise ResidualTemporalDataError("split action row is malformed")
        symbol = str(row.get("ticker") or "").strip().upper()
        day = str(row.get("execution_date") or "")
        if not symbol or not day:
            raise ResidualTemporalDataError(
                "split action identity is incomplete"
            )
        mapped.setdefault(symbol, set()).add(day)
    return (
        {symbol: sorted(days) for symbol, days in mapped.items()},
        {
            "requests": requests,
            "request_seconds": round(elapsed, 6),
            "cache_hits": int(disposition == "cached"),
            "failures": 0,
            "events": len(rows),
            "sha256": sha256_file(output),
            "disposition": disposition,
        },
    )


def collect_development(
    contract_inspection_path: Path,
    *,
    store_config: HistoricalStoreConfig | None = None,
    root: Path = ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    contract = _load_contract_inspection(
        contract_inspection_path, enforce_commit=enforce_commit
    )
    store = store_config or HistoricalStoreConfig.from_env(DEFAULT_ENV_PATH)
    private_root = (
        store.root
        / "_derived"
        / SUCCESSOR_ID
        / str(contract["artifact_sha256"])
        / "development"
    )
    identities = _load_all_identities(contract)
    symbols = sorted(
        {
            "SPY",
            *{
                symbol
                for values in identities.values()
                for symbol in values
            },
        }
    )
    if (
        len(symbols) != contract["development_symbol_count"]
        or canonical_sha256(symbols)
        != contract["development_symbols_sha256"]
    ):
        raise ResidualTemporalDataError("development symbol union drifted")
    provider = AlpacaBulkConfig.from_env(DEFAULT_ENV_PATH)
    batches = [
        symbols[index : index + provider.batch_size]
        for index in range(0, len(symbols), provider.batch_size)
    ]
    start = datetime.combine(
        date.fromisoformat(contract["development_dates"][0]),
        wall_time(0),
        tzinfo=EASTERN,
    )
    end = datetime.combine(
        date.fromisoformat(contract["development_dates"][-1])
        + timedelta(days=1),
        wall_time(0),
        tzinfo=EASTERN,
    )
    telemetry = {
        "requests": 0,
        "request_seconds": 0.0,
        "pacing_wait_seconds": 0.0,
        "cache_hits": 0,
        "failures": 0,
    }
    started_at = datetime.now(timezone.utc)
    rows_by_symbol: dict[str, list[dict[str, Any]]] = {}
    with AlpacaBulkBarsClient(provider) as client:
        for index, symbols_batch in enumerate(batches, 1):
            batch_id = canonical_sha256(
                {
                    "symbols": symbols_batch,
                    "start": contract["development_dates"][0],
                    "end": contract["development_dates"][-1],
                    "feed": "sip",
                    "adjustment": "raw",
                    "timeframe": "1Day",
                }
            )
            checkpoint = private_root / "batches" / f"{batch_id}.json.gz"
            if checkpoint.is_file():
                payload = _read_gzip(checkpoint)
                if not (
                    isinstance(payload, Mapping)
                    and payload.get("symbols") == symbols_batch
                    and isinstance(payload.get("rows_by_symbol"), Mapping)
                ):
                    raise ResidualTemporalDataError(
                        "development checkpoint drifted"
                    )
                batch_rows = dict(payload["rows_by_symbol"])
                telemetry["cache_hits"] += 1
            else:
                try:
                    raw, pages = client.fetch(
                        symbols_batch,
                        timeframe="1Day",
                        start=start,
                        end=end,
                    )
                except ScannerReplayError:
                    telemetry["failures"] += 1
                    raise
                telemetry["requests"] += pages
                batch_rows = {
                    symbol: v6._normalize_daily_rows(
                        symbol, raw.get(symbol, [])
                    )
                    for symbol in symbols_batch
                }
                v6._write_external(
                    checkpoint,
                    {
                        "schema_version": 1,
                        "symbols": symbols_batch,
                        "rows_by_symbol": batch_rows,
                    },
                    store,
                )
            rows_by_symbol.update(batch_rows)
            print(
                f"daily batch {index}/{len(batches)}: "
                f"{len(symbols_batch)} symbols",
                flush=True,
            )
        telemetry["pacing_wait_seconds"] = float(
            client.pacing_wait_seconds
        )
        telemetry["request_seconds"] = float(client.request_seconds)
    split_dates, split_telemetry = _collect_splits(
        contract, private_root=private_root, store=store
    )
    dataset = {
        "schema_version": 1,
        "family_id": FAMILY_ID,
        "evaluation_dates": list(contract["development_dates"]),
        "decision_dates": list(contract["development_signal_dates"]),
        "reference_identities_by_date": identities,
        "split_execution_dates_by_symbol": split_dates,
        "daily_bars": rows_by_symbol,
        "source_semantics": {
            "provider": "Alpaca historical SIP",
            "feed": "sip",
            "adjustment": "raw",
            "timeframe": "1Day",
            "interpolation": "forbidden",
            "substitution": "forbidden",
            "contaminated_training_signals": contract[
                "contaminated_training_signal_count"
            ],
            "fresh_development_signals": contract[
                "fresh_development_signal_count"
            ],
        },
    }
    dataset_path = private_root / "dataset.json.gz"
    v6._write_external(dataset_path, dataset, store)
    completed_at = datetime.now(timezone.utc)
    inspection = strategy_discovery.load_artifact(
        contract_inspection_path, expected_kind=CONTRACT_INSPECTION_KIND
    )
    payload = {
        "schema_version": 1,
        "artifact_kind": COLLECTION_KIND,
        "state": COLLECTION_STATE,
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "contract_path": inspection["contract_path"],
        "contract_sha256": contract["artifact_sha256"],
        "contract_inspection_path": _repo_path(
            contract_inspection_path
        ),
        "contract_inspection_sha256": inspection["artifact_sha256"],
        "external_relative_path": str(
            dataset_path.resolve().relative_to(store.root.resolve())
        ),
        "external_file_sha256": sha256_file(dataset_path),
        "dataset_sha256": canonical_sha256(dataset),
        "evaluation_dates": list(contract["development_dates"]),
        "decision_dates": list(contract["development_signal_dates"]),
        "symbols_requested": len(symbols),
        "symbols_with_rows": sum(bool(rows_by_symbol[symbol]) for symbol in symbols),
        "empty_series": sum(not rows_by_symbol[symbol] for symbol in symbols),
        "daily_rows": sum(len(rows) for rows in rows_by_symbol.values()),
        "provider_telemetry": {
            "daily_bars": telemetry,
            "split_actions": split_telemetry,
        },
        "collection_started_at": started_at.isoformat().replace(
            "+00:00", "Z"
        ),
        "collection_completed_at": completed_at.isoformat().replace(
            "+00:00", "Z"
        ),
        "strategy_metrics_computed": False,
        "confirmation_prices_accessed": False,
        "substitutions": 0,
        "broker_actions": 0,
    }
    return strategy_discovery._write_artifact(
        payload,
        root / "development-collection",
        "residual-temporal-development-collection",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze-contract")
    freeze.add_argument("reference_inspection", type=Path)
    freeze.add_argument("--created-at", required=True)
    collect = subparsers.add_parser("collect-development")
    collect.add_argument("contract_inspection", type=Path)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "freeze-contract":
            path, artifact = freeze_contract(
                args.reference_inspection,
                created_at=args.created_at,
                root=args.root,
            )
        else:
            path, artifact = collect_development(
                args.contract_inspection, root=args.root
            )
        print(
            json.dumps(
                {
                    "written": _repo_path(path),
                    "artifact_sha256": artifact["artifact_sha256"],
                    "state": artifact["state"],
                    "provider_telemetry": artifact.get(
                        "provider_telemetry", {}
                    ),
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        ResidualTemporalDataError,
        ScannerReplayError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
        OSError,
        ValueError,
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
