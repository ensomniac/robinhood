"""Derive schema-accurate provisional SEC EPS events from frozen legacy archives."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import zipfile
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import earnings_sec_eps_capacity as v5
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = v5.CAMPAIGN_ID
FAMILY_ID = v5.FAMILY_ID
SUCCESSOR_ID = "earnings-positive-surprise-drift-v6-sec-legacy-provisional"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous" / SUCCESSOR_ID
PRIVATE_NAMESPACE = "_derived/earnings_sec_legacy_capacity"
V5_TERMINAL_INSPECTION = (
    v5.DEFAULT_ROOT
    / "metadata-collection-inspection"
    / "inspection-88198d4ece6d14267852d4170547c2662c6bd5f6b7b1d31db65b9375c4f6a147.json"
)
ARCHIVE_INSPECTION = v5.RECOVERY_INSPECTION
TOP_PROVISIONAL_EVENTS_PER_ACCEPTED_DATE = 3


class EarningsSecLegacyCapacityError(RuntimeError):
    """The v6 provisional source or its frozen cache lineage drifted."""


def _read(path: Path) -> dict[str, Any]:
    return v5._read(path)


def _write(path: Path, value: Mapping[str, Any]) -> None:
    v5._write(path, value)


def _repo_path(path: Path) -> str:
    return v5._repo_path(path)


def _lineage() -> tuple[dict[str, Any], dict[str, Any]]:
    strategy_discovery.require_committed(V5_TERMINAL_INSPECTION)
    strategy_discovery.require_committed(ARCHIVE_INSPECTION)
    terminal = _read(V5_TERMINAL_INSPECTION)
    archives = _read(ARCHIVE_INSPECTION)
    if not (
        terminal.get("inspection_sha256")
        == v5.self_hash(terminal, "inspection_sha256")
        and terminal.get("state") == "INSUFFICIENT_SEC_EPS_METADATA_CAPACITY"
        and terminal.get("total_unique_events") == 0
        and terminal.get("market_prices_accessed") is False
        and archives.get("inspection_sha256")
        == v5.self_hash(archives, "inspection_sha256")
        and archives.get("state")
        == "SEC_EPS_COLLECTION_FAILURE_INSPECTED_RECOVERY_READY"
        and archives.get("archive_count") == len(v5.ARCHIVES)
        and archives.get("valid") is True
    ):
        raise EarningsSecLegacyCapacityError(
            "v5 terminal or archive inspection lineage differs"
        )
    return terminal, archives


def build_contract(*, created_at: str) -> dict[str, Any]:
    for path in (
        Path(__file__).resolve(),
        PROJECT_ROOT / "earnings_sec_legacy_capacity_inspection.py",
    ):
        strategy_discovery.require_committed(path)
    terminal, archives = _lineage()
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-sec-legacy-provisional-capacity-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "created_at": v5._timestamp(created_at, "created_at"),
        "source_lineage": {
            "v5_terminal_inspection_path": _repo_path(V5_TERMINAL_INSPECTION),
            "v5_terminal_inspection_sha256": terminal["inspection_sha256"],
            "archive_inspection_path": _repo_path(ARCHIVE_INSPECTION),
            "archive_inspection_sha256": archives["inspection_sha256"],
            "archive_artifacts": archives["archive_artifacts"],
        },
        "provider_request_contract": {
            "additional_provider_requests_permitted": 0,
            "exact_cached_archives_required": len(v5.ARCHIVES),
            "retries_permitted": 0,
            "substitutions_permitted": 0,
        },
        "observed_legacy_encoding": {
            "forms": ["10-Q"],
            "amendments_excluded": True,
            "provisional_identity_tag": "TradingSymbol",
            "provisional_identity_iprx": 0,
            "provisional_identity_dimn": 0,
            "exactly_one_ticker_per_accession": True,
            "current_or_external_ticker_mapping_permitted": False,
            "security_type_claimed_before_cover_verification": False,
            "eps_tags_priority": list(v5.EPS_TAGS),
            "eps_iprx": 0,
            "eps_dimn": 0,
            "eps_uom": "USD",
            "eps_qtrs": 1,
            "positive_yoy_eps_change_required": True,
            "prior_comparison_days": [300, 430],
            "duplicate_event_key_receives_zero_credit": True,
        },
        "provisional_ranking": {
            "group": "SEC accepted calendar date",
            "rank": [
                "descending eps_yoy_change_ratio",
                "descending eps_yoy_change",
                "ascending ticker",
                "ascending adsh",
            ],
            "retain_per_group": TOP_PROVISIONAL_EVENTS_PER_ACCEPTED_DATE,
            "no_replacement_after_cover_failure": True,
        },
        "partitions": {
            "development": [v5.DEVELOPMENT_START, v5.DEVELOPMENT_END],
            "embargo": [v5.EMBARGO_START, v5.EMBARGO_END],
            "confirmation": [v5.CONFIRMATION_START, v5.CONFIRMATION_END],
        },
        "capacity_thresholds": {
            "minimum_unique_events": v5.MINIMUM_UNIQUE_EVENTS,
            "minimum_development_event_dates": v5.MINIMUM_DEVELOPMENT_DATES,
            "minimum_confirmation_event_dates": v5.MINIMUM_CONFIRMATION_DATES,
        },
        "next_transition": {
            "filing_cover_contract_required": True,
            "filing_cover_provider_access_permitted": False,
            "market_price_access_permitted": False,
            "forward_return_access_permitted": False,
        },
        "outcome_exposure_index_sha256": outcome_exposure.audit()["index_sha256"],
        "implementation_hashes": {
            "earnings_sec_legacy_capacity.py": sha256_file(
                Path(__file__).resolve()
            ),
            "earnings_sec_legacy_capacity_inspection.py": sha256_file(
                PROJECT_ROOT / "earnings_sec_legacy_capacity_inspection.py"
            ),
            "earnings_sec_eps_capacity.py": sha256_file(
                PROJECT_ROOT / "earnings_sec_eps_capacity.py"
            ),
            "outcome_exposure.py": sha256_file(
                PROJECT_ROOT / "outcome_exposure.py"
            ),
        },
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    value["contract_sha256"] = v5.self_hash(value, "contract_sha256")
    return value


def freeze_contract(
    *, created_at: str, root: Path = DEFAULT_ROOT
) -> tuple[Path, dict[str, Any]]:
    value = build_contract(created_at=created_at)
    path = (
        root
        / "provisional-contract"
        / f"contract-{value['contract_sha256']}.json"
    )
    _write(path, value)
    return path, value


def _identities(
    archive: zipfile.ZipFile,
    submissions: Mapping[str, Mapping[str, str]],
) -> dict[str, dict[str, str]]:
    tickers: dict[str, set[str]] = defaultdict(set)
    for row in v5._rows(archive, "txt.tsv"):
        adsh = v5._normalized_text(row.get("adsh"))
        if (
            adsh in submissions
            and v5._tag(row.get("tag")) == "TradingSymbol"
            and v5._integer(row.get("iprx")) == 0
            and v5._integer(row.get("dimn"), 0) == 0
        ):
            ticker = v5._normalized_text(row.get("value")).upper()
            if v5.TICKER_PATTERN.fullmatch(ticker):
                tickers[adsh].add(ticker)
    return {
        adsh: {"ticker": next(iter(values))}
        for adsh, values in tickers.items()
        if len(values) == 1
    }


def _events(
    archive: zipfile.ZipFile,
    submissions: Mapping[str, Mapping[str, str]],
    identities: Mapping[str, Mapping[str, str]],
) -> list[dict[str, Any]]:
    facts: dict[str, dict[str, dict[str, list[float]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )
    for row in v5._rows(archive, "num.tsv"):
        adsh = v5._normalized_text(row.get("adsh"))
        tag = v5._tag(row.get("tag"))
        if adsh not in identities or tag not in v5.EPS_TAGS:
            continue
        value = v5._float(row.get("value"))
        if not (
            v5._normalized_text(row.get("uom")).upper() == "USD"
            and v5._integer(row.get("qtrs")) == 1
            and v5._integer(row.get("iprx")) == 0
            and v5._integer(row.get("dimn"), 0) == 0
            and not v5._normalized_text(row.get("coreg"))
            and value is not None
        ):
            continue
        facts[adsh][tag][v5._normalized_text(row.get("ddate"))].append(value)
    result: list[dict[str, Any]] = []
    for adsh, submission in submissions.items():
        if adsh not in identities:
            continue
        period = submission["period"]
        try:
            current_date = date(
                int(period[:4]), int(period[4:6]), int(period[6:8])
            )
        except (ValueError, IndexError):
            continue
        selected: tuple[str, float, float, str] | None = None
        for tag in v5.EPS_TAGS:
            by_date = facts.get(adsh, {}).get(tag, {})
            current = by_date.get(period, [])
            if len(current) != 1:
                continue
            prior: list[tuple[int, str, float]] = []
            for ddate, values in by_date.items():
                if ddate == period or len(values) != 1:
                    continue
                try:
                    prior_date = date(
                        int(ddate[:4]), int(ddate[4:6]), int(ddate[6:8])
                    )
                except (ValueError, IndexError):
                    continue
                age = (current_date - prior_date).days
                if 300 <= age <= 430:
                    prior.append((abs(age - 365), ddate, values[0]))
            prior.sort()
            if not prior or (
                len(prior) > 1 and prior[0][0] == prior[1][0]
            ):
                continue
            selected = (tag, current[0], prior[0][2], prior[0][1])
            break
        if selected is None:
            continue
        tag, current_eps, prior_eps, prior_period = selected
        change = current_eps - prior_eps
        if change <= 0:
            continue
        scale = max(abs(prior_eps), 0.05)
        result.append(
            {
                **submission,
                **identities[adsh],
                "eps_tag": tag,
                "current_eps": current_eps,
                "prior_year_eps": prior_eps,
                "prior_period": prior_period,
                "eps_yoy_change": change,
                "eps_yoy_change_ratio": change / scale,
                "security_identity_state": "PROVISIONAL_TRADING_SYMBOL_ONLY",
            }
        )
    return result


def derive_archive(path: Path) -> tuple[list[dict[str, Any]], dict[str, int]]:
    try:
        with zipfile.ZipFile(path) as archive:
            if archive.testzip() is not None:
                raise EarningsSecLegacyCapacityError("archive CRC failed")
            submissions = v5._submissions(archive)
            identities = _identities(archive, submissions)
            events = _events(archive, submissions, identities)
    except (OSError, zipfile.BadZipFile) as exc:
        raise EarningsSecLegacyCapacityError(
            f"cannot derive legacy SEC archive {path}"
        ) from exc
    return events, {
        "eligible_10q_submissions": len(submissions),
        "provisional_single_ticker_identities": len(identities),
        "positive_yoy_eps_events": len(events),
    }


def _rank(events: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for event in events:
        grouped[str(event["accepted"])[:10]].append(event)
    retained: list[dict[str, Any]] = []
    for accepted_date in sorted(grouped):
        ordered = sorted(
            grouped[accepted_date],
            key=lambda event: (
                -float(event["eps_yoy_change_ratio"]),
                -float(event["eps_yoy_change"]),
                str(event["ticker"]),
                str(event["adsh"]),
            ),
        )
        for rank, event in enumerate(
            ordered[:TOP_PROVISIONAL_EVENTS_PER_ACCEPTED_DATE], 1
        ):
            retained.append(
                {
                    **event,
                    "accepted_date": accepted_date,
                    "provisional_rank": rank,
                }
            )
    return retained


def derive(
    contract_path: Path,
    inspection_path: Path,
    *,
    derived_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = _read(contract_path)
    inspection = _read(inspection_path)
    if not (
        contract.get("contract_sha256")
        == v5.self_hash(contract, "contract_sha256")
        and inspection.get("contract_sha256") == contract["contract_sha256"]
        and inspection.get("state") == "SEC_LEGACY_CONTRACT_INSPECTED_READY"
        and inspection.get("valid") is True
    ):
        raise EarningsSecLegacyCapacityError(
            "committed v6 contract or inspection is invalid"
        )
    historical_store = store or HistoricalDayStore.from_env()
    required = {
        str(item["request_sha256"]): item
        for item in contract["source_lineage"]["archive_artifacts"]
    }
    all_events: list[dict[str, Any]] = []
    archive_counts: list[dict[str, Any]] = []
    for request in v5._requests():
        info = required[str(request["request_sha256"])]
        path = (
            historical_store.root
            / v5.PRIVATE_NAMESPACE
            / "archives"
            / f"{request['request_sha256']}.zip"
        )
        if (
            sha256_file(path) != info["file_sha256"]
            or path.stat().st_size != info["bytes"]
        ):
            raise EarningsSecLegacyCapacityError(
                f"cached archive differs for {request['quarter']}"
            )
        events, counts = derive_archive(path)
        all_events.extend(events)
        archive_counts.append({"quarter": request["quarter"], **counts})
    keys: dict[tuple[str, str, str], int] = defaultdict(int)
    for event in all_events:
        keys[(event["accepted"], event["ticker"], event["adsh"])] += 1
    unique = [
        event
        for event in all_events
        if keys[(event["accepted"], event["ticker"], event["adsh"])] == 1
    ]
    ranked = _rank(unique)
    private: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "private-earnings-sec-legacy-provisional-events",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "contract_sha256": contract["contract_sha256"],
        "archive_counts": archive_counts,
        "pre_rank_unique_events": len(unique),
        "events": ranked,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    private["content_sha256"] = v5.self_hash(private, "content_sha256")
    relative = (
        Path(PRIVATE_NAMESPACE)
        / "events"
        / f"{private['content_sha256']}.json.gz"
    )
    raw = gzip.compress(v5.canonical_bytes(private), mtime=0)
    private_path = historical_store.root / relative
    private_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = private_path.with_name(f".{private_path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(raw)
        os.replace(temporary, private_path)
    finally:
        temporary.unlink(missing_ok=True)
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-sec-legacy-provisional-capacity-result",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "SEC_LEGACY_PROVISIONAL_EVENTS_DERIVED_UNINSPECTED",
        "derived_at": v5._timestamp(derived_at, "derived_at"),
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "inspection_path": _repo_path(inspection_path),
        "inspection_file_sha256": sha256_file(inspection_path),
        "inspection_sha256": inspection["inspection_sha256"],
        "private_artifact": {
            "cache_relative_path": str(relative),
            "content_sha256": private["content_sha256"],
            "file_sha256": hashlib.sha256(raw).hexdigest(),
            "compressed_bytes": len(raw),
        },
        "pre_rank_unique_events": len(unique),
        "retained_provisional_events": len(ranked),
        "provider_telemetry": {
            "request_count": 0,
            "cache_hits": len(v5.ARCHIVES),
            "failures": 0,
            "retries": 0,
            "substitutions": 0,
        },
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    value["result_sha256"] = v5.self_hash(value, "result_sha256")
    output = root / "provisional-result" / f"result-{value['result_sha256']}.json"
    _write(output, value)
    return output, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze-contract")
    freeze.add_argument("--created-at", required=True)
    derive_parser = subparsers.add_parser("derive")
    derive_parser.add_argument("contract", type=Path)
    derive_parser.add_argument("inspection", type=Path)
    derive_parser.add_argument("--derived-at", required=True)
    args = parser.parse_args(argv)
    if args.command == "freeze-contract":
        path, value = freeze_contract(created_at=args.created_at)
        state = "SEC_LEGACY_CONTRACT_FROZEN"
        digest = value["contract_sha256"]
    else:
        path, value = derive(
            args.contract,
            args.inspection,
            derived_at=args.derived_at,
        )
        state = value["state"]
        digest = value["result_sha256"]
    print(
        json.dumps(
            {
                "path": _repo_path(path),
                "sha256": digest,
                "state": state,
                "provider_requests": 0,
                "retained_events": value.get("retained_provisional_events", 0),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
