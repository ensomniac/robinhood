"""Collect and derive the inspected 2012-2019 SEC PEAD metadata graph."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import time
import zipfile
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import requests

import earnings_sec_cover_identity as cover
import earnings_sec_expansion_capacity as capacity
import earnings_sec_legacy_capacity as legacy
import strategy_discovery
from historical_discovery import SecConfig
from historical_store import HistoricalDayStore, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ROOT = capacity.DEFAULT_ROOT
CONTRACT = (
    DEFAULT_ROOT
    / "metadata-contract"
    / "contract-"
    "214e97345b01a2fe67dfee3858b9cc8a9678be97cecb057a8a969c3e482e15fe"
    ".json"
)
CONTRACT_INSPECTION = (
    DEFAULT_ROOT
    / "metadata-contract-inspection"
    / "inspection-"
    "ae3509f40206f7e79326b400665a11d8961a6b0bf2db8eea4f5b31e6ad178d08"
    ".json"
)
PRIVATE_NAMESPACE = "_derived/earnings_sec_expansion_capacity"


class EarningsSecExpansionCollectionError(RuntimeError):
    """The inspected SEC expansion collection graph drifted."""


def _lineage() -> tuple[dict[str, Any], dict[str, Any]]:
    strategy_discovery.require_committed(CONTRACT)
    strategy_discovery.require_committed(CONTRACT_INSPECTION)
    contract = capacity._read(CONTRACT)
    inspection = capacity._read(CONTRACT_INSPECTION)
    if not (
        contract.get("contract_sha256")
        == capacity.self_hash(contract, "contract_sha256")
        and inspection.get("inspection_sha256")
        == capacity.self_hash(inspection, "inspection_sha256")
        and inspection.get("contract_sha256") == contract["contract_sha256"]
        and inspection.get("state")
        == "SEC_EXPANSION_CAPACITY_CONTRACT_INSPECTED_READY"
        and inspection.get("valid") is True
        and inspection.get("provider_access_authorized") is True
        and inspection.get("market_price_access_authorized") is False
    ):
        raise EarningsSecExpansionCollectionError(
            "SEC expansion capacity lineage is invalid"
        )
    return contract, inspection


def build_plan(*, created_at: str) -> dict[str, Any]:
    for path in (
        Path(__file__).resolve(),
        PROJECT_ROOT / "earnings_sec_expansion_collection_inspection.py",
        PROJECT_ROOT / "earnings_sec_legacy_capacity.py",
        PROJECT_ROOT / "earnings_sec_cover_identity.py",
    ):
        strategy_discovery.require_committed(path)
    contract, inspection = _lineage()
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-sec-expansion-metadata-collection-plan",
        "campaign_id": capacity.CAMPAIGN_ID,
        "family_id": capacity.FAMILY_ID,
        "successor_id": capacity.SUCCESSOR_ID,
        "state": "SEC_EXPANSION_COLLECTION_PLAN_FROZEN",
        "created_at": capacity._timestamp(created_at, "created_at"),
        "contract_path": capacity._repo_path(CONTRACT),
        "contract_file_sha256": sha256_file(CONTRACT),
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_path": capacity._repo_path(CONTRACT_INSPECTION),
        "contract_inspection_file_sha256": sha256_file(CONTRACT_INSPECTION),
        "contract_inspection_sha256": inspection["inspection_sha256"],
        "requests": contract["requests"],
        "request_count": len(contract["requests"]),
        "transport": {
            "provider": contract["provider"],
            "minimum_spacing_seconds": capacity.MINIMUM_SPACING_SECONDS,
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
        "implementation_hashes": {
            "earnings_sec_expansion_collection.py": sha256_file(
                Path(__file__).resolve()
            ),
            "earnings_sec_expansion_collection_inspection.py": sha256_file(
                PROJECT_ROOT / "earnings_sec_expansion_collection_inspection.py"
            ),
            "earnings_sec_legacy_capacity.py": sha256_file(
                PROJECT_ROOT / "earnings_sec_legacy_capacity.py"
            ),
            "earnings_sec_cover_identity.py": sha256_file(
                PROJECT_ROOT / "earnings_sec_cover_identity.py"
            ),
        },
        "provider_requests_executed": 0,
        "metadata_rows_accessed": 0,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    value["plan_sha256"] = capacity.self_hash(value, "plan_sha256")
    return value


def freeze_plan(
    *, created_at: str, root: Path = DEFAULT_ROOT
) -> tuple[Path, dict[str, Any]]:
    value = build_plan(created_at=created_at)
    path = (
        root
        / "metadata-collection-plan"
        / f"plan-{value['plan_sha256']}.json"
    )
    capacity._write(path, value)
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


def _archive_info(
    path: Path, request: Mapping[str, Any], store: HistoricalDayStore
) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        with zipfile.ZipFile(path) as archive:
            if archive.testzip() is not None:
                return None
            for required in ("sub.tsv", "num.tsv", "txt.tsv"):
                legacy.v5._member_name(archive, required)
    except (
        OSError,
        zipfile.BadZipFile,
        legacy.v5.EarningsSecEpsCapacityError,
    ):
        return None
    return {
        "quarter": request["quarter"],
        "request_sha256": request["request_sha256"],
        "cache_relative_path": str(path.resolve().relative_to(store.root)),
        "file_sha256": sha256_file(path),
        "bytes": path.stat().st_size,
        "etag": None,
        "last_modified": None,
        "cache_hit": True,
    }


def _download(
    request: Mapping[str, Any],
    *,
    destination: Path,
    store: HistoricalDayStore,
    session: requests.Session,
    timeout_seconds: float,
) -> tuple[dict[str, Any], float]:
    info, elapsed = legacy.v5._download(
        request,
        destination=destination,
        session=session,
        timeout_seconds=timeout_seconds,
    )
    info["cache_relative_path"] = str(
        destination.resolve().relative_to(store.root)
    )
    return info, elapsed


def _rank_and_cover(
    archive_paths: Sequence[Path],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    provisional: list[dict[str, Any]] = []
    archive_counts: list[dict[str, Any]] = []
    for path in archive_paths:
        rows, counts = legacy.derive_archive(path)
        provisional.extend(rows)
        archive_counts.append(counts)
    key_counts = Counter(
        (str(row["accepted"]), str(row["ticker"]), str(row["adsh"]))
        for row in provisional
    )
    unique = [
        row
        for row in provisional
        if key_counts[
            (str(row["accepted"]), str(row["ticker"]), str(row["adsh"]))
        ]
        == 1
    ]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in unique:
        grouped[str(row["accepted"])[:10]].append(row)
    ranked: list[dict[str, Any]] = []
    for accepted_date in sorted(grouped):
        ordered = sorted(
            grouped[accepted_date],
            key=lambda row: (
                -float(row["eps_yoy_change_ratio"]),
                -float(row["eps_yoy_change"]),
                str(row["ticker"]),
                str(row["adsh"]),
            ),
        )
        for rank, row in enumerate(ordered[:3], 1):
            ranked.append({**row, "accepted_date_rank": rank})
    by_archive_adsh = {
        str(row["adsh"]): row for row in ranked
    }
    verified: list[dict[str, Any]] = []
    reasons: Counter[str] = Counter()
    for path in archive_paths:
        with zipfile.ZipFile(path) as archive:
            submissions = legacy.v5._submissions(archive)
            subset = [
                by_archive_adsh[adsh]
                for adsh in submissions
                if adsh in by_archive_adsh
            ]
            covered, counts = cover._cover_facts(archive, subset)
            verified.extend(covered)
            reasons.update(counts)
    verified.sort(
        key=lambda row: (
            str(row["accepted"]),
            int(row["accepted_date_rank"]),
            str(row["ticker"]),
            str(row["adsh"]),
        )
    )
    summary = {
        "provisional_event_rows": len(provisional),
        "duplicate_event_rows_zero_credit": len(provisional) - len(unique),
        "ranked_event_rows": len(ranked),
        "verified_common_equity_events": len(verified),
        **dict(sorted(reasons.items())),
    }
    return verified, archive_counts, summary


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
    plan = capacity._read(plan_path)
    inspection = capacity._read(inspection_path)
    rebuilt = build_plan(created_at=str(plan["created_at"]))
    if not (
        plan == rebuilt
        and plan.get("plan_sha256")
        == capacity.self_hash(plan, "plan_sha256")
        and inspection.get("plan_sha256") == plan["plan_sha256"]
        and inspection.get("state")
        == "SEC_EXPANSION_COLLECTION_PLAN_INSPECTED_READY"
        and inspection.get("valid") is True
        and inspection.get("metadata_collection_authorized") is True
    ):
        raise EarningsSecExpansionCollectionError(
            "committed SEC expansion collection plan is invalid"
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
            info = _archive_info(destination, request, historical_store)
            if info is not None:
                telemetry["cache_hits"] += 1
            else:
                if ordinal and telemetry["requests"]:
                    time.sleep(capacity.MINIMUM_SPACING_SECONDS)
                    telemetry[
                        "pacing_wait_seconds"
                    ] += capacity.MINIMUM_SPACING_SECONDS
                try:
                    info, elapsed = _download(
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
        raise EarningsSecExpansionCollectionError(
            "SEC expansion request accounting is incomplete"
        )
    events, archive_counts, summary = _rank_and_cover(paths)
    private: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "private-earnings-sec-expansion-capacity",
        "campaign_id": capacity.CAMPAIGN_ID,
        "family_id": capacity.FAMILY_ID,
        "successor_id": capacity.SUCCESSOR_ID,
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
    private["content_sha256"] = capacity.self_hash(
        private, "content_sha256"
    )
    raw = gzip.compress(capacity.canonical_bytes(private), mtime=0)
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
        "artifact_kind": "earnings-sec-expansion-metadata-collection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "family_id": capacity.FAMILY_ID,
        "successor_id": capacity.SUCCESSOR_ID,
        "state": "SEC_EXPANSION_METADATA_COLLECTED_UNINSPECTED",
        "collected_at": capacity._timestamp(collected_at, "collected_at"),
        "plan_path": capacity._repo_path(plan_path),
        "plan_file_sha256": sha256_file(plan_path),
        "plan_sha256": plan["plan_sha256"],
        "inspection_path": capacity._repo_path(inspection_path),
        "inspection_file_sha256": sha256_file(inspection_path),
        "inspection_sha256": inspection["inspection_sha256"],
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
    value["collection_sha256"] = capacity.self_hash(
        value, "collection_sha256"
    )
    output = (
        root
        / "metadata-collection"
        / f"collection-{value['collection_sha256']}.json"
    )
    capacity._write(output, value)
    return output, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze-plan")
    freeze.add_argument("--created-at", required=True)
    collect_parser = subparsers.add_parser("collect")
    collect_parser.add_argument("plan", type=Path)
    collect_parser.add_argument("inspection", type=Path)
    collect_parser.add_argument("--collected-at", required=True)
    args = parser.parse_args(argv)
    if args.command == "freeze-plan":
        path, value = freeze_plan(created_at=args.created_at)
        digest = value["plan_sha256"]
        requests_executed = 0
    else:
        path, value = collect(
            args.plan,
            args.inspection,
            collected_at=args.collected_at,
        )
        digest = value["collection_sha256"]
        requests_executed = value["provider_telemetry"]["requests"]
    print(
        json.dumps(
            {
                "path": capacity._repo_path(path),
                "sha256": digest,
                "state": value["state"],
                "provider_requests": requests_executed,
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
