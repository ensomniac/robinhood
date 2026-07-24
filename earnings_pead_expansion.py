"""Collect outcome-blind 2026 earnings metadata for a disjoint PEAD expansion."""

from __future__ import annotations

import argparse
import gzip
import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import earnings_gap_continuation as earnings
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, canonical_sha256, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = "multi-strategy-portfolio-validation-v2"
FAMILY_ID = "earnings-positive-surprise-drift"
EXPANSION_ID = "earnings-positive-surprise-drift-v3-disjoint-2026"
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous"
    / EXPANSION_ID
)
IDENTITY_MANIFEST = (
    PROJECT_ROOT
    / "strategy_validation/equity_gap_continuation/manifests/"
    "equity-gap-continuation-v1-"
    "0145f77948ff8d7398c69ec7ae2229b72cfb7a07bab055c3dcfb15762d5cea43.json"
)
IDENTITY_PRIVATE = (
    "_derived/equity_gap_continuation_validation/"
    "dataset-equity-gap-continuation-validation-2026-07-21-v3/"
    "frozen-candidates.json.gz"
)
V2_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "earnings-positive-surprise-drift/development-inspection/"
    "earnings-positive-surprise-drift-development-inspection-"
    "1921a1b3228088abdc5320333b0d8aa56a81f4243701ee5a3936018c27d52c6d.json"
)
EVENT_START = "2026-01-01"
EVENT_END = "2026-07-23"
SYMBOL_COUNT = 128


class EarningsPeadExpansionError(RuntimeError):
    """The disjoint metadata contract or collection drifted."""


def _read(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise EarningsPeadExpansionError(f"{path} must contain an object")
    return value


def _gzip(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise EarningsPeadExpansionError(f"{path} must contain an object")
    return value


def _repo_path(path: Path) -> str:
    return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))


def _symbols(
    store: HistoricalDayStore, *, enforce_commit: bool
) -> list[str]:
    if enforce_commit:
        for path in (IDENTITY_MANIFEST, V2_INSPECTION):
            strategy_discovery.require_committed(path)
    manifest = _read(IDENTITY_MANIFEST)
    rejected = _read(V2_INSPECTION)
    identities = _gzip(store.root / IDENTITY_PRIVATE)
    if not (
        canonical_sha256(identities)
        == manifest["private_selection"]["content_sha256"]
        and rejected.get("state") == "REJECTED"
        and rejected.get("confirmation_access_permitted") is False
    ):
        raise EarningsPeadExpansionError(
            "identity or rejected-v2 source graph drifted"
        )
    counts: Counter[str] = Counter()
    first_seen: dict[str, str] = {}
    for phase in identities["phases"].values():
        for day, rows in phase["candidates_by_date"].items():
            for row in rows:
                symbol = str(row["symbol"])
                first_seen[symbol] = min(
                    first_seen.get(symbol, day), day
                )
                if day <= "2025-05-30":
                    counts[symbol] += 1
    eligible = [
        symbol
        for symbol in counts
        if first_seen[symbol] <= "2025-03-31"
        and len(
            list(
                (
                    store.root / symbol.lower() / "2026"
                ).glob("*.json.gz")
            )
        )
        >= 100
    ]
    selected = [
        symbol
        for symbol in sorted(
            eligible, key=lambda symbol: (-counts[symbol], symbol)
        )[:SYMBOL_COUNT]
    ]
    if len(selected) != SYMBOL_COUNT:
        raise EarningsPeadExpansionError(
            "outcome-blind symbol capacity is incomplete"
        )
    return selected


def build_contract(
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
    enforce_commit: bool = True,
) -> dict[str, Any]:
    earnings._timestamp(created_at, "created_at")
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    source = store or HistoricalDayStore.from_env()
    symbols = _symbols(source, enforce_commit=enforce_commit)
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-pead-expansion-metadata-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "expansion_id": EXPANSION_ID,
        "created_at": created_at,
        "provider": "Robinhood MCP read-only earnings results",
        "provider_method": "get_earnings_results",
        "symbols": symbols,
        "requests": [{"symbol": symbol} for symbol in symbols],
        "authorized_provider_requests": SYMBOL_COUNT,
        "event_start": EVENT_START,
        "event_end": EVENT_END,
        "selection_rule": (
            "Choose the 128 symbols with the most appearances through "
            "2025-05-30 in the committed point-in-time common-stock identity "
            "universe, require identity by 2025-03-31 and at least 100 cached "
            "2026 session files, then retain only unambiguous company-verified "
            "2026 reports with numeric actual EPS above estimated EPS."
        ),
        "ambiguity_rule": (
            "Exclude every symbol/report-date/timing identity represented by "
            "more than one returned row; never choose among duplicates."
        ),
        "v2_development_attempts": 32,
        "prior_trials_must_enter_selection_correction": True,
        "identity_manifest_path": _repo_path(IDENTITY_MANIFEST),
        "identity_manifest_file_sha256": sha256_file(
            IDENTITY_MANIFEST
        ),
        "v2_rejection_path": _repo_path(V2_INSPECTION),
        "v2_rejection_file_sha256": sha256_file(V2_INSPECTION),
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "implementation_sha256": sha256_file(Path(__file__).resolve()),
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
    }
    value["contract_sha256"] = earnings._self_hash(
        value, "contract_sha256"
    )
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
    earnings._write_json(path, value)
    return path, value


def inspect_contract(
    contract_path: Path,
    *,
    inspected_at: str,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    earnings._timestamp(inspected_at, "inspected_at")
    strategy_discovery.require_committed(contract_path)
    contract = _read(contract_path)
    rebuilt = build_contract(created_at=contract["created_at"])
    checks = {
        "exact_rebuild": rebuilt == contract,
        "request_count": len(contract["requests"]) == SYMBOL_COUNT,
        "unique_symbols": len(set(contract["symbols"])) == SYMBOL_COUNT,
        "completed_dates": contract["event_end"] == EVENT_END,
        "prior_trial_correction": contract[
            "prior_trials_must_enter_selection_correction"
        ]
        is True,
        "no_prices": contract["market_prices_accessed"] is False,
        "no_forward_returns": contract["forward_returns_accessed"] is False,
        "no_metrics": contract["strategy_metrics_computed"] == 0,
        "no_broker": contract["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise EarningsPeadExpansionError(
            "metadata contract inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-pead-expansion-metadata-contract-inspection"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "expansion_id": EXPANSION_ID,
        "state": "METADATA_CONTRACT_INSPECTED_READY",
        "inspected_at": inspected_at,
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "checks": checks,
        "provider_access_authorized": True,
        "authorized_provider_requests": SYMBOL_COUNT,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = earnings._self_hash(
        value, "inspection_sha256"
    )
    path = (
        root
        / "metadata-contract-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    earnings._write_json(path, value)
    return path, value


def _normalize(raw: Mapping[str, Any]) -> dict[str, Any] | None:
    symbol = str(raw.get("symbol", "")).strip().upper()
    report = raw.get("report")
    eps = raw.get("eps")
    if not isinstance(report, Mapping) or not isinstance(eps, Mapping):
        return None
    try:
        report_date = date.fromisoformat(str(report.get("date")))
        actual = float(eps.get("actual"))
        estimate = float(eps.get("estimate"))
    except (TypeError, ValueError):
        return None
    if not (
        symbol
        and EVENT_START <= report_date.isoformat() <= EVENT_END
        and report.get("timing") in {"am", "pm"}
        and report.get("verified") is True
    ):
        return None
    return {
        "symbol": symbol,
        "report_date": report_date.isoformat(),
        "timing": str(report["timing"]),
        "verified": True,
        "actual_eps": actual,
        "estimated_eps": estimate,
    }


def ingest(
    contract_path: Path,
    inspection_path: Path,
    lines: Sequence[str],
    *,
    collected_at: str,
    root: Path = DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    earnings._timestamp(collected_at, "collected_at")
    for path in (contract_path, inspection_path):
        strategy_discovery.require_committed(path)
    contract = _read(contract_path)
    inspection = _read(inspection_path)
    if not (
        inspection.get("state") == "METADATA_CONTRACT_INSPECTED_READY"
        and inspection.get("contract_sha256")
        == contract.get("contract_sha256")
    ):
        raise EarningsPeadExpansionError(
            "metadata collection is not authorized"
        )
    responses: dict[str, Any] = {}
    for line in lines:
        if not line.strip() or line.strip() == "__END__":
            continue
        item = json.loads(line)
        symbol = str(item.get("symbol", ""))
        if symbol in responses:
            raise EarningsPeadExpansionError(
                "metadata response symbol repeats"
            )
        responses[symbol] = item.get("response")
    if set(responses) != set(contract["symbols"]):
        raise EarningsPeadExpansionError(
            "metadata responses do not cover the frozen symbols"
        )
    grouped: dict[
        tuple[str, str, str], list[dict[str, Any]]
    ] = defaultdict(list)
    provider_rows = 0
    for symbol in contract["symbols"]:
        results = earnings._find_results(responses[symbol])
        if results is None:
            raise EarningsPeadExpansionError(
                f"{symbol}: earnings response is unusable"
            )
        provider_rows += len(results)
        for raw in results:
            if isinstance(raw, Mapping):
                row = _normalize(raw)
                if row is not None and row["symbol"] == symbol:
                    grouped[
                        (
                            row["symbol"],
                            row["report_date"],
                            row["timing"],
                        )
                    ].append(row)
    ambiguous = sum(1 for rows in grouped.values() if len(rows) != 1)
    events = sorted(
        (rows[0] for rows in grouped.values() if len(rows) == 1),
        key=lambda row: (
            row["report_date"],
            row["symbol"],
            row["timing"],
        ),
    )
    payload = {
        "schema_version": 1,
        "contract_sha256": contract["contract_sha256"],
        "inspection_sha256": inspection["inspection_sha256"],
        "collected_at": collected_at,
        "provider_requests": SYMBOL_COUNT,
        "provider_rows": provider_rows,
        "ambiguous_event_identities_excluded": ambiguous,
        "events": events,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
    }
    source = store or HistoricalDayStore.from_env()
    private = (
        source.root
        / "_derived/earnings_pead_expansion"
        / contract["contract_sha256"]
        / "events.json.gz"
    )
    earnings._write_gzip(private, payload)
    positive = [
        row
        for row in events
        if row["actual_eps"] > row["estimated_eps"]
    ]
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-pead-expansion-metadata-collection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "expansion_id": EXPANSION_ID,
        "state": "METADATA_COLLECTED_UNINSPECTED",
        "collected_at": collected_at,
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "inspection_path": _repo_path(inspection_path),
        "inspection_file_sha256": sha256_file(inspection_path),
        "provider_requests": SYMBOL_COUNT,
        "provider_rows": provider_rows,
        "normalized_events": len(events),
        "verified_positive_surprises": len(positive),
        "positive_surprise_dates": len(
            {row["report_date"] for row in positive}
        ),
        "positive_surprise_symbols": len(
            {row["symbol"] for row in positive}
        ),
        "ambiguous_event_identities_excluded": ambiguous,
        "private_payload": (
            "LOCAL_HISTORICAL_DATA_ROOT/"
            f"_derived/earnings_pead_expansion/"
            f"{contract['contract_sha256']}/events.json.gz"
        ),
        "private_payload_content_sha256": canonical_sha256(payload),
        "private_payload_file_sha256": sha256_file(private),
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
    }
    value["collection_sha256"] = earnings._self_hash(
        value, "collection_sha256"
    )
    path = (
        root
        / "metadata-collection"
        / f"collection-{value['collection_sha256']}.json"
    )
    earnings._write_json(path, value)
    return path, value


def inspect_collection(
    collection_path: Path,
    *,
    inspected_at: str,
    root: Path = DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    earnings._timestamp(inspected_at, "inspected_at")
    strategy_discovery.require_committed(collection_path)
    collection = _read(collection_path)
    source = store or HistoricalDayStore.from_env()
    raw = str(collection["private_payload"])
    private = source.root / raw.removeprefix(
        "LOCAL_HISTORICAL_DATA_ROOT/"
    )
    payload = _gzip(private)
    positive = [
        row
        for row in payload["events"]
        if row["actual_eps"] > row["estimated_eps"]
    ]
    checks = {
        "collection_hash": collection["collection_sha256"]
        == earnings._self_hash(collection, "collection_sha256"),
        "private_content": canonical_sha256(payload)
        == collection["private_payload_content_sha256"],
        "private_file": sha256_file(private)
        == collection["private_payload_file_sha256"],
        "request_count": payload["provider_requests"] == SYMBOL_COUNT,
        "positive_count": len(positive)
        == collection["verified_positive_surprises"],
        "no_prices": payload["market_prices_accessed"] is False,
        "no_forward_returns": payload["forward_returns_accessed"] is False,
        "no_metrics": payload["strategy_metrics_computed"] == 0,
        "no_broker": payload["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise EarningsPeadExpansionError(
            "metadata collection inspection failed"
        )
    signal_dates = {
        row["report_date"] for row in positive
    }
    state = (
        "METADATA_CAPACITY_READY"
        if len(positive) >= 50 and len(signal_dates) >= 20
        else "INSUFFICIENT_POWER_CAPACITY"
    )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-pead-expansion-metadata-collection-inspection"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "expansion_id": EXPANSION_ID,
        "state": state,
        "inspected_at": inspected_at,
        "collection_path": _repo_path(collection_path),
        "collection_file_sha256": sha256_file(collection_path),
        "collection_sha256": collection["collection_sha256"],
        "checks": checks,
        "verified_positive_surprises": len(positive),
        "positive_surprise_dates": len(signal_dates),
        "provider_requests": SYMBOL_COUNT,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_access_permitted": False,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = earnings._self_hash(
        value, "inspection_sha256"
    )
    path = (
        root
        / "metadata-collection-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    earnings._write_json(path, value)
    return path, value


def _artifact_sha(value: Mapping[str, Any]) -> str | None:
    for key in (
        "inspection_sha256",
        "collection_sha256",
        "contract_sha256",
    ):
        if key in value:
            return str(value[key])
    return None


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    freeze = sub.add_parser("freeze-contract")
    freeze.add_argument("--created-at", required=True)
    inspect = sub.add_parser("inspect-contract")
    inspect.add_argument("contract", type=Path)
    inspect.add_argument("--inspected-at", required=True)
    ingest_parser = sub.add_parser("ingest")
    ingest_parser.add_argument("contract", type=Path)
    ingest_parser.add_argument("inspection", type=Path)
    ingest_parser.add_argument("--collected-at", required=True)
    inspect_data = sub.add_parser("inspect-collection")
    inspect_data.add_argument("collection", type=Path)
    inspect_data.add_argument("--inspected-at", required=True)
    args = parser.parse_args(argv)
    if args.command == "freeze-contract":
        path, value = freeze_contract(created_at=args.created_at)
    elif args.command == "inspect-contract":
        path, value = inspect_contract(
            args.contract, inspected_at=args.inspected_at
        )
    elif args.command == "ingest":
        lines: list[str] = []
        for line in __import__("sys").stdin:
            lines.append(line)
            if line.strip() == "__END__":
                break
        path, value = ingest(
            args.contract,
            args.inspection,
            lines,
            collected_at=args.collected_at,
        )
    else:
        path, value = inspect_collection(
            args.collection, inspected_at=args.inspected_at
        )
    print(
        json.dumps(
            {
                "path": _repo_path(path),
                "state": value.get("state", value["artifact_kind"]),
                "sha256": _artifact_sha(value),
                "provider_requests": value.get("provider_requests", 0),
                "verified_positive_surprises": value.get(
                    "verified_positive_surprises", 0
                ),
                "market_prices_accessed": False,
                "forward_returns_accessed": False,
                "broker_actions": 0,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
