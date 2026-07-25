"""Verify provisional legacy SEC events with same-accession common-stock cover facts."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import zipfile
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import earnings_sec_eps_capacity as v5
import earnings_sec_legacy_capacity as provisional
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = v5.CAMPAIGN_ID
FAMILY_ID = v5.FAMILY_ID
SUCCESSOR_ID = "earnings-positive-surprise-drift-v7-sec-cover-common-equity"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous" / SUCCESSOR_ID
PRIVATE_NAMESPACE = "_derived/earnings_sec_cover_identity"
PROVISIONAL_RESULT = (
    provisional.DEFAULT_ROOT
    / "provisional-result"
    / "result-0ddf0a8e1513b8d8227497c0bce0b90c56f6ad4a921b7353398a232de8947a8a.json"
)
PROVISIONAL_INSPECTION = (
    provisional.DEFAULT_ROOT
    / "provisional-result-inspection"
    / "inspection-c52d5dd10afdaabc2adc8046e2df9e5831fc7ee13045d3a9f12c5462c2ae1716.json"
)
COVER_TAG = "EntityCommonStockSharesOutstanding"


class EarningsSecCoverIdentityError(RuntimeError):
    """The common-equity cover contract or exact cached result drifted."""


def _read(path: Path) -> dict[str, Any]:
    return v5._read(path)


def _write(path: Path, value: Mapping[str, Any]) -> None:
    v5._write(path, value)


def _repo_path(path: Path) -> str:
    return v5._repo_path(path)


def _private_provisional(
    store: HistoricalDayStore,
) -> tuple[dict[str, Any], dict[str, Any], bytes]:
    result = _read(PROVISIONAL_RESULT)
    info = result["private_artifact"]
    raw = (store.root / info["cache_relative_path"]).read_bytes()
    if hashlib.sha256(raw).hexdigest() != info["file_sha256"]:
        raise EarningsSecCoverIdentityError(
            "provisional private artifact hash differs"
        )
    value = json.loads(gzip.decompress(raw))
    if not (
        value.get("content_sha256")
        == info["content_sha256"]
        == v5.self_hash(value, "content_sha256")
    ):
        raise EarningsSecCoverIdentityError(
            "provisional private content differs"
        )
    return result, value, raw


def _lineage(
    store: HistoricalDayStore,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], bytes]:
    strategy_discovery.require_committed(PROVISIONAL_RESULT)
    strategy_discovery.require_committed(PROVISIONAL_INSPECTION)
    result, private, raw = _private_provisional(store)
    inspection = _read(PROVISIONAL_INSPECTION)
    if not (
        result.get("result_sha256")
        == v5.self_hash(result, "result_sha256")
        and result.get("retained_provisional_events") == 524
        and inspection.get("inspection_sha256")
        == v5.self_hash(inspection, "inspection_sha256")
        and inspection.get("state")
        == "SEC_LEGACY_PROVISIONAL_CAPACITY_READY"
        and inspection.get("result_sha256") == result["result_sha256"]
        and inspection.get("filing_cover_contract_freeze_permitted") is True
        and inspection.get("market_price_access_permitted") is False
        and len(private.get("events", [])) == 524
    ):
        raise EarningsSecCoverIdentityError(
            "provisional result or inspection lineage differs"
        )
    return result, inspection, private, raw


def build_contract(
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    for path in (
        Path(__file__).resolve(),
        PROJECT_ROOT / "earnings_sec_cover_identity_inspection.py",
    ):
        strategy_discovery.require_committed(path)
    historical_store = store or HistoricalDayStore.from_env()
    result, inspection, private, raw = _lineage(historical_store)
    provisional_contract = _read(
        PROJECT_ROOT / str(result["contract_path"])
    )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-sec-common-equity-cover-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "created_at": v5._timestamp(created_at, "created_at"),
        "source_lineage": {
            "provisional_result_path": _repo_path(PROVISIONAL_RESULT),
            "provisional_result_sha256": result["result_sha256"],
            "provisional_inspection_path": _repo_path(PROVISIONAL_INSPECTION),
            "provisional_inspection_sha256": inspection["inspection_sha256"],
            "private_provisional_content_sha256": private["content_sha256"],
            "private_provisional_file_sha256": hashlib.sha256(raw).hexdigest(),
            "retained_provisional_events": len(private["events"]),
            "archive_artifacts": provisional_contract["source_lineage"][
                "archive_artifacts"
            ],
        },
        "cover_identity_semantics": {
            "source": "same accession as-filed SEC XBRL cover fact",
            "required_tag": COVER_TAG,
            "tag_namespace": "dei",
            "iprx": 0,
            "qtrs": 0,
            "uom": "shares",
            "dimn": 0,
            "coreg": None,
            "positive_finite_value_required": True,
            "as_of_date_not_before_report_period": True,
            "as_of_date_not_after_filing_date": True,
            "latest_eligible_as_of_date_selected": True,
            "conflicting_values_on_latest_date": "ambiguous_zero_credit",
            "ticker_must_equal_same_accession_provisional_symbol": True,
            "current_or_external_identity_mapping_permitted": False,
            "cover_failure_replacement_permitted": False,
        },
        "partitions": provisional_contract["partitions"],
        "capacity_thresholds": provisional_contract["capacity_thresholds"],
        "provider_request_contract": {
            "additional_provider_requests_permitted": 0,
            "cache_hits_required": len(v5.ARCHIVES),
            "substitutions_permitted": 0,
        },
        "next_transition": {
            "development_market_data_contract_freeze_permitted": False,
            "market_price_access_permitted": False,
            "confirmation_outcome_access_permitted": False,
        },
        "implementation_hashes": {
            "earnings_sec_cover_identity.py": sha256_file(
                Path(__file__).resolve()
            ),
            "earnings_sec_cover_identity_inspection.py": sha256_file(
                PROJECT_ROOT / "earnings_sec_cover_identity_inspection.py"
            ),
            "earnings_sec_legacy_capacity.py": sha256_file(
                PROJECT_ROOT / "earnings_sec_legacy_capacity.py"
            ),
            "earnings_sec_eps_capacity.py": sha256_file(
                PROJECT_ROOT / "earnings_sec_eps_capacity.py"
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
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    value = build_contract(created_at=created_at, store=store)
    path = root / "cover-contract" / f"contract-{value['contract_sha256']}.json"
    _write(path, value)
    return path, value


def _cover_facts(
    archive: zipfile.ZipFile,
    events: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], Counter[str]]:
    by_adsh = {str(event["adsh"]): event for event in events}
    facts: dict[str, dict[str, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in v5._rows(archive, "num.tsv"):
        adsh = v5._normalized_text(row.get("adsh"))
        if (
            adsh not in by_adsh
            or v5._tag(row.get("tag")) != COVER_TAG
            or not v5._normalized_text(row.get("version")).startswith("dei/")
            or v5._integer(row.get("iprx")) != 0
            or v5._integer(row.get("qtrs")) != 0
            or v5._integer(row.get("dimn"), 0) != 0
            or v5._normalized_text(row.get("uom")).casefold() != "shares"
            or v5._normalized_text(row.get("coreg"))
        ):
            continue
        value = v5._float(row.get("value"))
        ddate = v5._normalized_text(row.get("ddate"))
        event = by_adsh[adsh]
        if not (
            value is not None
            and value > 0
            and event["period"] <= ddate <= event["filed"]
        ):
            continue
        facts[adsh][ddate].append(value)
    verified: list[dict[str, Any]] = []
    reasons: Counter[str] = Counter()
    for event in events:
        by_date = facts.get(str(event["adsh"]), {})
        if not by_date:
            reasons["NO_ELIGIBLE_COMMON_STOCK_SHARES_FACT"] += 1
            continue
        latest = max(by_date)
        values = sorted(set(by_date[latest]))
        if len(values) != 1:
            reasons["AMBIGUOUS_LATEST_COMMON_STOCK_SHARES_FACT"] += 1
            continue
        verified.append(
            {
                **event,
                "security_identity_state": "VERIFIED_COMMON_EQUITY_COVER_FACT",
                "common_stock_shares_outstanding": values[0],
                "common_stock_shares_as_of": latest,
                "common_stock_cover_tag": COVER_TAG,
            }
        )
        reasons["VERIFIED_COMMON_EQUITY_COVER_FACT"] += 1
    return verified, reasons


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
        and inspection.get("state") == "SEC_COVER_CONTRACT_INSPECTED_READY"
        and inspection.get("valid") is True
    ):
        raise EarningsSecCoverIdentityError(
            "committed cover contract or inspection is invalid"
        )
    historical_store = store or HistoricalDayStore.from_env()
    _result, private, _raw = _private_provisional(historical_store)
    events_by_adsh = {event["adsh"]: event for event in private["events"]}
    archive_map = {
        item["request_sha256"]: item
        for item in contract["source_lineage"]["archive_artifacts"]
    }
    verified: list[dict[str, Any]] = []
    reasons: Counter[str] = Counter()
    for request in v5._requests():
        info = archive_map[request["request_sha256"]]
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
            raise EarningsSecCoverIdentityError(
                f"cover archive differs for {request['quarter']}"
            )
        with zipfile.ZipFile(path) as archive:
            submissions = v5._submissions(archive)
            subset = [
                event
                for adsh, event in events_by_adsh.items()
                if adsh in submissions
            ]
            rows, counts = _cover_facts(archive, subset)
            verified.extend(rows)
            reasons.update(counts)
    key_counts = Counter(
        (event["accepted"], event["ticker"], event["adsh"])
        for event in verified
    )
    unique = [
        event
        for event in verified
        if key_counts[(event["accepted"], event["ticker"], event["adsh"])] == 1
    ]
    unique.sort(key=lambda event: (event["accepted"], event["ticker"], event["adsh"]))
    private_result: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "private-earnings-sec-common-equity-cover-result",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "contract_sha256": contract["contract_sha256"],
        "events": unique,
        "classification_counts": dict(sorted(reasons.items())),
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    private_result["content_sha256"] = v5.self_hash(
        private_result, "content_sha256"
    )
    raw = gzip.compress(v5.canonical_bytes(private_result), mtime=0)
    relative = (
        Path(PRIVATE_NAMESPACE)
        / "events"
        / f"{private_result['content_sha256']}.json.gz"
    )
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
        "artifact_kind": "earnings-sec-common-equity-cover-result",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "SEC_COVER_IDENTITIES_DERIVED_UNINSPECTED",
        "derived_at": v5._timestamp(derived_at, "derived_at"),
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "inspection_path": _repo_path(inspection_path),
        "inspection_file_sha256": sha256_file(inspection_path),
        "inspection_sha256": inspection["inspection_sha256"],
        "private_artifact": {
            "cache_relative_path": str(relative),
            "content_sha256": private_result["content_sha256"],
            "file_sha256": hashlib.sha256(raw).hexdigest(),
            "compressed_bytes": len(raw),
        },
        "input_provisional_events": len(private["events"]),
        "verified_common_equity_events": len(unique),
        "classification_counts": private_result["classification_counts"],
        "provider_telemetry": {
            "request_count": 0,
            "cache_hits": len(v5.ARCHIVES),
            "failures": 0,
            "substitutions": 0,
        },
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    value["result_sha256"] = v5.self_hash(value, "result_sha256")
    output = root / "cover-result" / f"result-{value['result_sha256']}.json"
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
        state = "SEC_COVER_CONTRACT_FROZEN"
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
                "verified_events": value.get("verified_common_equity_events", 0),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
