"""Record and inspect structural Yahoo failures without retry or substitution."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import insider_purchase_data as data
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
FAMILY_ID = data.FAMILY_ID
CAMPAIGN_ID = data.CAMPAIGN_ID
DEFAULT_ROOT = data.DEFAULT_ROOT
PRIVATE_NAMESPACE = data.PRIVATE_NAMESPACE


class InsiderPurchaseRecoveryError(RuntimeError):
    """A failed request boundary or permanent-missing transition drifted."""


def _controller_sha256() -> str:
    path = Path(__file__).resolve()
    strategy_discovery.require_committed(path)
    return sha256_file(path)


def _filtered_source_scope(
    source_scope: Mapping[str, Any], symbols: set[str]
) -> dict[str, Any]:
    if source_scope.get("symbols") is None:
        raise InsiderPurchaseRecoveryError("source scope is not a symbol graph")
    retained = sorted(set(map(str, source_scope["symbols"])) & symbols)
    if not retained:
        raise InsiderPurchaseRecoveryError("partial exposure scope is empty")
    return outcome_exposure.validate_scope(
        {"dates": list(source_scope["dates"]), "symbols": retained}
    )


def _failure_boundary(
    contract: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], Mapping[str, Any], int]:
    task_root = (
        HistoricalDayStore.from_env().root
        / PRIVATE_NAMESPACE
        / str(contract["artifact_sha256"])
        / "tasks"
    )
    cached: list[dict[str, Any]] = []
    rows_retained = 0
    failed: Mapping[str, Any] | None = None
    for request in contract["requests"]:
        path = task_root / f"{request['request_sha256']}.json.gz"
        if not path.is_file():
            failed = request
            break
        task = data.yahoo._read_private(path)
        content = {
            key: item for key, item in task.items() if key != "task_sha256"
        }
        if (
            task.get("request_sha256") != request["request_sha256"]
            or task.get("task_sha256") != data._hash(content)
        ):
            raise InsiderPurchaseRecoveryError("checkpointed task drifted")
        rows_retained += len(task["rows"])
        cached.append(dict(request))
    if failed is None:
        raise InsiderPurchaseRecoveryError("no failed request boundary remains")
    return cached, failed, rows_retained


def build_failure(
    contract_path: Path,
    inspection_path: Path,
    *,
    expected_symbol: str,
    expected_request_sha256: str,
    observed_at: str,
) -> dict[str, Any]:
    contract = data._load(
        contract_path, "form4-purchase-development-source-contract"
    )
    inspection = data._load(
        inspection_path, "form4-purchase-development-source-inspection"
    )
    if not (
        inspection["state"] == "SOURCE_CONTRACT_INSPECTED_READY"
        and inspection["contract_sha256"] == contract["artifact_sha256"]
        and contract["controller_hashes"]["insider_purchase_data.py"]
        == sha256_file(PROJECT_ROOT / "insider_purchase_data.py")
    ):
        raise InsiderPurchaseRecoveryError(
            "failure source authorization drifted"
        )
    cached, failed, rows_retained = _failure_boundary(contract)
    if not (
        failed["symbol"] == expected_symbol
        and failed["request_sha256"] == expected_request_sha256
    ):
        raise InsiderPurchaseRecoveryError(
            "operator-attested failed request does not match checkpoint boundary"
        )
    cached_symbols = {str(request["symbol"]) for request in cached}
    ordinal = len(cached) + 1
    return {
        "schema_version": 1,
        "artifact_kind": "form4-purchase-development-source-failure",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "observed_at": data._timestamp(observed_at, "observed_at"),
        "state": "INVALID_OHLCV_SOURCE_POLICY_FAILURE",
        "recovery_controller_sha256": _controller_sha256(),
        "contract_path": data._relative(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "inspection_path": data._relative(inspection_path),
        "inspection_sha256": inspection["artifact_sha256"],
        "failed_request": {
            "ordinal": ordinal,
            "symbol": failed["symbol"],
            "request_sha256": failed["request_sha256"],
            "endpoint": failed["endpoint"],
        },
        "error": {
            "category": "invalid_ohlcv",
            "sanitized_message": "Yahoo returned invalid OHLCV",
        },
        "partial_exposure_scope": _filtered_source_scope(
            contract["source_scope"], cached_symbols
        ),
        "failure_boundary": {
            "provider_requests": ordinal,
            "provider_responses": ordinal,
            "tasks_checkpointed": len(cached),
            "rows_retained": rows_retained,
            "symbols_with_checkpointed_tasks": sorted(cached_symbols),
            "development_prices_retained": True,
            "strategy_metrics_computed": 0,
            "winner_selection_executed": False,
            "confirmation_prices_accessed": False,
            "broker_actions": 0,
        },
        "disposition": {
            "failed_symbol_retry_permitted": False,
            "failed_symbol_permanent_missing_registration_permitted": True,
            "same_contract_resume_after_registration_permitted": True,
            "substitutions_permitted": 0,
            "confirmation_access_permitted": False,
        },
    }


def record_failure(
    contract_path: Path,
    inspection_path: Path,
    *,
    expected_symbol: str,
    expected_request_sha256: str,
    observed_at: str,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    path, artifact = data._write(
        build_failure(
            contract_path,
            inspection_path,
            expected_symbol=expected_symbol,
            expected_request_sha256=expected_request_sha256,
            observed_at=observed_at,
        ),
        DEFAULT_ROOT / "development-source-failure",
        "source-failure",
    )
    outcome_exposure.ensure_record(
        outcome_exposure.build_record(
            exposure_id=(
                f"development-partial-{FAMILY_ID}-"
                f"{artifact['artifact_sha256'][:16]}"
            ),
            campaign_id=CAMPAIGN_ID,
            lane="development",
            recorded_at=str(artifact["observed_at"]),
            source_path=data._relative(path),
            source_sha256=sha256_file(path),
            scope=artifact["partial_exposure_scope"],
        )
    )
    return path, artifact


def inspect_failure(
    failure_path: Path, inspected_at: str
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(failure_path)
    strategy_discovery.require_committed(outcome_exposure.DEFAULT_INDEX)
    failure = data._load(
        failure_path, "form4-purchase-development-source-failure"
    )
    contract_path = PROJECT_ROOT / str(failure["contract_path"])
    inspection_path = PROJECT_ROOT / str(failure["inspection_path"])
    failed = failure["failed_request"]
    rebuilt = build_failure(
        contract_path,
        inspection_path,
        expected_symbol=str(failed["symbol"]),
        expected_request_sha256=str(failed["request_sha256"]),
        observed_at=str(failure["observed_at"]),
    )
    if {
        key: item for key, item in failure.items() if key != "artifact_sha256"
    } != rebuilt:
        raise InsiderPurchaseRecoveryError("source failure rebuild differs")
    matches = [
        record
        for record in outcome_exposure.read_index()
        if record["source_path"] == data._relative(failure_path)
    ]
    if not (
        len(matches) == 1
        and matches[0]["scope"] == failure["partial_exposure_scope"]
        and matches[0]["lane"] == "development"
    ):
        raise InsiderPurchaseRecoveryError(
            "partial development exposure is not indexed"
        )
    payload = {
        "schema_version": 1,
        "artifact_kind": (
            "form4-purchase-development-source-failure-inspection"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "inspected_at": data._timestamp(inspected_at, "inspected_at"),
        "state": "INVALID_OHLCV_FAILURE_INSPECTED_TERMINAL",
        "failure_path": data._relative(failure_path),
        "failure_sha256": failure["artifact_sha256"],
        "checks": {
            "failure_exactly_rebuilt": True,
            "failed_request_boundary_rebuilt": True,
            "checkpointed_tasks_rebuilt": True,
            "partial_exposure_indexed": True,
            "zero_metrics_or_winner": True,
            "confirmation_closed": True,
            "failed_symbol_retry_forbidden": True,
            "permanent_missing_registration_bounded": True,
            "valid": True,
        },
        "failed_request": failure["failed_request"],
        "permanent_missing_registration_authorized": True,
        "same_contract_resume_authorized": True,
        "confirmation_access_permitted": False,
        "broker_actions": 0,
    }
    return data._write(
        payload,
        DEFAULT_ROOT / "development-source-failure-inspection",
        "source-failure-inspection",
    )


def register_permanent_missing(
    failure_inspection_path: Path, registered_at: str
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(failure_inspection_path)
    inspection = data._load(
        failure_inspection_path,
        "form4-purchase-development-source-failure-inspection",
    )
    if not (
        inspection["state"] == "INVALID_OHLCV_FAILURE_INSPECTED_TERMINAL"
        and inspection["permanent_missing_registration_authorized"] is True
        and inspection["same_contract_resume_authorized"] is True
    ):
        raise InsiderPurchaseRecoveryError(
            "permanent-missing registration is not authorized"
        )
    failure_path = PROJECT_ROOT / str(inspection["failure_path"])
    failure = data._load(
        failure_path, "form4-purchase-development-source-failure"
    )
    contract_path = PROJECT_ROOT / str(failure["contract_path"])
    contract = data._load(
        contract_path, "form4-purchase-development-source-contract"
    )
    ordinal = int(inspection["failed_request"]["ordinal"])
    request = contract["requests"][ordinal - 1]
    if request["request_sha256"] != inspection["failed_request"]["request_sha256"]:
        raise InsiderPurchaseRecoveryError("failed request binding drifted")
    task_content = {
        "schema_version": 1,
        "request_sha256": request["request_sha256"],
        "symbol": request["symbol"],
        "status": "PERMANENT_MISSING",
        "missing_reason": (
            "Inspected invalid Yahoo OHLCV retained without retry or substitution"
        ),
        "rows": [],
    }
    task = {**task_content, "task_sha256": data._hash(task_content)}
    task_path = (
        HistoricalDayStore.from_env().root
        / PRIVATE_NAMESPACE
        / contract["artifact_sha256"]
        / "tasks"
        / f"{request['request_sha256']}.json.gz"
    )
    if task_path.exists():
        raise InsiderPurchaseRecoveryError(
            "failed request already has a checkpoint; refusing overwrite"
        )
    data.yahoo._write_private(task_path, task)
    payload = {
        "schema_version": 1,
        "artifact_kind": "form4-purchase-permanent-missing-registration",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "registered_at": data._timestamp(registered_at, "registered_at"),
        "state": "PERMANENT_MISSING_REGISTERED_READY_TO_RESUME",
        "failure_inspection_path": data._relative(failure_inspection_path),
        "failure_inspection_sha256": inspection["artifact_sha256"],
        "request_sha256": request["request_sha256"],
        "symbol": request["symbol"],
        "task_sha256": task["task_sha256"],
        "provider_requests": 0,
        "retries": 0,
        "substitutions": 0,
        "confirmation_prices_accessed": False,
        "broker_actions": 0,
    }
    return data._write(
        payload,
        DEFAULT_ROOT / "development-permanent-missing-registration",
        "permanent-missing",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    record = subparsers.add_parser("record-failure")
    record.add_argument("contract", type=Path)
    record.add_argument("inspection", type=Path)
    record.add_argument("--expected-symbol", required=True)
    record.add_argument("--expected-request-sha256", required=True)
    record.add_argument("--observed-at", required=True)
    inspect = subparsers.add_parser("inspect-failure")
    inspect.add_argument("failure", type=Path)
    inspect.add_argument("--inspected-at", required=True)
    register = subparsers.add_parser("register-permanent-missing")
    register.add_argument("inspection", type=Path)
    register.add_argument("--registered-at", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "record-failure":
        path, value = record_failure(
            args.contract,
            args.inspection,
            expected_symbol=args.expected_symbol,
            expected_request_sha256=args.expected_request_sha256,
            observed_at=args.observed_at,
        )
        result: Any = {
            "path": data._relative(path),
            "state": value["state"],
            "failed_request": value["failed_request"],
            "failure_boundary": value["failure_boundary"],
            "artifact_sha256": value["artifact_sha256"],
        }
    elif args.command == "inspect-failure":
        path, value = inspect_failure(args.failure, args.inspected_at)
        result = {
            "path": data._relative(path),
            "state": value["state"],
            "checks": value["checks"],
            "artifact_sha256": value["artifact_sha256"],
        }
    else:
        path, value = register_permanent_missing(
            args.inspection, args.registered_at
        )
        result = {
            "path": data._relative(path),
            "state": value["state"],
            "symbol": value["symbol"],
            "provider_requests": value["provider_requests"],
            "artifact_sha256": value["artifact_sha256"],
        }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
