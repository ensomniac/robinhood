"""Record, inspect, and resume a bounded Form 4 response-schema failure.

The failed request is never retried.  After an independently inspected policy,
the exact failed symbol is checkpointed as permanently missing and later
structural Yahoo response errors receive the same whole-symbol missed-data
disposition after their first response.  Dates, symbols, and outcomes are never
substituted.
"""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import requests

import activist_earnings_data as yahoo
import insider_purchase_data as data
import insider_purchase_data_recovery as recovery
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = data.CAMPAIGN_ID
PRIVATE_NAMESPACE = data.PRIVATE_NAMESPACE
EXACT_FAILURE_MESSAGE = "Yahoo identity, timezone, or quote arrays drifted"
STRUCTURAL_RESPONSE_MESSAGES = (
    "Yahoo dates are not unique and chronological",
    "Yahoo identity, timezone, or quote arrays drifted",
    "Yahoo OHLCV arrays are incomplete",
    "Yahoo returned ambiguous chart results",
    "Yahoo returned invalid OHLCV",
)


class InsiderPurchaseReplicationRecoveryError(RuntimeError):
    """A replication failure boundary or resume authority drifted."""


def _root(family_id: str) -> Path:
    return (
        PROJECT_ROOT / "strategy_tournament/v2/discovery" / family_id
    )


def _family_id(value: Mapping[str, Any]) -> str:
    family_id = value.get("family_id")
    if not isinstance(family_id, str) or not family_id:
        raise InsiderPurchaseReplicationRecoveryError(
            "replication family_id is missing"
        )
    return family_id


def _controller_hashes() -> dict[str, str]:
    result: dict[str, str] = {}
    for relative in (
        "insider_purchase_replication_recovery.py",
        "insider_purchase_data.py",
        "activist_earnings_data.py",
    ):
        path = PROJECT_ROOT / relative
        strategy_discovery.require_committed(path)
        result[relative] = sha256_file(path)
    return result


def _require_pushed_head() -> str:
    return data._require_pushed_head()


def _permanent_missing_task(
    request: Mapping[str, Any], reason: str
) -> dict[str, Any]:
    value = {
        "schema_version": 1,
        "request_sha256": request["request_sha256"],
        "symbol": request["symbol"],
        "status": "PERMANENT_MISSING",
        "missing_reason": reason,
        "rows": [],
    }
    return {**value, "task_sha256": data._hash(value)}


def _is_structural_response_error(exc: BaseException) -> bool:
    return (
        type(exc) is yahoo.ActivistEarningsDataError
        and str(exc) in STRUCTURAL_RESPONSE_MESSAGES
    )


def build_failure(
    contract_path: Path,
    source_inspection_path: Path,
    *,
    expected_symbol: str,
    expected_request_sha256: str,
    observed_at: str,
) -> dict[str, Any]:
    contract = data._load(
        contract_path, "form4-purchase-development-source-contract"
    )
    source_inspection = data._load(
        source_inspection_path,
        "form4-purchase-development-source-inspection",
    )
    family_id = _family_id(contract)
    if not (
        source_inspection["state"] == "SOURCE_CONTRACT_INSPECTED_READY"
        and source_inspection["family_id"] == family_id
        and source_inspection["contract_sha256"]
        == contract["artifact_sha256"]
        and source_inspection[
            "development_provider_access_authorized"
        ]
        is True
        and source_inspection[
            "confirmation_provider_access_authorized"
        ]
        is False
        and contract["controller_hashes"]["insider_purchase_data.py"]
        == sha256_file(PROJECT_ROOT / "insider_purchase_data.py")
    ):
        raise InsiderPurchaseReplicationRecoveryError(
            "source authorization drifted"
        )
    cached, failed, rows_retained = recovery._failure_boundary(contract)
    if not (
        failed["symbol"] == expected_symbol
        and failed["request_sha256"] == expected_request_sha256
    ):
        raise InsiderPurchaseReplicationRecoveryError(
            "attested schema failure differs from checkpoint boundary"
        )
    cached_symbols = {str(request["symbol"]) for request in cached}
    ordinal = len(cached) + 1
    return {
        "schema_version": 1,
        "artifact_kind": (
            "form4-replication-development-response-schema-failure"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": family_id,
        "observed_at": data._timestamp(observed_at, "observed_at"),
        "state": "RESPONSE_SCHEMA_FAILURE_RECORDED_UNINSPECTED",
        "controller_hashes": _controller_hashes(),
        "contract_path": data._relative(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "source_inspection_path": data._relative(
            source_inspection_path
        ),
        "source_inspection_sha256": source_inspection[
            "artifact_sha256"
        ],
        "failed_request": {
            "ordinal": ordinal,
            "symbol": failed["symbol"],
            "request_sha256": failed["request_sha256"],
            "endpoint": failed["endpoint"],
        },
        "error": {
            "exception_type": (
                "activist_earnings_data.ActivistEarningsDataError"
            ),
            "sanitized_message": EXACT_FAILURE_MESSAGE,
        },
        "partial_exposure_scope": recovery._filtered_source_scope(
            contract["source_scope"], cached_symbols
        ),
        "failure_boundary": {
            "provider_requests_lifetime": ordinal,
            "provider_responses_lifetime": ordinal,
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
            "failed_symbol_registration_before_inspection_permitted": False,
            "substitutions_permitted": 0,
            "confirmation_access_permitted": False,
        },
    }


def record_failure(
    contract_path: Path,
    source_inspection_path: Path,
    *,
    expected_symbol: str,
    expected_request_sha256: str,
    observed_at: str,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(source_inspection_path)
    _require_pushed_head()
    payload = build_failure(
        contract_path,
        source_inspection_path,
        expected_symbol=expected_symbol,
        expected_request_sha256=expected_request_sha256,
        observed_at=observed_at,
    )
    family_id = _family_id(payload)
    path, artifact = data._write(
        payload,
        _root(family_id) / "development-response-schema-failure",
        "response-schema-failure",
    )
    outcome_exposure.ensure_record(
        outcome_exposure.build_record(
            exposure_id=(
                f"development-partial-{family_id}-"
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
        failure_path,
        "form4-replication-development-response-schema-failure",
    )
    rebuilt = build_failure(
        PROJECT_ROOT / str(failure["contract_path"]),
        PROJECT_ROOT / str(failure["source_inspection_path"]),
        expected_symbol=str(failure["failed_request"]["symbol"]),
        expected_request_sha256=str(
            failure["failed_request"]["request_sha256"]
        ),
        observed_at=str(failure["observed_at"]),
    )
    if {
        key: item
        for key, item in failure.items()
        if key != "artifact_sha256"
    } != rebuilt:
        raise InsiderPurchaseReplicationRecoveryError(
            "schema failure rebuild differs"
        )
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
        raise InsiderPurchaseReplicationRecoveryError(
            "partial response exposure is not indexed exactly"
        )
    family_id = _family_id(failure)
    payload = {
        "schema_version": 1,
        "artifact_kind": (
            "form4-replication-development-response-schema-"
            "failure-inspection"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": family_id,
        "inspected_at": data._timestamp(inspected_at, "inspected_at"),
        "state": "RESPONSE_SCHEMA_FAILURE_INSPECTED_TERMINAL",
        "failure_path": data._relative(failure_path),
        "failure_sha256": failure["artifact_sha256"],
        "failed_request": failure["failed_request"],
        "checks": {
            "failure_exactly_rebuilt": True,
            "failed_request_boundary_rebuilt": True,
            "checkpointed_tasks_rebuilt": True,
            "partial_exposure_indexed": True,
            "exact_exception_recorded": True,
            "zero_metrics_or_winner": True,
            "confirmation_closed": True,
            "retry_forbidden": True,
            "substitutions_forbidden": True,
            "valid": True,
        },
        "schema_policy_freeze_authorized": True,
        "confirmation_access_permitted": False,
        "broker_actions": 0,
    }
    return data._write(
        payload,
        _root(family_id)
        / "development-response-schema-failure-inspection",
        "response-schema-failure-inspection",
    )


def build_policy(
    failure_inspection_path: Path, created_at: str
) -> dict[str, Any]:
    inspection = data._load(
        failure_inspection_path,
        (
            "form4-replication-development-response-schema-"
            "failure-inspection"
        ),
    )
    failure = data._load(
        PROJECT_ROOT / str(inspection["failure_path"]),
        "form4-replication-development-response-schema-failure",
    )
    if not (
        inspection["state"]
        == "RESPONSE_SCHEMA_FAILURE_INSPECTED_TERMINAL"
        and inspection["failure_sha256"] == failure["artifact_sha256"]
        and inspection["schema_policy_freeze_authorized"] is True
        and inspection["confirmation_access_permitted"] is False
        and all(inspection["checks"].values())
    ):
        raise InsiderPurchaseReplicationRecoveryError(
            "schema failure inspection is invalid"
        )
    contract = data._load(
        PROJECT_ROOT / str(failure["contract_path"]),
        "form4-purchase-development-source-contract",
    )
    cached, failed, rows_retained = recovery._failure_boundary(contract)
    if not (
        len(cached) == failure["failure_boundary"]["tasks_checkpointed"]
        and failed["request_sha256"]
        == failure["failed_request"]["request_sha256"]
        and rows_retained == failure["failure_boundary"]["rows_retained"]
    ):
        raise InsiderPurchaseReplicationRecoveryError(
            "schema policy checkpoint boundary drifted"
        )
    family_id = _family_id(failure)
    return {
        "schema_version": 1,
        "artifact_kind": "form4-replication-response-schema-resume-policy",
        "campaign_id": CAMPAIGN_ID,
        "family_id": family_id,
        "created_at": data._timestamp(created_at, "created_at"),
        "state": "RESPONSE_SCHEMA_POLICY_FROZEN_UNINSPECTED",
        "controller_hashes": _controller_hashes(),
        "failure_inspection_path": data._relative(
            failure_inspection_path
        ),
        "failure_inspection_sha256": inspection["artifact_sha256"],
        "failure_path": inspection["failure_path"],
        "failure_sha256": failure["artifact_sha256"],
        "contract_path": failure["contract_path"],
        "contract_sha256": failure["contract_sha256"],
        "source_inspection_path": failure["source_inspection_path"],
        "source_inspection_sha256": failure[
            "source_inspection_sha256"
        ],
        "checkpoint_boundary": {
            "tasks_checkpointed": len(cached),
            "rows_retained": rows_retained,
            "next_request": failure["failed_request"],
        },
        "policy": {
            "exact_exception_type": (
                "activist_earnings_data.ActivistEarningsDataError"
            ),
            "exact_response_messages": list(
                STRUCTURAL_RESPONSE_MESSAGES
            ),
            "bootstrap_failed_request": (
                "checkpoint permanent missing without another provider request"
            ),
            "later_disposition": (
                "checkpoint whole symbol permanent missing after first response"
            ),
            "all_other_exceptions": "fail_closed",
            "retries_permitted": 0,
            "substitutions_permitted": 0,
            "confirmation_requests_permitted": 0,
        },
        "strategy_metrics_computed": 0,
        "winner_selection_executed": False,
        "confirmation_prices_accessed": False,
        "broker_actions": 0,
    }


def freeze_policy(
    failure_inspection_path: Path, created_at: str
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(failure_inspection_path)
    policy = build_policy(failure_inspection_path, created_at)
    return data._write(
        policy,
        _root(_family_id(policy))
        / "development-response-schema-policy",
        "response-schema-policy",
    )


def inspect_policy(
    policy_path: Path, inspected_at: str
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(policy_path)
    policy = data._load(
        policy_path, "form4-replication-response-schema-resume-policy"
    )
    rebuilt = build_policy(
        PROJECT_ROOT / str(policy["failure_inspection_path"]),
        str(policy["created_at"]),
    )
    if {
        key: item
        for key, item in policy.items()
        if key != "artifact_sha256"
    } != rebuilt:
        raise InsiderPurchaseReplicationRecoveryError(
            "response-schema policy rebuild differs"
        )
    family_id = _family_id(policy)
    payload = {
        "schema_version": 1,
        "artifact_kind": (
            "form4-replication-response-schema-resume-policy-inspection"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": family_id,
        "inspected_at": data._timestamp(inspected_at, "inspected_at"),
        "state": "RESPONSE_SCHEMA_POLICY_INSPECTED_READY",
        "policy_path": data._relative(policy_path),
        "policy_sha256": policy["artifact_sha256"],
        "contract_path": policy["contract_path"],
        "contract_sha256": policy["contract_sha256"],
        "checks": {
            "policy_exactly_rebuilt": True,
            "failure_inspection_rebuilt": True,
            "checkpoint_boundary_rebuilt": True,
            "exact_exception_type_bounded": True,
            "exact_response_messages_bounded": True,
            "bootstrap_retry_forbidden": True,
            "later_retries_forbidden": True,
            "substitutions_forbidden": True,
            "confirmation_closed": True,
            "zero_metrics_or_winner": True,
            "valid": True,
        },
        "resume_provider_access_authorized": True,
        "confirmation_access_permitted": False,
        "broker_actions": 0,
    }
    return data._write(
        payload,
        _root(family_id)
        / "development-response-schema-policy-inspection",
        "response-schema-policy-inspection",
    )


def resume_collection(
    policy_path: Path,
    policy_inspection_path: Path,
    collected_at: str,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(policy_path)
    strategy_discovery.require_committed(policy_inspection_path)
    published_commit = _require_pushed_head()
    policy = data._load(
        policy_path, "form4-replication-response-schema-resume-policy"
    )
    inspection = data._load(
        policy_inspection_path,
        (
            "form4-replication-response-schema-resume-policy-"
            "inspection"
        ),
    )
    if not (
        inspection["state"] == "RESPONSE_SCHEMA_POLICY_INSPECTED_READY"
        and inspection["policy_sha256"] == policy["artifact_sha256"]
        and inspection["resume_provider_access_authorized"] is True
        and inspection["confirmation_access_permitted"] is False
        and all(inspection["checks"].values())
        and policy["controller_hashes"] == _controller_hashes()
    ):
        raise InsiderPurchaseReplicationRecoveryError(
            "response-schema resume authority drifted"
        )
    contract_path = PROJECT_ROOT / str(policy["contract_path"])
    contract = data._load(
        contract_path, "form4-purchase-development-source-contract"
    )
    family_id = _family_id(contract)
    task_root = (
        HistoricalDayStore.from_env().root
        / PRIVATE_NAMESPACE
        / contract["artifact_sha256"]
        / "tasks"
    )
    bootstrap = policy["checkpoint_boundary"]["next_request"]
    telemetry: dict[str, Any] = {
        "requests": 0,
        "request_seconds": 0.0,
        "pacing_wait_seconds": 0.0,
        "cache_hits": 0,
        "failures": 0,
        "permanent_missing_responses": 0,
        "structural_permanent_missing": {
            message: [] for message in STRUCTURAL_RESPONSE_MESSAGES
        },
        "bootstrap_permanent_missing": [],
        "prior_provider_requests": int(
            data._load(
                PROJECT_ROOT / str(policy["failure_path"]),
                (
                    "form4-replication-development-response-"
                    "schema-failure"
                ),
            )["failure_boundary"]["provider_requests_lifetime"]
        ),
    }
    http = requests.Session()
    try:
        for index, request in enumerate(contract["requests"]):
            task_path = (
                task_root / f"{request['request_sha256']}.json.gz"
            )
            if task_path.is_file():
                task = yahoo._read_private(task_path)
                telemetry["cache_hits"] += 1
            elif (
                request["request_sha256"]
                == bootstrap["request_sha256"]
            ):
                task = _permanent_missing_task(
                    request,
                    (
                        "Inspected unusable Yahoo response retained "
                        "without retry or substitution"
                    ),
                )
                yahoo._write_private(task_path, task)
                telemetry["bootstrap_permanent_missing"].append(
                    request["symbol"]
                )
            else:
                if telemetry["requests"]:
                    time.sleep(data.PACE_SECONDS)
                    telemetry["pacing_wait_seconds"] += data.PACE_SECONDS
                try:
                    task = yahoo._fetch(request, http, telemetry)
                except yahoo.ActivistEarningsDataError as exc:
                    if not _is_structural_response_error(exc):
                        raise
                    task = _permanent_missing_task(
                        request,
                        (
                            "Exact-policy unusable Yahoo response retained "
                            "after first response without retry or substitution"
                        ),
                    )
                    telemetry["structural_permanent_missing"][
                        str(exc)
                    ].append(request["symbol"])
                yahoo._write_private(task_path, task)
            content = {
                key: item
                for key, item in task.items()
                if key != "task_sha256"
            }
            if (
                task.get("request_sha256")
                != request["request_sha256"]
                or task.get("task_sha256") != data._hash(content)
            ):
                raise InsiderPurchaseReplicationRecoveryError(
                    f"resumed task drifted at ordinal {index + 1}"
                )
    finally:
        http.close()
    if (
        telemetry["requests"]
        + telemetry["cache_hits"]
        + len(telemetry["bootstrap_permanent_missing"])
        != len(contract["requests"])
    ):
        raise InsiderPurchaseReplicationRecoveryError(
            "response-schema resume accounting is incomplete"
        )
    telemetry["provider_requests_lifetime"] = (
        telemetry["prior_provider_requests"] + telemetry["requests"]
    )
    collection_path, collection = data.collect(
        contract_path,
        PROJECT_ROOT / str(policy["source_inspection_path"]),
        collected_at,
    )
    payload = {
        "schema_version": 1,
        "artifact_kind": "form4-replication-development-schema-resume",
        "campaign_id": CAMPAIGN_ID,
        "family_id": family_id,
        "collected_at": data._timestamp(collected_at, "collected_at"),
        "state": "SCHEMA_RESUME_COMPLETED",
        "published_commit": published_commit,
        "policy_path": data._relative(policy_path),
        "policy_sha256": policy["artifact_sha256"],
        "inspection_path": data._relative(policy_inspection_path),
        "inspection_sha256": inspection["artifact_sha256"],
        "collection_path": data._relative(collection_path),
        "collection_sha256": collection["artifact_sha256"],
        "provider_telemetry": telemetry,
        "total_tasks": len(contract["requests"]),
        "confirmation_prices_accessed": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
    }
    return data._write(
        payload,
        _root(family_id) / "development-response-schema-resume",
        "response-schema-resume",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    record = subparsers.add_parser("record-failure")
    record.add_argument("contract", type=Path)
    record.add_argument("source_inspection", type=Path)
    record.add_argument("--expected-symbol", required=True)
    record.add_argument("--expected-request-sha256", required=True)
    record.add_argument("--observed-at", required=True)
    inspect_failure_parser = subparsers.add_parser("inspect-failure")
    inspect_failure_parser.add_argument("failure", type=Path)
    inspect_failure_parser.add_argument("--inspected-at", required=True)
    freeze = subparsers.add_parser("freeze-policy")
    freeze.add_argument("failure_inspection", type=Path)
    freeze.add_argument("--created-at", required=True)
    inspect_policy_parser = subparsers.add_parser("inspect-policy")
    inspect_policy_parser.add_argument("policy", type=Path)
    inspect_policy_parser.add_argument("--inspected-at", required=True)
    resume = subparsers.add_parser("resume")
    resume.add_argument("policy", type=Path)
    resume.add_argument("inspection", type=Path)
    resume.add_argument("--collected-at", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "record-failure":
        path, value = record_failure(
            args.contract,
            args.source_inspection,
            expected_symbol=args.expected_symbol,
            expected_request_sha256=args.expected_request_sha256,
            observed_at=args.observed_at,
        )
    elif args.command == "inspect-failure":
        path, value = inspect_failure(
            args.failure, args.inspected_at
        )
    elif args.command == "freeze-policy":
        path, value = freeze_policy(
            args.failure_inspection, args.created_at
        )
    elif args.command == "inspect-policy":
        path, value = inspect_policy(
            args.policy, args.inspected_at
        )
    else:
        path, value = resume_collection(
            args.policy, args.inspection, args.collected_at
        )
    result: Any = {
        "path": data._relative(path),
        "state": value["state"],
        "artifact_sha256": value["artifact_sha256"],
    }
    for field in (
        "failed_request",
        "checks",
        "collection_path",
        "provider_telemetry",
    ):
        if field in value:
            result[field] = value[field]
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
