"""Resume Form 4 collection under a bounded unusable-response policy."""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import requests

import activist_earnings_data as yahoo
import insider_purchase_data as data
import insider_purchase_data_recovery as recovery
import insider_purchase_data_resume as prior
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
FAMILY_ID = data.FAMILY_ID
CAMPAIGN_ID = data.CAMPAIGN_ID
DEFAULT_ROOT = data.DEFAULT_ROOT
PRIVATE_NAMESPACE = data.PRIVATE_NAMESPACE
EXACT_FAILURE_MESSAGE = "Yahoo identity, timezone, or quote arrays drifted"
STRUCTURAL_RESPONSE_MESSAGES = (
    "Yahoo dates are not unique and chronological",
    "Yahoo identity, timezone, or quote arrays drifted",
    "Yahoo OHLCV arrays are incomplete",
    "Yahoo returned ambiguous chart results",
    "Yahoo returned invalid OHLCV",
)


class InsiderPurchaseSchemaResumeError(RuntimeError):
    """An unusable-response failure or resume transition drifted."""


def _controller_hashes() -> dict[str, str]:
    result: dict[str, str] = {}
    for relative in (
        "insider_purchase_data_resume_schema.py",
        "insider_purchase_data_resume.py",
        "activist_earnings_data.py",
    ):
        path = PROJECT_ROOT / relative
        strategy_discovery.require_committed(path)
        result[relative] = sha256_file(path)
    return result


def _load_prior(
    policy_path: Path, inspection_path: Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    policy = data._load(
        policy_path, "form4-purchase-invalid-ohlcv-resume-policy"
    )
    inspection = data._load(
        inspection_path,
        "form4-purchase-invalid-ohlcv-resume-policy-inspection",
    )
    if not (
        policy["state"] == "RESUME_POLICY_FROZEN_UNINSPECTED"
        and inspection["state"] == "RESUME_POLICY_INSPECTED_READY"
        and inspection["policy_sha256"] == policy["artifact_sha256"]
        and inspection["resume_provider_access_authorized"] is True
        and inspection["confirmation_access_permitted"] is False
        and all(inspection["checks"].values())
        and policy["controller_hashes"] == prior._controller_hashes()
    ):
        raise InsiderPurchaseSchemaResumeError(
            "prior resume authority is invalid"
        )
    return policy, inspection


def _source_inspection_path() -> Path:
    paths = sorted(
        (
            PROJECT_ROOT
            / "strategy_tournament/v2/discovery"
            / FAMILY_ID
            / "development-source-contract-inspection"
        ).glob("*.json")
    )
    if len(paths) != 1:
        raise InsiderPurchaseSchemaResumeError(
            "expected exactly one source inspection"
        )
    return paths[0]


def _is_structural_response_error(exc: BaseException) -> bool:
    return (
        type(exc) is yahoo.ActivistEarningsDataError
        and str(exc) in STRUCTURAL_RESPONSE_MESSAGES
    )


def build_failure(
    prior_policy_path: Path,
    prior_inspection_path: Path,
    *,
    expected_symbol: str,
    expected_request_sha256: str,
    observed_at: str,
) -> dict[str, Any]:
    policy, inspection = _load_prior(
        prior_policy_path, prior_inspection_path
    )
    contract_path = PROJECT_ROOT / str(policy["contract_path"])
    contract = data._load(
        contract_path, "form4-purchase-development-source-contract"
    )
    cached, failed, rows_retained = recovery._failure_boundary(contract)
    if not (
        len(cached) == 538
        and failed["symbol"] == expected_symbol == "CLNS"
        and failed["request_sha256"]
        == expected_request_sha256
        == "92fbfc598c59c4ad2d079e0f67548c1702e13ebe6d15db568f55b0c36766da91"
    ):
        raise InsiderPurchaseSchemaResumeError(
            "operator-attested response-schema failure does not match boundary"
        )
    cached_symbols = {str(request["symbol"]) for request in cached}
    return {
        "schema_version": 1,
        "artifact_kind": "form4-purchase-development-resume-schema-failure",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "observed_at": data._timestamp(observed_at, "observed_at"),
        "state": "RESPONSE_SCHEMA_FAILURE_RECORDED_UNINSPECTED",
        "controller_hashes": _controller_hashes(),
        "prior_policy_path": data._relative(prior_policy_path),
        "prior_policy_sha256": policy["artifact_sha256"],
        "prior_inspection_path": data._relative(prior_inspection_path),
        "prior_inspection_sha256": inspection["artifact_sha256"],
        "contract_path": policy["contract_path"],
        "contract_sha256": policy["contract_sha256"],
        "failed_request": {
            "ordinal": len(cached) + 1,
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
            "provider_requests_lifetime": len(cached) + 1,
            "provider_responses_lifetime": len(cached) + 1,
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
    prior_policy_path: Path,
    prior_inspection_path: Path,
    *,
    expected_symbol: str,
    expected_request_sha256: str,
    observed_at: str,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(prior_policy_path)
    strategy_discovery.require_committed(prior_inspection_path)
    prior._require_pushed_head()
    path, artifact = data._write(
        build_failure(
            prior_policy_path,
            prior_inspection_path,
            expected_symbol=expected_symbol,
            expected_request_sha256=expected_request_sha256,
            observed_at=observed_at,
        ),
        DEFAULT_ROOT / "development-resume-schema-failure",
        "resume-schema-failure",
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
        failure_path, "form4-purchase-development-resume-schema-failure"
    )
    rebuilt = build_failure(
        PROJECT_ROOT / str(failure["prior_policy_path"]),
        PROJECT_ROOT / str(failure["prior_inspection_path"]),
        expected_symbol=str(failure["failed_request"]["symbol"]),
        expected_request_sha256=str(
            failure["failed_request"]["request_sha256"]
        ),
        observed_at=str(failure["observed_at"]),
    )
    if {
        key: item for key, item in failure.items() if key != "artifact_sha256"
    } != rebuilt:
        raise InsiderPurchaseSchemaResumeError(
            "response-schema failure rebuild differs"
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
        raise InsiderPurchaseSchemaResumeError(
            "partial response-schema exposure is not indexed"
        )
    payload = {
        "schema_version": 1,
        "artifact_kind": (
            "form4-purchase-development-resume-schema-failure-inspection"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
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
        DEFAULT_ROOT / "development-resume-schema-failure-inspection",
        "resume-schema-failure-inspection",
    )


def _failure_from_inspection(
    path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    inspection = data._load(
        path,
        "form4-purchase-development-resume-schema-failure-inspection",
    )
    failure = data._load(
        PROJECT_ROOT / str(inspection["failure_path"]),
        "form4-purchase-development-resume-schema-failure",
    )
    if not (
        inspection["state"] == "RESPONSE_SCHEMA_FAILURE_INSPECTED_TERMINAL"
        and inspection["failure_sha256"] == failure["artifact_sha256"]
        and inspection["schema_policy_freeze_authorized"] is True
        and inspection["confirmation_access_permitted"] is False
        and all(inspection["checks"].values())
        and failure["error"]["sanitized_message"] == EXACT_FAILURE_MESSAGE
    ):
        raise InsiderPurchaseSchemaResumeError(
            "response-schema failure inspection is invalid"
        )
    return inspection, failure


def build_policy(
    failure_inspection_path: Path, created_at: str
) -> dict[str, Any]:
    inspection, failure = _failure_from_inspection(
        failure_inspection_path
    )
    contract = data._load(
        PROJECT_ROOT / str(failure["contract_path"]),
        "form4-purchase-development-source-contract",
    )
    cached, failed, rows_retained = recovery._failure_boundary(contract)
    if not (
        len(cached) == 538
        and failed["request_sha256"]
        == failure["failed_request"]["request_sha256"]
        and rows_retained == failure["failure_boundary"]["rows_retained"]
    ):
        raise InsiderPurchaseSchemaResumeError(
            "schema-policy checkpoint boundary drifted"
        )
    return {
        "schema_version": 1,
        "artifact_kind": "form4-purchase-response-schema-resume-policy",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "created_at": data._timestamp(created_at, "created_at"),
        "state": "RESPONSE_SCHEMA_POLICY_FROZEN_UNINSPECTED",
        "controller_hashes": _controller_hashes(),
        "failure_inspection_path": data._relative(failure_inspection_path),
        "failure_inspection_sha256": inspection["artifact_sha256"],
        "failure_path": inspection["failure_path"],
        "failure_sha256": failure["artifact_sha256"],
        "prior_policy_path": failure["prior_policy_path"],
        "prior_policy_sha256": failure["prior_policy_sha256"],
        "prior_inspection_path": failure["prior_inspection_path"],
        "prior_inspection_sha256": failure["prior_inspection_sha256"],
        "contract_path": failure["contract_path"],
        "contract_sha256": failure["contract_sha256"],
        "checkpoint_boundary": {
            "tasks_checkpointed": len(cached),
            "rows_retained": rows_retained,
            "next_request": failure["failed_request"],
        },
        "policy": {
            "exact_exception_type": (
                "activist_earnings_data.ActivistEarningsDataError"
            ),
            "exact_response_messages": list(STRUCTURAL_RESPONSE_MESSAGES),
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
    return data._write(
        build_policy(failure_inspection_path, created_at),
        DEFAULT_ROOT / "development-resume-schema-policy",
        "resume-schema-policy",
    )


def inspect_policy(
    policy_path: Path, inspected_at: str
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(policy_path)
    policy = data._load(
        policy_path, "form4-purchase-response-schema-resume-policy"
    )
    rebuilt = build_policy(
        PROJECT_ROOT / str(policy["failure_inspection_path"]),
        str(policy["created_at"]),
    )
    if {
        key: item for key, item in policy.items() if key != "artifact_sha256"
    } != rebuilt:
        raise InsiderPurchaseSchemaResumeError(
            "response-schema policy rebuild differs"
        )
    payload = {
        "schema_version": 1,
        "artifact_kind": (
            "form4-purchase-response-schema-resume-policy-inspection"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
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
        DEFAULT_ROOT / "development-resume-schema-policy-inspection",
        "resume-schema-policy-inspection",
    )


def resume_collection(
    policy_path: Path,
    inspection_path: Path,
    collected_at: str,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(policy_path)
    strategy_discovery.require_committed(inspection_path)
    published_commit = prior._require_pushed_head()
    policy = data._load(
        policy_path, "form4-purchase-response-schema-resume-policy"
    )
    inspection = data._load(
        inspection_path,
        "form4-purchase-response-schema-resume-policy-inspection",
    )
    if not (
        inspection["state"] == "RESPONSE_SCHEMA_POLICY_INSPECTED_READY"
        and inspection["policy_sha256"] == policy["artifact_sha256"]
        and inspection["resume_provider_access_authorized"] is True
        and inspection["confirmation_access_permitted"] is False
        and all(inspection["checks"].values())
        and policy["controller_hashes"] == _controller_hashes()
    ):
        raise InsiderPurchaseSchemaResumeError(
            "response-schema resume authority drifted"
        )
    contract_path = PROJECT_ROOT / str(policy["contract_path"])
    contract = data._load(
        contract_path, "form4-purchase-development-source-contract"
    )
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
                "form4-purchase-development-resume-schema-failure",
            )["failure_boundary"]["provider_requests_lifetime"]
        ),
    }
    http = requests.Session()
    try:
        for index, request in enumerate(contract["requests"]):
            task_path = task_root / f"{request['request_sha256']}.json.gz"
            if task_path.is_file():
                task = yahoo._read_private(task_path)
                telemetry["cache_hits"] += 1
            elif request["request_sha256"] == bootstrap["request_sha256"]:
                task = prior._permanent_missing_task(
                    request,
                    (
                        "Inspected unusable Yahoo response retained without "
                        "retry or substitution"
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
                    task = prior._permanent_missing_task(
                        request,
                        (
                            "Exact-policy unusable Yahoo response retained "
                            "after first response without retry or substitution"
                        ),
                    )
                    telemetry["structural_permanent_missing"][str(exc)].append(
                        request["symbol"]
                    )
                yahoo._write_private(task_path, task)
            content = {
                key: item for key, item in task.items() if key != "task_sha256"
            }
            if (
                task.get("request_sha256") != request["request_sha256"]
                or task.get("task_sha256") != data._hash(content)
            ):
                raise InsiderPurchaseSchemaResumeError(
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
        raise InsiderPurchaseSchemaResumeError(
            "response-schema resume accounting is incomplete"
        )
    telemetry["provider_requests_lifetime"] = (
        telemetry["prior_provider_requests"] + telemetry["requests"]
    )
    collection_path, collection = data.collect(
        contract_path,
        _source_inspection_path(),
        collected_at,
    )
    payload = {
        "schema_version": 1,
        "artifact_kind": "form4-purchase-development-schema-resume",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "collected_at": data._timestamp(collected_at, "collected_at"),
        "state": "SCHEMA_RESUME_COMPLETED",
        "published_commit": published_commit,
        "policy_path": data._relative(policy_path),
        "policy_sha256": policy["artifact_sha256"],
        "inspection_path": data._relative(inspection_path),
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
        DEFAULT_ROOT / "development-resume-schema",
        "resume-schema",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    record = subparsers.add_parser("record-failure")
    record.add_argument("prior_policy", type=Path)
    record.add_argument("prior_inspection", type=Path)
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
            args.prior_policy,
            args.prior_inspection,
            expected_symbol=args.expected_symbol,
            expected_request_sha256=args.expected_request_sha256,
            observed_at=args.observed_at,
        )
    elif args.command == "inspect-failure":
        path, value = inspect_failure(args.failure, args.inspected_at)
    elif args.command == "freeze-policy":
        path, value = freeze_policy(
            args.failure_inspection, args.created_at
        )
    elif args.command == "inspect-policy":
        path, value = inspect_policy(args.policy, args.inspected_at)
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


if __name__ == "__main__":
    raise SystemExit(main())
