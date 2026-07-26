"""Freeze and execute the repeated invalid-OHLCV resume policy."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import requests

import activist_earnings_data as yahoo
import insider_purchase_data as data
import insider_purchase_data_recovery as recovery
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
FAMILY_ID = data.FAMILY_ID
CAMPAIGN_ID = data.CAMPAIGN_ID
DEFAULT_ROOT = data.DEFAULT_ROOT
PRIVATE_NAMESPACE = data.PRIVATE_NAMESPACE
EXACT_STRUCTURAL_MESSAGE = "Yahoo returned invalid OHLCV"


class InsiderPurchaseResumeError(RuntimeError):
    """A repeated-failure policy or resumed request graph drifted."""


def _controller_hashes() -> dict[str, str]:
    paths = (
        "insider_purchase_data_resume.py",
        "activist_earnings_data.py",
    )
    result: dict[str, str] = {}
    for relative in paths:
        path = PROJECT_ROOT / relative
        strategy_discovery.require_committed(path)
        result[relative] = sha256_file(path)
    return result


def _require_pushed_head() -> str:
    try:
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        upstream = subprocess.run(
            ["git", "rev-parse", "@{upstream}"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except subprocess.CalledProcessError as exc:
        raise InsiderPurchaseResumeError(
            "cannot verify pushed Git authority"
        ) from exc
    if not head or head != upstream:
        raise InsiderPurchaseResumeError(
            "resume provider access requires committed inputs and pushed HEAD"
        )
    return head


def _failure_from_inspection(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    inspection = data._load(
        path, "form4-purchase-development-source-failure-inspection"
    )
    if not (
        inspection["state"] == "INVALID_OHLCV_FAILURE_INSPECTED_TERMINAL"
        and inspection["permanent_missing_registration_authorized"] is True
        and inspection["confirmation_access_permitted"] is False
        and all(inspection["checks"].values())
    ):
        raise InsiderPurchaseResumeError(
            "failure inspection is not terminal and valid"
        )
    failure = data._load(
        PROJECT_ROOT / str(inspection["failure_path"]),
        "form4-purchase-development-source-failure",
    )
    if not (
        failure["artifact_sha256"] == inspection["failure_sha256"]
        and failure["error"]["category"] == "invalid_ohlcv"
        and failure["error"]["sanitized_message"] == EXACT_STRUCTURAL_MESSAGE
    ):
        raise InsiderPurchaseResumeError(
            "failure inspection does not bind the exact structural class"
        )
    return inspection, failure


def build_policy(
    contract_path: Path,
    inspection_paths: Sequence[Path],
    created_at: str,
) -> dict[str, Any]:
    if len(inspection_paths) != 2:
        raise InsiderPurchaseResumeError(
            "repeated-failure policy needs exactly two inspections"
        )
    contract = data._load(
        contract_path, "form4-purchase-development-source-contract"
    )
    evidence: list[dict[str, Any]] = []
    ordinals: list[int] = []
    for path in inspection_paths:
        inspection, failure = _failure_from_inspection(path)
        failed = failure["failed_request"]
        evidence.append(
            {
                "inspection_path": data._relative(path),
                "inspection_sha256": inspection["artifact_sha256"],
                "failure_path": inspection["failure_path"],
                "failure_sha256": failure["artifact_sha256"],
                "ordinal": failed["ordinal"],
                "symbol": failed["symbol"],
                "request_sha256": failed["request_sha256"],
                "error_category": failure["error"]["category"],
                "sanitized_message": failure["error"]["sanitized_message"],
            }
        )
        ordinals.append(int(failed["ordinal"]))
    if ordinals != sorted(set(ordinals)) or ordinals != [145, 439]:
        raise InsiderPurchaseResumeError(
            "repeated failure evidence does not bind ordinals 145 and 439"
        )
    cached, failed, rows_retained = recovery._failure_boundary(contract)
    if not (
        len(cached) == 438
        and failed["symbol"] == "CATC"
        and failed["request_sha256"] == evidence[-1]["request_sha256"]
    ):
        raise InsiderPurchaseResumeError(
            "current checkpoint boundary is not the second inspected failure"
        )
    return {
        "schema_version": 1,
        "artifact_kind": "form4-purchase-invalid-ohlcv-resume-policy",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "created_at": data._timestamp(created_at, "created_at"),
        "state": "RESUME_POLICY_FROZEN_UNINSPECTED",
        "contract_path": data._relative(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "failure_evidence": evidence,
        "controller_hashes": _controller_hashes(),
        "checkpoint_boundary": {
            "tasks_checkpointed": len(cached),
            "rows_retained": rows_retained,
            "next_request": {
                "ordinal": len(cached) + 1,
                "symbol": failed["symbol"],
                "request_sha256": failed["request_sha256"],
            },
        },
        "policy": {
            "bootstrap_failed_request": (
                "checkpoint permanent missing without another provider request"
            ),
            "later_exact_exception_type": (
                "activist_earnings_data.ActivistEarningsDataError"
            ),
            "later_exact_exception_message": EXACT_STRUCTURAL_MESSAGE,
            "later_disposition": (
                "checkpoint permanent missing after its first response"
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
    contract_path: Path,
    inspection_paths: Sequence[Path],
    created_at: str,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(contract_path)
    for path in inspection_paths:
        strategy_discovery.require_committed(path)
    return data._write(
        build_policy(contract_path, inspection_paths, created_at),
        DEFAULT_ROOT / "development-resume-policy",
        "resume-policy",
    )


def inspect_policy(
    policy_path: Path, inspected_at: str
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(policy_path)
    policy = data._load(
        policy_path, "form4-purchase-invalid-ohlcv-resume-policy"
    )
    contract_path = PROJECT_ROOT / str(policy["contract_path"])
    inspection_paths = [
        PROJECT_ROOT / str(row["inspection_path"])
        for row in policy["failure_evidence"]
    ]
    rebuilt = build_policy(
        contract_path, inspection_paths, str(policy["created_at"])
    )
    if {
        key: item for key, item in policy.items() if key != "artifact_sha256"
    } != rebuilt:
        raise InsiderPurchaseResumeError("resume policy rebuild differs")
    payload = {
        "schema_version": 1,
        "artifact_kind": "form4-purchase-invalid-ohlcv-resume-policy-inspection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "inspected_at": data._timestamp(inspected_at, "inspected_at"),
        "state": "RESUME_POLICY_INSPECTED_READY",
        "policy_path": data._relative(policy_path),
        "policy_sha256": policy["artifact_sha256"],
        "contract_path": policy["contract_path"],
        "contract_sha256": policy["contract_sha256"],
        "checks": {
            "policy_exactly_rebuilt": True,
            "two_independent_failures_rebuilt": True,
            "checkpoint_boundary_rebuilt": True,
            "exact_exception_class_bounded": True,
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
        DEFAULT_ROOT / "development-resume-policy-inspection",
        "resume-policy-inspection",
    )


def _permanent_missing_task(
    request: Mapping[str, Any], reason: str
) -> dict[str, Any]:
    content = {
        "schema_version": 1,
        "request_sha256": request["request_sha256"],
        "symbol": request["symbol"],
        "status": "PERMANENT_MISSING",
        "missing_reason": reason,
        "rows": [],
    }
    return {**content, "task_sha256": data._hash(content)}


def resume_collection(
    policy_path: Path,
    inspection_path: Path,
    collected_at: str,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(policy_path)
    strategy_discovery.require_committed(inspection_path)
    published_commit = _require_pushed_head()
    policy = data._load(
        policy_path, "form4-purchase-invalid-ohlcv-resume-policy"
    )
    inspection = data._load(
        inspection_path,
        "form4-purchase-invalid-ohlcv-resume-policy-inspection",
    )
    if not (
        inspection["state"] == "RESUME_POLICY_INSPECTED_READY"
        and inspection["policy_sha256"] == policy["artifact_sha256"]
        and inspection["resume_provider_access_authorized"] is True
        and inspection["confirmation_access_permitted"] is False
        and policy["controller_hashes"] == _controller_hashes()
    ):
        raise InsiderPurchaseResumeError("resume authorization drifted")
    contract_path = PROJECT_ROOT / str(policy["contract_path"])
    contract = data._load(
        contract_path, "form4-purchase-development-source-contract"
    )
    source_inspection_path = next(
        (
            PROJECT_ROOT / path
            for path in sorted(
                Path(
                    "strategy_tournament/v2/discovery/"
                    f"{FAMILY_ID}/development-source-contract-inspection"
                ).glob("*.json")
            )
        ),
        None,
    )
    if source_inspection_path is None:
        raise InsiderPurchaseResumeError("source inspection is missing")
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
        "invalid_ohlcv_permanent_missing": [],
        "bootstrap_permanent_missing": [],
        "prior_provider_requests": 439,
    }
    http = requests.Session()
    try:
        for index, request in enumerate(contract["requests"]):
            task_path = task_root / f"{request['request_sha256']}.json.gz"
            if task_path.is_file():
                task = yahoo._read_private(task_path)
                telemetry["cache_hits"] += 1
            elif request["request_sha256"] == bootstrap["request_sha256"]:
                task = _permanent_missing_task(
                    request,
                    (
                        "Twice-inspected invalid Yahoo OHLCV retained without "
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
                    if str(exc) != EXACT_STRUCTURAL_MESSAGE:
                        raise
                    task = _permanent_missing_task(
                        request,
                        (
                            "Exact-policy invalid Yahoo OHLCV retained after "
                            "its first response without retry or substitution"
                        ),
                    )
                    telemetry["invalid_ohlcv_permanent_missing"].append(
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
                raise InsiderPurchaseResumeError(
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
        raise InsiderPurchaseResumeError("resume request accounting is incomplete")
    telemetry["provider_requests_lifetime"] = (
        telemetry["prior_provider_requests"] + telemetry["requests"]
    )
    collection_path, collection = data.collect(
        contract_path,
        source_inspection_path,
        collected_at,
    )
    payload = {
        "schema_version": 1,
        "artifact_kind": "form4-purchase-development-resume",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "collected_at": data._timestamp(collected_at, "collected_at"),
        "state": "RESUME_COMPLETED",
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
        DEFAULT_ROOT / "development-resume",
        "resume",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze-policy")
    freeze.add_argument("contract", type=Path)
    freeze.add_argument("inspections", nargs=2, type=Path)
    freeze.add_argument("--created-at", required=True)
    inspect = subparsers.add_parser("inspect-policy")
    inspect.add_argument("policy", type=Path)
    inspect.add_argument("--inspected-at", required=True)
    resume = subparsers.add_parser("resume")
    resume.add_argument("policy", type=Path)
    resume.add_argument("inspection", type=Path)
    resume.add_argument("--collected-at", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "freeze-policy":
        path, value = freeze_policy(
            args.contract, args.inspections, args.created_at
        )
        result: Any = {
            "path": data._relative(path),
            "state": value["state"],
            "failure_evidence": value["failure_evidence"],
            "artifact_sha256": value["artifact_sha256"],
        }
    elif args.command == "inspect-policy":
        path, value = inspect_policy(args.policy, args.inspected_at)
        result = {
            "path": data._relative(path),
            "state": value["state"],
            "checks": value["checks"],
            "artifact_sha256": value["artifact_sha256"],
        }
    else:
        path, value = resume_collection(
            args.policy, args.inspection, args.collected_at
        )
        result = {
            "path": data._relative(path),
            "state": value["state"],
            "collection_path": value["collection_path"],
            "provider_telemetry": value["provider_telemetry"],
            "artifact_sha256": value["artifact_sha256"],
        }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
