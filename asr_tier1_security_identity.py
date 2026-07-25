"""Apply the frozen ASR security-identity rule to tier-one disclosures."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import asr_security_identity as identity
import asr_semantic_tier as semantic
import asr_submission_collection as submissions


PROJECT_ROOT = Path(__file__).resolve().parent
APPLICATION_ID = "tier1-frozen-filing-cover-identity-application"
TIER1_RESULT_SHA256 = "9806ae110e222e98b44f5b3b3da0c2207219be9df37f1a132e843ca8f1f0d79a"
TIER1_RESULT_PATH = semantic.DEFAULT_RESULT_ROOT / (
    f"{semantic.tier.capacity.CANDIDATE_ID}-semantic-tier1-{TIER1_RESULT_SHA256}.json"
)
TIER1_INSPECTION_SHA256 = (
    "ff6be084bd7a0f541c11b29a5fb45fcf733a76d237275630b4360c659244a86d"
)
TIER1_INSPECTION_PATH = (
    semantic.DEFAULT_RESULT_ROOT
    / "inspections"
    / (
        f"{semantic.tier.capacity.CANDIDATE_ID}-semantic-tier1-"
        f"{TIER1_INSPECTION_SHA256}.json"
    )
)
IDENTITY_CONTRACT_SHA256 = (
    "e724b4ec1028530bd480004d9b8c510b6f9ec54450e232e046d3d697310e7748"
)
IDENTITY_CONTRACT_PATH = identity.DEFAULT_CONTRACT_ROOT / (
    f"{semantic.tier.capacity.CANDIDATE_ID}-{IDENTITY_CONTRACT_SHA256}.json"
)
IDENTITY_INSPECTION_SHA256 = (
    "54e81cb3a5ee0cc60b1f85d6e6438bd74387de899ba19c861deaa40b0a60ff7f"
)
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/asr/security-identity/tier1"
DEFAULT_CONTRACT_ROOT = DEFAULT_ROOT / "contracts"
DEFAULT_CONTRACT_STATUS = DEFAULT_ROOT / "contract-status.json"
DEFAULT_RESULT_ROOT = DEFAULT_ROOT / "results"
PRIVATE_RESULT_NAMESPACE = "_derived/asr-tier1-security-identity-result"


class AsrTier1SecurityIdentityError(RuntimeError):
    """The tier-one identity application is invalid."""


def self_hash(value: Mapping[str, Any], field: str) -> str:
    return identity.self_hash(value, field)


def read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AsrTier1SecurityIdentityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise AsrTier1SecurityIdentityError(f"{path} must contain an object")
    return value


def write_object(value: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _write_gzip(value: Mapping[str, Any], path: Path) -> bytes:
    raw = gzip.compress(identity.canonical_bytes(value), mtime=0)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(raw)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return raw


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(gzip.decompress(path.read_bytes()))
    except (OSError, gzip.BadGzipFile, json.JSONDecodeError) as exc:
        raise AsrTier1SecurityIdentityError(
            f"cannot read private tier-one artifact {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise AsrTier1SecurityIdentityError(
            "private tier-one artifact must contain an object"
        )
    return value


def _lineage(
    root: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    result = read_object(TIER1_RESULT_PATH)
    inspection = read_object(TIER1_INSPECTION_PATH)
    collection = read_object(submissions.STATUS_PATH)
    result_info = result["private_result"]
    result_path = root / str(result_info["cache_relative_path"])
    result_raw = result_path.read_bytes() if result_path.is_file() else b""
    private_result = _read_gzip(result_path)
    collection_info = collection["private_collection"]
    collection_path = root / str(collection_info["cache_relative_path"])
    collection_raw = collection_path.read_bytes() if collection_path.is_file() else b""
    private_collection = _read_gzip(collection_path)
    identity_contract = identity.load_contract(IDENTITY_CONTRACT_PATH)
    identity_status = identity.read_object(identity.DEFAULT_CONTRACT_STATUS)
    if not (
        result.get("result_sha256") == TIER1_RESULT_SHA256
        and result.get("result_sha256") == self_hash(result, "result_sha256")
        and result.get("semantically_qualified_unique_event_count") == 59
        and result.get("market_outcomes_accessed") is False
        and inspection.get("inspection_sha256") == TIER1_INSPECTION_SHA256
        and inspection.get("inspection_sha256")
        == self_hash(inspection, "inspection_sha256")
        and inspection.get("semantically_qualified_unique_event_count") == 59
        and inspection.get("market_outcomes_accessed") is False
        and hashlib.sha256(result_raw).hexdigest() == result_info["file_sha256"]
        and len(result_raw) == result_info["bytes"]
        and private_result.get("private_result_sha256")
        == result_info["private_result_sha256"]
        and private_result.get("private_result_sha256")
        == self_hash(private_result, "private_result_sha256")
        and len(private_result.get("unique_events", [])) == 59
        and collection.get("success_count") == 196
        and collection.get("failure_count") == 5
        and collection.get("market_outcomes_accessed") is False
        and hashlib.sha256(collection_raw).hexdigest() == collection_info["file_sha256"]
        and len(collection_raw) == collection_info["bytes"]
        and private_collection.get("private_collection_sha256")
        == collection_info["private_collection_sha256"]
        and private_collection.get("private_collection_sha256")
        == self_hash(private_collection, "private_collection_sha256")
        and identity_contract.get("contract_sha256") == IDENTITY_CONTRACT_SHA256
        and identity_status.get("inspection_sha256") == IDENTITY_INSPECTION_SHA256
        and identity_status.get("local_identity_access_permitted") is True
        and identity_status.get("market_outcomes_accessed") is False
    ):
        raise AsrTier1SecurityIdentityError(
            "tier-one semantic, collection, or frozen identity lineage differs"
        )
    return result, inspection, private_result, private_collection


def build_contract(*, store_root: Path | None = None) -> dict[str, Any]:
    root = store_root or semantic.tier.shared._store().root
    result, inspection, private_result, private_collection = _lineage(root)
    contract: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "outcome-blind-asr-tier1-security-identity-contract",
        "campaign_id": semantic.tier.capacity.CAMPAIGN_ID,
        "candidate_id": semantic.tier.capacity.CANDIDATE_ID,
        "strategy_version": semantic.tier.capacity.STRATEGY_VERSION,
        "application_id": APPLICATION_ID,
        "source_lineage": {
            "tier1_result_sha256": result["result_sha256"],
            "tier1_inspection_sha256": inspection["inspection_sha256"],
            "private_result_sha256": private_result["private_result_sha256"],
            "private_collection_sha256": private_collection[
                "private_collection_sha256"
            ],
            "semantic_event_count": 59,
            "qualified_accession_count": 27,
            "inspected_submission_count": 196,
            "source_failure_count": 5,
        },
        "identity_rule": {
            "frozen_contract_sha256": IDENTITY_CONTRACT_SHA256,
            "frozen_contract_inspection_sha256": IDENTITY_INSPECTION_SHA256,
            "implementation_sha256": identity.file_hash(
                PROJECT_ROOT / "asr_security_identity.py"
            ),
            "classifier": "classify_security_identity",
            "rule_change_permitted": False,
        },
        "capacity_contract": {
            "report_verified_agreement_count": True,
            "report_independent_disclosure_signal_count": True,
            "same_accession_events_are_one_trade_opportunity": True,
            "combine_only_after_independent_inspection": True,
        },
        "access_contract": {
            "local_identity_access_before_independent_inspection_permitted": False,
            "local_identity_access_after_independent_inspection_permitted": True,
            "new_provider_access_permitted": False,
            "market_price_access_permitted": False,
            "forward_return_access_permitted": False,
            "broker_actions_permitted": False,
            "maturity_effect": "NONE",
        },
        "implementation_hashes": {
            relative: identity.file_hash(PROJECT_ROOT / relative)
            for relative in (
                "asr_tier1_security_identity.py",
                "asr_tier1_security_identity_inspection.py",
            )
        },
        "verified_event_count": None,
        "independent_disclosure_signal_count": None,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
    }
    contract["contract_sha256"] = self_hash(contract, "contract_sha256")
    return contract


def _contract_path() -> Path:
    matches = sorted(DEFAULT_CONTRACT_ROOT.glob("*.json"))
    if len(matches) != 1:
        raise AsrTier1SecurityIdentityError(
            "expected exactly one tier-one identity contract"
        )
    return matches[0]


def load_contract(path: Path) -> dict[str, Any]:
    value = read_object(path)
    digest = value.get("contract_sha256")
    if not (
        isinstance(digest, str)
        and digest == self_hash(value, "contract_sha256")
        and path.name == f"{semantic.tier.capacity.CANDIDATE_ID}-{digest}.json"
    ):
        raise AsrTier1SecurityIdentityError(
            "tier-one identity contract was mutated or renamed"
        )
    return value


def freeze(
    *,
    output_root: Path = DEFAULT_CONTRACT_ROOT,
    status_path: Path = DEFAULT_CONTRACT_STATUS,
) -> tuple[Path, dict[str, Any]]:
    contract = build_contract()
    path = output_root / (
        f"{semantic.tier.capacity.CANDIDATE_ID}-{contract['contract_sha256']}.json"
    )
    write_object(contract, path)
    write_object(
        {
            "schema_version": 1,
            "campaign_id": semantic.tier.capacity.CAMPAIGN_ID,
            "candidate_id": semantic.tier.capacity.CANDIDATE_ID,
            "application_id": APPLICATION_ID,
            "contract_sha256": contract["contract_sha256"],
            "status": "TIER1_IDENTITY_CONTRACT_PENDING_INSPECTION",
            "local_identity_access_permitted": False,
            "provider_access_permitted": False,
            "market_price_access_permitted": False,
            "outcome_access_permitted": False,
            "broker_actions_permitted": False,
            "valid": False,
        },
        status_path,
    )
    return path, contract


def rebuild_result(
    private_result: Mapping[str, Any],
    private_collection: Mapping[str, Any],
    root: Path,
) -> dict[str, Any]:
    adapted = {"events": list(private_result["unique_events"])}
    base = identity.rebuild_result(adapted, private_collection, root)
    signals = {
        (
            str(event["accession"]),
            str(event["ticker"]),
            str(event["acceptance_datetime_raw"]),
        )
        for event in base["verified_events"]
    }
    result: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "private-asr-tier1-security-identity-result",
        "accession_terminal_counts": base["accession_terminal_counts"],
        "event_terminal_counts": base["event_terminal_counts"],
        "verified_events": base["verified_events"],
        "verified_event_count": base["verified_event_count"],
        "independent_disclosure_signal_count": len(signals),
        "security_identity_resolution_complete": True,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
    }
    result["private_result_sha256"] = self_hash(result, "private_result_sha256")
    return result


def evaluate(
    *,
    result_root: Path = DEFAULT_RESULT_ROOT,
    store_root: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    root = store_root or semantic.tier.shared._store().root
    contract = load_contract(_contract_path())
    status = read_object(DEFAULT_CONTRACT_STATUS)
    _source, _inspection, private_result, private_collection = _lineage(root)
    if not (
        status.get("status") == "TIER1_IDENTITY_CONTRACT_INSPECTED"
        and status.get("contract_sha256") == contract["contract_sha256"]
        and status.get("inspection_sha256") == self_hash(status, "inspection_sha256")
        and status.get("local_identity_access_permitted") is True
        and status.get("provider_access_permitted") is False
        and status.get("market_price_access_permitted") is False
        and status.get("outcome_access_permitted") is False
        and status.get("broker_actions_permitted") is False
        and status.get("valid") is True
    ):
        raise AsrTier1SecurityIdentityError(
            "tier-one identity contract is not independently inspected"
        )
    private = rebuild_result(private_result, private_collection, root)
    private_path = (
        root / PRIVATE_RESULT_NAMESPACE / f"{private['private_result_sha256']}.json.gz"
    )
    compressed = _write_gzip(private, private_path)
    result: dict[str, Any] = {
        "schema_version": 1,
        "result_kind": "outcome-blind-asr-tier1-security-identity-result",
        "campaign_id": semantic.tier.capacity.CAMPAIGN_ID,
        "candidate_id": semantic.tier.capacity.CANDIDATE_ID,
        "application_id": APPLICATION_ID,
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_sha256": status["inspection_sha256"],
        "semantic_event_count": 59,
        "accession_terminal_counts": private["accession_terminal_counts"],
        "event_terminal_counts": private["event_terminal_counts"],
        "verified_event_count": private["verified_event_count"],
        "independent_disclosure_signal_count": private[
            "independent_disclosure_signal_count"
        ],
        "security_identity_resolution_complete": True,
        "private_result": {
            "cache_relative_path": str(private_path.relative_to(root)),
            "private_result_sha256": private["private_result_sha256"],
            "file_sha256": hashlib.sha256(compressed).hexdigest(),
            "bytes": len(compressed),
        },
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "state": "TIER1_IDENTITY_RESULT_PENDING_INSPECTION",
        "valid": True,
    }
    result["result_sha256"] = self_hash(result, "result_sha256")
    path = result_root / (
        f"{semantic.tier.capacity.CANDIDATE_ID}-{result['result_sha256']}.json"
    )
    write_object(result, path)
    return path, result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "evaluate"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, value = freeze()
            result: dict[str, Any] = {
                "written": str(path.relative_to(PROJECT_ROOT)),
                "contract_sha256": value["contract_sha256"],
                "local_identity_access_permitted": False,
                "market_outcomes_accessed": False,
            }
        else:
            path, value = evaluate()
            result = {**value, "written": str(path.relative_to(PROJECT_ROOT))}
    except (AsrTier1SecurityIdentityError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
