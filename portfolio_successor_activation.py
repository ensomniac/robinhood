"""Freeze explicit user authorization for portfolio research campaign v2."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
CAMPAIGN_ID = "multi-strategy-portfolio-validation-v2"
PROPOSAL_SHA256 = (
    "67f12bf617cc7f541dd0beba0d574b309effa7adefe5aacb4e4413c3d5c2858e"
)
PROPOSAL_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/successor_proposal/"
    f"multi-strategy-portfolio-validation-v2-proposal-{PROPOSAL_SHA256}.json"
)
PLAN_PATH = PROJECT_ROOT / "PORTFOLIO_VALIDATION_V2.md"
INSPECTOR_PATH = PROJECT_ROOT / "portfolio_successor_activation_inspection.py"
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "strategy_tournament/successor_proposal/authorizations"
)
DEFAULT_STATUS = (
    PROJECT_ROOT
    / "strategy_tournament/successor_proposal/successor-authorization-status.json"
)
AUTHORIZED_AT = "2026-07-22T16:57:17Z"
AUTHORIZATION_TEXT = (
    "Authorize superseding portfolio research campaign v2 with up to three new "
    "mechanism families per ISO week, preserving all existing evidence, "
    "anti-tuning, privacy, broker-safety, promotion, and risk gates, until "
    "FIRST_PILOT_READY_LIVE_STARTED is machine-earned."
)


class PortfolioSuccessorActivationError(RuntimeError):
    """The successor authorization artifact or its source differs."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _hash_json(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise PortfolioSuccessorActivationError(f"cannot hash {path}: {exc}") from exc
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PortfolioSuccessorActivationError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise PortfolioSuccessorActivationError(f"{path} must contain an object")
    return value


def _self_hash(value: Mapping[str, Any], field: str) -> str:
    return _hash_json({key: item for key, item in value.items() if key != field})


def _load_proposal() -> dict[str, Any]:
    proposal = _read_json(PROPOSAL_PATH)
    if not (
        proposal.get("proposal_sha256") == PROPOSAL_SHA256
        and proposal.get("proposal_sha256")
        == _self_hash(proposal, "proposal_sha256")
        and proposal.get("authorization_state") == "REQUIRED_NOT_GRANTED"
        and proposal.get("required_authorization_text") == AUTHORIZATION_TEXT
        and proposal.get("campaign_activation_permitted") is False
        and proposal.get("provider_access_permitted") is False
        and proposal.get("outcome_access_permitted") is False
        and proposal.get("broker_actions_permitted") is False
    ):
        raise PortfolioSuccessorActivationError("successor proposal differs")
    return proposal


def build_authorization() -> dict[str, Any]:
    proposal = _load_proposal()
    files = (
        Path(__file__),
        INSPECTOR_PATH,
        PLAN_PATH,
        PROPOSAL_PATH,
        PROJECT_ROOT / "portfolio_config.toml",
        PROJECT_ROOT / "portfolio_maturity.py",
        PROJECT_ROOT / "portfolio_validation.py",
    )
    bindings = {
        str(path.relative_to(PROJECT_ROOT)): _hash_file(path) for path in files
    }
    authorization: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "successor-campaign-authorization",
        "campaign_id": CAMPAIGN_ID,
        "authorized_at": AUTHORIZED_AT,
        "authorization_source": "EXPLICIT_USER_MESSAGE",
        "authorization_text": AUTHORIZATION_TEXT,
        "proposal_sha256": PROPOSAL_SHA256,
        "objective": "FIRST_PILOT_READY_LIVE_STARTED",
        "scope": proposal["requested_superseding_authority"],
        "preservation_contract": {
            "v1_evidence_remains_adverse_and_immutable": True,
            "failed_corpus_parameter_repair_allowed": False,
            "prior_evidence_relabeling_allowed": False,
            "promotion_or_risk_gate_weakening_allowed": False,
        },
        "initial_theme_order": [
            item["theme_id"] for item in proposal["candidate_themes"]
        ],
        "first_authorized_action": (
            "PRIORITY_ONE_OUTCOME_BLIND_CAPACITY_PREFLIGHT"
        ),
        "candidate_preregistration_permitted_before_inspection": False,
        "provider_access_permitted_before_inspection": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "implementation_and_plan_hashes": dict(sorted(bindings.items())),
        "claim_limit": (
            "Authorization activates only after independent inspection. It does "
            "not itself freeze a strategy, authorize an outcome, award maturity, "
            "or permit a broker action."
        ),
    }
    authorization["authorization_sha256"] = _self_hash(
        authorization, "authorization_sha256"
    )
    return authorization


def write_authorization(
    output_root: Path = DEFAULT_OUTPUT_ROOT, status_path: Path = DEFAULT_STATUS
) -> tuple[Path, dict[str, Any]]:
    authorization = build_authorization()
    digest = authorization["authorization_sha256"]
    path = output_root / f"{CAMPAIGN_ID}-authorization-{digest}.json"
    if path.exists() and _read_json(path) != authorization:
        raise PortfolioSuccessorActivationError(
            "content-addressed authorization has other content"
        )
    output_root.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(authorization, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    status = {
        "schema_version": SCHEMA_VERSION,
        "campaign_id": CAMPAIGN_ID,
        "authorization_sha256": digest,
        "status": "AUTHORIZED_PENDING_INSPECTION",
        "inspected": False,
        "candidate_preregistration_permitted": False,
        "provider_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
    }
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(
        json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return path, authorization


def load_authorization(path: Path) -> dict[str, Any]:
    value = _read_json(path)
    digest = value.get("authorization_sha256")
    if not (
        value.get("schema_version") == SCHEMA_VERSION
        and value.get("campaign_id") == CAMPAIGN_ID
        and isinstance(digest, str)
        and digest == _self_hash(value, "authorization_sha256")
        and path.name == f"{CAMPAIGN_ID}-authorization-{digest}.json"
    ):
        raise PortfolioSuccessorActivationError(
            "successor authorization was mutated or renamed"
        )
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("freeze")
    subparsers.add_parser("status")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, authorization = write_authorization()
            result: dict[str, Any] = {
                "path": str(path.relative_to(PROJECT_ROOT)),
                **authorization,
            }
        else:
            result = _read_json(DEFAULT_STATUS)
    except (PortfolioSuccessorActivationError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
