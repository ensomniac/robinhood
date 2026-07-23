"""Freeze the user-authorized rolling strategy-discovery slot policy."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
CAMPAIGN_ID = "multi-strategy-portfolio-validation-v2"
AUTHORIZED_AT = "2026-07-23T00:00:00-04:00"
AUTHORIZATION_TEXT = (
    "We do not yet have a validated strategy and must not wait for a literal "
    "future date to continue strategy discovery on historical data. Replace "
    "the ISO-week activation wait with three rolling active-family slots; a "
    "terminal disposition releases its slot immediately. Preserve cumulative "
    "selection accounting and every evidence, confirmation, risk, protection, "
    "privacy, broker-confirmation, shadow, and live-validation gate."
)
POLICY = "ROLLING_TERMINAL_REPLACEMENT"
MAXIMUM_ACTIVE_FAMILIES = 3
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/rolling_authorization"
DEFAULT_STATUS = DEFAULT_ROOT / "status.json"
PRIOR_AUTHORIZATION = (
    PROJECT_ROOT
    / "strategy_tournament/successor_proposal/authorizations/"
    "multi-strategy-portfolio-validation-v2-authorization-"
    "6a7e27481ff80cd3e457d972c10347ea831801d6532fdc34b9e9fb0453c0fd6e.json"
)
TERMINAL_BINDINGS = {
    "multi-asset-etf-tsmom-v1": (
        PROJECT_ROOT
        / "strategy_tournament/v2/multi_asset_etf_tsmom/stage0/inspections/"
        "multi-asset-etf-tsmom-v1-result-"
        "3cabba283e1b9a94cae818c3c57d6c216ad42ecd57e289d4468ea220bd2319ee.json"
    ),
    "schedule-13d-activist-continuation-v1": (
        PROJECT_ROOT
        / "strategy_tournament/v2/schedule13d/validation/retirements/"
        "schedule-13d-activist-continuation-v1-development-retirement.json"
    ),
    "accelerated-share-repurchase-continuation-v1": (
        PROJECT_ROOT
        / "strategy_tournament/v2/asr/dispositions/"
        "accelerated-share-repurchase-continuation-v1-"
        "4395f72eecb5b1d37fa08475e270e7dd59fd4d05fe80339f98b0ec6d6f0b9b01.json"
    ),
}


class RollingDiscoveryAuthorizationError(RuntimeError):
    """The rolling-slot authorization or its terminal predecessors differ."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _self_hash(value: Mapping[str, Any], field: str) -> str:
    return hashlib.sha256(
        _canonical_bytes({key: item for key, item in value.items() if key != field})
    ).hexdigest()


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise RollingDiscoveryAuthorizationError(
            f"cannot hash authority input {path}: {exc}"
        ) from exc
    return digest.hexdigest()


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RollingDiscoveryAuthorizationError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise RollingDiscoveryAuthorizationError(f"{path} must contain an object")
    return value


def _path_text(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def _terminal_state(candidate_id: str, value: Mapping[str, Any]) -> bool:
    if candidate_id == "multi-asset-etf-tsmom-v1":
        return (
            value.get("inspection_kind") == "stage0-result-inspection"
            and value.get("stage0_survived") is False
            and value.get("valid") is True
            and value.get("maturity_effect") == "NONE"
            and value.get("broker_actions") == 0
        )
    if candidate_id == "schedule-13d-activist-continuation-v1":
        return (
            value.get("artifact_kind") == "development-retirement"
            and value.get("disposition") == "RETIRED_DEVELOPMENT"
            and value.get("maturity_effect") == "RETIRED_NOT_PILOT_READY"
            and value.get("broker_actions") == 0
        )
    return (
        value.get("artifact_kind") == "outcome-blind-capacity-source-retirement"
        and value.get("disposition") == "RETIRED_INSUFFICIENT_SOURCE_COMPLETENESS"
        and value.get("market_outcomes_accessed") is False
        and value.get("maturity_effect") == "RETIRED_NOT_PILOT_READY"
        and value.get("broker_actions") == 0
    )


def build_authorization() -> dict[str, Any]:
    prior = _read(PRIOR_AUTHORIZATION)
    if not (
        prior.get("campaign_id") == CAMPAIGN_ID
        and prior.get("authorization_sha256")
        == _self_hash(prior, "authorization_sha256")
        and prior.get("objective") == "FIRST_PILOT_READY_LIVE_STARTED"
    ):
        raise RollingDiscoveryAuthorizationError(
            "prior campaign authorization is invalid"
        )
    terminal: list[dict[str, Any]] = []
    for candidate_id, path in TERMINAL_BINDINGS.items():
        value = _read(path)
        if not _terminal_state(candidate_id, value):
            raise RollingDiscoveryAuthorizationError(
                f"prior active family is not terminal: {candidate_id}"
            )
        terminal.append(
            {
                "candidate_id": candidate_id,
                "path": str(path.relative_to(PROJECT_ROOT)),
                "file_sha256": _file_hash(path),
                "terminal": True,
            }
        )
    authorization: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "rolling-discovery-authorization",
        "campaign_id": CAMPAIGN_ID,
        "authorized_at": AUTHORIZED_AT,
        "authorization_source": "EXPLICIT_USER_MESSAGE",
        "authorization_text": AUTHORIZATION_TEXT,
        "supersedes_activation_throttle": "THREE_NEW_FAMILIES_PER_ISO_WEEK",
        "activation_policy": POLICY,
        "maximum_concurrent_active_mechanism_families": MAXIMUM_ACTIVE_FAMILIES,
        "terminal_predecessors": terminal,
        "active_family_count": 0,
        "released_slot_count": 3,
        "available_slot_count": 3,
        "selection_accounting": {
            "all_prior_trials_retained": True,
            "all_prior_dispositions_retained": True,
            "global_outcome_exposure_index_required": True,
            "lineage_attempts_included_in_multiple_testing": True,
        },
        "preservation_contract": {
            "failed_corpus_parameter_repair_allowed": False,
            "confirmation_reuse_allowed": False,
            "evidence_or_promotion_gate_weakening_allowed": False,
            "risk_protection_or_broker_gate_weakening_allowed": False,
            "maximum_parallel_outcome_active_families": 3,
        },
        "provider_access_permitted_before_inspection": False,
        "target_outcome_access_permitted_before_family_freeze": False,
        "broker_actions_permitted": False,
        "claim_limit": (
            "This authorization removes only the idle calendar delay. It does "
            "not itself freeze evidence, permit target outcomes, award maturity, "
            "or authorize broker action."
        ),
    }
    authorization["authorization_sha256"] = _self_hash(
        authorization, "authorization_sha256"
    )
    return authorization


def write_authorization(
    *,
    root: Path = DEFAULT_ROOT,
    status_path: Path = DEFAULT_STATUS,
) -> tuple[Path, dict[str, Any]]:
    authorization = build_authorization()
    digest = authorization["authorization_sha256"]
    path = root / f"rolling-discovery-authorization-{digest}.json"
    rendered = json.dumps(authorization, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") != rendered:
        raise RollingDiscoveryAuthorizationError(
            "content-addressed rolling authorization differs"
        )
    root.mkdir(parents=True, exist_ok=True)
    path.write_text(rendered, encoding="utf-8")
    status = {
        "schema_version": SCHEMA_VERSION,
        "campaign_id": CAMPAIGN_ID,
        "authorization_path": _path_text(path),
        "authorization_sha256": digest,
        "state": "PENDING_INDEPENDENT_INSPECTION",
        "activation_policy": POLICY,
        "available_slot_count": 0,
        "provider_access_permitted": False,
        "target_outcome_access_permitted": False,
        "broker_actions_permitted": False,
    }
    temporary = status_path.with_name(f".{status_path.name}.{os.getpid()}.tmp")
    status_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        temporary.write_text(
            json.dumps(status, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, status_path)
    finally:
        temporary.unlink(missing_ok=True)
    return path, authorization


def load_authorization(path: Path) -> dict[str, Any]:
    value = _read(path)
    digest = value.get("authorization_sha256")
    if not (
        value.get("schema_version") == SCHEMA_VERSION
        and value.get("artifact_kind") == "rolling-discovery-authorization"
        and value.get("campaign_id") == CAMPAIGN_ID
        and isinstance(digest, str)
        and digest == _self_hash(value, "authorization_sha256")
        and path.name == f"rolling-discovery-authorization-{digest}.json"
    ):
        raise RollingDiscoveryAuthorizationError(
            "rolling authorization was mutated or renamed"
        )
    return value


def load_ready_status(status_path: Path = DEFAULT_STATUS) -> dict[str, Any]:
    status = _read(status_path)
    path_text = status.get("authorization_path")
    if not isinstance(path_text, str):
        raise RollingDiscoveryAuthorizationError(
            "rolling authorization path is missing"
        )
    path = Path(path_text)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    authorization = load_authorization(path)
    if not (
        status.get("state") == "ROLLING_DISCOVERY_AUTHORIZED"
        and status.get("authorization_sha256")
        == authorization["authorization_sha256"]
        and status.get("activation_policy") == POLICY
        and status.get("available_slot_count") == 3
        and status.get("provider_access_permitted") is True
        and status.get("target_outcome_access_permitted") is False
        and status.get("broker_actions_permitted") is False
    ):
        raise RollingDiscoveryAuthorizationError(
            "rolling discovery authorization is not independently ready"
        )
    return status


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "status"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, authorization = write_authorization()
            result: dict[str, Any] = {
                "path": _path_text(path),
                **authorization,
            }
        else:
            result = _read(DEFAULT_STATUS)
    except (OSError, ValueError, RollingDiscoveryAuthorizationError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
