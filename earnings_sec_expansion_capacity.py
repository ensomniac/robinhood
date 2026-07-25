"""Freeze the no-calendar-wait 2012-2019 SEC PEAD capacity graph.

This module deliberately stops before opening an archive, market price, or
forward return.  The later collector must preserve this exact request graph,
derive events from as-filed facts, and keep the confirmation partition sealed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import outcome_exposure
import strategy_discovery
from historical_store import sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = "multi-strategy-portfolio-validation-v2"
FAMILY_ID = "earnings-positive-surprise-drift"
SUCCESSOR_ID = "earnings-positive-surprise-drift-v12-sec-2012-2019-expansion"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous" / SUCCESSOR_ID
ROLLING_AUTHORIZATION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/rolling_authorization/"
    "rolling-discovery-authorization-"
    "bedfb0ea139cfe806b1b780828e59f1d98e3a125c50ae2b8d86b662d5d5e5585.json"
)
ROLLING_STATUS = (
    PROJECT_ROOT / "strategy_tournament/v2/rolling_authorization/status.json"
)
ARCHIVE_ROOT = (
    "https://www.sec.gov/files/dera/data/"
    "financial-statement-and-notes-data-sets"
)
DATASET_PAGE = (
    "https://www.sec.gov/data-research/sec-markets-data/"
    "financial-statement-notes-data-sets"
)
DOCUMENTATION_URL = "https://www.sec.gov/dera/data/fsnds.pdf"
DEVELOPMENT_START = "2012-01-01"
DEVELOPMENT_END = "2017-12-15"
EMBARGO_START = "2017-12-16"
EMBARGO_END = "2018-01-07"
CONFIRMATION_START = "2018-01-08"
CONFIRMATION_END = "2019-12-31"
MINIMUM_UNIQUE_EVENTS = 100
MINIMUM_DEVELOPMENT_DATES = 50
MINIMUM_CONFIRMATION_DATES = 20
MINIMUM_SPACING_SECONDS = 0.20
PRIOR_EVALUATED_TRIALS = 32
ARCHIVES = tuple(
    (f"{year}q{quarter}", f"{year}q{quarter}_notes.zip")
    for year in range(2012, 2020)
    for quarter in range(1, 5)
)


class EarningsSecExpansionCapacityError(RuntimeError):
    """The frozen SEC expansion capacity boundary drifted."""


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def self_hash(value: Mapping[str, Any], field: str) -> str:
    return hashlib.sha256(
        canonical_bytes(
            {key: item for key, item in value.items() if key != field}
        )
    ).hexdigest()


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EarningsSecExpansionCapacityError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise EarningsSecExpansionCapacityError(
            f"{path} must contain an object"
        )
    return value


def _write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def _timestamp(value: str, name: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EarningsSecExpansionCapacityError(
            f"{name} must be ISO-8601"
        ) from exc
    if parsed.tzinfo is None:
        raise EarningsSecExpansionCapacityError(
            f"{name} must include a timezone"
        )
    return parsed.isoformat().replace("+00:00", "Z")


def _repo_path(path: Path) -> str:
    return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))


def requests() -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for ordinal, (quarter, filename) in enumerate(ARCHIVES):
        row: dict[str, Any] = {
            "ordinal": ordinal,
            "quarter": quarter,
            "method": "GET",
            "url": f"{ARCHIVE_ROOT}/{filename}",
            "filename": filename,
        }
        row["request_sha256"] = hashlib.sha256(
            canonical_bytes(row)
        ).hexdigest()
        result.append(row)
    return result


def rolling_authority() -> dict[str, Any]:
    strategy_discovery.require_committed(ROLLING_AUTHORIZATION)
    strategy_discovery.require_committed(ROLLING_STATUS)
    authorization = _read(ROLLING_AUTHORIZATION)
    status = _read(ROLLING_STATUS)
    if not (
        authorization.get("authorization_sha256")
        == self_hash(authorization, "authorization_sha256")
        and authorization.get("activation_policy")
        == "ROLLING_TERMINAL_REPLACEMENT"
        and int(
            authorization.get(
                "maximum_concurrent_active_mechanism_families", 0
            )
        )
        == 3
        and status.get("authorization_sha256")
        == authorization["authorization_sha256"]
        and status.get("state") == "ROLLING_DISCOVERY_AUTHORIZED"
        and status.get("valid") is True
        and int(status.get("active_family_count", -1))
        + int(status.get("available_slot_count", -1))
        == 3
        and status.get("selection_accounting_complete") is True
    ):
        raise EarningsSecExpansionCapacityError(
            "rolling discovery authority is not valid"
        )
    return {
        "authorization_path": _repo_path(ROLLING_AUTHORIZATION),
        "authorization_file_sha256": sha256_file(ROLLING_AUTHORIZATION),
        "authorization_sha256": authorization["authorization_sha256"],
        "status_path": _repo_path(ROLLING_STATUS),
        "status_file_sha256": sha256_file(ROLLING_STATUS),
        "activation_policy": "ROLLING_TERMINAL_REPLACEMENT",
        "calendar_wait_required": False,
        "existing_mechanism_family": True,
        "new_mechanism_family_slot_consumed": False,
    }


def build_contract(*, created_at: str) -> dict[str, Any]:
    for path in (
        Path(__file__).resolve(),
        PROJECT_ROOT / "earnings_sec_expansion_capacity_inspection.py",
        PROJECT_ROOT / "earnings_sec_legacy_capacity.py",
        PROJECT_ROOT / "earnings_sec_cover_identity.py",
    ):
        strategy_discovery.require_committed(path)
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-sec-expansion-capacity-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "SEC_EXPANSION_CAPACITY_CONTRACT_FROZEN",
        "created_at": _timestamp(created_at, "created_at"),
        "research_rationale": {
            "prior_exact_version_disposition": "REJECTED",
            "prior_failure_mode": (
                "no prior trial had more than seven OOF fills; zero trials "
                "passed DSR, Holm, PBO, or rolling-fold stability"
            ),
            "repair_on_prior_corpus_permitted": False,
            "new_evidence_purpose": (
                "test the unchanged reaction-confirmed PEAD mechanism on a "
                "larger, later, symbol-date-disjoint SEC denominator"
            ),
        },
        "rolling_authority": rolling_authority(),
        "provider": "U.S. SEC Financial Statement and Notes Data Sets",
        "dataset_page": DATASET_PAGE,
        "documentation_url": DOCUMENTATION_URL,
        "requests": requests(),
        "authorized_metadata_requests": len(ARCHIVES),
        "request_policy": {
            "exact_archives_only": True,
            "minimum_spacing_seconds": MINIMUM_SPACING_SECONDS,
            "retries_permitted": 0,
            "substitutions_permitted": 0,
            "interrupted_collection_may_reuse_hash_valid_zip": True,
            "unexpected_archive_members_fail_closed": True,
            "market_price_requests_permitted": 0,
        },
        "event_semantics": {
            "forms": ["10-Q"],
            "amendments_excluded": True,
            "as_filed_acceptance_timestamp_required": True,
            "same_accession_trading_symbol_required": True,
            "same_accession_common_stock_shares_cover_fact_required": True,
            "external_or_current_ticker_mapping_permitted": False,
            "eps_tags_priority": [
                "EarningsPerShareDiluted",
                "EarningsPerShareBasicAndDiluted",
                "EarningsPerShareBasic",
            ],
            "quarter_duration": 1,
            "consolidated_nondimensional_only": True,
            "current_period_matches_submission_period": True,
            "prior_comparison_days": [300, 430],
            "positive_yoy_eps_change_required": True,
            "maximum_events_per_accepted_date": 3,
            "event_rank": [
                "descending EPS change ratio",
                "descending EPS absolute change",
                "canonical ticker",
                "accession",
            ],
            "duplicate_accession_or_event_key_receives_zero_credit": True,
            "event_key": ["accepted", "ticker", "adsh"],
            "reaction_session": (
                "first fully observable regular session after SEC acceptance"
            ),
            "entry_observation_boundary": (
                "next regular-session open after completed reaction session"
            ),
        },
        "partitions": {
            "development": [DEVELOPMENT_START, DEVELOPMENT_END],
            "embargo": [EMBARGO_START, EMBARGO_END],
            "confirmation": [CONFIRMATION_START, CONFIRMATION_END],
            "maximum_hold_sessions": 5,
            "five_complete_session_embargo_required": True,
            "development_and_confirmation_filtered_against_global_exposure": True,
            "confirmation_market_outcomes_remain_untouched": True,
        },
        "capacity_thresholds": {
            "minimum_unique_events": MINIMUM_UNIQUE_EVENTS,
            "minimum_development_event_dates": MINIMUM_DEVELOPMENT_DATES,
            "minimum_confirmation_event_dates": MINIMUM_CONFIRMATION_DATES,
            "required_total_signals_formula": "max(50, frozen_power_target)",
            "required_confirmation_signals_formula": (
                "max(20, ceil(required_total_signals * 0.30))"
            ),
        },
        "selection_accounting": {
            "prior_evaluated_trials_same_mechanism": PRIOR_EVALUATED_TRIALS,
            "future_search_trial_cap": 32,
            "prior_trials_must_enter_deflated_sharpe_correction": True,
            "prior_trials_must_enter_family_overfitting_accounting": True,
            "confirmation_cannot_influence_trial_selection": True,
        },
        "implementation_hashes": {
            "earnings_sec_expansion_capacity.py": sha256_file(
                Path(__file__).resolve()
            ),
            "earnings_sec_expansion_capacity_inspection.py": sha256_file(
                PROJECT_ROOT / "earnings_sec_expansion_capacity_inspection.py"
            ),
            "earnings_sec_legacy_capacity.py": sha256_file(
                PROJECT_ROOT / "earnings_sec_legacy_capacity.py"
            ),
            "earnings_sec_cover_identity.py": sha256_file(
                PROJECT_ROOT / "earnings_sec_cover_identity.py"
            ),
            "outcome_exposure.py": sha256_file(
                PROJECT_ROOT / "outcome_exposure.py"
            ),
            "strategy_discovery.py": sha256_file(
                PROJECT_ROOT / "strategy_discovery.py"
            ),
        },
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "provider_requests_executed": 0,
        "metadata_rows_accessed": 0,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    value["contract_sha256"] = self_hash(value, "contract_sha256")
    return value


def freeze_contract(
    *, created_at: str, root: Path = DEFAULT_ROOT
) -> tuple[Path, dict[str, Any]]:
    value = build_contract(created_at=created_at)
    path = (
        root
        / "metadata-contract"
        / f"contract-{value['contract_sha256']}.json"
    )
    _write(path, value)
    return path, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Freeze the 2012-2019 SEC PEAD metadata capacity graph."
    )
    parser.add_argument("command", choices=("freeze-contract",))
    parser.add_argument("--created-at", required=True)
    args = parser.parse_args(argv)
    path, value = freeze_contract(created_at=args.created_at)
    print(
        json.dumps(
            {
                "path": _repo_path(path),
                "contract_sha256": value["contract_sha256"],
                "state": value["state"],
                "authorized_metadata_requests": value[
                    "authorized_metadata_requests"
                ],
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
