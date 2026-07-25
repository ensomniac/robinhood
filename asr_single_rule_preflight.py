"""Freeze the preserved ASR single-rule lane and test untouched capacity.

This module never reads market prices.  It rebuilds the independently inspected
SEC disclosure inventory, applies one outcome-blind daily ranking rule, and
tests whether a chronological confirmation reserve can still contain the
mandatory twenty globally untouched signals.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import asr_combined_capacity as combined
import asr_security_identity as tier2
import asr_tier1_security_identity as tier1
import outcome_exposure
import strategy_discovery
from historical_store import canonical_sha256, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = "multi-strategy-portfolio-validation-v2"
FAMILY_ID = "accelerated-share-repurchase-continuation"
STRATEGY_ID = "accelerated-share-repurchase-continuation-single-rule"
STRATEGY_VERSION = "1.0.0"
SELECTION_MODE = "preselected_primary"
TRIAL_ID = "asr-single-rule-v1"
MINIMUM_DEVELOPMENT_SIGNALS = 50
MINIMUM_CONFIRMATION_SIGNALS = 20
ATR_LOOKBACK_SESSIONS = 14
DATA_WARMUP_SESSIONS = 20
MAXIMUM_HOLD_SESSIONS = 5
EMBARGO_SESSIONS = 5
EASTERN = ZoneInfo("America/New_York")
CALENDAR_PATHS = (
    PROJECT_ROOT
    / "historical_batches/continuous_v2/session-calendar-2014-01-through-2022-12.json",
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1/session-calendar-2023-01-through-2026-07.json",
)
COMBINED_RESULT_PATH = combined.DEFAULT_ROOT / (
    "asr-combined-capacity-"
    "46c369e6692a455d7b1a64eb4785c50b4dffe238b1b524315c349bd23ed3c5f1.json"
)
COMBINED_INSPECTION_PATH = combined.DEFAULT_ROOT / "inspections" / (
    "asr-combined-"
    "571ce78a9557d91d44454fbfe685f2604a69c2cc9389c7f273bcae8ef2820d82.json"
)
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/asr/single-rule"
DEFAULT_CONTRACT_ROOT = DEFAULT_ROOT / "contracts"


class AsrSingleRulePreflightError(RuntimeError):
    """The outcome-blind ASR single-rule capacity graph is invalid."""


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise AsrSingleRulePreflightError("artifact escaped the repository") from exc


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AsrSingleRulePreflightError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise AsrSingleRulePreflightError(f"{path} must contain an object")
    return value


def _write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _timestamp(value: str, field: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise AsrSingleRulePreflightError(f"{field} is not an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise AsrSingleRulePreflightError(f"{field} needs a timezone")


def _authority(*, enforce_commit: bool) -> tuple[dict[str, Any], dict[str, Any]]:
    if enforce_commit:
        for path in (COMBINED_RESULT_PATH, COMBINED_INSPECTION_PATH):
            strategy_discovery.require_committed(path)
    result = _read(COMBINED_RESULT_PATH)
    inspection = _read(COMBINED_INSPECTION_PATH)
    if not (
        result.get("result_sha256")
        == "46c369e6692a455d7b1a64eb4785c50b4dffe238b1b524315c349bd23ed3c5f1"
        and result.get("result_sha256")
        == tier2.self_hash(result, "result_sha256")
        and result.get("independent_disclosure_signal_count") == 89
        and result.get("capacity_disposition")
        == "PRESERVED_LATER_SINGLE_RULE_RESEARCH"
        and result.get("development_search_contract_freeze_permitted") is False
        and result.get("market_outcomes_accessed") is False
        and inspection.get("inspection_sha256")
        == "571ce78a9557d91d44454fbfe685f2604a69c2cc9389c7f273bcae8ef2820d82"
        and inspection.get("inspection_sha256")
        == tier2.self_hash(inspection, "inspection_sha256")
        and inspection.get("result_sha256") == result["result_sha256"]
        and inspection.get("capacity_disposition")
        == "PRESERVED_LATER_SINGLE_RULE_RESEARCH"
        and inspection.get("outcome_access_permitted") is False
        and inspection.get("valid") is True
    ):
        raise AsrSingleRulePreflightError("combined ASR authority drifted")
    return result, inspection


def _verified_events() -> list[dict[str, Any]]:
    root = tier2.resolution.tier1.shared._store().root
    events: list[dict[str, Any]] = []
    for path, reader in (
        (combined.TIER2A_RESULT_PATH, tier2._read_gzip),
        (combined.TIER1_RESULT_PATH, tier1._read_gzip),
    ):
        public = combined._read(path)
        private = combined._private(public, root, reader)
        events.extend(dict(item) for item in private["verified_events"])
    if len(events) != 185:
        raise AsrSingleRulePreflightError("verified ASR agreement count drifted")
    return events


def _calendar(*, enforce_commit: bool) -> tuple[list[str], dict[str, str]]:
    dates: list[str] = []
    hashes: dict[str, str] = {}
    for path in CALENDAR_PATHS:
        if enforce_commit:
            strategy_discovery.require_committed(path)
        try:
            rows = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise AsrSingleRulePreflightError("session calendar cannot be read") from exc
        if (
            not isinstance(rows, list)
            or not rows
            or any(
                not isinstance(row, Mapping)
                or set(row) != {"date", "open_et", "close_et"}
                or row["open_et"] != "09:30"
                or row["close_et"] not in {"13:00", "16:00"}
                for row in rows
            )
        ):
            raise AsrSingleRulePreflightError("session calendar schema drifted")
        dates.extend(str(row["date"]) for row in rows)
        hashes[_repo_path(path)] = sha256_file(path)
    dates = sorted(set(dates))
    if (
        len(dates) != 3153
        or dates[0] != "2014-01-02"
        or dates[-1] != "2026-07-17"
    ):
        raise AsrSingleRulePreflightError("combined session calendar drifted")
    return dates, hashes


def _signal_inventory(
    calendar: Sequence[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    positions = {day: index for index, day in enumerate(calendar)}
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for event in _verified_events():
        key = (
            str(event["accession"]),
            str(event["ticker"]),
            str(event["acceptance_datetime_raw"]),
        )
        grouped.setdefault(key, []).append(event)
    if len(grouped) != 89:
        raise AsrSingleRulePreflightError("independent disclosure count drifted")
    disclosures: list[dict[str, Any]] = []
    for (accession, symbol, accepted_raw), agreements in grouped.items():
        try:
            accepted = datetime.strptime(accepted_raw, "%Y%m%d%H%M%S").replace(
                tzinfo=EASTERN
            )
        except ValueError as exc:
            raise AsrSingleRulePreflightError(
                "SEC acceptance timestamp is invalid"
            ) from exc
        accepted_day = accepted.date().isoformat()
        if accepted_day not in positions:
            raise AsrSingleRulePreflightError(
                "SEC acceptance date is absent from the session calendar"
            )
        accepted_index = positions[accepted_day]
        entry_index = (
            accepted_index
            if accepted.time() < time(9, 30)
            else accepted_index + 1
        )
        if (
            entry_index < DATA_WARMUP_SESSIONS
            or entry_index + MAXIMUM_HOLD_SESSIONS >= len(calendar)
        ):
            raise AsrSingleRulePreflightError(
                "ASR signal lacks frozen warmup or settlement sessions"
            )
        disclosures.append(
            {
                "accession": accession,
                "symbol": symbol,
                "acceptance_timestamp_et": accepted_raw,
                "entry_date": calendar[entry_index],
                "entry_index": entry_index,
                "agreement_count": len(agreements),
                "maximum_committed_notional_dollars": max(
                    int(item["committed_notional_dollars"]) for item in agreements
                ),
            }
        )
    disclosures.sort(
        key=lambda item: (
            item["entry_date"],
            item["acceptance_timestamp_et"],
            item["symbol"],
            item["accession"],
        )
    )
    by_entry_date: dict[str, dict[str, Any]] = {}
    for signal in disclosures:
        incumbent = by_entry_date.get(signal["entry_date"])
        rank = (
            -signal["maximum_committed_notional_dollars"],
            signal["acceptance_timestamp_et"],
            signal["symbol"],
            signal["accession"],
        )
        if incumbent is None:
            by_entry_date[signal["entry_date"]] = signal
            continue
        incumbent_rank = (
            -incumbent["maximum_committed_notional_dollars"],
            incumbent["acceptance_timestamp_et"],
            incumbent["symbol"],
            incumbent["accession"],
        )
        if rank < incumbent_rank:
            by_entry_date[signal["entry_date"]] = signal
    daily = sorted(
        by_entry_date.values(),
        key=lambda item: (
            item["entry_date"],
            item["acceptance_timestamp_et"],
            item["symbol"],
            item["accession"],
        ),
    )
    if len(daily) != 86:
        raise AsrSingleRulePreflightError("daily-ranked ASR capacity drifted")
    return disclosures, daily


def _exposed_pairs(
    records: Sequence[Mapping[str, Any]],
) -> tuple[set[str], dict[str, set[str]]]:
    wildcard_dates: set[str] = set()
    symbols_by_date: dict[str, set[str]] = {}
    for record in records:
        for day, symbol in outcome_exposure.scope_pairs(record["scope"]):
            if symbol == "*":
                wildcard_dates.add(day)
            else:
                symbols_by_date.setdefault(day, set()).add(symbol)
    return wildcard_dates, symbols_by_date


def _is_untouched(
    signal: Mapping[str, Any],
    calendar: Sequence[str],
    wildcard_dates: set[str],
    symbols_by_date: Mapping[str, set[str]],
) -> bool:
    index = int(signal["entry_index"])
    symbol = str(signal["symbol"])
    requested_dates = calendar[
        index - DATA_WARMUP_SESSIONS : index + MAXIMUM_HOLD_SESSIONS + 1
    ]
    return all(
        day not in wildcard_dates
        and symbol not in symbols_by_date.get(day, set())
        for day in requested_dates
    )


def build_inventory(
    *, enforce_commit: bool = True
) -> dict[str, Any]:
    authority, inspection = _authority(enforce_commit=enforce_commit)
    calendar, calendar_hashes = _calendar(enforce_commit=enforce_commit)
    disclosures, daily = _signal_inventory(calendar)
    development = daily[:MINIMUM_DEVELOPMENT_SIGNALS]
    if len(development) != MINIMUM_DEVELOPMENT_SIGNALS:
        raise AsrSingleRulePreflightError("ASR development capacity is below 50")
    final_development_exit_index = (
        int(development[-1]["entry_index"]) + MAXIMUM_HOLD_SESSIONS
    )
    first_confirmation_index = (
        final_development_exit_index + EMBARGO_SESSIONS + 1
    )
    confirmation = [
        item
        for item in daily[MINIMUM_DEVELOPMENT_SIGNALS:]
        if int(item["entry_index"]) >= first_confirmation_index
    ]
    records = outcome_exposure.read_index()
    wildcard_dates, symbols_by_date = _exposed_pairs(records)
    clean_confirmation = [
        item
        for item in confirmation
        if _is_untouched(
            item,
            calendar,
            wildcard_dates,
            symbols_by_date,
        )
    ]

    def compact(items: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "accession": item["accession"],
                "symbol": item["symbol"],
                "acceptance_timestamp_et": item["acceptance_timestamp_et"],
                "entry_date": item["entry_date"],
                "maximum_committed_notional_dollars": item[
                    "maximum_committed_notional_dollars"
                ],
            }
            for item in items
        ]

    embargo_dates = calendar[
        final_development_exit_index + 1 : first_confirmation_index
    ]
    if len(embargo_dates) != EMBARGO_SESSIONS:
        raise AsrSingleRulePreflightError("ASR evidence embargo drifted")
    return {
        "combined_result_sha256": authority["result_sha256"],
        "combined_inspection_sha256": inspection["inspection_sha256"],
        "verified_agreement_count": len(_verified_events()),
        "independent_disclosure_count": len(disclosures),
        "daily_ranked_signal_capacity": len(daily),
        "disclosure_inventory_sha256": canonical_sha256(compact(disclosures)),
        "daily_ranked_inventory_sha256": canonical_sha256(compact(daily)),
        "development_candidate_count": len(development),
        "development_inventory_sha256": canonical_sha256(compact(development)),
        "development_entry_date_range": [
            development[0]["entry_date"],
            development[-1]["entry_date"],
        ],
        "development_final_exit_date": calendar[final_development_exit_index],
        "embargo_dates": embargo_dates,
        "confirmation_candidate_count": len(confirmation),
        "confirmation_candidate_inventory_sha256": canonical_sha256(
            compact(confirmation)
        ),
        "untouched_confirmation_signal_capacity": len(clean_confirmation),
        "untouched_confirmation_inventory_sha256": canonical_sha256(
            compact(clean_confirmation)
        ),
        "untouched_confirmation_entry_dates": sorted(
            {str(item["entry_date"]) for item in clean_confirmation}
        ),
        "calendar_hashes": calendar_hashes,
        "calendar_session_count": len(calendar),
        "outcome_exposure_index_sha256": outcome_exposure.audit()["index_sha256"],
    }


def build_contract(
    *, created_at: str, enforce_commit: bool = True
) -> dict[str, Any]:
    _timestamp(created_at, "created_at")
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    inventory = build_inventory(enforce_commit=enforce_commit)
    implementation_files = [
        "asr_single_rule_preflight.py",
        "asr_combined_capacity.py",
        "asr_security_identity.py",
        "asr_tier1_security_identity.py",
        "outcome_exposure.py",
    ]
    implementation_hashes = {
        name: sha256_file(PROJECT_ROOT / name) for name in implementation_files
    }
    capacity_ready = (
        inventory["untouched_confirmation_signal_capacity"]
        >= MINIMUM_CONFIRMATION_SIGNALS
    )
    contract: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "outcome-blind-asr-single-rule-capacity-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "created_at": created_at,
        "selection_mode": SELECTION_MODE,
        "trial_id": TRIAL_ID,
        "trial_count": 1,
        "new_mechanism_family_slot_consumed": False,
        "source_disposition": "PRESERVED_LATER_SINGLE_RULE_RESEARCH",
        "source_lineage": {
            "combined_result_path": _repo_path(COMBINED_RESULT_PATH),
            "combined_result_sha256": inventory["combined_result_sha256"],
            "combined_inspection_path": _repo_path(COMBINED_INSPECTION_PATH),
            "combined_inspection_sha256": inventory[
                "combined_inspection_sha256"
            ],
        },
        "single_rule": {
            "direction": "long_only",
            "eligible_event": (
                "one independently verified executed ASR disclosure with "
                "committed notional, continuing mechanics, exact SEC acceptance "
                "time, and one unambiguous U.S.-listed common-equity identity"
            ),
            "disclosure_deduplication": (
                "one accession, ticker, and exact acceptance timestamp"
            ),
            "daily_ranking": (
                "highest maximum committed agreement notional, then earliest "
                "acceptance timestamp, ticker, and accession"
            ),
            "maximum_new_entries_per_day": 1,
            "entry": (
                "same-session open only when SEC acceptance is strictly before "
                "09:30 ET; otherwise next-session open"
            ),
            "minimum_prior_close_dollars": 10.0,
            "minimum_prior_20_session_median_dollar_volume": 50_000_000,
            "expected_gross_move": "completed ATR14 divided by prior close",
            "minimum_expected_gross_move_fraction": 0.005,
            "stop": "1.5 completed ATR14 below entry",
            "stop_atr_multiple": 1.5,
            "maximum_hold_sessions": MAXIMUM_HOLD_SESSIONS,
            "exit": "protective stop or fifth-session close",
            "same_interval_ambiguity": "stop_first",
            "missing_or_invalid_data": "missed_trade_no_substitution",
        },
        "costs_bps_per_side": [5, 10, 20],
        "evidence_policy": {
            "minimum_development_filled_signals": MINIMUM_DEVELOPMENT_SIGNALS,
            "minimum_confirmation_signals": MINIMUM_CONFIRMATION_SIGNALS,
            "dynamic_total_target": "max(50, frozen power target)",
            "dynamic_confirmation_target": (
                "max(20, ceil(required_total_signals * 0.30))"
            ),
            "embargo_sessions": EMBARGO_SESSIONS,
            "atr_lookback_sessions": ATR_LOOKBACK_SESSIONS,
            "requested_warmup_sessions": DATA_WARMUP_SESSIONS,
            "confirmation_scope": (
                "every warmup, entry, mark, stop, and exit date-symbol pair "
                "must be absent from the global outcome-exposure index"
            ),
            "development_allocation": (
                "first 50 chronological daily-ranked disclosure opportunities"
            ),
        },
        "inventory": inventory,
        "capacity_disposition": (
            "CAPACITY_READY"
            if capacity_ready
            else "INSUFFICIENT_POWER_CAPACITY"
        ),
        "implementation_hashes": implementation_hashes,
        "provider_requests": 0,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "provider_access_permitted": False,
        "market_price_access_permitted": False,
        "confirmation_outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "maturity_effect": "NONE",
        "state": "ASR_SINGLE_RULE_CAPACITY_PENDING_INSPECTION",
        "valid": True,
    }
    contract["contract_sha256"] = tier2.self_hash(contract, "contract_sha256")
    return contract


def freeze(
    *,
    created_at: str,
    output_root: Path = DEFAULT_CONTRACT_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    contract = build_contract(created_at=created_at, enforce_commit=enforce_commit)
    path = output_root / f"asr-single-rule-{contract['contract_sha256']}.json"
    _write(path, contract)
    return path, contract


def load_contract(path: Path) -> dict[str, Any]:
    contract = _read(path)
    expected = tier2.self_hash(contract, "contract_sha256")
    if (
        contract.get("contract_sha256") != expected
        or path.name != f"asr-single-rule-{expected}.json"
    ):
        raise AsrSingleRulePreflightError(
            "ASR single-rule contract was mutated or renamed"
        )
    return contract


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze_parser = subparsers.add_parser("freeze")
    freeze_parser.add_argument("--created-at", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        path, contract = freeze(created_at=args.created_at)
    except (
        AsrSingleRulePreflightError,
        outcome_exposure.OutcomeExposureError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(
        json.dumps(
            {**contract, "written": _repo_path(path)},
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
