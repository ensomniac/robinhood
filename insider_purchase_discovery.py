"""Freeze the clustered Form 4 open-market-purchase discovery family.

This transition is outcome blind.  It maps the already-inspected SEC event
inventory to the next full exchange-session open, reserves exact development
and confirmation pair scopes, and freezes the complete 32-trial family before
any market prices or forward returns may be accessed.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import subprocess
from bisect import bisect_right
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import outcome_exposure
from historical_store import HistoricalDayStore, canonical_sha256, sha256_file
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE, validate_hypothesis_contract


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = "multi-strategy-portfolio-validation-v2"
FAMILY_ID = "clustered-form4-open-market-purchase-continuation"
MECHANISM_FAMILY = "insider-open-market-purchase-continuation"
VERSION_ID = f"{FAMILY_ID}-v1"
EXPERIMENT_ID = f"{VERSION_ID}-development-search"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/form4_insider_purchase"
FAMILY_ROOT = DEFAULT_ROOT / "family"
MANIFEST_ROOT = FAMILY_ROOT / "capacity"
CONTRACT_ROOT = FAMILY_ROOT / "contract"
PRIVATE_NAMESPACE = Path("_derived/form4_insider_purchase_family")
CALENDAR_PATHS = (
    Path(
        "historical_batches/continuous_v2/"
        "session-calendar-2014-01-through-2022-12.json"
    ),
    Path(
        "historical_batches/challenger_orb_retest_v1/"
        "session-calendar-2023-01-through-2026-07.json"
    ),
)
DEVELOPMENT_START = "2018-01-02"
DEVELOPMENT_END = "2022-12-23"
DEVELOPMENT_FILING_END = "2022-12-15"
EMBARGO_START = "2022-12-27"
EMBARGO_END = "2023-01-03"
CONFIRMATION_START = "2023-01-04"
CONFIRMATION_END = "2025-01-10"
CONFIRMATION_FILING_START = "2023-01-04"
CONFIRMATION_FILING_END = "2024-12-31"
MINIMUM_PURCHASE_NOTIONAL = 50_000.0
MAXIMUM_HOLD_SESSIONS = 5
PARAMETER_GRID = {
    "maximum_hold_sessions": [3, 5],
    "maximum_prior_20_session_return_fraction": [-0.05, 0.0],
    "minimum_distinct_reporting_owners": [1, 2],
    "minimum_purchase_notional": [50_000.0, 250_000.0],
    "stop_atr14": [1.5, 2.0],
}


class InsiderPurchaseDiscoveryError(RuntimeError):
    """A predecessor, point-in-time event, or frozen family binding drifted."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _relative(path: Path) -> str:
    return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))


def _require_committed(path: Path) -> None:
    relative = _relative(path)
    tracked = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "--", relative],
        cwd=PROJECT_ROOT,
        capture_output=True,
    )
    clean = subprocess.run(
        ["git", "diff", "--quiet", "HEAD", "--", relative],
        cwd=PROJECT_ROOT,
    )
    if tracked.returncode or clean.returncode:
        raise InsiderPurchaseDiscoveryError(
            f"predecessor must be committed and unchanged: {relative}"
        )


def _load_inspection(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    supplied = value.get("artifact_sha256")
    content = {key: item for key, item in value.items() if key != "artifact_sha256"}
    expected = _hash(content)
    if not (
        supplied == expected
        and path.name.endswith(f"-{expected}.json")
        and value.get("artifact_kind")
        == "form4-insider-purchase-capacity-inspection"
        and value.get("state") == "FORM4_CAPACITY_READY"
        and value.get("valid") is True
        and value.get("market_prices_accessed") is False
        and value.get("forward_returns_accessed") is False
        and value.get("confirmation_accessed") is False
        and value.get("broker_actions") == 0
        and value.get("family_id") == FAMILY_ID
        and value.get("mechanism_family") == MECHANISM_FAMILY
        and value.get("version_id") == VERSION_ID
    ):
        raise InsiderPurchaseDiscoveryError(
            "Form 4 capacity inspection is not hash-valid and ready"
        )
    return value


def _load_calendar() -> list[str]:
    by_date: dict[str, dict[str, Any]] = {}
    for relative in CALENDAR_PATHS:
        path = PROJECT_ROOT / relative
        rows = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(rows, list) or not rows:
            raise InsiderPurchaseDiscoveryError(f"calendar is invalid: {relative}")
        for row in rows:
            day = str(row.get("date", ""))
            normalized = {
                "date": day,
                "open_et": str(row.get("open_et", "")),
                "close_et": str(row.get("close_et", "")),
            }
            if day in by_date and by_date[day] != normalized:
                raise InsiderPurchaseDiscoveryError(
                    f"calendar sources disagree on {day}"
                )
            by_date[day] = normalized
    dates = sorted(by_date)
    if (
        not dates
        or dates != sorted(set(dates))
        or DEVELOPMENT_START not in dates
        or CONFIRMATION_END not in dates
    ):
        raise InsiderPurchaseDiscoveryError("merged exchange calendar is incomplete")
    return dates


def _private_events(inspection: Mapping[str, Any]) -> list[dict[str, Any]]:
    binding = inspection.get("private_event_binding")
    if not isinstance(binding, Mapping):
        raise InsiderPurchaseDiscoveryError("private Form 4 event binding is missing")
    store = HistoricalDayStore.from_env()
    relative = Path(str(binding.get("relative_path", "")))
    if (
        relative.is_absolute()
        or ".." in relative.parts
        or binding.get("format") != "json.gz"
    ):
        raise InsiderPurchaseDiscoveryError("private Form 4 event binding is unsafe")
    path = store.root / relative
    if not path.is_file() or sha256_file(path) != binding.get("file_sha256"):
        raise InsiderPurchaseDiscoveryError("private Form 4 event file drifted")
    with gzip.open(path, "rt", encoding="utf-8") as source:
        value = json.load(source)
    if (
        not isinstance(value, list)
        or canonical_sha256(value) != binding.get("content_sha256")
    ):
        raise InsiderPurchaseDiscoveryError("private Form 4 event content drifted")
    return [dict(row) for row in value]


def _scope(events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    symbols_by_date: dict[str, set[str]] = defaultdict(set)
    for event in events:
        for day in event["scope_dates"]:
            symbols_by_date[str(day)].add(str(event["symbol"]))
    dates = sorted(symbols_by_date)
    if not dates:
        raise InsiderPurchaseDiscoveryError("event scope is empty")
    return outcome_exposure.validate_scope(
        {
            "dates": dates,
            "symbols_by_date": {
                day: sorted(symbols_by_date[day]) for day in dates
            },
        }
    )


def _map_event(
    raw: Mapping[str, Any], calendar: Sequence[str]
) -> dict[str, Any] | None:
    notional = float(raw["purchase_notional"])
    if notional < MINIMUM_PURCHASE_NOTIONAL:
        return None
    filing_date = str(raw["filing_date"])
    entry_index = bisect_right(calendar, filing_date)
    if entry_index + MAXIMUM_HOLD_SESSIONS > len(calendar):
        return None
    scope_dates = list(calendar[entry_index : entry_index + MAXIMUM_HOLD_SESSIONS])
    if len(scope_dates) != MAXIMUM_HOLD_SESSIONS:
        return None
    return {
        "symbol": str(raw["symbol"]),
        "issuer_cik": str(raw["issuer_cik"]),
        "filing_date": filing_date,
        "entry_date": scope_dates[0],
        "scope_dates": scope_dates,
        "purchase_notional": notional,
        "distinct_reporting_owners": int(raw["distinct_reporting_owners"]),
        "transaction_count": int(raw["transaction_count"]),
        "accession_numbers": list(raw["accession_numbers"]),
        "owner_ciks": list(raw["owner_ciks"]),
        "transaction_dates": list(raw["transaction_dates"]),
        "event_semantics": "ORIGINAL_FORM4_DIRECT_OPEN_MARKET_PURCHASE",
    }


def _aggregate_entry_events(
    events: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Combine every filing observable before the same symbol's next open."""

    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for event in events:
        grouped[(str(event["entry_date"]), str(event["symbol"]))].append(event)
    result: list[dict[str, Any]] = []
    for (entry_date, symbol), rows in sorted(grouped.items()):
        issuer_ciks = {str(row["issuer_cik"]) for row in rows}
        if len(issuer_ciks) != 1:
            # An as-filed symbol collision cannot be assigned a truthful issuer
            # identity at this stage and is therefore omitted outcome-blind.
            continue
        owner_ciks = sorted(
            {
                str(owner)
                for row in rows
                for owner in row["owner_ciks"]
            }
        )
        filing_dates = sorted({str(row["filing_date"]) for row in rows})
        scope_dates = list(rows[0]["scope_dates"])
        if any(list(row["scope_dates"]) != scope_dates for row in rows):
            raise InsiderPurchaseDiscoveryError(
                "same-entry Form 4 event scopes disagree"
            )
        result.append(
            {
                "symbol": symbol,
                "issuer_cik": next(iter(issuer_ciks)),
                "filing_date": filing_dates[-1],
                "filing_dates": filing_dates,
                "entry_date": entry_date,
                "scope_dates": scope_dates,
                "purchase_notional": sum(
                    float(row["purchase_notional"]) for row in rows
                ),
                "distinct_reporting_owners": len(owner_ciks),
                "transaction_count": sum(
                    int(row["transaction_count"]) for row in rows
                ),
                "accession_numbers": sorted(
                    {
                        str(accession)
                        for row in rows
                        for accession in row["accession_numbers"]
                    }
                ),
                "owner_ciks": owner_ciks,
                "transaction_dates": sorted(
                    {
                        str(day)
                        for row in rows
                        for day in row["transaction_dates"]
                    }
                ),
                "event_semantics": (
                    "ORIGINAL_FORM4_DIRECT_OPEN_MARKET_PURCHASE"
                ),
            }
        )
    return result


def derive_inventory(
    inspection_path: Path,
) -> dict[str, Any]:
    inspection = _load_inspection(inspection_path)
    calendar = _load_calendar()
    mapped_rows = [
        event
        for raw in _private_events(inspection)
        if (event := _map_event(raw, calendar)) is not None
    ]
    mapped = _aggregate_entry_events(mapped_rows)
    development = [
        event
        for event in mapped
        if DEVELOPMENT_START <= event["entry_date"] <= DEVELOPMENT_END
        and event["filing_date"] <= DEVELOPMENT_FILING_END
    ]
    proposed_confirmation = [
        event
        for event in mapped
        if CONFIRMATION_START <= event["entry_date"] <= CONFIRMATION_END
        and CONFIRMATION_FILING_START
        <= event["filing_date"]
        <= CONFIRMATION_FILING_END
    ]
    exposure_records = outcome_exposure.read_index()
    exposed_pairs: set[tuple[str, str]] = set()
    wildcard_dates: set[str] = set()
    for record in exposure_records:
        for day, symbol in outcome_exposure.scope_pairs(record["scope"]):
            if symbol == "*":
                wildcard_dates.add(day)
            else:
                exposed_pairs.add((day, symbol))
    confirmation: list[dict[str, Any]] = []
    contaminated_confirmation: list[dict[str, Any]] = []
    for event in proposed_confirmation:
        if any(
            day in wildcard_dates or (day, str(event["symbol"])) in exposed_pairs
            for day in event["scope_dates"]
        ):
            contaminated_confirmation.append(event)
        else:
            confirmation.append(event)
    development.sort(
        key=lambda row: (row["entry_date"], row["symbol"], row["issuer_cik"])
    )
    confirmation.sort(
        key=lambda row: (row["entry_date"], row["symbol"], row["issuer_cik"])
    )
    confirmation_scope = _scope(confirmation)
    outcome_exposure.assert_untouched(confirmation_scope, exposure_records)
    return {
        "schema_version": 1,
        "family_id": FAMILY_ID,
        "version_id": VERSION_ID,
        "capacity_inspection_sha256": inspection["artifact_sha256"],
        "calendar_sha256": _hash(
            {
                str(path): sha256_file(PROJECT_ROOT / path)
                for path in CALENDAR_PATHS
            }
        ),
        "outcome_exposure_index_sha256": sha256_file(
            outcome_exposure.DEFAULT_INDEX
        ),
        "selection": {
            "minimum_purchase_notional": MINIMUM_PURCHASE_NOTIONAL,
            "entry": "next_exchange_session_after_filing_date",
            "maximum_hold_sessions": MAXIMUM_HOLD_SESSIONS,
            "development_filing_end": DEVELOPMENT_FILING_END,
            "confirmation_filing_start": CONFIRMATION_FILING_START,
            "confirmation_filing_end": CONFIRMATION_FILING_END,
            "confirmation_pair_policy": (
                "all entry-through-five-session symbol pairs globally untouched"
            ),
        },
        "development_events": development,
        "confirmation_events": confirmation,
        "counts": {
            "mapped_notional_eligible_filing_events": len(mapped_rows),
            "mapped_entry_symbol_clusters": len(mapped),
            "development_events": len(development),
            "development_entry_dates": len(
                {row["entry_date"] for row in development}
            ),
            "development_symbols": len({row["symbol"] for row in development}),
            "proposed_confirmation_events": len(proposed_confirmation),
            "contaminated_confirmation_events": len(contaminated_confirmation),
            "confirmation_events": len(confirmation),
            "confirmation_entry_dates": len(
                {row["entry_date"] for row in confirmation}
            ),
            "confirmation_symbols": len({row["symbol"] for row in confirmation}),
        },
    }


def _write_private_inventory(value: Mapping[str, Any]) -> dict[str, Any]:
    content_sha256 = canonical_sha256(value)
    relative = (
        PRIVATE_NAMESPACE
        / str(value["capacity_inspection_sha256"])
        / f"inventory-{content_sha256}.json.gz"
    )
    path = HistoricalDayStore.from_env().root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        temporary = path.with_suffix(".json.gz.tmp")
        with gzip.open(temporary, "wt", encoding="utf-8") as destination:
            json.dump(value, destination, sort_keys=True, separators=(",", ":"))
        temporary.replace(path)
    with gzip.open(path, "rt", encoding="utf-8") as source:
        rebuilt = json.load(source)
    if canonical_sha256(rebuilt) != content_sha256:
        raise InsiderPurchaseDiscoveryError("private family inventory drifted")
    return {
        "storage": "LOCAL_HISTORICAL_DATA_ROOT",
        "relative_path": str(relative),
        "content_sha256": content_sha256,
        "file_sha256": sha256_file(path),
        "compression": "gzip",
        "format": "canonical-json",
    }


def _calendar_slice(start: str, end: str) -> list[str]:
    return [day for day in _load_calendar() if start <= day <= end]


def freeze_family(
    inspection_path: Path, created_at: str
) -> tuple[Path, Path, dict[str, Any]]:
    _require_committed(inspection_path)
    for relative in CALENDAR_PATHS:
        _require_committed(PROJECT_ROOT / relative)
    _require_committed(outcome_exposure.DEFAULT_INDEX)
    inventory = derive_inventory(inspection_path)
    counts = inventory["counts"]
    if (
        counts["development_events"] < 100
        or counts["confirmation_events"] < 20
        or counts["confirmation_entry_dates"] < 20
    ):
        raise InsiderPurchaseDiscoveryError(
            "pair-clean development or confirmation capacity is insufficient"
        )
    private_binding = _write_private_inventory(inventory)
    development_dates = _calendar_slice(DEVELOPMENT_START, DEVELOPMENT_END)
    embargo_dates = _calendar_slice(EMBARGO_START, EMBARGO_END)
    confirmation_dates = _calendar_slice(CONFIRMATION_START, CONFIRMATION_END)
    if len(embargo_dates) != 5:
        raise InsiderPurchaseDiscoveryError("family embargo is not five sessions")
    capacity_inspection = _load_inspection(inspection_path)
    evidence_paths = [
        _relative(inspection_path),
        *[str(path) for path in CALENDAR_PATHS],
        _relative(outcome_exposure.DEFAULT_INDEX),
    ]
    manifest_value = {
        "schema_version": 1,
        "dataset_id": f"dataset-{EXPERIMENT_ID}-capacity",
        "registered_at": created_at,
        "requested_dates": development_dates,
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "evidence_paths": evidence_paths,
            "inspected": True,
            "point_in_time_evidence": True,
            "form4_purchase_capacity": {
                "family_id": FAMILY_ID,
                "experiment_id": EXPERIMENT_ID,
                "capacity_inspection_path": _relative(inspection_path),
                "capacity_inspection_sha256": capacity_inspection[
                    "artifact_sha256"
                ],
                "private_inventory": private_binding,
                "counts": counts,
                "provider_requests": 0,
                "market_prices_accessed": False,
                "confirmation_access_permitted": False,
            },
        },
    }
    manifest_path, _manifest = freeze_dataset_contract(
        manifest_value, MANIFEST_ROOT
    )
    development_events = inventory["development_events"]
    confirmation_events = inventory["confirmation_events"]
    contract = {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "experiment_id": EXPERIMENT_ID,
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "strategy_id": FAMILY_ID,
        "created_at": created_at,
        "status": "INVENTED",
        "dataset_lane": "development",
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "mechanism": (
            "A disclosed, direct open-market purchase by an issuer officer or "
            "director may reveal informed demand; larger purchases and purchases "
            "clustered across insiders may support short continuation after the "
            "filing becomes public."
        ),
        "expected_holding_behavior": (
            "Enter the next full exchange-session open after the Form 4 filing "
            "date and hold at most five sessions under an ATR structural stop."
        ),
        "entry_rule": (
            "Require an original as-filed Form 4 direct open-market common-equity "
            "purchase, frozen purchase-notional and owner-count thresholds, prior "
            "close at least $10, prior 20-session median dollar volume at least "
            "$50 million, prior 20-session return no higher than the frozen limit, "
            "and ATR cost-floor feasibility; rank one issuer per entry date."
        ),
        "stop_rule": (
            "Stop 1.5 or 2.0 ATR14 below entry; missing, nonpositive, or invalid "
            "structural stops are rejected and same-session ambiguity is stop-first."
        ),
        "exit_rule": (
            "Exit at the stop or the close of the third or fifth session, with a "
            "five-session absolute maximum."
        ),
        "ranking_rule": (
            "More distinct reporting owners, then higher disclosed purchase "
            "notional, then lower prior 20-session return, then canonical symbol."
        ),
        "selection_rule": (
            "Evaluate every frozen trial through rolling-origin OOF account paths "
            "and apply the repository development-search winner rule exactly."
        ),
        "primary_outcome": (
            "Selection-adjusted chronological net account growth after "
            "5/10/20-bps per-side costs."
        ),
        "material_difference_rationale": (
            "This uses issuer-insider capital committed through executed direct "
            "open-market purchases reported on Form 4; it is distinct from retired "
            "issuer repurchase, activist 13D, earnings, and price-only families."
        ),
        "parameter_grid": PARAMETER_GRID,
        "development_dates": development_dates,
        "development_signal_dates": sorted(
            {row["entry_date"] for row in development_events}
        ),
        "embargo_dates": embargo_dates,
        "confirmation_dates": confirmation_dates,
        "confirmation_signal_dates": sorted(
            {row["entry_date"] for row in confirmation_events}
        ),
        "confirmation_signal_capacity": len(
            {row["entry_date"] for row in confirmation_events}
        ),
        "development_scope": _scope(development_events),
        "confirmation_scope": _scope(confirmation_events),
        "universe": {
            "development_symbol_count": counts["development_symbols"],
            "confirmation_symbol_count": counts["confirmation_symbols"],
            "identity": "as-filed SEC issuer CIK and symbol",
        },
        "universe_requirements": {
            "security_type": "long U.S. common or ordinary equity",
            "prior_close_minimum": 10.0,
            "prior_20_session_median_dollar_volume_minimum": 50_000_000,
            "identity": "point-in-time SEC issuer CIK and as-filed symbol",
        },
        "execution_assumptions": {
            "direction": "long_only",
            "entry": "next exchange-session open after filing date",
            "maximum_new_entries_per_family_per_day": 1,
            "maximum_hold_sessions": 5,
            "same_interval_ambiguity": "stop_first",
            "missing_data": "missed_trade_no_substitution",
            "missing_candidate_data_blocks_date": True,
            "minimum_expected_gross_to_primary_round_trip_cost": 5.0,
        },
        "falsification_criteria": {
            "selection_aware_gates": "DEVELOPMENT_SEARCH_RULE",
            "stress_growth": "positive at 20 bps per side",
            "stress_profit_factor": "at least 1.20",
            "drawdown": "at most 6R",
            "concentration": "positive without five best trades",
            "chronology": "both halves positive",
        },
        "falsifiers": {
            "mechanism": "fails selection-aware development",
            "confirmation": "exact frozen winner fails untouched evidence",
            "implementation": "any rule, capture, or hash violation",
        },
        "minimum_evidence": {
            "required_total_signals": "max(50, frozen power target)",
            "required_confirmation_signals": (
                "max(20, ceil(required_total_signals * 0.30))"
            ),
            "confidence": "one-sided stationary-bootstrap 90 percent",
        },
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "embargo_sessions": 5,
            "development_contamination": "explicit_training_only",
        },
        "costs_bps_per_side": [5, 10, 20],
        "capacity_policy": {"retire_below": 50, "fast_lane_at": 100},
        "capacity_manifest": _relative(manifest_path),
        "implementation_files": [
            "insider_purchase_discovery.py",
            "insider_purchase_plugin.py",
            "dense_strategy_runtime.py",
            "strategy_discovery.py",
        ],
        "plugin": {
            "module": "insider_purchase_plugin",
            "preflight": "preflight",
            "evaluate_development": "evaluate_development",
            "evaluate_confirmation": "evaluate_confirmation",
            "evaluate_production": "evaluate_production",
        },
        "contamination_risks": [
            (
                "Development outcomes may overlap prior inspected data and remain "
                "explicitly contaminated training only."
            ),
            (
                "Confirmation is reserved pair-by-pair through the maximum hold "
                "and must remain globally untouched until exact-winner freeze."
            ),
        ],
        "production_compatibility_risks": [
            "SEC filing availability and issuer identity must be fresh.",
            "Overnight positions require confirmed GTC protection and gap-risk sizing.",
            "As-filed symbols may need point-in-time broker symbol reconciliation.",
        ],
    }
    validated = validate_hypothesis_contract(contract)
    digest = _hash(validated)
    CONTRACT_ROOT.mkdir(parents=True, exist_ok=True)
    contract_path = CONTRACT_ROOT / f"contract-{digest}.json"
    rendered = json.dumps(validated, indent=2, sort_keys=True) + "\n"
    if contract_path.exists() and contract_path.read_text(encoding="utf-8") != rendered:
        raise InsiderPurchaseDiscoveryError("family contract content collision")
    if not contract_path.exists():
        temporary = contract_path.with_suffix(".json.tmp")
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(contract_path)
    return manifest_path, contract_path, counts


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inspection", type=Path)
    parser.add_argument("--created-at", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    manifest, contract, counts = freeze_family(args.inspection, args.created_at)
    print(
        json.dumps(
            {
                "state": "FAMILY_FROZEN",
                "capacity_manifest": _relative(manifest),
                "family_contract": _relative(contract),
                "counts": counts,
                "market_prices_accessed": False,
                "forward_returns_accessed": False,
                "confirmation_accessed": False,
                "broker_actions": 0,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
