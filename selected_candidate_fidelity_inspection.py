"""Independently inspect the frozen selected-candidate fidelity artifacts."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import statistics
import sys
from collections import Counter
from collections.abc import Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from historical_store import HistoricalDayStore
from learning_data import LearningDataError, load_frozen_dataset_contract
from selected_candidate_fidelity import (
    DATASET_ID,
    SEC_LOOKBACK_DAYS,
    SOURCE_DATASET_ID,
    _private_sec_path,
    _private_selection_path,
    _private_trigger_path,
    _sha256_file,
    _sha256_json,
)
from sip_trade_conditions import classify_trade_conditions


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-19-selected-candidate-fidelity-inspection.json"
)


class FidelityInspectionError(RuntimeError):
    """Raised when independent reconstruction finds an evidence mismatch."""


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise FidelityInspectionError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise FidelityInspectionError(f"{path} must contain an object")
    return value


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def _sec_counts(
    selection: Mapping[str, Any], sec: Mapping[str, Any], store: HistoricalDayStore
) -> tuple[dict[str, int], int]:
    pairs = {(str(row["date"]), str(row["symbol"])): row for row in selection["pairs"]}
    records = sec.get("records")
    if not isinstance(records, list) or len(records) != len(pairs):
        raise FidelityInspectionError("SEC record population is incomplete")
    counts: Counter[str] = Counter()
    urls: set[str] = set()
    seen: set[tuple[str, str]] = set()
    for record in records:
        if not isinstance(record, Mapping):
            raise FidelityInspectionError("SEC record is not an object")
        key = (str(record["date"]), str(record["symbol"]))
        pair = pairs.get(key)
        if pair is None or key in seen or record.get("cik") != pair.get("cik"):
            raise FidelityInspectionError(
                "SEC record identity differs from frozen CIK map"
            )
        seen.add(key)
        cutoff = datetime.fromisoformat(str(record["cutoff_et"]))
        start = cutoff - timedelta(days=SEC_LOOKBACK_DAYS, hours=9, minutes=35)
        filings = record.get("filings")
        if not isinstance(filings, list):
            raise FidelityInspectionError("SEC filings must be an array")
        counts["selected_pairs"] += 1
        counts["pairs_with_primary_filing"] += int(bool(filings))
        counts["pairs_with_material_primary_candidate"] += int(
            any(row.get("primary_catalyst_candidate") is True for row in filings)
        )
        for filing in filings:
            accepted = datetime.fromisoformat(str(filing["accepted_at"]))
            if not start <= accepted <= cutoff:
                raise FidelityInspectionError(
                    "SEC filing violates the information window"
                )
            url = str(filing["source_url"])
            name = hashlib.sha256(url.encode("utf-8")).hexdigest() + ".html"
            document_path = store.root / "_sources" / "sec" / "documents" / name
            content = document_path.read_bytes().decode("utf-8", errors="replace")
            observed_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
            if observed_hash != filing.get("content_sha256"):
                raise FidelityInspectionError("SEC primary-document hash mismatch")
            urls.add(url)
            counts["filings"] += 1
            counts[f"form_{filing['form']}"] += 1
            counts["material_primary_candidates"] += int(
                filing.get("primary_catalyst_candidate") is True
            )
            counts["item_2_02_candidates"] += int(
                "2.02" in filing.get("material_items", [])
            )
            counts["dilution_conflicts"] += int(filing.get("dilution_conflict") is True)
        day_document = store.load(key[1], key[0])
        if day_document is None or not any(
            context.get("kind") == "selected_candidate_primary_catalysts"
            and context.get("provider") == "sec"
            and context.get("payload", {}).get("dataset_id") == DATASET_ID
            for context in day_document["contexts"]
        ):
            raise FidelityInspectionError("canonical SEC context is missing")
    if seen != set(pairs):
        raise FidelityInspectionError("SEC pair coverage differs from selection")
    return dict(sorted(counts.items())), len(urls)


def _trigger_counts(
    triggers: Mapping[str, Any], store: HistoricalDayStore
) -> tuple[dict[str, int], dict[str, Any], dict[str, Any]]:
    records = triggers.get("records")
    if not isinstance(records, list) or len(records) != 325:
        raise FidelityInspectionError("clean-trigger population must contain 325 rows")
    seen: set[tuple[str, str]] = set()
    counts: Counter[str] = Counter()
    shifts: list[float] = []
    spread_fractions: list[float] = []
    for record in records:
        if not isinstance(record, Mapping):
            raise FidelityInspectionError("clean-trigger row is not an object")
        key = (str(record["date"]), str(record["symbol"]))
        if key in seen:
            raise FidelityInspectionError("duplicate clean-trigger identity")
        seen.add(key)
        opening_high = float(record["opening_high"])
        raw = record["first_raw_cross"]
        raw_decision = classify_trade_conditions(raw.get("tape"), raw.get("conditions"))
        if float(raw["price"]) <= opening_high:
            raise FidelityInspectionError("raw trigger did not cross opening high")
        counts["crossing_windows"] += 1
        counts["first_raw_cross_updates_bar_high"] += int(
            raw_decision.updates_minute_high_low
        )
        counts["first_raw_cross_is_clean"] += int(
            raw_decision.establishes_continuous_cross
        )
        counts["first_raw_cross_rejected"] += int(
            not raw_decision.establishes_continuous_cross
        )
        clean = record.get("clean_cross")
        if not isinstance(clean, Mapping):
            counts["no_clean_cross_in_crossing_minute"] += 1
            continue
        decision = classify_trade_conditions(clean.get("tape"), clean.get("conditions"))
        if (
            not decision.establishes_continuous_cross
            or float(clean["price"]) <= opening_high
        ):
            raise FidelityInspectionError(
                "claimed clean cross violates frozen semantics"
            )
        raw_at = datetime.fromisoformat(str(raw["observed_at_et"]))
        clean_at = datetime.fromisoformat(str(clean["observed_at_et"]))
        shift = (clean_at - raw_at).total_seconds()
        if (
            shift < 0
            or abs(shift - float(clean["shift_from_first_raw_cross_seconds"])) > 1e-6
        ):
            raise FidelityInspectionError("clean-cross timing shift is invalid")
        shifts.append(shift)
        counts["clean_crosses"] += 1
        snapshots = record.get("quote_snapshots")
        if not isinstance(snapshots, list):
            raise FidelityInspectionError("quote snapshots must be an array")
        counts["three_snapshot_windows"] += int(len(snapshots) == 3)
        basic = len(snapshots) == 3
        for snapshot in snapshots:
            target = datetime.fromisoformat(str(snapshot["target_at_et"]))
            observed = datetime.fromisoformat(str(snapshot["observed_at_et"]))
            age = (target - observed).total_seconds()
            if observed > target or abs(age - float(snapshot["age_seconds"])) > 1e-6:
                raise FidelityInspectionError("quote snapshot has lookahead or bad age")
            bid = float(snapshot["bid"])
            ask = float(snapshot["ask"])
            basic = basic and age <= 5 and bid > 0 and ask > bid
            if age <= 5 and bid > 0 and ask > bid:
                spread_fractions.append((ask - bid) / ((ask + bid) / 2))
        if basic != (record.get("basic_fresh_uncrossed") is True):
            raise FidelityInspectionError("basic quote-window decision mismatch")
        chase = basic and float(snapshots[-1]["ask"]) <= opening_high * 1.0015
        if chase != (record.get("final_ask_within_chase_cap") is True):
            raise FidelityInspectionError("chase-cap decision mismatch")
        counts["basic_fresh_uncrossed_windows"] += int(basic)
        counts["final_ask_within_chase_cap"] += int(chase)
        day_document = store.load(key[1], key[0])
        if day_document is None or not any(
            context.get("kind") == "selected_candidate_clean_trigger"
            and context.get("provider") == "alpaca"
            and context.get("payload", {}).get("dataset_id") == DATASET_ID
            for context in day_document["contexts"]
        ):
            raise FidelityInspectionError("canonical clean-trigger context is missing")
    shift_summary = {
        "count": len(shifts),
        "median_seconds": statistics.median(shifts),
        "maximum_seconds": max(shifts),
        "positive_shift_count": sum(value > 0 for value in shifts),
    }
    spread_summary = {
        "observation_count": len(spread_fractions),
        "median_fraction": statistics.median(spread_fractions),
        "median_percent": statistics.median(spread_fractions) * 100,
        "at_or_below_0_10_percent": sum(value <= 0.001 for value in spread_fractions),
        "at_or_below_0_08_percent": sum(value <= 0.0008 for value in spread_fractions),
    }
    return dict(sorted(counts.items())), shift_summary, spread_summary


def inspect(
    *, manifest_path: Path, env_path: Path, output_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    try:
        manifest = load_frozen_dataset_contract(manifest_path)
    except LearningDataError as exc:
        raise FidelityInspectionError(str(exc)) from exc
    if manifest.get("dataset_id") != DATASET_ID:
        raise FidelityInspectionError("unexpected fidelity dataset")
    selection = _read_gzip(_private_selection_path(store.root))
    if _sha256_json(selection) != manifest["selection_contract"].get(
        "private_selection_content_sha256"
    ):
        raise FidelityInspectionError("selection hash mismatch")
    sec_path = _private_sec_path(store.root)
    trigger_path = _private_trigger_path(store.root)
    sec = _read_gzip(sec_path)
    triggers = _read_gzip(trigger_path)
    for artifact in (sec, triggers):
        if artifact.get("manifest_sha256") != manifest["manifest_sha256"]:
            raise FidelityInspectionError("private artifact is not manifest-bound")
        if artifact.get("errors") != []:
            raise FidelityInspectionError("private artifact contains collection errors")
    sec_counts, unique_documents = _sec_counts(selection, sec, store)
    trigger_counts, shift_summary, spread_summary = _trigger_counts(triggers, store)
    if sec_counts != sec.get("counts") or trigger_counts != triggers.get("counts"):
        raise FidelityInspectionError("independent counts differ from collector")
    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "inspected": True,
        "inspection_implementation_sha256": _sha256_file(Path(__file__)),
        "condition_implementation_sha256": _sha256_file(
            PROJECT_ROOT / "sip_trade_conditions.py"
        ),
        "source_dataset_id": SOURCE_DATASET_ID,
        "selection_content_sha256": _sha256_json(selection),
        "private_sec_index_sha256": _sha256_file(sec_path),
        "private_trigger_index_sha256": _sha256_file(trigger_path),
        "sec_counts": sec_counts,
        "unique_sec_document_count": unique_documents,
        "clean_trigger_counts": trigger_counts,
        "clean_trigger_shift_summary": shift_summary,
        "clean_trigger_spread_summary": spread_summary,
        "checks": {
            "point_in_time_cik_population_exact": True,
            "sec_information_cutoffs_rebuilt": True,
            "primary_document_hashes_rebuilt": True,
            "canonical_contexts_reconciled": True,
            "clean_condition_decisions_rebuilt": True,
            "quote_no_lookahead_and_age_rebuilt": True,
            "aggregate_counts_match_collector": True,
            "symbols_ciks_filings_and_raw_rows_public": False,
        },
        "claim_boundary": (
            "Independent fidelity inspection on already-inspected dates; not alpha, "
            "confirmation, promotion, or a production rule change."
        ),
    }
    _write_json(output_path, result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    try:
        result = inspect(
            manifest_path=args.manifest,
            env_path=args.env_file,
            output_path=args.output,
        )
    except FidelityInspectionError as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
