"""Independently inspect catalyst recency/classification and official halt joins."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import sys
from collections import Counter
from collections.abc import Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from champion_input_fidelity import (
    DATASET_ID,
    SOURCE_DATASET_ID,
    _private_result_path,
    _private_selection_path,
    _sha256_file,
    _sha256_json,
    _source_paths,
)
from historical_store import HistoricalDayStore
from learning_data import LearningDataError, load_frozen_dataset_contract
from nasdaq_halts import halt_overlaps, parse_halt_html
from primary_catalyst_evidence import (
    classify_primary_catalyst,
    parse_submission_documents,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-19-champion-input-fidelity-inspection.json"
)


class ChampionInputInspectionError(RuntimeError):
    """Independent reconstruction found an evidence mismatch."""


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise ChampionInputInspectionError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ChampionInputInspectionError(f"{path} must contain an object")
    return value


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def _rebuild_catalysts(
    private: Mapping[str, Any], store: HistoricalDayStore
) -> tuple[dict[str, int], int]:
    counts: Counter[str] = Counter()
    submission_hashes: set[str] = set()
    source_sec = _read_gzip(_source_paths(store.root)["sec"])
    source_filings = {
        (
            str(record["date"]),
            str(record["symbol"]),
            str(filing["accession"]),
        ): filing
        for record in source_sec["records"]
        for filing in record["filings"]
    }
    for record in private["catalyst_records"]:
        prior_close = datetime.fromisoformat(str(record["prior_session_close_et"]))
        cutoff = datetime.fromisoformat(str(record["cutoff_et"]))
        rebuilt: list[dict[str, Any]] = []
        for filing in record["filings"]:
            source_filing = source_filings.get(
                (
                    str(record["date"]),
                    str(record["symbol"]),
                    str(filing["accession"]),
                )
            )
            if source_filing is None:
                raise ChampionInputInspectionError(
                    "source SEC filing is missing from frozen parent evidence"
                )
            url = str(filing["complete_submission_url"])
            name = hashlib.sha256(url.encode("utf-8")).hexdigest() + ".txt"
            content = (
                store.root / "_sources" / "sec" / "complete-submissions" / name
            ).read_text(encoding="utf-8", errors="replace")
            content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
            if content_hash != filing["complete_submission_sha256"]:
                raise ChampionInputInspectionError(
                    "complete SEC submission hash mismatch"
                )
            submission_hashes.add(content_hash)
            accepted = datetime.fromisoformat(str(filing["accepted_at"]))
            evidence = classify_primary_catalyst(
                source_filing,
                parse_submission_documents(content),
                accepted_after_prior_close=prior_close < accepted <= cutoff,
            )
            if evidence != filing["classification"]:
                raise ChampionInputInspectionError("catalyst classification mismatch")
            rebuilt.append({**filing, "classification": evidence})
            counts["filings_classified"] += 1
            counts[f"filing_{evidence['disposition'].lower()}"] += 1
        recent = [
            row
            for row in rebuilt
            if row["classification"]["accepted_after_prior_close"] is True
        ]
        conflict = any(
            row["classification"]["dilution_or_negative_conflict"] is True
            for row in recent
        )
        positive = [
            row
            for row in recent
            if row["classification"]["verified_positive_direction"] is True
        ]
        material = [
            row
            for row in recent
            if row["classification"]["verified_material_catalyst"] is True
        ]
        disposition = (
            "REJECT_CONFLICT"
            if conflict
            else "VERIFIED_POSITIVE_PRIMARY"
            if positive
            else "VERIFIED_MATERIAL_DIRECTION_UNRESOLVED"
            if material
            else "UNRESOLVED_RECENT_PRIMARY"
            if recent
            else "NO_RECENT_PRIMARY"
        )
        verified = bool(material) and not conflict
        expected = {
            "disposition": disposition,
            "verified_material_catalyst": verified,
            "verified_positive_direction": bool(positive) and not conflict,
            "dilution_or_negative_conflict": conflict,
        }
        if any(record.get(field) != value for field, value in expected.items()):
            raise ChampionInputInspectionError("pair catalyst disposition mismatch")
        counts["selected_pairs"] += 1
        counts[f"pair_{disposition.lower()}"] += 1
        counts["verified_material_catalysts"] += int(verified)
        counts["verified_positive_directions"] += int(bool(positive) and not conflict)
        counts["conflict_pairs"] += int(conflict)
        day_document = store.load(str(record["symbol"]), str(record["date"]))
        if day_document is None or not any(
            context.get("kind") == "champion_primary_catalyst_classification"
            and context.get("provider") == "sec"
            and context.get("payload", {}).get("dataset_id") == DATASET_ID
            for context in day_document["contexts"]
        ):
            raise ChampionInputInspectionError("canonical catalyst context is missing")
    return dict(sorted(counts.items())), len(submission_hashes)


def _rebuild_halts(
    private: Mapping[str, Any], store: HistoricalDayStore
) -> tuple[dict[str, int], dict[str, int]]:
    counts: Counter[str] = Counter()
    by_day: dict[str, list[dict[str, Any]]] = {}
    for day, source in private["halt_sources"].items():
        path = Path(str(source["cache_path"]))
        if not path.is_relative_to(store.root):
            raise ChampionInputInspectionError("halt cache escaped historical root")
        payload = json.loads(path.read_text(encoding="utf-8"))
        records = parse_halt_html(str(payload["result"]))
        if len(records) != int(source["record_count"]):
            raise ChampionInputInspectionError("official halt source count mismatch")
        by_day[str(day)] = records
        counts["halt_dates_collected"] += 1
        counts["official_halt_records"] += len(records)
    disposition_counts: Counter[str] = Counter()
    for record in private["halt_records"]:
        evaluated = record.get("evaluated") is True
        active: list[dict[str, Any]] = []
        if evaluated:
            start = datetime.fromisoformat(str(record["window"]["start_et"]))
            end = datetime.fromisoformat(str(record["window"]["end_et"]))
            if end - start != timedelta(seconds=10):
                raise ChampionInputInspectionError("halt evaluation window changed")
            active = [
                row
                for row in by_day[str(record["date"])]
                if halt_overlaps(row, str(record["symbol"]), start, end)
            ]
            counts["trigger_halt_windows_evaluated"] += 1
            counts["trigger_halt_risk_pairs"] += int(bool(active))
        if active != record["active_halts"] or record.get("halt_risk") != (
            bool(active) if evaluated else None
        ):
            raise ChampionInputInspectionError("halt interval decision mismatch")
        disposition_counts[
            "evaluated_no_halt"
            if evaluated and not active
            else "evaluated_halt"
            if evaluated
            else "no_clean_trigger"
        ] += 1
        day_document = store.load(str(record["symbol"]), str(record["date"]))
        if day_document is None or not any(
            context.get("kind") == "champion_official_halt_state"
            and context.get("provider") == "nasdaq"
            and context.get("payload", {}).get("dataset_id") == DATASET_ID
            for context in day_document["contexts"]
        ):
            raise ChampionInputInspectionError("canonical halt context is missing")
    return dict(sorted(counts.items())), dict(sorted(disposition_counts.items()))


def inspect(
    *, manifest_path: Path, env_path: Path, output_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    try:
        manifest = load_frozen_dataset_contract(manifest_path)
    except LearningDataError as exc:
        raise ChampionInputInspectionError(str(exc)) from exc
    if manifest.get("dataset_id") != DATASET_ID:
        raise ChampionInputInspectionError("unexpected champion-input dataset")
    selection = _read_gzip(_private_selection_path(store.root))
    if _sha256_json(selection) != manifest["selection_contract"].get(
        "private_selection_content_sha256"
    ):
        raise ChampionInputInspectionError("private selection hash mismatch")
    private_path = _private_result_path(store.root)
    private = _read_gzip(private_path)
    if private.get("manifest_sha256") != manifest["manifest_sha256"]:
        raise ChampionInputInspectionError("private evidence is not manifest-bound")
    catalyst_counts, unique_submissions = _rebuild_catalysts(private, store)
    halt_counts, halt_dispositions = _rebuild_halts(private, store)
    rebuilt_counts = dict(sorted({**catalyst_counts, **halt_counts}.items()))
    if rebuilt_counts != private.get("counts"):
        raise ChampionInputInspectionError("independent aggregate counts differ")
    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "inspected": True,
        "source_dataset_id": SOURCE_DATASET_ID,
        "inspection_implementation_sha256": _sha256_file(Path(__file__)),
        "classifier_implementation_sha256": _sha256_file(
            PROJECT_ROOT / "primary_catalyst_evidence.py"
        ),
        "halt_implementation_sha256": _sha256_file(PROJECT_ROOT / "nasdaq_halts.py"),
        "private_selection_content_sha256": _sha256_json(selection),
        "private_result_sha256": _sha256_file(private_path),
        "counts": rebuilt_counts,
        "unique_complete_sec_submissions": unique_submissions,
        "halt_dispositions": halt_dispositions,
        "checks": {
            "complete_submission_hashes_rebuilt": True,
            "prior_close_recency_rebuilt": True,
            "classifications_rebuilt": True,
            "official_halt_html_reparsed": True,
            "halt_intervals_rebuilt": True,
            "canonical_contexts_reconciled": True,
            "aggregate_counts_match_collector": True,
            "symbols_ciks_filings_and_raw_rows_public": False,
        },
        "claim_boundary": (
            "Independent pipeline-fidelity inspection on already-inspected dates; "
            "not alpha, confirmation, promotion, or a strategy variant."
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
    except (ChampionInputInspectionError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
