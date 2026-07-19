"""Freeze and collect primary-catalyst classification plus official halt state.

The source corpus is already inspected, so this dataset is pipeline-fidelity
evidence only. It preserves the exact 389 selected pairs and never observes or
evaluates target-session returns.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import shutil
import sys
from collections import Counter
from collections.abc import Mapping
from datetime import UTC, date, datetime, time as wall_time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

from historical_concurrency import ordered_bounded_results
from historical_discovery import HistoricalDiscoveryError, SecClient, SecConfig
from historical_store import HistoricalDayStore, HistoricalStoreConfig, build_context
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)
from nasdaq_halts import NasdaqHaltClient, NasdaqHaltError, halt_overlaps
from primary_catalyst_evidence import (
    CLASSIFIER_VERSION,
    classify_primary_catalyst,
    parse_submission_documents,
)
from selected_candidate_fidelity import MINIMUM_FREE_BYTES


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-champion-input-fidelity-2026-07-19-v2"
SOURCE_DATASET_ID = "dataset-selected-candidate-fidelity-2026-07-19-v1"
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "selected_candidate_fidelity"
    / "manifests"
    / "dataset-selected-candidate-fidelity-2026-07-19-v1-38f74c64c6f3f86d3cefbe656efd075345c21e7fcc22329936e5c5e78c2a7dcd.json"
)
CALENDAR_PATH = (
    PROJECT_ROOT
    / "historical_batches"
    / "scanner_replay"
    / "session-calendar-2025-12-through-2026-06.json"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches" / "champion_input_fidelity" / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "champion_input_fidelity"
    / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-champion-input-fidelity.json"
)
EASTERN = ZoneInfo("America/New_York")


class ChampionInputFidelityError(RuntimeError):
    """A frozen input, source request, or evidence reconstruction failed."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise ChampionInputFidelityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ChampionInputFidelityError(f"{path} must contain an object")
    return value


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def _write_gzip(path: Path, value: Any) -> None:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as stream:
        stream.write(json.dumps(value, indent=2, sort_keys=True).encode("utf-8"))
        stream.write(b"\n")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(buffer.getvalue())
    os.replace(temporary, path)


def _timestamp_now() -> str:
    return datetime.now(UTC).isoformat()


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise ChampionInputFidelityError(
            f"public evidence path must be repository-relative: {path}"
        ) from exc


def _private_root(store_root: Path) -> Path:
    return store_root / "_derived" / "champion_input_fidelity" / DATASET_ID


def _private_selection_path(store_root: Path) -> Path:
    return _private_root(store_root) / "selection.json.gz"


def _private_result_path(store_root: Path) -> Path:
    return _private_root(store_root) / "evidence-index.json.gz"


def _source_paths(store_root: Path) -> dict[str, Path]:
    source_root = (
        store_root / "_derived" / "selected_candidate_fidelity" / SOURCE_DATASET_ID
    )
    return {
        "selection": source_root / "selection-and-cik-map.json.gz",
        "sec": source_root / "primary-catalyst-index.json.gz",
        "trigger": source_root / "clean-trigger-index.json.gz",
    }


def _load_calendar() -> list[str]:
    try:
        value = json.loads(CALENDAR_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ChampionInputFidelityError(f"cannot read calendar: {exc}") from exc
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ChampionInputFidelityError("session calendar is malformed")
    return value


def _prior_close(day: str, calendar: list[str]) -> datetime:
    try:
        position = calendar.index(day)
    except ValueError as exc:
        raise ChampionInputFidelityError(f"calendar lacks target {day}") from exc
    if position < 1:
        raise ChampionInputFidelityError(f"calendar lacks prior session for {day}")
    return datetime.combine(
        date.fromisoformat(calendar[position - 1]),
        wall_time(16, 0),
        tzinfo=EASTERN,
    )


def freeze_inputs(*, env_path: Path, output_root: Path) -> tuple[Path, dict[str, Any]]:
    store_root = HistoricalStoreConfig.from_env(env_path).root
    try:
        source_manifest = load_frozen_dataset_contract(SOURCE_MANIFEST)
    except LearningDataError as exc:
        raise ChampionInputFidelityError(str(exc)) from exc
    if source_manifest.get("dataset_id") != SOURCE_DATASET_ID:
        raise ChampionInputFidelityError("unexpected source dataset")
    paths = _source_paths(store_root)
    source_selection = _read_gzip(paths["selection"])
    source_sec = _read_gzip(paths["sec"])
    source_trigger = _read_gzip(paths["trigger"])
    if (
        source_sec.get("status") != "SEC_COLLECTION_COMPLETE"
        or source_trigger.get("status") != "CLEAN_TRIGGER_INSPECTION_COMPLETE"
    ):
        raise ChampionInputFidelityError("source fidelity evidence is incomplete")
    calendar = _load_calendar()
    pairs = [
        {
            **dict(pair),
            "prior_session_close_et": _prior_close(
                str(pair["date"]), calendar
            ).isoformat(),
        }
        for pair in source_selection["pairs"]
    ]
    private = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "source_dataset_id": SOURCE_DATASET_ID,
        "source_manifest_sha256": source_manifest["manifest_sha256"],
        "selected_pair_count": len(pairs),
        "pairs": pairs,
    }
    private_path = _private_selection_path(store_root)
    if private_path.exists():
        if _sha256_json(_read_gzip(private_path)) != _sha256_json(private):
            raise ChampionInputFidelityError("private selection changed")
    else:
        _write_gzip(private_path, private)
    free_bytes = shutil.disk_usage(store_root).free
    if free_bytes < MINIMUM_FREE_BYTES:
        raise ChampionInputFidelityError("historical reserve is below 10 GiB")
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": _timestamp_now(),
        "requested_dates": list(source_manifest["requested_dates"]),
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(SOURCE_MANIFEST),
                "SELECTED_CANDIDATE_FIDELITY.md",
                "STRATEGY_LEARNING_EXECUTION_PLAN.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            "source_dataset_id": SOURCE_DATASET_ID,
            "source_manifest_sha256": source_manifest["manifest_sha256"],
            "selected_pair_count": len(pairs),
            "private_selection_content_sha256": _sha256_json(private),
            "source_artifacts": {
                name: _sha256_file(path) for name, path in sorted(paths.items())
            },
            "calendar_sha256": _sha256_file(CALENDAR_PATH),
            "symbols_ciks_and_filings_public": False,
        },
        "collection_contract": {
            "collector_sha256": _sha256_file(Path(__file__)),
            "classifier_version": CLASSIFIER_VERSION,
            "classifier_sha256": _sha256_file(
                PROJECT_ROOT / "primary_catalyst_evidence.py"
            ),
            "halt_client_sha256": _sha256_file(PROJECT_ROOT / "nasdaq_halts.py"),
            "sec_source": "SEC EDGAR complete submission text and issuer exhibits",
            "catalyst_recency": "after prior regular-session close through target 09:35 ET",
            "halt_source": "Nasdaq Trader official historical halt RPC",
            "halt_window": "condition-valid clean cross through final +10 second quote target",
            "raw_and_symbol_rows_public": False,
            "canonical_store_required": True,
            "source_corpus_already_inspected": True,
            "target_outcomes_allowed": False,
            "alpha_or_confirmation_claim_allowed": False,
        },
        "capacity_contract": {
            "minimum_free_bytes": MINIMUM_FREE_BYTES,
            "observed_free_bytes_at_freeze": free_bytes,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def _load_contract(
    manifest_path: Path, store_root: Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        manifest = load_frozen_dataset_contract(manifest_path)
    except LearningDataError as exc:
        raise ChampionInputFidelityError(str(exc)) from exc
    if manifest.get("dataset_id") != DATASET_ID:
        raise ChampionInputFidelityError("unexpected champion-input dataset")
    expected = manifest["collection_contract"]
    current = {
        "collector_sha256": _sha256_file(Path(__file__)),
        "classifier_sha256": _sha256_file(
            PROJECT_ROOT / "primary_catalyst_evidence.py"
        ),
        "halt_client_sha256": _sha256_file(PROJECT_ROOT / "nasdaq_halts.py"),
    }
    for field, digest in current.items():
        if expected.get(field) != digest:
            raise ChampionInputFidelityError(f"{field} differs from frozen contract")
    selection = _read_gzip(_private_selection_path(store_root))
    if _sha256_json(selection) != manifest["selection_contract"].get(
        "private_selection_content_sha256"
    ):
        raise ChampionInputFidelityError("private selection differs from manifest")
    return manifest, selection


def _submission_url(cik: str, filing: Mapping[str, Any]) -> str:
    accession_path = str(filing["accession"]).replace("-", "")
    return (
        f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_path}/"
        f"{filing['accession']}.txt"
    )


def collect(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, selection = _load_contract(manifest_path, store.root)
    source_paths = _source_paths(store.root)
    source_sec = _read_gzip(source_paths["sec"])
    source_trigger = _read_gzip(source_paths["trigger"])
    sec_cache = store.root / "_sources" / "sec"
    sec_client = SecClient(SecConfig.from_env(env_path, sec_cache, workers=4))

    unique_filings: dict[str, tuple[str, Mapping[str, Any]]] = {}
    for record in source_sec["records"]:
        cik = str(record["cik"])
        for filing in record["filings"]:
            url = _submission_url(cik, filing)
            unique_filings[url] = (cik, filing)

    def load_submission(
        item: tuple[str, tuple[str, Mapping[str, Any]]],
    ) -> tuple[str, str]:
        url, _ = item
        name = hashlib.sha256(url.encode("utf-8")).hexdigest() + ".txt"
        return url, sec_client.text(url, sec_cache / "complete-submissions" / name)

    submissions: dict[str, str] = {}
    for outcome in ordered_bounded_results(
        sorted(unique_filings.items()), load_submission, max_workers=4
    ):
        url, content = outcome.unwrap()
        submissions[url] = content

    pairs_by_key = {
        (str(pair["date"]), str(pair["symbol"])): pair for pair in selection["pairs"]
    }
    catalyst_records: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for source_record in source_sec["records"]:
        key = (str(source_record["date"]), str(source_record["symbol"]))
        pair = pairs_by_key[key]
        prior_close = datetime.fromisoformat(str(pair["prior_session_close_et"]))
        cutoff = datetime.fromisoformat(str(source_record["cutoff_et"]))
        classified: list[dict[str, Any]] = []
        for filing in source_record["filings"]:
            accepted = datetime.fromisoformat(str(filing["accepted_at"]))
            url = _submission_url(str(pair["cik"]), filing)
            content = submissions[url]
            evidence = classify_primary_catalyst(
                filing,
                parse_submission_documents(content),
                accepted_after_prior_close=prior_close < accepted <= cutoff,
            )
            classified.append(
                {
                    "accession": filing["accession"],
                    "accepted_at": filing["accepted_at"],
                    "form": filing["form"],
                    "items": filing["items"],
                    "complete_submission_url": url,
                    "complete_submission_sha256": hashlib.sha256(
                        content.encode("utf-8")
                    ).hexdigest(),
                    "classification": evidence,
                }
            )
            counts["filings_classified"] += 1
            counts[f"filing_{evidence['disposition'].lower()}"] += 1
        recent = [
            row
            for row in classified
            if row["classification"]["accepted_after_prior_close"] is True
        ]
        conflict = any(
            row["classification"]["dilution_or_negative_conflict"] is True
            for row in recent
        )
        positives = [
            row
            for row in recent
            if row["classification"]["verified_positive_direction"] is True
        ]
        material = [
            row
            for row in recent
            if row["classification"]["verified_material_catalyst"] is True
        ]
        if conflict:
            disposition = "REJECT_CONFLICT"
        elif positives:
            disposition = "VERIFIED_POSITIVE_PRIMARY"
        elif material:
            disposition = "VERIFIED_MATERIAL_DIRECTION_UNRESOLVED"
        elif recent:
            disposition = "UNRESOLVED_RECENT_PRIMARY"
        else:
            disposition = "NO_RECENT_PRIMARY"
        verified = bool(material) and not conflict
        counts["selected_pairs"] += 1
        counts[f"pair_{disposition.lower()}"] += 1
        counts["verified_material_catalysts"] += int(verified)
        counts["verified_positive_directions"] += int(bool(positives) and not conflict)
        counts["conflict_pairs"] += int(conflict)
        record = {
            "date": key[0],
            "symbol": key[1],
            "instrument_id": pair["instrument_id"],
            "cik": pair["cik"],
            "prior_session_close_et": prior_close.isoformat(),
            "cutoff_et": cutoff.isoformat(),
            "filings": classified,
            "disposition": disposition,
            "verified_material_catalyst": verified,
            "verified_positive_direction": bool(positives) and not conflict,
            "dilution_or_negative_conflict": conflict,
        }
        catalyst_records.append(record)
        store.merge(
            key[1],
            key[0],
            contexts=[
                build_context(
                    kind="champion_primary_catalyst_classification",
                    provider="sec",
                    observed_at=cutoff.isoformat(),
                    payload={"dataset_id": DATASET_ID, **record},
                    provenance={
                        "source_type": "SEC EDGAR complete submissions and exhibits",
                        "captured_at": _timestamp_now(),
                    },
                )
            ],
        )

    halt_client = NasdaqHaltClient(store.root / "_sources" / "nasdaq_halts")
    halt_days: dict[str, list[dict[str, Any]]] = {}
    halt_sources: dict[str, dict[str, Any]] = {}
    for day in manifest["requested_dates"]:
        records, source = halt_client.fetch_day(str(day))
        halt_days[str(day)] = records
        halt_sources[str(day)] = source
        counts["official_halt_records"] += len(records)
        counts["halt_dates_collected"] += 1
    trigger_by_key = {
        (str(row["date"]), str(row["symbol"])): row for row in source_trigger["records"]
    }
    halt_records: list[dict[str, Any]] = []
    for pair in selection["pairs"]:
        key = (str(pair["date"]), str(pair["symbol"]))
        trigger = trigger_by_key.get(key)
        if trigger is None or not isinstance(trigger.get("clean_cross"), Mapping):
            active: list[dict[str, Any]] = []
            evaluated = False
            risk: bool | None = None
            window = None
        else:
            start = datetime.fromisoformat(
                str(trigger["clean_cross"]["observed_at_et"])
            )
            end = start + timedelta(seconds=10)
            active = [
                row
                for row in halt_days[key[0]]
                if halt_overlaps(row, key[1], start, end)
            ]
            evaluated = True
            risk = bool(active)
            window = {"start_et": start.isoformat(), "end_et": end.isoformat()}
            counts["trigger_halt_windows_evaluated"] += 1
            counts["trigger_halt_risk_pairs"] += int(risk)
        record = {
            "date": key[0],
            "symbol": key[1],
            "instrument_id": pair["instrument_id"],
            "evaluated": evaluated,
            "window": window,
            "active_halts": active,
            "halt_risk": risk,
            "point_in_time_common_active_listing": True,
            "historical_long_tradability_proxy": evaluated and risk is False,
            "broker_specific_historical_tradability_reconstructed": False,
        }
        halt_records.append(record)
        observed_at = (
            window["end_et"]
            if window
            else datetime.combine(
                date.fromisoformat(key[0]), wall_time(9, 35), tzinfo=EASTERN
            ).isoformat()
        )
        store.merge(
            key[1],
            key[0],
            contexts=[
                build_context(
                    kind="champion_official_halt_state",
                    provider="nasdaq",
                    observed_at=observed_at,
                    payload={"dataset_id": DATASET_ID, **record},
                    provenance={
                        "source_type": "Nasdaq Trader historical halt records",
                        "captured_at": _timestamp_now(),
                    },
                )
            ],
        )
    private = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "COLLECTION_COMPLETE",
        "updated_at": _timestamp_now(),
        "counts": dict(sorted(counts.items())),
        "sec_client": sec_client.stats(),
        "halt_client": {
            "cache_hits": halt_client.cache_hits,
            "downloads": halt_client.downloads,
        },
        "halt_sources": halt_sources,
        "catalyst_records": catalyst_records,
        "halt_records": halt_records,
        "errors": [],
    }
    private_path = _private_result_path(store.root)
    _write_gzip(private_path, private)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": private["status"],
        "counts": private["counts"],
        "sec_client": private["sec_client"],
        "halt_client": private["halt_client"],
        "private_result_sha256": _sha256_file(private_path),
        "symbols_ciks_filings_and_raw_rows_public": False,
        "broker_specific_historical_tradability_reconstructed": False,
    }
    _write_json(public_status_path, public)
    return public


def inspect(
    *, manifest_path: Path, env_path: Path, public_result_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, selection = _load_contract(manifest_path, store.root)
    private_path = _private_result_path(store.root)
    private = _read_gzip(private_path)
    if private.get("manifest_sha256") != manifest["manifest_sha256"]:
        raise ChampionInputFidelityError("private result is not manifest-bound")
    counts = private.get("counts", {})
    complete = (
        private.get("status") == "COLLECTION_COMPLETE"
        and counts.get("selected_pairs") == selection["selected_pair_count"]
        and counts.get("halt_dates_collected") == len(manifest["requested_dates"])
        and counts.get("trigger_halt_windows_evaluated") == 325
        and private.get("errors") == []
    )
    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY" if complete else "INCOMPLETE",
        "inspected": complete,
        "claim_scope": "DEVELOPMENT_ONLY",
        "source_dataset_id": SOURCE_DATASET_ID,
        "counts": counts,
        "private_selection_content_sha256": manifest["selection_contract"][
            "private_selection_content_sha256"
        ],
        "private_result_sha256": _sha256_file(private_path),
        "findings": {
            "filing_presence_never_implies_positive_direction": True,
            "prior_close_recency_enforced": True,
            "official_halt_window_join_complete": counts.get(
                "trigger_halt_windows_evaluated"
            )
            == 325,
            "broker_specific_historical_tradability_available": False,
            "production_rule_change_earned": False,
        },
        "remaining_fidelity_gaps": [
            "non-SEC issuer events and attributed analyst actions need direct-source corroboration",
            "broker-specific historical tradability cannot be reconstructed and requires prospective execution qualification",
            "resistance, stop invalidation/noise, and exact market-alignment inputs remain unresolved",
        ],
        "claim_boundary": (
            "Primary-source and halt pipeline fidelity on already-inspected dates; "
            "not alpha, confirmation, promotion, or a strategy variant."
        ),
        "symbols_ciks_filings_and_raw_rows_public": False,
    }
    _write_json(public_result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("freeze", "collect", "inspect"):
        command = commands.add_parser(name)
        command.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
        command.add_argument("--manifest", type=Path)
        command.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
        command.add_argument("--status", type=Path, default=DEFAULT_PUBLIC_STATUS)
        command.add_argument("--result", type=Path, default=DEFAULT_PUBLIC_RESULT)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "freeze":
            path, result = freeze_inputs(
                env_path=args.env_file, output_root=args.output_root
            )
            output = {"manifest_path": str(path), **result}
        else:
            if args.manifest is None:
                raise ChampionInputFidelityError("--manifest is required")
            if args.command == "collect":
                output = collect(
                    manifest_path=args.manifest,
                    env_path=args.env_file,
                    public_status_path=args.status,
                )
            else:
                output = inspect(
                    manifest_path=args.manifest,
                    env_path=args.env_file,
                    public_result_path=args.result,
                )
    except (
        ChampionInputFidelityError,
        HistoricalDiscoveryError,
        LearningDataError,
        NasdaqHaltError,
        OSError,
        requests.RequestException,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
