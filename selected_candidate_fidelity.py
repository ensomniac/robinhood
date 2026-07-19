"""Close primary-catalyst and clean-trigger gaps on a frozen replay corpus.

This development-only workflow consumes the exact 389 security-date pairs
selected by the inspected dynamic 09:35 scanner replay. It does not choose a
new universe, change a strategy rule, or make an alpha claim. Exact symbols,
CIKs, filings, documents, and row-level trigger evidence stay under
``LOCAL_HISTORICAL_DATA_ROOT``; only hashes and aggregate coverage are public.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import shutil
import statistics
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time as wall_time, timedelta
from pathlib import Path
from typing import Any

from historical_concurrency import ordered_bounded_results
from historical_discovery import (
    MATERIAL_ITEMS,
    SEC_SUBMISSIONS_ROOT,
    HistoricalDiscoveryError,
    SecClient,
    SecConfig,
    _accepted_at,
    _document_has_dilution,
    _filing_items,
    _submission_recent,
)
from historical_providers import (
    AlpacaConfig,
    AlpacaHistoricalClient,
    HistoricalProviderError,
)
from historical_service import LocalHistoricalClient, RecordingHistoricalClient
from historical_store import HistoricalDayStore, HistoricalStoreConfig, build_context
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)
from selected_candidate_join import (
    EASTERN,
    MINIMUM_FREE_BYTES,
    _RateGate,
    _load_private_selection,
    _precise_timestamp,
    _rate_interval,
    _retry,
    _select_quote_snapshots,
)
from sip_trade_conditions import (
    CONTINUOUS_CROSS_VERSION,
    RULE_VERSION,
    SOURCE_URL as CONDITION_SOURCE_URL,
    classify_trade_conditions,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-selected-candidate-fidelity-2026-07-19-v1"
SOURCE_DATASET_ID = "dataset-selected-candidate-join-2026-07-19-v1"
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "selected_candidate_join"
    / "manifests"
    / "dataset-selected-candidate-join-2026-07-19-v1-a46ac360d3d7578757dd7ae420b6a6abe3d820d7d3c4895b083cd576ca05fba4.json"
)
REFERENCE_ROOT = PROJECT_ROOT / "learning_runs" / "scanner_replay" / "reference"
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches" / "selected_candidate_fidelity" / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "selected_candidate_fidelity"
    / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-selected-candidate-fidelity.json"
)
SEC_LOOKBACK_DAYS = 4
RELEVANT_FORMS = frozenset({"8-K", "8-K/A", "6-K", "6-K/A"})


class SelectedCandidateFidelityError(RuntimeError):
    """A frozen-input, collection, or fidelity-contract failure."""


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


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SelectedCandidateFidelityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SelectedCandidateFidelityError(f"{path} must contain an object")
    return value


def _read_gzip(path: Path) -> Any:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            return json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise SelectedCandidateFidelityError(f"cannot read {path}: {exc}") from exc


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def _write_gzip_json(path: Path, value: Any) -> None:
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
        raise SelectedCandidateFidelityError(
            f"public evidence path must be repository-relative: {path}"
        ) from exc


def _private_root(store_root: Path, dataset_id: str = DATASET_ID) -> Path:
    return store_root / "_derived" / "selected_candidate_fidelity" / dataset_id


def _private_selection_path(store_root: Path) -> Path:
    return _private_root(store_root) / "selection-and-cik-map.json.gz"


def _private_sec_path(store_root: Path) -> Path:
    return _private_root(store_root) / "primary-catalyst-index.json.gz"


def _private_trigger_path(store_root: Path) -> Path:
    return _private_root(store_root) / "clean-trigger-index.json.gz"


def _point_in_time_cik_map(
    pairs: Sequence[Mapping[str, Any]], reference_root: Path
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_day: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for pair in pairs:
        by_day[str(pair["date"])].append(pair)
    mapped: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    for day in sorted(by_day):
        path = reference_root / f"{day}.json.gz"
        rows = _read_gzip(path)
        if not isinstance(rows, list):
            raise SelectedCandidateFidelityError(
                f"point-in-time reference snapshot is not an array: {path}"
            )
        by_identity = {
            (str(row.get("ticker") or ""), str(row.get("share_class_figi") or "")): row
            for row in rows
            if isinstance(row, Mapping)
        }
        for pair in by_day[day]:
            symbol = str(pair["symbol"])
            instrument_id = str(pair["instrument_id"])
            figi = instrument_id.partition(":")[2]
            row = by_identity.get((symbol, figi))
            match_basis = "ticker_and_share_class_figi"
            if not isinstance(row, Mapping):
                symbol_rows = [
                    item
                    for item in rows
                    if isinstance(item, Mapping) and item.get("ticker") == symbol
                ]
                if len(symbol_rows) == 1:
                    row = symbol_rows[0]
                    match_basis = "unique_ticker_in_dated_snapshot"
            if not isinstance(row, Mapping):
                raise SelectedCandidateFidelityError(
                    f"cannot map frozen scanner identity on {day}"
                )
            cik = str(row.get("cik") or "").lstrip("0")
            if not cik:
                raise SelectedCandidateFidelityError(
                    f"point-in-time source lacks CIK on {day}"
                )
            mapped.append(
                {
                    **dict(pair),
                    "cik": cik,
                    "issuer_name": str(row.get("name") or ""),
                    "cik_match_basis": match_basis,
                    "cik_source": {
                        "snapshot_path": _repo_path(path),
                        "snapshot_sha256": _sha256_file(path),
                        "last_updated_utc": row.get("last_updated_utc"),
                    },
                }
            )
        sources.append(
            {
                "date": day,
                "row_count": len(rows),
                "sha256": _sha256_file(path),
            }
        )
    return mapped, sources


def _load_source_selection(store_root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        manifest = load_frozen_dataset_contract(SOURCE_MANIFEST)
    except LearningDataError as exc:
        raise SelectedCandidateFidelityError(str(exc)) from exc
    if manifest.get("dataset_id") != SOURCE_DATASET_ID:
        raise SelectedCandidateFidelityError("unexpected selected-candidate source")
    selection = _load_private_selection(manifest, store_root)
    if len(selection.get("selected_pairs", [])) != 389:
        raise SelectedCandidateFidelityError("source selection must contain 389 pairs")
    return manifest, selection


def freeze_fidelity(
    *, env_path: Path, output_root: Path, reference_root: Path = REFERENCE_ROOT
) -> tuple[Path, dict[str, Any]]:
    store_root = HistoricalStoreConfig.from_env(env_path).root
    source_manifest, source_selection = _load_source_selection(store_root)
    pairs, reference_sources = _point_in_time_cik_map(
        source_selection["selected_pairs"], reference_root
    )
    private = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "source_dataset_id": SOURCE_DATASET_ID,
        "source_manifest_sha256": source_manifest["manifest_sha256"],
        "selected_pair_count": len(pairs),
        "unique_cik_count": len({str(row["cik"]) for row in pairs}),
        "pairs": pairs,
    }
    private_path = _private_selection_path(store_root)
    if private_path.exists():
        if _sha256_json(_read_gzip(private_path)) != _sha256_json(private):
            raise SelectedCandidateFidelityError("private CIK mapping changed")
    else:
        _write_gzip_json(private_path, private)
    free_bytes = shutil.disk_usage(store_root).free
    if free_bytes < MINIMUM_FREE_BYTES:
        raise SelectedCandidateFidelityError("historical reserve is below 10 GiB")
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
                "SELECTED_CANDIDATE_JOIN.md",
                "STRATEGY_LEARNING_EXECUTION_PLAN.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            "source_dataset_id": SOURCE_DATASET_ID,
            "source_manifest_sha256": source_manifest["manifest_sha256"],
            "selected_pair_count": len(pairs),
            "unique_cik_count": private["unique_cik_count"],
            "private_selection_content_sha256": _sha256_json(private),
            "reference_snapshots": reference_sources,
            "symbols_and_ciks_public": False,
        },
        "collection_contract": {
            "collector_sha256": _sha256_file(Path(__file__)),
            "sec_source": "SEC EDGAR submissions JSON and primary filing documents",
            "sec_forms": sorted(RELEVANT_FORMS),
            "sec_window": (
                f"{SEC_LOOKBACK_DAYS} calendar days before target through target 09:35 ET"
            ),
            "cik_mapping": (
                "dated Massive reference snapshot matched by ticker and share-class "
                "FIGI, with unique dated-ticker fallback when the source has no FIGI"
            ),
            "condition_source_url": CONDITION_SOURCE_URL,
            "condition_rule_version": RULE_VERSION,
            "continuous_cross_version": CONTINUOUS_CROSS_VERSION,
            "trigger_contract": (
                "first price above the frozen opening high whose conditions both "
                "update a minute high and establish a continuous regular-sale cross"
            ),
            "raw_and_symbol_rows_public": False,
            "canonical_store_required": True,
            "source_corpus_already_inspected": True,
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
        raise SelectedCandidateFidelityError(str(exc)) from exc
    if manifest.get("dataset_id") != DATASET_ID:
        raise SelectedCandidateFidelityError("unexpected fidelity dataset")
    if manifest["collection_contract"].get("collector_sha256") != _sha256_file(
        Path(__file__)
    ):
        raise SelectedCandidateFidelityError("collector differs from frozen contract")
    private = _read_gzip(_private_selection_path(store_root))
    if _sha256_json(private) != manifest["selection_contract"].get(
        "private_selection_content_sha256"
    ):
        raise SelectedCandidateFidelityError("private CIK map differs from manifest")
    return manifest, private


def _filing_candidates(
    payload: Mapping[str, Any], start: datetime, cutoff: datetime
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for row in _submission_recent(payload).values():
        form = str(row.get("form") or "")
        accepted = _accepted_at(row.get("acceptanceDateTime"))
        primary = str(row.get("primaryDocument") or "").strip()
        accession = str(row.get("accessionNumber") or "").strip()
        if (
            form not in RELEVANT_FORMS
            or accepted is None
            or not start <= accepted <= cutoff
            or not primary
            or not accession
        ):
            continue
        candidates.append(
            {
                "form": form,
                "accession": accession,
                "accepted_at": accepted.isoformat(),
                "filing_date": row.get("filingDate"),
                "report_date": row.get("reportDate"),
                "items": _filing_items(row.get("items")),
                "primary_document": primary,
            }
        )
    return sorted(candidates, key=lambda row: str(row["accepted_at"]))


def _filing_url(cik: str, filing: Mapping[str, Any]) -> str:
    accession = str(filing["accession"]).replace("-", "")
    return (
        f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/"
        f"{filing['primary_document']}"
    )


def collect_sec(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, selection = _load_contract(manifest_path, store.root)
    cache_root = store.root / "_sources" / "sec"
    config = SecConfig.from_env(env_path, cache_root, workers=4)
    client = SecClient(config)
    ciks = sorted({str(row["cik"]) for row in selection["pairs"]})

    def load_submission(cik: str) -> tuple[str, Mapping[str, Any]]:
        padded = cik.zfill(10)
        return cik, client.json(
            f"{SEC_SUBMISSIONS_ROOT}/CIK{padded}.json",
            cache_root / "submissions" / f"CIK{padded}.json",
        )

    submissions: dict[str, Mapping[str, Any]] = {}
    for outcome in ordered_bounded_results(
        ciks, load_submission, max_workers=config.workers
    ):
        cik, payload = outcome.unwrap()
        submissions[cik] = payload

    filings_by_pair: dict[tuple[str, str], list[dict[str, Any]]] = {}
    documents: dict[str, tuple[str, dict[str, Any]]] = {}
    for pair in selection["pairs"]:
        day = str(pair["date"])
        parsed = date.fromisoformat(day)
        cutoff = datetime.combine(parsed, wall_time(9, 35), tzinfo=EASTERN)
        start = datetime.combine(
            parsed - timedelta(days=SEC_LOOKBACK_DAYS),
            wall_time(0),
            tzinfo=EASTERN,
        )
        cik = str(pair["cik"])
        rows = _filing_candidates(submissions[cik], start, cutoff)
        for filing in rows:
            url = _filing_url(cik, filing)
            filing["source_url"] = url
            documents[url] = (cik, filing)
        filings_by_pair[(day, str(pair["symbol"]))] = rows

    def load_document(
        item: tuple[str, tuple[str, dict[str, Any]]],
    ) -> tuple[str, str, bool]:
        url, (_cik, filing) = item
        name = hashlib.sha256(url.encode("utf-8")).hexdigest() + ".html"
        content = client.text(url, cache_root / "documents" / name)
        return (
            url,
            hashlib.sha256(content.encode("utf-8")).hexdigest(),
            _document_has_dilution(content, filing["items"]),
        )

    document_evidence: dict[str, dict[str, Any]] = {}
    for outcome in ordered_bounded_results(
        sorted(documents.items()), load_document, max_workers=config.workers
    ):
        url, content_sha256, dilution = outcome.unwrap()
        document_evidence[url] = {
            "content_sha256": content_sha256,
            "dilution_conflict": dilution,
        }

    counts: Counter[str] = Counter()
    records: list[dict[str, Any]] = []
    for pair in selection["pairs"]:
        day = str(pair["date"])
        symbol = str(pair["symbol"])
        filings = filings_by_pair[(day, symbol)]
        enriched: list[dict[str, Any]] = []
        for filing in filings:
            evidence = document_evidence[filing["source_url"]]
            material_items = sorted(MATERIAL_ITEMS.intersection(filing["items"]))
            row = {
                **filing,
                **evidence,
                "material_items": material_items,
                "primary_catalyst_candidate": bool(material_items)
                or str(filing["form"]).startswith("6-K"),
                "verified_positive_catalyst": False,
                "classification_required": True,
            }
            enriched.append(row)
            counts["filings"] += 1
            counts[f"form_{filing['form']}"] += 1
            counts["material_primary_candidates"] += int(
                row["primary_catalyst_candidate"]
            )
            counts["item_2_02_candidates"] += int("2.02" in material_items)
            counts["dilution_conflicts"] += int(evidence["dilution_conflict"])
        counts["selected_pairs"] += 1
        counts["pairs_with_primary_filing"] += int(bool(enriched))
        counts["pairs_with_material_primary_candidate"] += int(
            any(row["primary_catalyst_candidate"] for row in enriched)
        )
        record = {
            "date": day,
            "symbol": symbol,
            "instrument_id": pair["instrument_id"],
            "cik": pair["cik"],
            "cutoff_et": datetime.combine(
                date.fromisoformat(day), wall_time(9, 35), tzinfo=EASTERN
            ).isoformat(),
            "filings": enriched,
            "verified_positive_catalyst": False,
            "evidence_boundary": (
                "A time-valid primary filing is discovery evidence; its positive "
                "direction and long-thesis materiality remain unclassified."
            ),
        }
        records.append(record)
        cutoff = datetime.combine(
            date.fromisoformat(day), wall_time(9, 35), tzinfo=EASTERN
        )
        context = build_context(
            kind="selected_candidate_primary_catalysts",
            provider="sec",
            observed_at=cutoff.isoformat(),
            payload={"dataset_id": DATASET_ID, **record},
            provenance={
                "source_type": "SEC EDGAR submissions and primary documents",
                "captured_at": _timestamp_now(),
            },
        )
        store.merge(symbol, day, contexts=[context])
    private = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "SEC_COLLECTION_COMPLETE",
        "updated_at": _timestamp_now(),
        "counts": dict(sorted(counts.items())),
        "unique_cik_count": len(ciks),
        "unique_document_count": len(documents),
        "sec_client": client.stats(),
        "records": records,
        "errors": [],
    }
    private_path = _private_sec_path(store.root)
    _write_gzip_json(private_path, private)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": private["status"],
        "counts": private["counts"],
        "unique_cik_count": len(ciks),
        "unique_document_count": len(documents),
        "sec_client": client.stats(),
        "private_sec_index_sha256": _sha256_file(private_path),
        "symbols_ciks_and_filings_public": False,
        "verified_positive_catalyst_count": 0,
        "classification_boundary": (
            "Primary-source presence is not positive-catalyst verification."
        ),
    }
    _write_json(public_status_path, public)
    return public


def _source_trigger_index(store_root: Path) -> dict[str, Any]:
    path = (
        store_root
        / "_derived"
        / "selected_candidate_join"
        / SOURCE_DATASET_ID
        / "trigger-index.json.gz"
    )
    value = _read_gzip(path)
    if not isinstance(value, dict):
        raise SelectedCandidateFidelityError("source trigger index is malformed")
    return value


def collect_clean_triggers(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, _selection = _load_contract(manifest_path, store.root)
    trigger_index = _source_trigger_index(store.root)
    triggers = [
        row
        for row in trigger_index.get("records", [])
        if isinstance(row, Mapping)
        and row.get("status") == "CROSSING_MINUTE_IDENTIFIED"
    ]
    local = LocalHistoricalClient(store, "alpaca", feed="sip", adjustment="raw")
    config = AlpacaConfig.optional_from_env(env_path)
    if config is None:
        raise SelectedCandidateFidelityError("Alpaca credentials are not configured")
    gate = _RateGate(_rate_interval(env_path))
    counts: Counter[str] = Counter()
    shifts: list[float] = []
    records: list[dict[str, Any]] = []
    with AlpacaHistoricalClient(config) as alpaca:
        recorder = RecordingHistoricalClient(alpaca, store)
        for index, trigger in enumerate(triggers, 1):
            symbol = str(trigger["symbol"])
            day = str(trigger["date"])
            minute = datetime.fromisoformat(
                str(trigger["first_crossing_minute_et"])
            ).astimezone(EASTERN)
            trades = local.fetch_trades(
                symbol, minute, minute + timedelta(minutes=1), use_rth=True
            )
            opening_high = float(trigger["opening_high"])
            price_crosses = [
                row for row in trades if float(row["price"]) > opening_high
            ]
            bar_cross = next(
                (
                    row
                    for row in price_crosses
                    if classify_trade_conditions(
                        row.get("tape"), row.get("conditions")
                    ).updates_minute_high_low
                ),
                None,
            )
            clean_cross = next(
                (
                    row
                    for row in price_crosses
                    if classify_trade_conditions(
                        row.get("tape"), row.get("conditions")
                    ).establishes_continuous_cross
                ),
                None,
            )
            if bar_cross is None:
                raise SelectedCandidateFidelityError(
                    "raw SIP conditions cannot reproduce the crossing bar high"
                )
            first_cross = price_crosses[0]
            first_decision = classify_trade_conditions(
                first_cross.get("tape"), first_cross.get("conditions")
            )
            counts["crossing_windows"] += 1
            counts["first_raw_cross_updates_bar_high"] += int(
                first_decision.updates_minute_high_low
            )
            counts["first_raw_cross_is_clean"] += int(
                first_decision.establishes_continuous_cross
            )
            counts["first_raw_cross_rejected"] += int(
                not first_decision.establishes_continuous_cross
            )
            record: dict[str, Any] = {
                "date": day,
                "symbol": symbol,
                "instrument_id": trigger["instrument_id"],
                "rank": trigger["rank"],
                "opening_high": opening_high,
                "crossing_minute_et": minute.isoformat(),
                "price_cross_count": len(price_crosses),
                "first_raw_cross": {
                    "observed_at_et": _precise_timestamp(first_cross).isoformat(),
                    "price": float(first_cross["price"]),
                    "conditions": first_cross.get("conditions"),
                    "tape": first_cross.get("tape"),
                    "decision": first_decision.__dict__,
                },
                "first_bar_eligible_cross_at_et": _precise_timestamp(
                    bar_cross
                ).isoformat(),
                "condition_rule_version": RULE_VERSION,
                "continuous_cross_version": CONTINUOUS_CROSS_VERSION,
            }
            if clean_cross is None:
                counts["no_clean_cross_in_crossing_minute"] += 1
                record.update(
                    {
                        "status": "NO_CLEAN_CONTINUOUS_CROSS_IN_MINUTE",
                        "clean_cross": None,
                        "quote_snapshots": [],
                    }
                )
            else:
                counts["clean_crosses"] += 1
                clean_at = _precise_timestamp(clean_cross)
                shift = (clean_at - _precise_timestamp(first_cross)).total_seconds()
                shifts.append(shift)
                quote_start = clean_at - timedelta(seconds=1)
                quote_end = clean_at + timedelta(seconds=11)
                try:
                    quotes = local.fetch_bid_ask_ticks(
                        symbol, quote_start, quote_end, use_rth=True
                    )
                except HistoricalProviderError as exc:
                    if exc.category != "local_cache_miss":
                        raise
                    quotes = []
                snapshots = _select_quote_snapshots(quotes, clean_at)
                if len(snapshots) != 3 or any(
                    float(row["age_seconds"]) > 5 for row in snapshots
                ):
                    _retry(
                        gate,
                        lambda: recorder.fetch_bid_ask_ticks(
                            symbol, quote_start, quote_end, use_rth=True
                        ),
                    )
                    quotes = local.fetch_bid_ask_ticks(
                        symbol, quote_start, quote_end, use_rth=True
                    )
                    snapshots = _select_quote_snapshots(quotes, clean_at)
                basic = len(snapshots) == 3 and all(
                    float(row["age_seconds"]) <= 5
                    and float(row["bid"]) > 0
                    and float(row["ask"]) > float(row["bid"])
                    for row in snapshots
                )
                chase = basic and float(snapshots[-1]["ask"]) <= opening_high * 1.0015
                counts["three_snapshot_windows"] += int(len(snapshots) == 3)
                counts["basic_fresh_uncrossed_windows"] += int(basic)
                counts["final_ask_within_chase_cap"] += int(chase)
                record.update(
                    {
                        "status": "CLEAN_CONTINUOUS_CROSS_IDENTIFIED",
                        "clean_cross": {
                            "observed_at_et": clean_at.isoformat(),
                            "price": float(clean_cross["price"]),
                            "size": int(clean_cross["size"]),
                            "exchange": clean_cross.get("exchange"),
                            "conditions": clean_cross.get("conditions"),
                            "tape": clean_cross.get("tape"),
                            "shift_from_first_raw_cross_seconds": shift,
                        },
                        "quote_snapshots": snapshots,
                        "basic_fresh_uncrossed": basic,
                        "final_ask_within_chase_cap": chase,
                    }
                )
            records.append(record)
            observed_at = (
                record.get("clean_cross", {}).get("observed_at_et")
                if isinstance(record.get("clean_cross"), Mapping)
                else (minute + timedelta(minutes=1)).isoformat()
            )
            store.merge(
                symbol,
                day,
                contexts=[
                    build_context(
                        kind="selected_candidate_clean_trigger",
                        provider="alpaca",
                        observed_at=str(observed_at),
                        payload={"dataset_id": DATASET_ID, **record},
                        provenance={
                            "source_type": "Alpaca historical SIP trades and quotes",
                            "captured_at": _timestamp_now(),
                            "condition_source_url": CONDITION_SOURCE_URL,
                        },
                    )
                ],
            )
            if index % 25 == 0 or index == len(triggers):
                print(f"clean-trigger inspection {index}/{len(triggers)}", flush=True)
    shift_summary = {
        "count": len(shifts),
        "median_seconds": statistics.median(shifts) if shifts else None,
        "maximum_seconds": max(shifts) if shifts else None,
        "positive_shift_count": sum(value > 0 for value in shifts),
    }
    private = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "CLEAN_TRIGGER_INSPECTION_COMPLETE",
        "updated_at": _timestamp_now(),
        "counts": dict(sorted(counts.items())),
        "shift_summary": shift_summary,
        "records": records,
        "errors": [],
    }
    private_path = _private_trigger_path(store.root)
    _write_gzip_json(private_path, private)
    prior = _read_object(public_status_path) if public_status_path.exists() else {}
    public = {
        **prior,
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": private["status"],
        "clean_trigger_counts": private["counts"],
        "clean_trigger_shift_summary": shift_summary,
        "private_trigger_index_sha256": _sha256_file(private_path),
        "symbols_ciks_and_filings_public": False,
        "raw_rows_public": False,
    }
    _write_json(public_status_path, public)
    return public


def inspect_fidelity(
    *, manifest_path: Path, env_path: Path, public_result_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, selection = _load_contract(manifest_path, store.root)
    sec = _read_gzip(_private_sec_path(store.root))
    triggers = _read_gzip(_private_trigger_path(store.root))
    if (
        sec.get("manifest_sha256") != manifest["manifest_sha256"]
        or triggers.get("manifest_sha256") != manifest["manifest_sha256"]
    ):
        raise SelectedCandidateFidelityError("private evidence is not manifest-bound")
    sec_counts = dict(sec["counts"])
    trigger_counts = dict(triggers["counts"])
    complete = (
        int(sec_counts.get("selected_pairs", 0))
        == int(selection["selected_pair_count"])
        and int(trigger_counts.get("crossing_windows", 0)) == 325
        and not sec.get("errors")
        and not triggers.get("errors")
    )
    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY" if complete else "INCOMPLETE",
        "inspected": complete,
        "claim_scope": "DEVELOPMENT_ONLY",
        "source_dataset_id": SOURCE_DATASET_ID,
        "source_corpus_already_inspected": True,
        "selected_pair_count": int(selection["selected_pair_count"]),
        "sec_counts": sec_counts,
        "clean_trigger_counts": trigger_counts,
        "clean_trigger_shift_summary": triggers["shift_summary"],
        "private_selection_content_sha256": manifest["selection_contract"][
            "private_selection_content_sha256"
        ],
        "private_sec_index_sha256": _sha256_file(_private_sec_path(store.root)),
        "private_trigger_index_sha256": _sha256_file(_private_trigger_path(store.root)),
        "findings": {
            "point_in_time_cik_coverage_complete": True,
            "primary_filing_presence_is_not_positive_verification": True,
            "clean_cross_is_narrower_than_bar_high_eligibility": True,
            "production_rule_change_earned": False,
        },
        "remaining_fidelity_gaps": [
            "primary filing candidates require source-grounded directional classification",
            "historical full-depth liquidity is unavailable from Alpaca top-of-book",
            "point-in-time tradability and halt state are not yet joined",
            "resistance and sector-relative-strength contracts remain unspecified",
        ],
        "claim_boundary": (
            "Pipeline-fidelity evidence on an already-inspected corpus; not alpha, "
            "confirmation, promotion, or permission to invent a strategy variant."
        ),
        "symbols_ciks_filings_and_raw_rows_public": False,
    }
    _write_json(public_result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("freeze", "collect-sec", "collect-clean-triggers", "inspect"):
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
            path, result = freeze_fidelity(
                env_path=args.env_file, output_root=args.output_root
            )
            output = {"manifest_path": str(path), **result}
        else:
            if args.manifest is None:
                raise SelectedCandidateFidelityError("--manifest is required")
            if args.command == "collect-sec":
                output = collect_sec(
                    manifest_path=args.manifest,
                    env_path=args.env_file,
                    public_status_path=args.status,
                )
            elif args.command == "collect-clean-triggers":
                output = collect_clean_triggers(
                    manifest_path=args.manifest,
                    env_path=args.env_file,
                    public_status_path=args.status,
                )
            else:
                output = inspect_fidelity(
                    manifest_path=args.manifest,
                    env_path=args.env_file,
                    public_result_path=args.result,
                )
    except (
        HistoricalDiscoveryError,
        LearningDataError,
        SelectedCandidateFidelityError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
