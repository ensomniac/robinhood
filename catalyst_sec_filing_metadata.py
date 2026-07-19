"""Recover SEC filing-detail metadata for the frozen accession source set."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import re
import shutil
import sys
import time
from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import requests

import catalyst_source_recovery as recovery
from historical_store import HistoricalDayStore
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-catalyst-sec-filing-metadata-2026-07-19-expansion-v1"
SOURCE_DATASET_ID = "dataset-catalyst-source-recovery-sec-2026-07-19-expansion-v1"
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_source_recovery"
    / "manifests"
    / (
        "dataset-catalyst-source-recovery-sec-2026-07-19-expansion-v1-"
        "2f4258b23e2aac76529754f51e5df6618db3e655d01491bb7cb01e14a5b0d374.json"
    )
)
SOURCE_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-source-recovery-sec.json"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches" / "catalyst_sec_filing_metadata" / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_sec_filing_metadata"
    / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-sec-filing-metadata.json"
)

EXPECTED_ACCESSIONS = 24
EXPECTED_PAIR_SOURCE_JOINS = 34
EXPECTED_PAIRS = 28
ACCEPTED_PATTERN = re.compile(
    r"\bAccepted\b\s*(?:<[^>]+>|\s|&nbsp;)*"
    r"(20\d{2}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})",
    re.IGNORECASE,
)
CIK_PATTERN = re.compile(r"\bCIK\s*:?\s*(\d{1,10})\b", re.IGNORECASE)


class SecFilingMetadataError(RuntimeError):
    """The frozen SEC filing-metadata contract cannot be honored."""


class _VisibleTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        if data.strip():
            self.parts.append(data.strip())


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SecFilingMetadataError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SecFilingMetadataError(f"{path} must contain an object")
    return value


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise SecFilingMetadataError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SecFilingMetadataError(f"{path} must contain an object")
    return value


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write_gzip(path: Path, value: Any) -> None:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as target:
        target.write(json.dumps(value, indent=2, sort_keys=True).encode())
        target.write(b"\n")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(buffer.getvalue())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write_bytes(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(value)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _timestamp_now() -> str:
    return datetime.now(UTC).isoformat()


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise SecFilingMetadataError(
            f"public path must be repository-relative: {path}"
        ) from exc


def _source_root(store_root: Path) -> Path:
    return store_root / "_derived" / "catalyst_source_recovery" / SOURCE_DATASET_ID


def _private_root(store_root: Path) -> Path:
    return store_root / "_derived" / "catalyst_sec_filing_metadata" / DATASET_ID


def _selection_path(store_root: Path) -> Path:
    return _private_root(store_root) / "selection.json.gz"


def _index_path(store_root: Path) -> Path:
    return _private_root(store_root) / "capture-index.json"


def _response_path(store_root: Path, source_sha256: str) -> Path:
    return _private_root(store_root) / "responses" / f"{source_sha256}.bin"


def filing_detail_url(filing_cik: str, accession: str) -> str:
    if not re.fullmatch(r"\d{1,10}", filing_cik):
        raise SecFilingMetadataError("filing CIK is invalid")
    if not re.fullmatch(r"\d{18}", accession):
        raise SecFilingMetadataError("accession is invalid")
    dashed = f"{accession[:10]}-{accession[10:12]}-{accession[12:]}"
    return (
        f"https://www.sec.gov/Archives/edgar/data/{int(filing_cik)}/"
        f"{accession}/{dashed}-index.html"
    )


def build_selection(source: Mapping[str, Any]) -> dict[str, Any]:
    records = []
    pairs: set[tuple[str, str]] = set()
    joins = 0
    for row in source.get("records", []):
        if row.get("accession") is None:
            continue
        bound_pairs = list(row.get("pairs", []))
        records.append(
            {
                "source_sha256": row["source_sha256"],
                "accession": row["accession"],
                "filing_cik": row["filing_cik"],
                "metadata_url": filing_detail_url(
                    str(row["filing_cik"]), str(row["accession"])
                ),
                "pairs": bound_pairs,
            }
        )
        joins += len(bound_pairs)
        pairs.update((str(pair["date"]), str(pair["symbol"])) for pair in bound_pairs)
    records.sort(key=lambda row: row["source_sha256"])
    if (
        len(records) != EXPECTED_ACCESSIONS
        or joins != EXPECTED_PAIR_SOURCE_JOINS
        or len(pairs) != EXPECTED_PAIRS
    ):
        raise SecFilingMetadataError("SEC filing-metadata selection counts differ")
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "status": "FROZEN",
        "records": records,
        "counts": {
            "accessions": len(records),
            "pair_source_joins": joins,
            "pairs": len(pairs),
        },
        "symbols_dates_urls_accessions_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }


def extract_filing_metadata(body: bytes) -> dict[str, Any]:
    raw = body.decode("utf-8", errors="replace")
    parser = _VisibleTextParser()
    parser.feed(raw)
    visible = " ".join(parser.parts)
    accepted = ACCEPTED_PATTERN.findall(raw)
    if not accepted:
        accepted = ACCEPTED_PATTERN.findall(visible)
    accepted_values = sorted({f"{day}T{clock}" for day, clock in accepted})
    ciks = sorted({str(int(value)) for value in CIK_PATTERN.findall(visible)})
    return {
        "accepted_datetime_candidates_et": accepted_values,
        "cik_candidates": ciks,
        "visible_text_sha256": hashlib.sha256(visible.encode()).hexdigest(),
        "visible_text_excerpt": visible[:4000],
    }


def freeze_inputs(*, env_path: Path, output_root: Path) -> tuple[Path, dict[str, Any]]:
    store = HistoricalDayStore.from_env(env_path)
    if shutil.disk_usage(store.root).free < store.min_free_bytes:
        raise SecFilingMetadataError("historical store disk reserve is not ready")
    source_manifest = load_frozen_dataset_contract(SOURCE_MANIFEST)
    source_result = _read_json(SOURCE_RESULT)
    if not (source_result.get("status") == "READY" and source_result.get("inspected")):
        raise SecFilingMetadataError("SEC source recovery is not inspected READY")
    source_selection_path = _source_root(store.root) / "selection.json.gz"
    source_index_path = _source_root(store.root) / "capture-index.json"
    selection = build_selection(_read_gzip(source_selection_path))
    selection_path = _selection_path(store.root)
    _write_gzip(selection_path, selection)
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
                _repo_path(SOURCE_RESULT),
                "CATALYST_SOURCE_RECOVERY.md",
                "PRODUCTION_STRATEGY_VALIDATION.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            "source_manifest_sha256": source_manifest["manifest_sha256"],
            "source_result_sha256": _sha256_file(SOURCE_RESULT),
            "source_private_selection_sha256": _sha256_file(source_selection_path),
            "source_private_index_sha256": _sha256_file(source_index_path),
            "private_selection_sha256": _sha256_file(selection_path),
            **selection["counts"],
            "symbols_dates_urls_accessions_and_rows_public": False,
        },
        "collection_contract": {
            "collector_sha256": _sha256_file(Path(__file__)),
            "request_dependency_sha256": _sha256_file(Path(recovery.__file__)),
            "sec_accession_detail_endpoint_only": True,
            "user_agent": recovery.USER_AGENT,
            "minimum_interval_seconds": recovery.MINIMUM_INTERVAL_SECONDS,
            "timeout_seconds": recovery.REQUEST_TIMEOUT_SECONDS,
            "maximum_redirects": recovery.MAX_REDIRECTS,
            "maximum_response_bytes": recovery.MAX_RESPONSE_BYTES,
            "maximum_attempts": 3,
            "checkpoint_unit": "accession",
            "provider_substitution_allowed": False,
            "semantics_classification_allowed": False,
            "target_outcomes_observed_or_derived": False,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def _verify_contract(
    manifest_path: Path, store: HistoricalDayStore
) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise SecFilingMetadataError("unexpected SEC filing-metadata dataset")
    selection_path = _selection_path(store.root)
    selection = _read_gzip(selection_path)
    contract = manifest["collection_contract"]
    if _sha256_file(selection_path) != manifest["selection_contract"].get(
        "private_selection_sha256"
    ):
        raise SecFilingMetadataError("SEC filing-metadata selection changed")
    if _sha256_file(Path(__file__)) != contract.get("collector_sha256"):
        raise SecFilingMetadataError("metadata collector differs from contract")
    if _sha256_file(Path(recovery.__file__)) != contract.get(
        "request_dependency_sha256"
    ):
        raise SecFilingMetadataError("SEC request dependency differs from contract")
    return manifest, selection


def _public_summary(
    manifest: Mapping[str, Any], selection: Mapping[str, Any], index: Mapping[str, Any]
) -> dict[str, Any]:
    records = index.get("records", {})
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": index["status"],
        "selection_counts": selection["counts"],
        "terminal_accessions": len(records),
        "capture_status_counts": dict(
            sorted(Counter(str(row.get("status")) for row in records.values()).items())
        ),
        "http_status_counts": dict(
            sorted(
                Counter(
                    str(row.get("http_status"))
                    for row in records.values()
                    if row.get("http_status") is not None
                ).items()
            )
        ),
        "response_bytes": sum(
            int(row.get("body_bytes") or 0) for row in records.values()
        ),
        "symbols_dates_urls_accessions_responses_and_rows_public": False,
        "semantics_classified": False,
        "target_outcomes_observed_or_derived": False,
    }


def collect(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, selection = _verify_contract(manifest_path, store)
    if shutil.disk_usage(store.root).free < store.min_free_bytes:
        raise SecFilingMetadataError("historical store disk reserve is not ready")
    index_path = _index_path(store.root)
    index = (
        _read_json(index_path)
        if index_path.exists()
        else {
            "schema_version": 1,
            "dataset_id": DATASET_ID,
            "manifest_sha256": manifest["manifest_sha256"],
            "status": "COLLECTING",
            "records": {},
            "target_outcomes_observed_or_derived": False,
        }
    )
    if index.get("manifest_sha256") != manifest["manifest_sha256"]:
        raise SecFilingMetadataError("checkpoint belongs to another manifest")
    session = requests.Session()
    last_started = 0.0
    try:
        for selected in selection["records"]:
            source_hash = str(selected["source_sha256"])
            if source_hash in index["records"]:
                continue
            remaining = recovery.MINIMUM_INTERVAL_SECONDS - (
                time.monotonic() - last_started
            )
            if remaining > 0:
                time.sleep(remaining)
            last_started = time.monotonic()
            result = recovery._capture_with_retry(
                session, str(selected["metadata_url"])
            )
            body = result.pop("body", None)
            if body is not None:
                path = _response_path(store.root, source_hash)
                _write_bytes(path, body)
                result["body_sha256"] = _sha256_file(path)
                result["body_bytes"] = len(body)
            result.update(
                {"source_sha256": source_hash, "captured_at": _timestamp_now()}
            )
            index["records"][source_hash] = result
            _write_json(index_path, index)
            _write_json(public_status_path, _public_summary(manifest, selection, index))
    finally:
        session.close()
    index["status"] = "COLLECTION_COMPLETE"
    _write_json(index_path, index)
    public = _public_summary(manifest, selection, index)
    _write_json(public_status_path, public)
    return public


def inspect(
    *, manifest_path: Path, env_path: Path, public_result_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, selection = _verify_contract(manifest_path, store)
    index_path = _index_path(store.root)
    index = _read_json(index_path)
    if not (
        index.get("manifest_sha256") == manifest["manifest_sha256"]
        and index.get("status") == "COLLECTION_COMPLETE"
        and len(index.get("records", {})) == EXPECTED_ACCESSIONS
        and index.get("target_outcomes_observed_or_derived") is False
    ):
        raise SecFilingMetadataError("SEC filing-metadata capture is incomplete")
    expected = {row["source_sha256"] for row in selection["records"]}
    if set(index["records"]) != expected:
        raise SecFilingMetadataError("SEC filing-metadata row set differs")
    metadata_counts = Counter()
    extraction_hashes = {}
    for source_hash, record in index["records"].items():
        if record.get("status") != "RESPONSE_CAPTURED":
            continue
        path = _response_path(store.root, source_hash)
        if not path.exists() or _sha256_file(path) != record.get("body_sha256"):
            raise SecFilingMetadataError("SEC filing-detail response hash mismatch")
        extracted = extract_filing_metadata(path.read_bytes())
        metadata_counts["accepted_datetime_present"] += bool(
            extracted["accepted_datetime_candidates_et"]
        )
        metadata_counts["cik_present"] += bool(extracted["cik_candidates"])
        extraction_hashes[source_hash] = hashlib.sha256(
            json.dumps(extracted, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    summary = _public_summary(manifest, selection, index)
    result = {
        **summary,
        "status": "READY",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "private_index_sha256": _sha256_file(index_path),
        "private_extraction_hash_set_sha256": hashlib.sha256(
            json.dumps(
                extraction_hashes, sort_keys=True, separators=(",", ":")
            ).encode()
        ).hexdigest(),
        "metadata_presence_counts": dict(sorted(metadata_counts.items())),
        "inspection": {
            "selection_rebuilt": True,
            "terminal_counts_rebuilt": True,
            "response_hashes_verified": True,
            "metadata_extraction_rebuilt": True,
        },
        "production_rule_change_earned": False,
        "next_required_stage": (
            "Freeze the 37-join SEC ownership, document-CIK, acceptance-time, "
            "relevance, direction, and financing-conflict semantics review."
        ),
        "claim_boundary": (
            "SEC accession filing-detail capture and timestamp/CIK-shaped field "
            "presence only; not accepted causality, semantics, alpha, or outcomes."
        ),
    }
    _write_json(public_result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "collect", "inspect"))
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--public-status", type=Path, default=DEFAULT_PUBLIC_STATUS)
    parser.add_argument("--public-result", type=Path, default=DEFAULT_PUBLIC_RESULT)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "freeze":
            path, manifest = freeze_inputs(
                env_path=args.env_file, output_root=args.output_root
            )
            output = {"manifest": _repo_path(path), **manifest}
        elif args.manifest is None:
            raise SecFilingMetadataError("--manifest is required")
        elif args.command == "collect":
            output = collect(
                manifest_path=args.manifest,
                env_path=args.env_file,
                public_status_path=args.public_status,
            )
        else:
            output = inspect(
                manifest_path=args.manifest,
                env_path=args.env_file,
                public_result_path=args.public_result,
            )
    except (SecFilingMetadataError, LearningDataError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
