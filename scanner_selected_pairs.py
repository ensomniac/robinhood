"""Freeze an inspected scanner's exact private security-date selection.

This is a selection boundary only.  It reads no catalyst, trigger, quote,
post-09:35 path, or outcome field and it cannot evaluate a strategy.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import sys
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DATASET_ID = "dataset-selected-candidate-contract-2026-07-19-expansion-v1"
DEFAULT_SUMMARY = PROJECT_ROOT / "research_results/2026-07-19-scanner-expansion.json"
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches/scanner_expansion_selected_pairs/manifests"
)
SCANNER_FIELDS = (
    "open_price",
    "opening_high",
    "opening_low",
    "opening_close",
    "opening_volume",
    "opening_relative_volume",
    "opening_return",
    "average_daily_volume_14",
    "daily_atr_14",
    "prior_close",
)


class ScannerSelectionError(RuntimeError):
    """The source scanner or private selection boundary is invalid."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


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
        raise ScannerSelectionError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ScannerSelectionError(f"{path} must contain an object")
    return value


def _detail_path(summary: Mapping[str, Any]) -> Path:
    artifact = summary.get("detailed_artifact")
    if not isinstance(artifact, Mapping) or artifact.get("public") is not False:
        raise ScannerSelectionError("scanner detail privacy contract is missing")
    raw = str(artifact.get("local_path") or "")
    relative = Path(raw)
    if not raw or relative.is_absolute() or ".." in relative.parts:
        raise ScannerSelectionError("scanner detail path must be repository relative")
    return PROJECT_ROOT / relative


def load_scanner_selection(
    summary_path: Path, *, dataset_id: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    summary = _read_object(summary_path)
    expected = {
        "status": "READY",
        "selection_time_et": "09:35:00",
        "information_cutoff": "TARGET_SESSION_09:35_ET",
        "complete_universe": True,
        "selection_is_dynamic": True,
    }
    for field, value in expected.items():
        if summary.get(field) != value:
            raise ScannerSelectionError(f"scanner summary {field} must be {value!r}")
    source_id = str(summary.get("dataset_id") or "")
    if not source_id.startswith("dataset-production-scanner-replay-"):
        raise ScannerSelectionError("scanner source dataset namespace is invalid")
    detail_path = _detail_path(summary)
    artifact = summary["detailed_artifact"]
    if not detail_path.is_file() or _sha256_file(detail_path) != artifact.get("sha256"):
        raise ScannerSelectionError("scanner detail hash does not match summary")
    detail = _read_object(detail_path)
    if detail.get("dataset_id") != source_id:
        raise ScannerSelectionError("scanner detail dataset identity differs")
    for field in (
        "scanner_rules_sha256",
        "security_master_sha256",
        "split_actions_sha256",
    ):
        if detail.get(field) != summary.get(field):
            raise ScannerSelectionError(f"scanner detail {field} mismatch")

    public_dates = summary.get("dates")
    detail_dates = detail.get("dates")
    if not isinstance(public_dates, list) or not isinstance(detail_dates, Mapping):
        raise ScannerSelectionError("scanner dates are malformed")
    pairs: list[dict[str, Any]] = []
    daily_shortlists: list[dict[str, Any]] = []
    for public_day in public_dates:
        if not isinstance(public_day, Mapping):
            raise ScannerSelectionError("scanner public date must be an object")
        day = str(public_day.get("date") or "")
        date.fromisoformat(day)
        raw_day = detail_dates.get(day)
        if not isinstance(raw_day, Mapping):
            raise ScannerSelectionError(f"scanner detail lacks {day}")
        symbols = raw_day.get("selected_symbols")
        evaluations = raw_day.get("evaluations")
        if not isinstance(symbols, list) or not isinstance(evaluations, list):
            raise ScannerSelectionError(f"scanner selected rows are malformed for {day}")
        if len(symbols) != len(set(symbols)):
            raise ScannerSelectionError(f"scanner selected symbols repeat for {day}")
        eligible_rows = [
            row
            for row in evaluations
            if isinstance(row, Mapping)
            and isinstance(row.get("symbol"), str)
            and row.get("disposition") == "eligible"
        ]
        if len({str(row["symbol"]) for row in eligible_rows}) != len(eligible_rows):
            raise ScannerSelectionError(f"scanner eligible identities repeat for {day}")
        by_symbol = {str(row["symbol"]): row for row in eligible_rows}
        shortlist: list[dict[str, Any]] = []
        for expected_rank, symbol_value in enumerate(symbols, 1):
            symbol = str(symbol_value)
            row = by_symbol.get(symbol)
            if row is None or int(row.get("opening_rvol_rank", -1)) != expected_rank:
                raise ScannerSelectionError(f"scanner rank/eligibility differs for {day}")
            selected = {
                "date": day,
                "symbol": symbol,
                "instrument_id": str(row["instrument_id"]),
                "primary_exchange": str(row["primary_exchange"]),
                "rank": expected_rank,
                "scanner_fields": {field: row[field] for field in SCANNER_FIELDS},
            }
            pairs.append(selected)
            shortlist.append(
                {
                    "symbol": symbol,
                    "instrument_id": selected["instrument_id"],
                    "opening_relative_volume": selected["scanner_fields"][
                        "opening_relative_volume"
                    ],
                    "opening_return": selected["scanner_fields"]["opening_return"],
                    "rank": expected_rank,
                }
            )
        if len(shortlist) != int(public_day.get("shortlist_count", -1)):
            raise ScannerSelectionError(f"scanner shortlist count mismatch for {day}")
        shortlist_hash = _sha256_json(shortlist)
        if shortlist_hash != public_day.get("shortlist_sha256"):
            raise ScannerSelectionError(f"scanner shortlist hash mismatch for {day}")
        daily_shortlists.append(
            {
                "date": day,
                "shortlist_count": len(shortlist),
                "shortlist_sha256": shortlist_hash,
            }
        )
    requested = [str(item) for item in summary.get("requested_dates", [])]
    if requested != [item["date"] for item in daily_shortlists] or len(
        requested
    ) != len(set(requested)):
        raise ScannerSelectionError("scanner requested-date identity differs")
    private = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "source_dataset_id": source_id,
        "source_summary_sha256": _sha256_file(summary_path),
        "source_detail_sha256": str(artifact["sha256"]),
        "selection_time_et": "09:35:00",
        "information_cutoff": "TARGET_SESSION_09:35_ET",
        "selected_pair_count": len(pairs),
        "selected_pairs": pairs,
    }
    public = {
        "source_dataset_id": source_id,
        "source_manifest_sha256": str(summary.get("source", {}).get("contract_sha256") or ""),
        "source_summary_sha256": _sha256_file(summary_path),
        "source_detail_sha256": str(artifact["sha256"]),
        "requested_dates": requested,
        "selected_pair_count": len(pairs),
        "daily_shortlists": daily_shortlists,
        "private_selection_content_sha256": _sha256_json(private),
    }
    return private, public


def _private_path(root: Path, dataset_id: str) -> Path:
    return root / "_derived/scanner_selected_pairs" / dataset_id / "selected-pairs.json.gz"


def _gzip_bytes(value: Any) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as target:
        target.write(json.dumps(value, indent=2, sort_keys=True).encode())
        target.write(b"\n")
    return buffer.getvalue()


def _write_private(path: Path, value: Any) -> None:
    rendered = _gzip_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(rendered)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def freeze_selection_contract(
    *,
    dataset_id: str,
    summary_path: Path,
    env_path: Path,
    output_root: Path,
) -> tuple[Path, dict[str, Any]]:
    if not dataset_id.startswith("dataset-selected-candidate-contract-"):
        raise ScannerSelectionError("selected-candidate dataset namespace is invalid")
    config = HistoricalStoreConfig.from_env(env_path)
    private, public = load_scanner_selection(summary_path, dataset_id=dataset_id)
    private_path = _private_path(config.root, dataset_id)
    if private_path.exists():
        with gzip.open(private_path, "rt", encoding="utf-8") as source:
            existing = json.load(source)
        if _sha256_json(existing) != public["private_selection_content_sha256"]:
            raise ScannerSelectionError("private selected-pair artifact changed")
    else:
        _write_private(private_path, private)
    downstream = {
        "selection_only": True,
        "targets_frozen_before_downstream_collection": True,
        "substitutions_allowed": False,
        "source_outcomes_observed_or_derived": False,
        "permitted_next_inputs": [
            "point-in-time primary catalyst evidence",
            "clean trigger and NBBO evidence",
            "unchanged-v3 post-trigger outcomes",
        ],
        "strategy_variant_or_production_change_allowed": False,
        "private_selection_location": (
            "LOCAL_HISTORICAL_DATA_ROOT/_derived/scanner_selected_pairs/"
            f"{dataset_id}/selected-pairs.json.gz"
        ),
    }
    builder_hash = _sha256_file(Path(__file__))
    existing_manifests = sorted(output_root.glob(f"{dataset_id}-*.json"))
    if len(existing_manifests) > 1:
        raise ScannerSelectionError("selected-candidate dataset has multiple manifests")
    if existing_manifests:
        existing = load_frozen_dataset_contract(existing_manifests[0])
        if any(
            (
                existing.get("dataset_id") != dataset_id,
                existing.get("selection_contract") != public,
                existing.get("downstream_contract") != downstream,
                existing.get("builder_sha256") != builder_hash,
            )
        ):
            raise ScannerSelectionError("existing selected-candidate manifest differs")
        return existing_manifests[0], existing
    contract = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "registered_at": datetime.now(timezone.utc).isoformat(),
        "requested_dates": public["requested_dates"],
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                str(summary_path.resolve().relative_to(PROJECT_ROOT)),
                "SCANNER_EXPANSION.md",
                "STRATEGY_LEARNING_EXECUTION_PLAN.md",
            ],
            "inspected": False,
        },
        "selection_contract": public,
        "downstream_contract": downstream,
        "builder_sha256": builder_hash,
    }
    return freeze_dataset_contract(contract, output_root)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--dataset-id", default=DEFAULT_DATASET_ID)
    parser.add_argument("--summary", type=Path, default=DEFAULT_SUMMARY)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        path, contract = freeze_selection_contract(
            dataset_id=args.dataset_id,
            summary_path=args.summary,
            env_path=args.env,
            output_root=args.output_root,
        )
        print(
            json.dumps(
                {
                    "dataset_id": contract["dataset_id"],
                    "manifest_sha256": contract["manifest_sha256"],
                    "path": str(path),
                    "selected_pair_count": contract["selection_contract"][
                        "selected_pair_count"
                    ],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        HistoricalStoreError,
        LearningDataError,
        ScannerSelectionError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
