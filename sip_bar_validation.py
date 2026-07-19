"""Freeze and validate complete SIP minute-bar aggregation semantics."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import math
import os
import statistics
import sys
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from historical_service import LocalHistoricalClient
from historical_store import HistoricalDayStore, expand_bar
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)
from sip_bar_aggregation import RULE_VERSION, SOURCE_URL, aggregate_minute


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-sip-bar-aggregation-validation-2026-07-19-v1"
SOURCE_DATASET_ID = "dataset-selected-candidate-fidelity-2026-07-19-v1"
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "selected_candidate_fidelity"
    / "manifests"
    / "dataset-selected-candidate-fidelity-2026-07-19-v1-38f74c64c6f3f86d3cefbe656efd075345c21e7fcc22329936e5c5e78c2a7dcd.json"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches" / "sip_bar_validation" / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "sip_bar_validation"
    / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-sip-bar-validation.json"
)


class SipBarValidationError(RuntimeError):
    """The frozen raw-trade/bar corpus cannot validate aggregation semantics."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_gzip(path: Path) -> Any:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            return json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise SipBarValidationError(f"cannot read {path}: {exc}") from exc


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


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise SipBarValidationError("public path must be repository-relative") from exc


def _timestamp_now() -> str:
    return datetime.now(UTC).isoformat()


def _source_path(root: Path) -> Path:
    return (
        root
        / "_derived"
        / "selected_candidate_fidelity"
        / SOURCE_DATASET_ID
        / "clean-trigger-index.json.gz"
    )


def _private_result_path(root: Path) -> Path:
    return (
        root
        / "_derived"
        / "sip_bar_validation"
        / DATASET_ID
        / "validation-index.json.gz"
    )


def freeze_inputs(*, env_path: Path, output_root: Path) -> tuple[Path, dict[str, Any]]:
    store = HistoricalDayStore.from_env(env_path)
    try:
        source_manifest = load_frozen_dataset_contract(SOURCE_MANIFEST)
    except LearningDataError as exc:
        raise SipBarValidationError(str(exc)) from exc
    source_path = _source_path(store.root)
    source = _read_gzip(source_path)
    if (
        source_manifest.get("dataset_id") != SOURCE_DATASET_ID
        or source.get("status") != "CLEAN_TRIGGER_INSPECTION_COMPLETE"
        or source.get("counts", {}).get("crossing_windows") != 325
    ):
        raise SipBarValidationError("clean-trigger source is incomplete")
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
                "CHAMPION_INPUT_READINESS.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            "source_dataset_id": SOURCE_DATASET_ID,
            "source_manifest_sha256": source_manifest["manifest_sha256"],
            "clean_trigger_source_sha256": _sha256_file(source_path),
            "crossing_minute_count": 325,
            "raw_rows_and_symbols_public": False,
        },
        "validation_contract": {
            "rule_version": RULE_VERSION,
            "rule_source_url": SOURCE_URL,
            "aggregation_implementation_sha256": _sha256_file(
                PROJECT_ROOT / "sip_bar_aggregation.py"
            ),
            "validator_implementation_sha256": _sha256_file(Path(__file__)),
            "source_trade_window": "complete provider crossing minute",
            "oracle": "corresponding Alpaca raw SIP one-minute bar",
            "ohlcv_count_match": "exact",
            "wap_absolute_tolerance": 0.000001,
            "target_outcomes_observed_or_derived": False,
            "strategy_variant_invented": False,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def _verify(
    manifest_path: Path, store: HistoricalDayStore
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        manifest = load_frozen_dataset_contract(manifest_path)
    except LearningDataError as exc:
        raise SipBarValidationError(str(exc)) from exc
    if manifest.get("dataset_id") != DATASET_ID:
        raise SipBarValidationError("unexpected SIP validation dataset")
    contract = manifest["validation_contract"]
    expected = {
        "aggregation_implementation_sha256": _sha256_file(
            PROJECT_ROOT / "sip_bar_aggregation.py"
        ),
        "validator_implementation_sha256": _sha256_file(Path(__file__)),
    }
    for field, digest in expected.items():
        if contract.get(field) != digest:
            raise SipBarValidationError(f"{field} differs from frozen contract")
    source_path = _source_path(store.root)
    if _sha256_file(source_path) != manifest["selection_contract"].get(
        "clean_trigger_source_sha256"
    ):
        raise SipBarValidationError("clean-trigger source changed")
    return manifest, _read_gzip(source_path)


def _provider_bar(
    store: HistoricalDayStore, symbol: str, day: str, minute: datetime
) -> dict[str, Any]:
    dataset = store.select_dataset(
        symbol,
        day,
        kind="bars",
        channel="trades",
        timeframe="1m",
        providers=("alpaca",),
        require_complete=True,
        feed="sip",
        adjustment="raw",
    )
    if dataset is None:
        raise SipBarValidationError("provider one-minute bar is missing")
    for compact in dataset["rows"]:
        row = expand_bar(compact)
        if datetime.fromisoformat(str(row["time_et"])) == minute:
            return row
    raise SipBarValidationError("provider crossing-minute bar is missing")


def validate(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, source = _verify(manifest_path, store)
    local = LocalHistoricalClient(store, "alpaca", feed="sip", adjustment="raw")
    counts: Counter[str] = Counter()
    ratios: list[float] = []
    details: list[dict[str, Any]] = []
    tolerance = float(manifest["validation_contract"]["wap_absolute_tolerance"])
    for record in source["records"]:
        minute = datetime.fromisoformat(str(record["crossing_minute_et"]))
        trades = local.fetch_trades(
            str(record["symbol"]), minute, minute + timedelta(minutes=1), use_rth=True
        )
        rebuilt = aggregate_minute(trades)
        if rebuilt is None:
            raise SipBarValidationError(
                "raw crossing minute emitted no reconstructed bar"
            )
        oracle = _provider_bar(
            store, str(record["symbol"]), str(record["date"]), minute
        )
        exact = {
            field: rebuilt[field] == oracle[field]
            for field in ("open", "high", "low", "close", "volume", "count")
        }
        wap_match = math.isclose(
            float(rebuilt["wap"]),
            float(oracle["wap"]),
            rel_tol=0.0,
            abs_tol=tolerance,
        )
        for field, passed in exact.items():
            counts[f"{field}_matches"] += int(passed)
        counts["wap_matches"] += int(wap_match)
        counts["minutes"] += 1
        counts["source_trades"] += len(trades)
        counts["unsupported_trades"] += int(rebuilt["unsupported_trade_count"])
        differs = int(rebuilt["vwap_eligible_volume"]) != int(rebuilt["volume"])
        counts["vwap_denominator_differs_from_reported_volume"] += int(differs)
        ratios.append(float(rebuilt["vwap_eligible_volume"]) / float(rebuilt["volume"]))
        details.append(
            {
                "date": record["date"],
                "symbol": record["symbol"],
                "minute_et": minute.isoformat(),
                "source_trade_count": len(trades),
                "unsupported_trade_count": rebuilt["unsupported_trade_count"],
                "exact_field_matches": exact,
                "wap_match": wap_match,
                "wap_absolute_error": abs(float(rebuilt["wap"]) - float(oracle["wap"])),
                "reported_volume": rebuilt["volume"],
                "vwap_eligible_volume": rebuilt["vwap_eligible_volume"],
            }
        )
    private = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "VALIDATION_COMPLETE",
        "updated_at": _timestamp_now(),
        "counts": dict(sorted(counts.items())),
        "vwap_eligible_to_reported_volume_ratio": {
            "minimum": min(ratios),
            "median": statistics.median(ratios),
            "maximum": max(ratios),
        },
        "records": details,
        "errors": [],
        "target_outcomes_observed_or_derived": False,
    }
    private_path = _private_result_path(store.root)
    _write_gzip(private_path, private)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": private["status"],
        "counts": private["counts"],
        "vwap_eligible_to_reported_volume_ratio": private[
            "vwap_eligible_to_reported_volume_ratio"
        ],
        "private_result_sha256": _sha256_file(private_path),
        "raw_rows_and_symbols_public": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(public_status_path, public)
    return public


def inspect(
    *, manifest_path: Path, env_path: Path, public_result_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, _source = _verify(manifest_path, store)
    private_path = _private_result_path(store.root)
    private = _read_gzip(private_path)
    counts = private.get("counts", {})
    fields = ("open", "high", "low", "close", "volume", "count", "wap")
    complete = (
        private.get("manifest_sha256") == manifest["manifest_sha256"]
        and private.get("status") == "VALIDATION_COMPLETE"
        and counts.get("minutes") == 325
        and all(counts.get(f"{field}_matches") == 325 for field in fields)
        and counts.get("unsupported_trades") == 0
        and private.get("errors") == []
        and private.get("target_outcomes_observed_or_derived") is False
    )
    if not complete:
        raise SipBarValidationError("SIP bar validation is incomplete")
    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "counts": counts,
        "vwap_eligible_to_reported_volume_ratio": private[
            "vwap_eligible_to_reported_volume_ratio"
        ],
        "private_result_sha256": _sha256_file(private_path),
        "findings": {
            "all_ohlcv_count_fields_match": True,
            "all_wap_values_match_within_one_microdollar": True,
            "unsupported_trade_conditions": 0,
            "published_volume_is_not_vwap_denominator": (
                counts["vwap_denominator_differs_from_reported_volume"] == 325
            ),
            "raw_prefix_required_for_exact_session_vwap": True,
            "production_rule_change_earned": False,
        },
        "claim_boundary": (
            "Source-oracle validation of pre-entry SIP aggregation mechanics; "
            "not target outcomes, alpha, confirmation, or a strategy variant."
        ),
        "raw_rows_and_symbols_public": False,
    }
    _write_json(public_result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "validate", "inspect"))
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
        else:
            if args.manifest is None:
                raise SipBarValidationError("--manifest is required")
            if args.command == "validate":
                output = validate(
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
    except (SipBarValidationError, LearningDataError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
