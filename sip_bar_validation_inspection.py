"""Independently inspect the frozen SIP trade-to-minute-bar validation."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import statistics
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from historical_service import LocalHistoricalClient
from historical_store import HistoricalDayStore, expand_bar
from learning_data import LearningDataError, load_frozen_dataset_contract
from sip_bar_aggregation import aggregate_minute


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-sip-bar-aggregation-validation-2026-07-19-v1"
SOURCE_DATASET_ID = "dataset-selected-candidate-fidelity-2026-07-19-v1"
DEFAULT_OUTPUT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-sip-bar-validation-inspection.json"
)


class SipBarInspectionError(RuntimeError):
    """The independent SIP validation reconstruction found a mismatch."""


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SipBarInspectionError(f"cannot read {path}: {exc}") from exc


def _read_gzip(path: Path) -> Any:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            return json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise SipBarInspectionError(f"cannot read {path}: {exc}") from exc


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


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
        raise SipBarInspectionError("provider one-minute bar is missing")
    for compact in dataset["rows"]:
        expanded = expand_bar(compact)
        if datetime.fromisoformat(str(expanded["time_et"])) == minute:
            return expanded
    raise SipBarInspectionError("provider crossing-minute bar is missing")


def inspect(
    *,
    manifest_path: Path,
    env_path: Path,
    public_status_path: Path,
    public_result_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    try:
        manifest = load_frozen_dataset_contract(manifest_path)
    except LearningDataError as exc:
        raise SipBarInspectionError(str(exc)) from exc
    if manifest.get("dataset_id") != DATASET_ID:
        raise SipBarInspectionError("unexpected validation manifest")

    store = HistoricalDayStore.from_env(env_path)
    source_path = (
        store.root
        / "_derived"
        / "selected_candidate_fidelity"
        / SOURCE_DATASET_ID
        / "clean-trigger-index.json.gz"
    )
    private_path = (
        store.root
        / "_derived"
        / "sip_bar_validation"
        / DATASET_ID
        / "validation-index.json.gz"
    )
    source = _read_gzip(source_path)
    private = _read_gzip(private_path)
    public_status = _read_json(public_status_path)
    public_result = _read_json(public_result_path)
    local = LocalHistoricalClient(store, "alpaca", feed="sip", adjustment="raw")

    tolerance = float(manifest["validation_contract"]["wap_absolute_tolerance"])
    counts: Counter[str] = Counter()
    ratios: list[float] = []
    for record in source.get("records", []):
        minute = datetime.fromisoformat(str(record["crossing_minute_et"]))
        trades = local.fetch_trades(
            str(record["symbol"]), minute, minute + timedelta(minutes=1), use_rth=True
        )
        rebuilt = aggregate_minute(trades)
        if rebuilt is None:
            raise SipBarInspectionError("raw minute did not reconstruct")
        oracle = _provider_bar(
            store, str(record["symbol"]), str(record["date"]), minute
        )
        for field in ("open", "high", "low", "close", "volume", "count"):
            counts[f"{field}_matches"] += int(rebuilt[field] == oracle[field])
        counts["wap_matches"] += int(
            math.isclose(
                float(rebuilt["wap"]),
                float(oracle["wap"]),
                rel_tol=0.0,
                abs_tol=tolerance,
            )
        )
        counts["minutes"] += 1
        counts["source_trades"] += len(trades)
        counts["unsupported_trades"] += int(rebuilt["unsupported_trade_count"])
        differs = int(rebuilt["vwap_eligible_volume"]) != int(rebuilt["volume"])
        counts["vwap_denominator_differs_from_reported_volume"] += int(differs)
        ratios.append(float(rebuilt["vwap_eligible_volume"]) / float(rebuilt["volume"]))

    rebuilt_counts = dict(sorted(counts.items()))
    rebuilt_ratios = {
        "minimum": min(ratios),
        "median": statistics.median(ratios),
        "maximum": max(ratios),
    }
    expected_hash = _sha256_file(private_path)
    exact_fields = ("open", "high", "low", "close", "volume", "count", "wap")
    checks = {
        "manifest_hash_matches": all(
            value.get("manifest_sha256") == manifest["manifest_sha256"]
            for value in (private, public_status, public_result)
        ),
        "private_hash_matches_public_artifacts": all(
            value.get("private_result_sha256") == expected_hash
            for value in (public_status, public_result)
        ),
        "private_counts_match_rebuild": private.get("counts") == rebuilt_counts,
        "public_counts_match_rebuild": all(
            value.get("counts") == rebuilt_counts
            for value in (public_status, public_result)
        ),
        "ratios_match_rebuild": all(
            value.get("vwap_eligible_to_reported_volume_ratio") == rebuilt_ratios
            for value in (private, public_status, public_result)
        ),
        "all_expected_minutes_present": rebuilt_counts.get("minutes") == 325,
        "all_oracle_fields_match": all(
            rebuilt_counts.get(f"{field}_matches") == 325 for field in exact_fields
        ),
        "all_conditions_supported": rebuilt_counts.get("unsupported_trades") == 0,
        "outcomes_remain_unobserved": (
            private.get("target_outcomes_observed_or_derived") is False
            and public_status.get("target_outcomes_observed_or_derived") is False
        ),
        "public_outputs_are_ready": (
            public_status.get("status") == "VALIDATION_COMPLETE"
            and public_result.get("status") == "READY"
            and public_result.get("inspected") is True
        ),
    }
    if not all(checks.values()):
        failed = sorted(name for name, passed in checks.items() if not passed)
        raise SipBarInspectionError(f"independent checks failed: {failed}")

    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "independently_inspected": True,
        "checks": checks,
        "counts": rebuilt_counts,
        "vwap_eligible_to_reported_volume_ratio": rebuilt_ratios,
        "findings": {
            "raw_trade_aggregation_matches_provider_bar_oracle": True,
            "published_volume_cannot_reconstruct_exact_vwap": True,
            "raw_prefix_required_for_intraminute_session_vwap": True,
            "production_rule_change_earned": False,
        },
        "claim_boundary": (
            "Independent source-oracle verification of pre-entry aggregation; "
            "not target outcomes, alpha, confirmation, or a strategy variant."
        ),
        "raw_rows_and_symbols_public": False,
    }
    _write_json(output_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument(
        "--public-status",
        type=Path,
        default=(
            PROJECT_ROOT
            / "historical_batches"
            / "sip_bar_validation"
            / "collection-status.json"
        ),
    )
    parser.add_argument(
        "--public-result",
        type=Path,
        default=(
            PROJECT_ROOT / "research_results" / "2026-07-19-sip-bar-validation.json"
        ),
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        result = inspect(
            manifest_path=args.manifest,
            env_path=args.env_file,
            public_status_path=args.public_status,
            public_result_path=args.public_result,
            output_path=args.output,
        )
    except (SipBarInspectionError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
