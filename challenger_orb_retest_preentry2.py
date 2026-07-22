"""Run causal pre-entry acquisition for challenger ORB retest tranche two.

This adapter preserves the already frozen first-corpus implementation while
giving the disjoint second tranche its own immutable inputs, private namespace,
public artifacts, and independent inspection path.  It never opens post-entry
rows, fills, returns, or target outcomes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import challenger_orb_retest_preentry as base
import challenger_orb_retest_selected_pairs2 as selected_pairs2
import challenger_sec_accession_chain_recovery2 as recovery2
import development_catalyst_source_semantics as source_semantics
import development_sec_accession_chain_recovery as accession_recovery
from historical_providers import HistoricalProviderError
from historical_store import HistoricalStoreError
from learning_data import LearningDataError


PROJECT_ROOT = Path(__file__).resolve().parent
BASE_PATH = Path(base.__file__).resolve()
BASE_IMPLEMENTATION_SHA256 = (
    "5543beb0da907fa476c8f3d6b5e11179d2f8334ff02492c64abd43890abdd5ad"
)
DATASET_ID = selected_pairs2.PREENTRY_DATASET_ID
SELECTED_PAIR_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche2/"
    "selected_pair_manifests"
    / (
        "dataset-selected-candidate-contract-2026-07-22-"
        "challenger-orb-retest-tranche2-v1-"
        "9cdb9fd014870b7518613ee096922dd838a1c9e7094c3f6bd4e7b392ae95a511.json"
    )
)
SOURCE_SEMANTICS_RESULT = recovery2.SOURCE_SEMANTICS_RESULT
ACCESSION_RESULT = recovery2.DEFAULT_PUBLIC_RESULT
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche2/preentry_manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche2/preentry-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT
    / "research_results/"
    "2026-07-22-challenger-orb-retest-preentry-tranche2.json"
)
INSPECTOR = PROJECT_ROOT / "challenger_orb_retest_preentry_inspection2.py"
PRIVATE_NAMESPACE = "_derived/challenger_orb_retest_preentry"
EXPECTED_POSITIVE_PAIRS = 113
EXPECTED_POSITIVE_DATES = 55
EXPECTED_TOTAL_REQUESTS = 336


class ChallengerPreentry2Error(RuntimeError):
    """The second-tranche pre-entry adapter or frozen base has drifted."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _upstream_paths(store_root: Path) -> dict[str, Path]:
    return {
        "selected_pairs": selected_pairs2._private_path(
            store_root, selected_pairs2.DATASET_ID
        ),
        "source_review": source_semantics._reviewed_path(
            store_root,
            recovery2.SOURCE_SEMANTICS_DATASET_ID,
            recovery2.SOURCE_REVIEW_DATASET_ID,
        ),
        "accession_review": (
            store_root
            / recovery2.PRIVATE_NAMESPACE
            / recovery2.DATASET_ID
            / "reviewed-result.json.gz"
        ),
    }


def build_selection(store_root: Path) -> dict[str, Any]:
    """Rebuild the exact 113-pair outcome-blind acquisition graph."""

    paths = _upstream_paths(store_root)
    selected = base._read_gzip(paths["selected_pairs"])
    primary = accession_recovery._read_gzip(paths["source_review"])
    recovered = accession_recovery._read_gzip(paths["accession_review"])
    if not (
        selected.get("dataset_id") == selected_pairs2.DATASET_ID
        and primary.get("dataset_id") == recovery2.SOURCE_REVIEW_DATASET_ID
        and primary.get("status") == "REVIEW_COMPLETE"
        and primary.get("verified_positive_pairs") == 18
        and recovered.get("dataset_id") == recovery2.DATASET_ID
        and recovered.get("status") == "REVIEW_COMPLETE"
        and recovered.get("combined_verified_positive_pairs")
        == EXPECTED_POSITIVE_PAIRS
        and recovered.get("target_outcomes_observed_or_derived") is False
    ):
        raise ChallengerPreentry2Error("second-tranche source capacity differs")
    positives = {
        str(pair_hash)
        for review in (primary, recovered)
        for pair_hash, disposition in review["pair_dispositions"].items()
        if disposition == "VERIFIED_POSITIVE_PRIMARY"
    }
    pairs = [
        dict(row)
        for row in selected["selected_pairs"]
        if source_semantics._sha256_json(
            (str(row["date"]), str(row["instrument_id"]))
        )
        in positives
    ]
    pairs.sort(
        key=lambda row: (
            str(row["date"]),
            int(row["rank"]),
            str(row["instrument_id"]),
        )
    )
    dates = sorted({str(row["date"]) for row in pairs})
    if (
        len(positives) != EXPECTED_POSITIVE_PAIRS
        or len(pairs) != EXPECTED_POSITIVE_PAIRS
        or len(dates) != EXPECTED_POSITIVE_DATES
    ):
        raise ChallengerPreentry2Error("verified-positive pair capacity differs")
    requests = []
    for pair in pairs:
        requests.extend(
            base._request(kind, str(pair["symbol"]), str(pair["date"]))
            for kind in ("candidate_bars", "candidate_trades")
        )
    for day in dates:
        requests.extend(
            base._request("benchmark_bars", symbol, day)
            for symbol in base.BENCHMARKS
        )
    requests.sort(key=lambda row: str(row["request_sha256"]))
    if (
        len(requests) != EXPECTED_TOTAL_REQUESTS
        or len({row["request_sha256"] for row in requests}) != len(requests)
    ):
        raise ChallengerPreentry2Error("pre-entry request graph differs")
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "status": "FROZEN_SELECTION",
        "pairs": pairs,
        "requests": requests,
        "counts": {
            "verified_positive_pairs": len(pairs),
            "verified_positive_dates": len(dates),
            "candidate_bar_requests": len(pairs),
            "candidate_trade_requests": len(pairs),
            "benchmark_bar_requests": len(dates) * len(base.BENCHMARKS),
            "total_requests": len(requests),
            "maximum_daily_signals": len(dates),
        },
        "pair_identity_sha256": base._sha256_json(
            [(row["date"], row["instrument_id"]) for row in pairs]
        ),
        "request_graph_sha256": base._sha256_json(requests),
        "symbols_dates_rows_and_sources_public": False,
        "post_entry_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
    }


def configure_base() -> None:
    """Install second-tranche constants into the hash-pinned base engine."""

    if _sha256_file(BASE_PATH) != BASE_IMPLEMENTATION_SHA256:
        raise ChallengerPreentry2Error(
            "frozen base pre-entry implementation drifted"
        )
    values: dict[str, Any] = {
        "DATASET_ID": DATASET_ID,
        "SELECTED_PAIR_MANIFEST": SELECTED_PAIR_MANIFEST,
        "SOURCE_SEMANTICS_RESULT": SOURCE_SEMANTICS_RESULT,
        "ACCESSION_RESULT": ACCESSION_RESULT,
        "DEFAULT_OUTPUT_ROOT": DEFAULT_OUTPUT_ROOT,
        "DEFAULT_PUBLIC_STATUS": DEFAULT_PUBLIC_STATUS,
        "DEFAULT_PUBLIC_RESULT": DEFAULT_PUBLIC_RESULT,
        "INSPECTOR": INSPECTOR,
        "PRIVATE_NAMESPACE": PRIVATE_NAMESPACE,
        "EXPECTED_POSITIVE_PAIRS": EXPECTED_POSITIVE_PAIRS,
        "EXPECTED_POSITIVE_DATES": EXPECTED_POSITIVE_DATES,
        "selected_pairs": selected_pairs2,
        "_upstream_paths": _upstream_paths,
        "build_selection": build_selection,
        "__file__": str(Path(__file__).resolve()),
    }
    for name, value in values.items():
        setattr(base, name, value)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "collect", "derive", "status"))
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--public-status", type=Path, default=DEFAULT_PUBLIC_STATUS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        configure_base()
        if args.command == "freeze":
            path, manifest = base.freeze_inputs(
                env_path=args.env_file,
                output_root=args.output_root,
                status_path=args.public_status,
            )
            value = {"manifest": base._repo_path(path), **manifest}
        elif args.command == "status":
            value = base._read_json(args.public_status)
        elif args.manifest is None:
            raise ChallengerPreentry2Error("--manifest is required")
        elif args.command == "collect":
            value = base.collect(
                manifest_path=args.manifest,
                env_path=args.env_file,
                status_path=args.public_status,
            )
        else:
            value = base.derive(
                manifest_path=args.manifest,
                env_path=args.env_file,
                status_path=args.public_status,
            )
    except (
        ChallengerPreentry2Error,
        base.ChallengerPreentryError,
        HistoricalProviderError,
        HistoricalStoreError,
        LearningDataError,
        OSError,
        subprocess.SubprocessError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
