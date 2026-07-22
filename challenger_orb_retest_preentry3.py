"""Run causal pre-entry acquisition for challenger ORB retest tranche three.

This isolated adapter binds the repaired third-tranche selected-pair contract
and the directly capacity-passing SEC semantic review.  It never opens a
recovery corpus, post-entry row, fill, return, or target outcome.
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
import challenger_orb_retest_selected_pairs3_v2 as selected_pairs3
import development_catalyst_source_semantics as source_semantics
from historical_providers import HistoricalProviderError
from historical_store import HistoricalStoreError
from learning_data import LearningDataError


PROJECT_ROOT = Path(__file__).resolve().parent
BASE_PATH = Path(base.__file__).resolve()
BASE_IMPLEMENTATION_SHA256 = (
    "5543beb0da907fa476c8f3d6b5e11179d2f8334ff02492c64abd43890abdd5ad"
)
DATASET_ID = selected_pairs3.PREENTRY_DATASET_ID
SOURCE_REVIEW_DATASET_ID = (
    "dataset-development-sec-source-semantics-2026-07-22-"
    "challenger-orb-retest-tranche3-v1"
)
SOURCE_SEMANTICS_DATASET_ID = (
    "dataset-primary-source-semantics-contract-2026-07-22-"
    "challenger-orb-retest-tranche3-v2"
)
SELECTED_PAIR_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/"
    "selected_pair_manifests"
    / (
        "dataset-selected-candidate-contract-2026-07-22-"
        "challenger-orb-retest-tranche3-v2-"
        "f50fe2068ff38e9a130691468eac181e6332351a56b4a78209f0bc708eef225a.json"
    )
)
SOURCE_SEMANTICS_RESULT = (
    PROJECT_ROOT
    / "research_results/"
    "2026-07-22-challenger-orb-retest-sec-source-semantics-tranche3-"
    "inspection.json"
)
# The base engine records a legacy recovery-result binding.  This tranche
# passed source capacity directly, so the inspected primary result is the
# authoritative no-recovery boundary for both public evidence slots.
ACCESSION_RESULT = SOURCE_SEMANTICS_RESULT
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/preentry_manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/preentry-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT
    / "research_results/2026-07-22-challenger-orb-retest-preentry-tranche3.json"
)
INSPECTOR = PROJECT_ROOT / "challenger_orb_retest_preentry_inspection3.py"
PRIVATE_NAMESPACE = "_derived/challenger_orb_retest_preentry"
EXPECTED_POSITIVE_PAIRS = 23
EXPECTED_POSITIVE_DATES = 20
EXPECTED_TOTAL_REQUESTS = 86


class ChallengerPreentry3Error(RuntimeError):
    """The third-tranche pre-entry adapter or frozen base has drifted."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _upstream_paths(store_root: Path) -> dict[str, Path]:
    return {
        "selected_pairs": selected_pairs3._private_path(
            store_root, selected_pairs3.DATASET_ID
        ),
        "source_review": source_semantics._reviewed_path(
            store_root,
            SOURCE_SEMANTICS_DATASET_ID,
            SOURCE_REVIEW_DATASET_ID,
        ),
    }


def build_selection(store_root: Path) -> dict[str, Any]:
    """Rebuild the exact 23-pair outcome-blind acquisition graph."""

    paths = _upstream_paths(store_root)
    selected = base._read_gzip(paths["selected_pairs"])
    reviewed = source_semantics._read_gzip_object(paths["source_review"])
    if not (
        selected.get("dataset_id") == selected_pairs3.DATASET_ID
        and reviewed.get("dataset_id") == SOURCE_REVIEW_DATASET_ID
        and reviewed.get("status") == "REVIEW_COMPLETE"
        and reviewed.get("verified_positive_pairs") == EXPECTED_POSITIVE_PAIRS
        and reviewed.get("next_phase") == "DEVELOPMENT_ACQUISITION"
        and reviewed.get("outcome_contract_permitted") is False
        and reviewed.get("target_outcomes_observed_or_derived") is False
    ):
        raise ChallengerPreentry3Error("third-tranche source capacity differs")
    positives = {
        str(pair_hash)
        for pair_hash, disposition in reviewed["pair_dispositions"].items()
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
        raise ChallengerPreentry3Error("verified-positive pair capacity differs")
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
        raise ChallengerPreentry3Error("pre-entry request graph differs")
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
    """Install third-tranche constants into the hash-pinned base engine."""

    if _sha256_file(BASE_PATH) != BASE_IMPLEMENTATION_SHA256:
        raise ChallengerPreentry3Error(
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
        "selected_pairs": selected_pairs3,
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
            raise ChallengerPreentry3Error("--manifest is required")
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
        ChallengerPreentry3Error,
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
